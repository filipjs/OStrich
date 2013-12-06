#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/list.h"
#include "src/common/timers.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "src/slurmctld/locks.h"
#include "src/slurmctld/slurmctld.h"
#include "src/plugins/sched/ostrich/ostrich.h"


#ifndef SCHEDULE_INTERVAL
#  define SCHEDULE_INTERVAL 1
#endif

#ifndef THRESHOLD
#  define THRESHOLD 60 * 10
#endif

#define DEFAULT_PART_LIMIT 60 * 24 * 7
#define DEFAULT_JOB_LIMIT  60 * 24 * 7 * 365

#define DEBUG_FLAG 1


/*********************** local structures *********************/
struct ostrich_campaign {
	uint32_t camp_id;		/* campaign id */

	uint32_t time_offset;		/* time needed to complete previous campaigns */
	uint32_t completed_time;	/* total time of completed jobs in the campaign */
	uint32_t remaining_time;	/* total time of still running jobs in the campaign */
	double virtual_time;		/* time assigned from users virtual time pool */

	time_t creation_time;		/* time the campaign was created */
	List jobs;			/* list of jobs in the campaign */
};

struct ostrich_user {
	uint32_t user_id;		/* id of the user */

	uint32_t active_campaigns;	/* number of campaings running in the virtual schedule */
	uint32_t last_campaign_id;	/* ID of the last created campaign */

	double virtual_time;		/* accumulated time from the virtual schedule */
	uint32_t shares;		/* number of time shares to assign to the user */

	List campaigns;			/* list of all campaigns */
	List waiting_jobs;		/* list of new awaiting jobs */
};

struct ostrich_schedule {		/* one virtual schedule per partition */
	char *part_name;		/* name of the partition */
	uint16_t priority;		/* priority of the partition */
	uint32_t max_time;		/* max_time for jobs in the partition */
	uint32_t cpus_per_node;		/* average number of cpus per node in the partition */

	uint32_t allocated_cpus;	/* number of cpus that are currently allocated to jobs */
	uint32_t user_shares;		/* total number of time shares of users with active campaigns */

	List users;			/* users using the partition */
};

/*********************** local variables **********************/
static bool stop_thread = false;
static bool config_flag = false;

static pthread_mutex_t term_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  term_cond = PTHREAD_COND_INITIALIZER;

static int schedule_interval = SCHEDULE_INTERVAL;
static int threshold = THRESHOLD;
static int defer_sched = 0;

static List ostrich_sched_list;		/* list of ostrich_schedule entries */
static List incoming_jobs;		/* list of jobs entering the system */
static List priority_jobs;		/* list of jobs using reservations */

static time_t last_sched_time;		/* time of last scheduling pass */

/*********************** local functions **********************/
static uint32_t _job_pred_runtime(struct job_record *job_ptr);
static uint32_t _job_real_runtime(struct job_record *job_ptr);
static uint32_t _job_resources(struct job_record *job_ptr);
static uint32_t _campaign_time_left(struct ostrich_campaign *camp);
static int _is_priority_job(struct job_record *job_ptr);
static int _is_in_partition(struct job_record *job_ptr,
			    struct ostrich_schedule *sched);
static int _is_in_campaign(struct job_record *job_ptr,
			   struct ostrich_campaign *camp);
static void _load_config(void);
static void _update_struct(void);
static void _my_sleep(int secs);
static void _place_waiting_job(struct job_record *job_ptr, char *part_name);
static void _manage_incoming_jobs(void);
static void _move_to_incoming(struct job_record *job_ptr);
static int _distribute_time(struct ostrich_user *user, double *time_share);
static int _validate_user_campaigns(struct ostrich_user *user,
				    struct ostrich_schedule *sched);
static int _check_user_activity(struct ostrich_user *user,
				struct ostrich_schedule *sched);
static int _gather_campaigns(struct ostrich_user *user, List *l);
static void _set_multi_prio(struct job_record *job_ptr, uint32_t prio,
			    struct ostrich_schedule *sched);
static void _assign_priorities(struct ostrich_schedule *sched);

/*********************** operations on lists ******************/
static int _list_find_schedule(struct ostrich_schedule *sched, char *name)
{
	return (strcmp(sched->part_name, name) == 0);
}

static int _list_find_user(struct ostrich_user *user, uint32_t *uid)
{
	return (user->user_id == *uid);
}

static int _list_find_job(struct job_record *job_ptr, uint32_t *jid)
{
	return (job_ptr->job_id == *jid);
}

static struct ostrich_schedule *_find_schedule(char *name)
{
	return list_find_first(ostrich_sched_list,
			       (ListFindF) _list_find_schedule,
			       name);
}

static void _list_delete_schedule(struct ostrich_schedule *sched)
{
	xassert (sched != NULL);

	xfree(sched->part_name);
	list_destroy(sched->users);
	xfree(sched);
}

static void _list_delete_user(struct ostrich_user *user)
{
	xassert (user != NULL);

	list_destroy(user->campaigns);
	list_destroy(user->waiting_jobs);
	xfree(user);
}

static void _list_delete_campaign(struct ostrich_campaign *camp)
{
	xassert (camp != NULL);

	list_destroy(camp->jobs);
	xfree(camp);
}

/* Sort in ascending order of remaining campaign time,
 * for equal elements sort in ascending order of creation time. */
static int _list_sort_camp_remaining_time(struct ostrich_campaign *x,
					  struct ostrich_campaign *y)
{
	int diff = ( (_campaign_time_left(x) + x->time_offset) -
			(_campaign_time_left(y) + y->time_offset) );
	if (diff == 0)
		return (x->creation_time - y->creation_time);
	return diff;
}

/* Sort in ascending order of job begin time.
 * Can be used only on lists without finished jobs. */
static int _list_sort_job_begin_time(struct job_record *x,
				     struct job_record *y)
{
	return (x->details->begin_time - y->details->begin_time);
}

/* Sort in ascending order of job predicted runtime,
 * for equal elements sort in ascending order of begin time. */
static int _list_sort_job_runtime(struct job_record *x,
				  struct job_record *y)
{
	int diff = _job_pred_runtime(x) - _job_pred_runtime(y);
	if (diff == 0)
		return _list_sort_job_begin_time(x, y);
	return diff;
}

static int _list_remove_finished(struct job_record *job_ptr, void *key)
{
	return IS_JOB_FINISHED(job_ptr);
}

/*********************** implementation ***********************/
static void _print_finished_job(struct job_record *job_ptr, uint32_t camp_id)
{
	int jid = job_ptr->job_id;
	if (job_ptr->comment)
		jid = atoi(job_ptr->comment);

	time_t begin = job_ptr->start_time;
	if (job_ptr->details)
		begin = job_ptr->details->begin_time;

	verbose("OStrich Log: Job finished: %d %d %d %ld %ld %d %d %d %s",
		jid,
		job_ptr->user_id,
		camp_id,
		begin,					/* begin time */
		job_ptr->start_time - begin,		/* wait time */
		_job_real_runtime(job_ptr),		/* run time */
		_job_pred_runtime(job_ptr),		/* predicted run time */
		job_ptr->total_cpus,			/* cpu count */
		job_state_string(job_ptr->job_state)	/* job state */
	);
}

/* _job_pred_runtime - return job time limit in seconds
 *	or DEFAULT_JOB_LIMIT if not set */
static uint32_t _job_pred_runtime(struct job_record *job_ptr)
{
	/* All values are in minutes, change to seconds. */
	if (job_ptr->time_limit == NO_VAL || job_ptr->time_limit == INFINITE)
		return DEFAULT_JOB_LIMIT * 60;
	return job_ptr->time_limit * 60;
}

/* _job_real_runtime - calculate the job run time depending on its state */
static uint32_t _job_real_runtime(struct job_record *job_ptr)
{
	time_t end;

	if (IS_JOB_FINISHED(job_ptr))
		end = job_ptr->end_time;
	else if (IS_JOB_SUSPENDED(job_ptr))
		end = job_ptr->suspend_time;
	else if (IS_JOB_RUNNING(job_ptr))
		end = time(NULL);
	else
		return 0; /* pending */

	if (end > job_ptr->start_time + job_ptr->tot_sus_time)
		return end - job_ptr->start_time - job_ptr->tot_sus_time;
	return 0;
}

/* _job_resources - calculate the number of cpus used by the job */
static uint32_t _job_resources(struct job_record *job_ptr)
{
	if (IS_JOB_STARTED(job_ptr))
		return job_ptr->total_cpus;
	else
		/* pending, give an estimate */
		//TODO better estimate based on "select type"
		return job_ptr->details->cpus_per_task;
}

/* _campaign_time_left - calculate the time needed for the campaign
 *	to finish in the virtual schedule */
static uint32_t _campaign_time_left(struct ostrich_campaign *camp)
{
	uint32_t workload = camp->completed_time + camp->remaining_time;
	if (workload > (int) camp->virtual_time)
		return workload - (int) camp->virtual_time;
	return 0;
}

/* _is_priority_job - jobs with reservations or with custom priority
 *	are prioritized over normal jobs */
static int _is_priority_job(struct job_record *job_ptr)
{
	return (job_ptr->resv_name || job_ptr->direct_set_prio);
}

/* _is_in_partition - check if the job can be run on the partition */
static int _is_in_partition(struct job_record *job_ptr,
			    struct ostrich_schedule *sched)
{
	struct part_record *part_ptr;
	ListIterator iter;
	int result = 0;

	if (job_ptr->part_ptr_list) {
		iter = list_iterator_create(job_ptr->part_ptr_list);
		while ((part_ptr = (struct part_record *) list_next(iter)))
			if (strcmp(part_ptr->name, sched->part_name) == 0) {
				result = 1;
				break;
			}
		list_iterator_destroy(iter);
	} else if (job_ptr->part_ptr) {
		result = (strcmp(job_ptr->part_ptr->name, sched->part_name) == 0);
	}
	return result;
}
/* _is_in_campaign - check if the job belongs to the campaign,
 *	according to the campaign creation time and the threshold value */
static int _is_in_campaign(struct job_record *job_ptr,
			   struct ostrich_campaign *camp)
{
	return (job_ptr->details->begin_time <= camp->creation_time + threshold);
}

static void _load_config(void)
{
	char *sched_params, *select_type, *preempt_type, *prio_type, *tmp_ptr;
	int req_job_age;

	sched_params = slurm_get_sched_params();

	if (sched_params && strstr(sched_params, "defer"))
		defer_sched = 1;
	else
		defer_sched = 0;

	if (sched_params && (tmp_ptr=strstr(sched_params, "sched_interval=")))
		schedule_interval = atoi(tmp_ptr + 15);
	if (schedule_interval < 1)
		fatal("OStrich: invalid schedule interval: %d", schedule_interval);

	if (sched_params && (tmp_ptr=strstr(sched_params, "threshold=")))
		threshold = atoi(tmp_ptr + 10);
	if (threshold < 0)
		fatal("OStrich: invalid threshold: %d", threshold);

	xfree(sched_params);

// TODO _job_resources based on "select type"
//	select_type = slurm_get_select_type();
//	if (strcmp(select_type, "select/serial"))
//		fatal("OStrich: scheduler supports only select/serial (%s given)",
//			select_type);
//	xfree(select_type);

	preempt_type = slurm_get_preempt_type();
	if (strcmp(preempt_type, "preempt/none"))
		fatal("OStrich: scheduler supports only preempt/none (%s given)", preempt_type);
	xfree(preempt_type);

	if (slurm_get_preempt_mode() != PREEMPT_MODE_OFF)
		fatal("OStrich: scheduler supports only PreemptMode=OFF");

	prio_type = slurm_get_priority_type();
	if (strcmp(prio_type, "priority/basic"))
		fatal("OStrich: scheduler supports only priority/basic (%s given)", prio_type);
	xfree(prio_type);

	req_job_age = 4 * schedule_interval;
	if (slurmctld_conf.min_job_age > 0 && slurmctld_conf.min_job_age < req_job_age)
		fatal("OStrich: MinJobAge must be greater or equal to %d", req_job_age);
}

/* _update_struct - update the internal list of virtual schedules
 *	to maintain one schedule per existing partition
 * global: part_list - pointer to global partition list
 */
static void _update_struct(void)
{
	struct ostrich_schedule *sched;
	struct part_record *part_ptr;
	ListIterator iter;

	/* Remove unnecessary ostrich_schedule entries. */
	iter = list_iterator_create(ostrich_sched_list);

	while ((sched = (struct ostrich_schedule *) list_next(iter))) {
		part_ptr = find_part_record(sched->part_name);
		if (!part_ptr)
			list_delete_item(iter);
	}

	list_iterator_destroy(iter);

	/* Create missing ostrich_schedule entries. */
	iter = list_iterator_create(part_list);

	while ((part_ptr = (struct part_record *) list_next(iter))) {
		sched = _find_schedule(part_ptr->name);
		if (!sched) {
			sched = xmalloc(sizeof(struct ostrich_schedule));
			sched->part_name = xstrdup(part_ptr->name);
			sched->allocated_cpus = 0;
			sched->user_shares = 0;

			sched->users = list_create( (ListDelF) _list_delete_user );

			list_append(ostrich_sched_list, sched);
		}
		/* Set/update priority. */
		sched->priority = part_ptr->priority;
		/* Set/update max_time. */
		if (part_ptr->max_time == NO_VAL || part_ptr->max_time == INFINITE)
			sched->max_time = DEFAULT_PART_LIMIT;
		else
			sched->max_time = part_ptr->max_time;
		/* Set/update average cpu count. */
		if (part_ptr->total_nodes >= part_ptr->total_cpus)
			sched->cpus_per_node = 1;
		else
			sched->cpus_per_node = part_ptr->total_cpus / part_ptr->total_nodes;
	}

	list_iterator_destroy(iter);
}

static void _my_sleep(int secs)
{
	struct timespec ts = {0, 0};

	ts.tv_sec = time(NULL) + secs;
	pthread_mutex_lock(&term_lock);
	if (!stop_thread)
		pthread_cond_timedwait(&term_cond, &term_lock, &ts);
	pthread_mutex_unlock(&term_lock);
}

/* _place_waiting_job - put the job to the owners waiting list in the specified partition */
static void _place_waiting_job(struct job_record *job_ptr, char *part_name)
{
	struct ostrich_schedule *sched;
	struct ostrich_user *user;

	sched = _find_schedule(part_name);

	xassert(sched != NULL);

	user = list_find_first(sched->users,
			       (ListFindF) _list_find_user,
			       &(job_ptr->user_id));

	if (!user) {
		user = xmalloc(sizeof(struct ostrich_user));

		user->user_id = job_ptr->user_id;

		user->active_campaigns = 0;
		user->last_campaign_id = 0;

		user->virtual_time = 0;
		user->shares = 1;
		//TODO retrieve share count from an outside source (slurmdb?)

		user->campaigns = list_create( (ListDelF) _list_delete_campaign );
		user->waiting_jobs = list_create(NULL); /* job pointers, no delete function */

		list_append(sched->users, user);
	}

	list_append(user->waiting_jobs, job_ptr);
}

/* _manage_incoming_jobs - move the newly submitted jobs to the appropriate schedules */
static void _manage_incoming_jobs(void)
{
	struct part_record *part_ptr;
	struct job_record *job_ptr;
	ListIterator iter;

	while ((job_ptr = (struct job_record *) list_dequeue(incoming_jobs))) {
		if (_is_priority_job(job_ptr)) {
			list_append(priority_jobs, job_ptr);
		} else if (job_ptr->part_ptr_list) {
			iter = list_iterator_create(job_ptr->part_ptr_list);
			while ((part_ptr = (struct part_record *) list_next(iter)))
				_place_waiting_job(job_ptr, part_ptr->name);
			list_iterator_destroy(iter);
		} else if (job_ptr->part_ptr) {
			_place_waiting_job(job_ptr, job_ptr->part_ptr->name);
		} else {
			error("OStrich: skipping job %d, no partition specified",
			       job_ptr->job_id);
		}
	}
}

/* _move_to_incoming - put the job into the 'incoming_jobs' queue and reset
 *	the priority back to zero */
static void _move_to_incoming(struct job_record *job_ptr)
{
	/* Priority array must be removed, so that the 'global' priority
	 * is used and the job is in a held state on all of the partitions. */
	xfree(job_ptr->priority_array);
	job_ptr->priority = 0;
	list_enqueue(incoming_jobs, job_ptr);
}

/* _distribute_time - add specified amount of virtual time to active users. */
static int _distribute_time(struct ostrich_user *user, double *time_share)
{
	if (user->active_campaigns)
		user->virtual_time += *time_share * user->shares;
	return 0;
}

/* _validate_user_campaigns - initial processing of the user on the given partition.
 *	1) Move new jobs from waiting list to matching campaigns.
 *	2) Confirm that all the jobs still belong to their respective campaigns.
 *	3) Count cpus used by jobs.
 */
static int _validate_user_campaigns(struct ostrich_user *user,
				    struct ostrich_schedule *sched)
{
	struct ostrich_campaign *camp;
	struct job_record *job_ptr, *dup;
	ListIterator camp_iter, job_iter;
	uint32_t job_cpus, job_runtime, job_time_limit;
	time_t now = time(NULL);
	bool orig_list_ended = false;

	/* Remove finished jobs from the waiting list. */
	list_delete_all(user->waiting_jobs,
			(ListFindF) _list_remove_finished,
			NULL);
	/* Only active jobs left, now sort by begin time. */
	list_sort(user->waiting_jobs,
		  (ListCmpF) _list_sort_job_begin_time);

	camp_iter = list_iterator_create(user->campaigns);
	job_iter = list_iterator_create(user->waiting_jobs);

	camp = (struct ostrich_campaign *) list_next(camp_iter);
	job_ptr = (struct job_record *) list_next(job_iter);

	/* Assign waiting jobs to existing campaigns or create new
	 * campaigns. Don't bother with jobs that cannot run now.
	 * Note: campaigns are already sorted by creation time.
	 */
	while(job_ptr && job_ptr->details->begin_time <= now) {
		if (!camp) { /* create a new campaign */
			camp = xmalloc(sizeof(struct ostrich_campaign));

			camp->camp_id = ++user->last_campaign_id;

			camp->time_offset = 0;
			camp->completed_time = 0;
			camp->remaining_time = 0;
			camp->virtual_time = 0;

			camp->creation_time = job_ptr->details->begin_time;
			camp->jobs = list_create(NULL); /* job pointers, no delete function */

			/* We iterated through the whole original list of campaigns
			 * and now we cannot continue to use 'camp_iter'. */
			list_append(user->campaigns, camp);
			orig_list_ended = true;
		}
		if (_is_in_campaign(job_ptr, camp)) {
			/* Look for job duplicate in the campaign. */
			dup = list_find_first(camp->jobs,
					      (ListFindF) _list_find_job,
					      &(job_ptr->job_id));
			if (!dup)
				list_append(camp->jobs, job_ptr);
			list_remove(job_iter);
			/* Advance to the next job. */
			job_ptr = (struct job_record *) list_next(job_iter);
		} else if (orig_list_ended) {
			/* This will force new campaigns if there are any jobs left. */
			camp = NULL;
		} else {
			/* Advance to the next campaign. */
			camp = (struct ostrich_campaign *) list_next(camp_iter);
		}
	}
	/* Check the boundaries of the remaining waiting jobs. */
	if (job_ptr)
		do {
			if (_is_priority_job(job_ptr) ||
			    !_is_in_partition(job_ptr, sched)) {
				_move_to_incoming(job_ptr);
				list_remove(job_iter);
			}
		} while ((job_ptr = (struct job_record *) list_next(job_iter)));

	list_iterator_destroy(camp_iter);
	list_iterator_destroy(job_iter);

	/* Check the boundaries of all the jobs in the campaigns.
	 * Calculate the number of allocated cpus in this partition. */
	camp_iter = list_iterator_create(user->campaigns);

	while ((camp = (struct ostrich_campaign *) list_next(camp_iter))) {

		camp->remaining_time = 0;
		job_iter = list_iterator_create(camp->jobs);

		while ((job_ptr = (struct job_record *) list_next(job_iter))) {

			job_cpus = _job_resources(job_ptr);
			job_runtime = _job_real_runtime(job_ptr);
			job_time_limit = MIN(_job_pred_runtime(job_ptr),
					     sched->max_time);
			/* Remove finished jobs. */
			if (IS_JOB_FINISHED(job_ptr)) {
				/* Use real time. */
				camp->completed_time += job_runtime * job_cpus;

				if (DEBUG_FLAG)
					_print_finished_job(job_ptr, camp->camp_id);

				list_remove(job_iter);
				continue;
			}
			/* Check job boundaries. */
			if (_is_priority_job(job_ptr) ||
			    !_is_in_partition(job_ptr, sched) ||
			    !_is_in_campaign(job_ptr, camp) ||
			    (job_ptr->details->begin_time > now)) {
				_move_to_incoming(job_ptr);
				list_remove(job_iter);
				continue;
			}
			/* Job belongs to the campaign, process it further. */
			if (IS_JOB_SUSPENDED(job_ptr)) {
				/* Use real time. */
				camp->remaining_time += job_runtime * job_cpus;
			} else if (IS_JOB_RUNNING(job_ptr)) {
				/* Count allocated resources only if the job is running. */
				sched->allocated_cpus += job_cpus;
				/* Use prediction. */
				camp->remaining_time += job_time_limit * job_cpus;
			} else  {
				/* Job is pending, use prediction. */
				camp->remaining_time += job_time_limit * job_cpus;
			}
		}

		list_iterator_destroy(job_iter);
	}

	list_iterator_destroy(camp_iter);
	return 0;
}

/* _check_user_activity - further processing of the user on the given partition.
 *	4) Redistribute virtual time between campaigns so that earlier created
 *	campaigns finish first in the virtual schedule.
 *	5) Count shares of active users.
 */
static int _check_user_activity(struct ostrich_user *user,
				struct ostrich_schedule *sched)
{
	struct ostrich_campaign *camp;
	ListIterator iter;
	double virt_time;
	uint32_t workload, offset = 0;

	user->active_campaigns = 0;
	iter = list_iterator_create(user->campaigns);

	/* Combine the virtual time from all of the campaigns. */
	while ((camp = (struct ostrich_campaign *) list_next(iter))) {
		user->virtual_time += camp->virtual_time;
		camp->virtual_time = 0;
	}

	list_iterator_reset(iter);

	/* Redistribute the virtual time by following the default
	 * campaign ordering by creation time. */
	while ((camp = (struct ostrich_campaign *) list_next(iter))) {
		workload = camp->completed_time + camp->remaining_time;

		virt_time = MIN((double) workload, user->virtual_time);

		camp->virtual_time += virt_time;
		user->virtual_time -= virt_time;

		/* Offset is the total virtual time still
		 * needed by the previous campaigns. */
		camp->time_offset = offset;
		offset += _campaign_time_left(camp);

		if (_campaign_time_left(camp) == 0) {
			/* Campaign ended in the virtual schedule. */
			if (list_is_empty(camp->jobs))
				//TODO check here if now > create + thresh?
				/* It also ended in the real schedule. */
				list_delete_item(iter);
		} else {
			/* Campaign is active. */
			user->active_campaigns++;
		}
	}

	list_iterator_destroy(iter);

	if (user->active_campaigns)
		sched->user_shares += user->shares;
	/* The virtual time overflow is lost if not fully redistributed.
	 * This is done to prevent potential abuses of the system. */
	user->virtual_time = 0;
	return 0;
}

/* _gather_campaigns - merge campaigns into one common list */
static int _gather_campaigns(struct ostrich_user *user, List *l)
{
	list_append_list(*l, user->campaigns);
	return 0;
}

/* _set_multi_prio - set the priority of the job to 'prio'.
 * Note: priority is set only for the given partition by using priority_array.
 * 	Requires version > 2.6.x of SLURM to work.
 */
static void _set_multi_prio(struct job_record *job_ptr, uint32_t prio,
			    struct ostrich_schedule *sched)
{
	struct part_record *part_ptr;
	ListIterator iter;
	int inx = 0, size;

	if (job_ptr->part_ptr_list) {
		if (!job_ptr->priority_array) {
			size = list_count(job_ptr->part_ptr_list) * sizeof(uint32_t);
			job_ptr->priority_array = xmalloc(size);
			/* If the partition count changes in the future,
			 * the priority_array will be deallocated automatically
			 * by SLURM in job_mgr.c 'update_job'.
			 */
		}
		iter = list_iterator_create(job_ptr->part_ptr_list);
		while ((part_ptr = (struct part_record *) list_next(iter))) {
			if (strcmp(part_ptr->name, sched->part_name) == 0)
				job_ptr->priority_array[inx] = prio;
				/* Continue on with the loop, there might be
				 *  multiple entries of the same partition. */
			inx++;
		}
		list_iterator_destroy(iter);
	}
	if (job_ptr->part_ptr) {
		if (strcmp(job_ptr->part_ptr->name, sched->part_name) == 0)
			job_ptr->priority = prio;
	}
}

/* _assign_priorities - calculate and set the priority for all the jobs
 *	in the partition. Jobs inside campaigns are ordered by runtime */
static void _assign_priorities(struct ostrich_schedule *sched)
{
	struct ostrich_campaign *camp;
	struct job_record *job_ptr;
	List all_campaigns;
	ListIterator camp_iter, job_iter;
	int prio, normal_queue =  500000; /* Starting priority. */

	all_campaigns = list_create(NULL); /* Tmp list, no delete function. */

	list_for_each(sched->users,
		      (ListForF) _gather_campaigns,
		      &all_campaigns);

	/* Sort the combined list of campaigns. */
	list_sort(all_campaigns,
		  (ListCmpF) _list_sort_camp_remaining_time);

	camp_iter = list_iterator_create(all_campaigns);

	while ((camp = (struct ostrich_campaign *) list_next(camp_iter))) {
		/* Sort the jobs inside each campaign. */
		list_sort(camp->jobs,
			  (ListCmpF) _list_sort_job_runtime);

		job_iter = list_iterator_create(camp->jobs);
		while ((job_ptr = (struct job_record *) list_next(job_iter))) {
			/* Take into account partition priority. */
			prio = normal_queue-- + sched->priority;
			if (prio < 1)
				prio = 1;
			_set_multi_prio(job_ptr, prio, sched);
		}
		list_iterator_destroy(job_iter);
	}

	list_iterator_destroy(camp_iter);
	list_destroy(all_campaigns);
}

/* Terminate ostrich_agent */
extern void stop_ostrich_agent(void)
{
	pthread_mutex_lock(&term_lock);
	stop_thread = true;
	pthread_cond_signal(&term_cond);
	pthread_mutex_unlock(&term_lock);
}

/* Notify scheduler about a new job in the system */
extern void enqueue_new_job(struct job_record *job_ptr)
{
	list_enqueue(incoming_jobs, job_ptr);
}

/* Notify that slurm.conf has changed */
extern void ostrich_reconfig(void)
{
	config_flag = true;
}

/* ostrich_agent - detached thread maintains the virtual schedules
 *	and based on those calculates the job priorities */
extern void *ostrich_agent(void *args)
{
	struct ostrich_schedule *sched;
	ListIterator iter;
	uint32_t prev_allocated;
	double time_share;
	DEF_TIMERS;

	/* Read config, and partitions; Write jobs. */
	slurmctld_lock_t all_locks = {
		READ_LOCK, WRITE_LOCK, NO_LOCK, READ_LOCK };

	/* Create empty lists. */
	ostrich_sched_list = list_create( (ListDelF) _list_delete_schedule );
	incoming_jobs = list_create(NULL); /* job pointers, no delete function */
	priority_jobs = list_create(NULL); /* job pointers, no delete function */
	/* Read settings and setup structures. */
	_load_config();
	_update_struct();

	last_sched_time = time(NULL);

	while (!stop_thread) {
		_my_sleep(schedule_interval);

		if (stop_thread)
			break;

		lock_slurmctld(all_locks);

		START_TIMER;

		if (config_flag) {
			config_flag = false;
			_load_config();
			_update_struct();
		}

		/* Add newly submitted jobs. */
		_manage_incoming_jobs();
		// TODO teraz prace z reserwacjami

		/* Time past since last iteration. */
		time_share = difftime(time(NULL), last_sched_time);
		last_sched_time = time(NULL);

		/* Deal with each schedule separately. */
		iter = list_iterator_create(ostrich_sched_list);

		while ((sched = (struct ostrich_schedule *) list_next(iter))) {
			/* Count taken cpus in the partition. Do initial validation. */
			prev_allocated = sched->allocated_cpus;
			sched->allocated_cpus = 0;
			list_for_each(sched->users,
				      (ListForF) _validate_user_campaigns,
				      sched);

// struct part_record *part_ptr;
// int i, online_cpus = 0;
// uint16_t alloc_cpus;
//
// part_ptr = find_part_record(sched->part_name);
//
// 	for (i = 0; i < node_record_count; i++)
// 	if (bit_test(part_ptr->node_bitmap, i))
// 	if (bit_test(avail_node_bitmap, i)) {
//
// select_g_select_nodeinfo_set_all();
// select_g_select_nodeinfo_get(node_record_table_ptr[i].select_nodeinfo,
// 	SELECT_NODEDATA_SUBCNT,
// 	NODE_STATE_ALLOCATED,
// 	&alloc_cpus);
// online_cpus += alloc_cpus;
// 	}
// verbose("SCHED->ALLOC %d VS ONLINE %d", sched->allocated_cpus, online_cpus);

			/* Distribute the virtual time from the previous period. */
			if (sched->user_shares) {
				// TODO max or avg or just one end - does it matter??
				time_share *= MAX(prev_allocated,
						  sched->allocated_cpus);
				time_share /= sched->user_shares;
				list_for_each(sched->users,
					      (ListForF) _distribute_time,
					      &time_share);
			}
			/* Count shares. Recalculate the virtual schedule. */
			sched->user_shares = 0;
			list_for_each(sched->users,
				      (ListForF) _check_user_activity,
				      sched);
			/* Set new job priorities */
			_assign_priorities(sched);
		}

		list_iterator_destroy(iter);

		unlock_slurmctld(all_locks);

		// TODO CO JAKIS CZAS SPRWDZAC "SHARES" W BAZIE

// 		> 		/*
// > 		 * In defer mode, avoid triggering the scheduler logic
// > 		 * for every epilog complete message.
// > 		 * As one epilog message is sent from every node of each
// > 		 * job at termination, the number of simultaneous schedule
// > 		 * calls can be very high for large machine or large number
// > 		 * of managed jobs.
// > 		 */
// > 		if (!defer_sched)
// > 			(void) schedule(0);

// > 			(void) schedule(schedule_cnt); /* has own locks */

// 	job_iterator = list_iterator_create(job_list);
// 	while ((job_q_ptr = (struct job_record *) list_next(job_iterator))) {
// 		if (!IS_JOB_PENDING(job_q_ptr) || !job_q_ptr->details ||
// 		    (job_q_ptr->part_ptr != job_ptr->part_ptr) ||
// 		    (job_q_ptr->priority < job_ptr->priority) ||
// 		    (job_q_ptr->job_id == job_ptr->job_id))
// 			continue;
// 		if (job_q_ptr->details->min_nodes == NO_VAL)
// 			job_size_nodes = 1;
// 		else
// 			job_size_nodes = job_q_ptr->details->min_nodes;
// 		if (job_q_ptr->details->min_cpus == NO_VAL)
// 			job_size_cpus = 1;
// 		else
// 			job_size_cpus = job_q_ptr->details->min_nodes;
// 		job_size_cpus = MAX(job_size_cpus,
// 				    (job_size_nodes * part_cpus_per_node));
//
		END_TIMER2("OStrich: ostrich_agent");
		debug2("OStrich: schedule iteration %s", TIME_STR);

 		if (DEBUG_FLAG)
			schedule(0);    // force actual scheduling
		// TODO schedule("PENDING IN PRIO" + QUEUE PARAM FROM CONFIG");
	}
	/* Cleanup. */
	list_destroy(ostrich_sched_list);
	list_destroy(incoming_jobs);
	list_destroy(priority_jobs);

	return NULL;
}
