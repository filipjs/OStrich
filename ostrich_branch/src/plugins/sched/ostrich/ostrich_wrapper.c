#include <pthread.h>
#include <stdio.h>

#include "slurm/slurm_errno.h"

#include "src/common/plugin.h"
#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/slurmctld/job_scheduler.h"
#include "src/slurmctld/slurmctld.h"
#include "src/plugins/sched/backfill/backfill.h"
#include "src/plugins/sched/ostrich/ostrich.h"


const char		plugin_name[]	= "OStrich Scheduler plugin";
const char		plugin_type[]	= "sched/ostrich";
const uint32_t		plugin_version	= 100;

/* A plugin-global errno. */
static int plugin_errno = SLURM_SUCCESS;

static pthread_t ostrich_thread = 0;
static pthread_t backfill_thread = 0;
static pthread_mutex_t thread_flag_mutex = PTHREAD_MUTEX_INITIALIZER;

/**************************************************************************/
/*  TAG(                              init                              ) */
/**************************************************************************/
int init( void )
{
	pthread_attr_t attr;

	verbose("sched: OStrich scheduler plugin loaded");

	pthread_mutex_lock( &thread_flag_mutex );
	if ( ostrich_thread || backfill_thread ) {
		debug2("OStrich: scheduler thread already running, "
			"not starting another");
		pthread_mutex_unlock( &thread_flag_mutex );
		return SLURM_ERROR;
	}

	slurm_attr_init( &attr );
	/* since we do a join on this later we don't make it detached */
	if (pthread_create( &ostrich_thread, &attr, ostrich_agent, NULL))
		error("OStrich: unable to start OStrich scheduler thread");
	if (pthread_create( &backfill_thread, &attr, backfill_agent, NULL))
		error("OStrich: unable to start backfill scheduler thread");
	pthread_mutex_unlock( &thread_flag_mutex );
	slurm_attr_destroy( &attr );

	return SLURM_SUCCESS;
}

/**************************************************************************/
/*  TAG(                              fini                              ) */
/**************************************************************************/
void fini( void )
{
	verbose("sched: OStrich scheduler plugin shutting down");

	pthread_mutex_lock( &thread_flag_mutex );
	if ( ostrich_thread ) {
		stop_ostrich_agent();
		pthread_join(ostrich_thread, NULL);
		ostrich_thread = 0;
	}
	if ( backfill_thread ) {
		stop_backfill_agent();
		pthread_join(backfill_thread, NULL);
		backfill_thread = 0;
	}
	pthread_mutex_unlock( &thread_flag_mutex );
}

/**************************************************************************/
/* TAG(              slurm_sched_plugin_reconfig                        ) */
/**************************************************************************/
int slurm_sched_plugin_reconfig( void )
{
	ostrich_reconfig();
	backfill_reconfig();
	return SLURM_SUCCESS;
}

/***************************************************************************/
/*  TAG(                   slurm_sched_plugin_schedule                   ) */
/***************************************************************************/
int
slurm_sched_plugin_schedule( void )
{
	return SLURM_SUCCESS;
}

/***************************************************************************/
/*  TAG(                   slurm_sched_plugin_newalloc                   ) */
/***************************************************************************/
int
slurm_sched_plugin_newalloc( struct job_record *job_ptr )
{
	return SLURM_SUCCESS;
}

/***************************************************************************/
/*  TAG(                   slurm_sched_plugin_freealloc                  ) */
/***************************************************************************/
int
slurm_sched_plugin_freealloc( struct job_record *job_ptr )
{
	return SLURM_SUCCESS;
}


/**************************************************************************/
/* TAG(                   slurm_sched_plugin_initial_priority           ) */
/**************************************************************************/
uint32_t
slurm_sched_plugin_initial_priority( uint32_t last_prio,
				     struct job_record *job_ptr )
{
// 	verbose("JOB %d", job_ptr->job_id);
//  	verbose("TAKS %d CPUS_PER_TASK %d",job_ptr->details->num_tasks, job_ptr->details->cpus_per_task);

// 	if (job_ptr->details->num_tasks == 0)
// 		job_ptr->details->num_tasks = 1;

	enqueue_new_job(job_ptr);
	return 0;
}

/**************************************************************************/
/* TAG(              slurm_sched_plugin_job_is_pending                  ) */
/**************************************************************************/
void slurm_sched_plugin_job_is_pending( void )
{
	/* Empty. */
}

/**************************************************************************/
/* TAG(              slurm_sched_plugin_partition_change                ) */
/**************************************************************************/
void slurm_sched_plugin_partition_change( void )
{
	ostrich_reconfig(); /* reconfigure is also needed here */
}

/**************************************************************************/
/* TAG(              slurm_sched_get_errno                              ) */
/**************************************************************************/
int slurm_sched_get_errno( void )
{
	return plugin_errno;
}

/**************************************************************************/
/* TAG(              slurm_sched_strerror                               ) */
/**************************************************************************/
char *slurm_sched_strerror( int errnum )
{
	return NULL;
}

/**************************************************************************/
/* TAG(              slurm_sched_plugin_requeue                         ) */
/**************************************************************************/
void slurm_sched_plugin_requeue( struct job_record *job_ptr, char *reason )
{
	/* Empty. */
}

/**************************************************************************/
/* TAG(              slurm_sched_get_conf                               ) */
/**************************************************************************/
char *slurm_sched_get_conf( void )
{
	return NULL;
}
