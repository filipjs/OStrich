#ifndef _SLURM_OSTRICH_H
#define _SLURM_OSTRICH_H

/* ostrich_agent - detached thread with scheduling logic */
extern void *ostrich_agent(void *args);

/* Terminate ostrich_agent */
extern void stop_ostrich_agent(void);

/* Note that slurm.conf has changed */
extern void ostrich_reconfig(void);

/* Notify scheduler about a new job in the system */
extern void enqueue_new_job(struct job_record *job_ptr);

#endif	/* _SLURM_OSTRICH_H */
