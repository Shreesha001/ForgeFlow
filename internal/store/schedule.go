package store

import (
	"context"
	"time"
)

// DueJobs returns active jobs whose next_run_at (recurring) or run_at
// (one-off) has arrived. One-off jobs are recognised by having no cron_expr
// and no runs yet materialised (next_run_at doubles as the "already enqueued"
// marker for them: NULL run_at handled at creation time by enqueueing directly).
func (s *Store) DueJobs(ctx context.Context, limit int) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+jobCols+` FROM jobs
		WHERE status = 'active' AND next_run_at IS NOT NULL AND next_run_at <= now()
		ORDER BY next_run_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// AdvanceJob enqueues a run (stamped scheduledFor) and moves the job's clock
// forward in one transaction, so a scheduler crash between the two steps
// cannot double-fire. nextRun == nil marks a one-off job as fully scheduled.
// The guard on the current next_run_at makes it idempotent under leader
// handover: a second leader observing the same tick updates zero rows.
func (s *Store) AdvanceJob(ctx context.Context, jobID int64, guard, scheduledFor time.Time, nextRun *time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE jobs SET next_run_at = $3, updated_at = now()
		WHERE id = $1 AND next_run_at = $2`, jobID, guard, nextRun)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // another leader already advanced this tick
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO runs (job_id, priority, max_attempts, scheduled_for)
		SELECT id, priority, max_attempts, $2 FROM jobs WHERE id = $1`, jobID, scheduledFor)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
