package store

import (
	"context"
	"time"
)

// CreateJob inserts a job. If an idempotency key is set and already exists,
// the existing job is returned instead of creating a duplicate.
func (s *Store) CreateJob(ctx context.Context, j Job) (Job, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (name, idempotency_key, executor, payload, cron_expr, run_at,
			timezone, priority, max_attempts, timeout_seconds, catch_up, next_run_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (idempotency_key) DO UPDATE SET updated_at = jobs.updated_at
		RETURNING `+jobCols,
		j.Name, j.IdempotencyKey, j.Executor, j.Payload, j.CronExpr, j.RunAt,
		j.Timezone, j.Priority, j.MaxAttempts, j.TimeoutSeconds, j.CatchUp, j.NextRunAt)
	return scanJob(row)
}

func (s *Store) GetJob(ctx context.Context, id int64) (Job, error) {
	return scanJob(s.pool.QueryRow(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = $1`, id))
}

func (s *Store) ListJobs(ctx context.Context, limit int) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+jobCols+` FROM jobs ORDER BY id DESC LIMIT $1`, limit)
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

// SetJobStatus pauses, resumes or cancels a job. Cancelling also cancels its
// pending runs; running runs finish their current attempt.
func (s *Store) SetJobStatus(ctx context.Context, id int64, status string) (Job, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback(ctx)

	j, err := scanJob(tx.QueryRow(ctx, `
		UPDATE jobs SET status = $2, updated_at = now() WHERE id = $1
		RETURNING `+jobCols, id, status))
	if err != nil {
		return Job{}, err
	}
	if status == "cancelled" || status == "paused" {
		if _, err := tx.Exec(ctx, `
			UPDATE runs SET status = 'cancelled', finished_at = now()
			WHERE job_id = $1 AND status = 'pending'`, id); err != nil {
			return Job{}, err
		}
	}
	return j, tx.Commit(ctx)
}

// EnqueueRun materialises a pending run for a job at the given time.
func (s *Store) EnqueueRun(ctx context.Context, jobID int64, at time.Time) (Run, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO runs (job_id, priority, max_attempts, scheduled_for)
		SELECT id, priority, max_attempts, $2 FROM jobs WHERE id = $1 AND status = 'active'
		RETURNING `+runCols, jobID, at)
	return scanRun(row)
}

func (s *Store) GetRun(ctx context.Context, id int64) (Run, error) {
	return scanRun(s.pool.QueryRow(ctx, `SELECT `+runCols+` FROM runs WHERE id = $1`, id))
}

// ListRuns returns run history, optionally filtered by job.
func (s *Store) ListRuns(ctx context.Context, jobID int64, limit int) ([]Run, error) {
	q := `SELECT ` + runCols + ` FROM runs WHERE ($1 = 0 OR job_id = $1) ORDER BY id DESC LIMIT $2`
	rows, err := s.pool.Query(ctx, q, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

// Stats returns run counts by status for the dashboard.
func (s *Store) Stats(ctx context.Context) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM runs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	stats := map[string]int64{}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		stats[status] = n
	}
	return stats, rows.Err()
}
