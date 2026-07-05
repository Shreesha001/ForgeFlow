package store

import (
	"context"
	"time"
)

// Claim atomically claims up to n due pending runs for a worker.
//
// This is the heart of ForgeFlow: FOR UPDATE SKIP LOCKED lets many workers
// contend on the queue without blocking each other, and the row transition
// pending -> running guarantees a run is executed by exactly one worker at a
// time. The claim carries a lease; if the worker dies, RecoverExpiredLeases
// returns the run to the queue after the lease expires.
func (s *Store) Claim(ctx context.Context, workerID string, n int, lease time.Duration) ([]ClaimedRun, error) {
	rows, err := s.pool.Query(ctx, `
		WITH claimed AS (
			SELECT r.id FROM runs r
			JOIN jobs j ON j.id = r.job_id
			WHERE r.status = 'pending'
			  AND r.scheduled_for <= now()
			  AND j.status = 'active'
			ORDER BY r.priority, r.scheduled_for
			LIMIT $2
			FOR UPDATE OF r SKIP LOCKED
		)
		UPDATE runs r SET
			status = 'running',
			worker_id = $1,
			attempt = r.attempt + 1,
			started_at = now(),
			lease_expires = now() + $3
		FROM claimed, jobs j
		WHERE r.id = claimed.id AND j.id = r.job_id
		RETURNING r.id, r.job_id, r.status, r.priority, r.attempt, r.max_attempts,
			r.scheduled_for, r.worker_id, r.lease_expires, r.started_at, r.finished_at,
			r.output, r.error, r.created_at,
			j.name, j.executor, j.payload, j.timeout_seconds`,
		workerID, n, lease)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var claims []ClaimedRun
	for rows.Next() {
		var c ClaimedRun
		if err := rows.Scan(&c.ID, &c.JobID, &c.Status, &c.Priority, &c.Attempt, &c.MaxAttempts,
			&c.ScheduledFor, &c.WorkerID, &c.LeaseExpires, &c.StartedAt, &c.FinishedAt,
			&c.Output, &c.Error, &c.CreatedAt,
			&c.JobName, &c.Executor, &c.Payload, &c.TimeoutSeconds); err != nil {
			return nil, err
		}
		claims = append(claims, c)
	}
	return claims, rows.Err()
}

// Heartbeat extends the lease on all runs a worker currently holds.
// The worker_id guard means a worker that lost its lease (and whose run was
// re-queued) cannot resurrect it.
func (s *Store) Heartbeat(ctx context.Context, workerID string, lease time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE runs SET lease_expires = now() + $2
		WHERE worker_id = $1 AND status = 'running'`, workerID, lease)
	return tag.RowsAffected(), err
}

// CompleteRun marks a run succeeded, guarded by worker identity: a slow worker
// whose lease expired (and whose run was handed to someone else) cannot
// overwrite the new owner's state. Returns false if the guard rejected it.
func (s *Store) CompleteRun(ctx context.Context, runID int64, workerID, output string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE runs SET status = 'succeeded', finished_at = now(), output = $3,
			worker_id = NULL, lease_expires = NULL
		WHERE id = $1 AND worker_id = $2 AND status = 'running'`,
		runID, workerID, output)
	return tag.RowsAffected() == 1, err
}

// FailRun records a failed attempt. If attempts remain, the run goes back to
// pending scheduled at retryAt (backoff computed by the caller); otherwise it
// is dead-lettered.
func (s *Store) FailRun(ctx context.Context, runID int64, workerID, errMsg string, retryAt time.Time) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE runs SET
			status = CASE WHEN attempt >= max_attempts THEN 'dead' ELSE 'pending' END,
			scheduled_for = CASE WHEN attempt >= max_attempts THEN scheduled_for ELSE $4 END,
			finished_at = CASE WHEN attempt >= max_attempts THEN now() ELSE NULL END,
			error = $3, worker_id = NULL, lease_expires = NULL, started_at = NULL
		WHERE id = $1 AND worker_id = $2 AND status = 'running'`,
		runID, workerID, errMsg, retryAt)
	return tag.RowsAffected() == 1, err
}

// RecoverExpiredLeases returns crashed workers' runs to the queue. A run whose
// lease expired is treated exactly like a failed attempt: retried if attempts
// remain, dead-lettered otherwise. Every node runs this periodically.
func (s *Store) RecoverExpiredLeases(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE runs SET
			status = CASE WHEN attempt >= max_attempts THEN 'dead' ELSE 'pending' END,
			finished_at = CASE WHEN attempt >= max_attempts THEN now() ELSE NULL END,
			error = 'lease expired: worker ' || coalesce(worker_id, '?') || ' presumed dead',
			worker_id = NULL, lease_expires = NULL, started_at = NULL
		WHERE status = 'running' AND lease_expires < now()`)
	return tag.RowsAffected(), err
}
