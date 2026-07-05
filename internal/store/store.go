// Package store is ForgeFlow's Postgres persistence layer. Postgres is the
// single source of truth: jobs, runs, leases and leader election all live here.
package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed all:migrations
var migrationsFS embed.FS

var ErrNotFound = errors.New("not found")

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// migrateLockKey serialises concurrent migrations: when several nodes boot at
// once, racing CREATE TABLE IF NOT EXISTS statements can fail on internal
// catalog uniqueness (pg_type), so only one node migrates at a time.
const migrateLockKey = 0xF0F6D

// Migrate applies embedded SQL migrations in lexical order.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return err
	}
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrateLockKey)

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		sql, err := fs.ReadFile(migrationsFS, "migrations/"+name)
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}

// ---- Models ----

type Job struct {
	ID             int64           `json:"id"`
	Name           string          `json:"name"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Executor       string          `json:"executor"`
	Payload        json.RawMessage `json:"payload"`
	CronExpr       *string         `json:"cron_expr,omitempty"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
	Timezone       string          `json:"timezone"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	CatchUp        string          `json:"catch_up"`
	Status         string          `json:"status"`
	NextRunAt      *time.Time      `json:"next_run_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type Run struct {
	ID           int64      `json:"id"`
	JobID        int64      `json:"job_id"`
	Status       string     `json:"status"`
	Priority     int        `json:"priority"`
	Attempt      int        `json:"attempt"`
	MaxAttempts  int        `json:"max_attempts"`
	ScheduledFor time.Time  `json:"scheduled_for"`
	WorkerID     *string    `json:"worker_id,omitempty"`
	LeaseExpires *time.Time `json:"lease_expires,omitempty"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	Output       *string    `json:"output,omitempty"`
	Error        *string    `json:"error,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
}

// ClaimedRun bundles a claimed run with the job fields a worker needs to execute it.
type ClaimedRun struct {
	Run
	JobName        string
	Executor       string
	Payload        json.RawMessage
	TimeoutSeconds int
}

const jobCols = `id, name, idempotency_key, executor, payload, cron_expr, run_at, timezone,
	priority, max_attempts, timeout_seconds, catch_up, status, next_run_at, created_at, updated_at`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Name, &j.IdempotencyKey, &j.Executor, &j.Payload, &j.CronExpr,
		&j.RunAt, &j.Timezone, &j.Priority, &j.MaxAttempts, &j.TimeoutSeconds, &j.CatchUp,
		&j.Status, &j.NextRunAt, &j.CreatedAt, &j.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, ErrNotFound
	}
	return j, err
}

const runCols = `id, job_id, status, priority, attempt, max_attempts, scheduled_for,
	worker_id, lease_expires, started_at, finished_at, output, error, created_at`

func scanRun(row pgx.Row) (Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.JobID, &r.Status, &r.Priority, &r.Attempt, &r.MaxAttempts,
		&r.ScheduledFor, &r.WorkerID, &r.LeaseExpires, &r.StartedAt, &r.FinishedAt,
		&r.Output, &r.Error, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}
