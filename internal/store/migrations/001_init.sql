-- ForgeFlow core schema.
-- jobs: the user-facing definition (what to run, when, how to retry).
-- runs: individual executions of a job. Workers claim runs, never jobs.

CREATE TABLE IF NOT EXISTS jobs (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT        NOT NULL,
    idempotency_key TEXT        UNIQUE,
    -- executor: "shell" runs payload as a command, "http" POSTs payload to a URL
    executor        TEXT        NOT NULL CHECK (executor IN ('shell', 'http')),
    payload         JSONB       NOT NULL DEFAULT '{}',
    -- schedule: cron expression for recurring jobs; NULL means one-off
    cron_expr       TEXT,
    run_at          TIMESTAMPTZ,          -- one-off: when to run (NULL+NULL cron = run now)
    timezone        TEXT        NOT NULL DEFAULT 'UTC',
    priority        INT         NOT NULL DEFAULT 5 CHECK (priority BETWEEN 1 AND 9),
    max_attempts    INT         NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    timeout_seconds INT         NOT NULL DEFAULT 300 CHECK (timeout_seconds > 0),
    -- catch_up: if the scheduler was down when a tick was due: 'skip' or 'once'
    catch_up        TEXT        NOT NULL DEFAULT 'skip' CHECK (catch_up IN ('skip', 'once')),
    status          TEXT        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'cancelled')),
    next_run_at     TIMESTAMPTZ,          -- scheduler's bookkeeping for recurring jobs
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_due
    ON jobs (next_run_at)
    WHERE status = 'active' AND cron_expr IS NOT NULL;

CREATE TABLE IF NOT EXISTS runs (
    id            BIGSERIAL PRIMARY KEY,
    job_id        BIGINT      NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    -- pending -> claimed -> running -> succeeded | failed | dead
    -- 'retryable' is represented as pending with scheduled_for in the future.
    status        TEXT        NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'dead', 'cancelled')),
    priority      INT         NOT NULL DEFAULT 5,
    attempt       INT         NOT NULL DEFAULT 0,
    max_attempts  INT         NOT NULL DEFAULT 3,
    scheduled_for TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- lease: the claiming worker's identity and expiry; heartbeats extend it.
    worker_id     TEXT,
    lease_expires TIMESTAMPTZ,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    output        TEXT,
    error         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The hot index: workers claim the oldest due pending run, highest priority first.
CREATE INDEX IF NOT EXISTS idx_runs_claimable
    ON runs (priority, scheduled_for)
    WHERE status = 'pending';

-- Recovery scan: running runs whose lease expired.
CREATE INDEX IF NOT EXISTS idx_runs_lease
    ON runs (lease_expires)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_runs_job ON runs (job_id, id DESC);
