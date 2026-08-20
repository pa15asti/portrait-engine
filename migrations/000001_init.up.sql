-- Portrait Engine initial schema.
--
-- PostgreSQL is the source of truth for job state. Status columns are
-- constrained via CHECK to the domain's known values; transitions are enforced
-- in application code (guarded UPDATE ... WHERE status = <expected>).

BEGIN;

-- jobs: the aggregate root of the processing lifecycle.
CREATE TABLE jobs (
    id               UUID        PRIMARY KEY,
    status           TEXT        NOT NULL
        CHECK (status IN ('PENDING','QUEUED','PROCESSING','COMPLETED','FAILED','CANCELLED','RETRYING')),
    pipeline         TEXT        NOT NULL,
    pipeline_version TEXT        NOT NULL,
    input_object_key TEXT        NOT NULL,
    correlation_id   TEXT        NOT NULL DEFAULT '',
    attempt_count    INTEGER     NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts     INTEGER     NOT NULL CHECK (max_attempts >= 1),
    last_error       TEXT        NOT NULL DEFAULT '',
    next_retry_at    TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Worker queries by state; index the hot lookups.
CREATE INDEX idx_jobs_status ON jobs (status);
-- Retry sweeper looks up RETRYING jobs whose delay has elapsed.
CREATE INDEX idx_jobs_next_retry_at ON jobs (next_retry_at)
    WHERE next_retry_at IS NOT NULL;

-- attempts: one row per processing attempt against a job.
CREATE TABLE attempts (
    id          UUID        PRIMARY KEY,
    job_id      UUID        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    number      INTEGER     NOT NULL CHECK (number >= 1),
    status      TEXT        NOT NULL
        CHECK (status IN ('RUNNING','SUCCEEDED','FAILED','CANCELLED')),
    error       TEXT        NOT NULL DEFAULT '',
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    -- An attempt number is unique within a job.
    UNIQUE (job_id, number)
);

CREATE INDEX idx_attempts_job_id ON attempts (job_id);

-- processing_steps: one row per processor execution within an attempt.
CREATE TABLE processing_steps (
    id          UUID        PRIMARY KEY,
    attempt_id  UUID        NOT NULL REFERENCES attempts (id) ON DELETE CASCADE,
    name        TEXT        NOT NULL,
    status      TEXT        NOT NULL
        CHECK (status IN ('RUNNING','SUCCEEDED','FAILED','SKIPPED')),
    duration_ms BIGINT      NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
    error       TEXT        NOT NULL DEFAULT '',
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);

CREATE INDEX idx_processing_steps_attempt_id ON processing_steps (attempt_id);

-- artifacts: output objects produced by a job.
CREATE TABLE artifacts (
    id           UUID        PRIMARY KEY,
    job_id       UUID        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    kind         TEXT        NOT NULL,
    object_key   TEXT        NOT NULL,
    content_type TEXT        NOT NULL DEFAULT '',
    size_bytes   BIGINT      NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Deterministic output keys mean re-processing overwrites rather than
    -- duplicates; the unique constraint enforces that at the DB level.
    UNIQUE (job_id, object_key)
);

CREATE INDEX idx_artifacts_job_id ON artifacts (job_id);

-- idempotency_keys: makes POST /v1/jobs safe to retry. The primary key gives us
-- the uniqueness guarantee that serializes concurrent duplicate requests.
CREATE TABLE idempotency_keys (
    key          TEXT        PRIMARY KEY,
    job_id       UUID        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    -- request_hash detects the same key being reused with a different request
    -- body, which must be rejected rather than silently returning a stale job.
    request_hash TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
