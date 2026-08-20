package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/pa15asti/portrait-engine/internal/domain"
)

// querier is the pgx subset used here, satisfied by both *pgxpool.Pool and
// pgx.Tx so helpers work inside or outside a transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

const jobColumns = `id, status, pipeline, pipeline_version, input_object_key,
	correlation_id, attempt_count, max_attempts, last_error, next_retry_at,
	created_at, updated_at`

// JobRepository persists jobs, attempts, steps, and artifacts.
type JobRepository struct {
	pool *pgxpool.Pool
}

// NewJobRepository constructs a JobRepository over the given pool.
func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

// NewJobParams describes a job to create. The caller supplies the ID so the
// idempotent path can reference it before the row is committed.
type NewJobParams struct {
	ID              uuid.UUID
	Pipeline        string
	PipelineVersion string
	InputObjectKey  string
	CorrelationID   string
	MaxAttempts     int
}

// CreateJob inserts a new PENDING job. With a non-empty idempotencyKey it's
// idempotent: concurrent duplicates resolve through the idempotency_keys primary
// key and return the original job (created=false). A key reused with a different
// requestHash returns ErrIdempotencyConflict.
func (r *JobRepository) CreateJob(ctx context.Context, p NewJobParams, idempotencyKey, requestHash string) (domain.Job, bool, error) {
	if idempotencyKey == "" {
		job, err := insertJob(ctx, r.pool, p)
		return job, err == nil, err
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	job, err := insertJob(ctx, tx, p)
	if err != nil {
		return domain.Job{}, false, err
	}

	// Claim the key. If someone already owns it, no row comes back and the whole
	// tx (including the job insert above) rolls back.
	var claimedJobID uuid.UUID
	err = tx.QueryRow(ctx,
		`INSERT INTO idempotency_keys (key, job_id, request_hash)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (key) DO NOTHING
		 RETURNING job_id`,
		idempotencyKey, p.ID, requestHash,
	).Scan(&claimedJobID)

	switch {
	case err == nil:
		if err := tx.Commit(ctx); err != nil {
			return domain.Job{}, false, fmt.Errorf("commit tx: %w", err)
		}
		return job, true, nil
	case errors.Is(err, pgx.ErrNoRows):
		// Key already owned by a committed job. Discard our candidate (rollback
		// via defer) and return the existing job.
		_ = tx.Rollback(ctx)
		return r.resolveExisting(ctx, idempotencyKey, requestHash)
	default:
		return domain.Job{}, false, fmt.Errorf("claim idempotency key: %w", err)
	}
}

func (r *JobRepository) resolveExisting(ctx context.Context, key, requestHash string) (domain.Job, bool, error) {
	var existingID uuid.UUID
	var storedHash string
	err := r.pool.QueryRow(ctx,
		`SELECT job_id, request_hash FROM idempotency_keys WHERE key = $1`, key,
	).Scan(&existingID, &storedHash)
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("lookup idempotency key: %w", err)
	}
	if storedHash != requestHash {
		return domain.Job{}, false, ErrIdempotencyConflict
	}
	job, err := r.GetJob(ctx, existingID)
	if err != nil {
		return domain.Job{}, false, err
	}
	return job, false, nil
}

func insertJob(ctx context.Context, q querier, p NewJobParams) (domain.Job, error) {
	row := q.QueryRow(ctx,
		`INSERT INTO jobs (id, status, pipeline, pipeline_version, input_object_key,
			correlation_id, max_attempts)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING `+jobColumns,
		p.ID, domain.StatusPending, p.Pipeline, p.PipelineVersion, p.InputObjectKey,
		p.CorrelationID, p.MaxAttempts,
	)
	return scanJob(row)
}

// GetJob loads a job by ID, returning ErrNotFound if absent.
func (r *JobRepository) GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	row := r.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, ErrNotFound
	}
	return job, err
}

// JobUpdate describes a guarded status transition and the field mutations to
// apply atomically with it.
type JobUpdate struct {
	To             domain.JobStatus
	IncAttempt     bool
	SetLastError   bool
	LastError      string
	SetNextRetryAt bool
	NextRetryAt    time.Time
	ClearNextRetry bool
}

// UpdateJobStatus transitions from → upd.To and applies the field mutations in
// the same statement. Validated against the state machine and guarded in SQL
// (WHERE status = from) so a concurrent transition can't be lost. Returns
// ErrInvalidTransition (state machine forbids it), ErrConflict (current status
// isn't `from`), or ErrNotFound.
func (r *JobRepository) UpdateJobStatus(ctx context.Context, id uuid.UUID, from domain.JobStatus, upd JobUpdate) (domain.Job, error) {
	if err := domain.ValidateTransition(from, upd.To); err != nil {
		return domain.Job{}, err
	}

	inc := 0
	if upd.IncAttempt {
		inc = 1
	}

	row := r.pool.QueryRow(ctx,
		`UPDATE jobs SET
			status        = $2,
			updated_at    = now(),
			attempt_count = attempt_count + $3,
			last_error    = CASE WHEN $4 THEN $5 ELSE last_error END,
			next_retry_at = CASE WHEN $6 THEN $7
			                     WHEN $8 THEN NULL
			                     ELSE next_retry_at END
		 WHERE id = $1 AND status = $9
		 RETURNING `+jobColumns,
		id, upd.To, inc,
		upd.SetLastError, upd.LastError,
		upd.SetNextRetryAt, upd.NextRetryAt, upd.ClearNextRetry,
		from,
	)
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, r.classifyMissingUpdate(ctx, id)
	}
	return job, err
}

// classifyMissingUpdate distinguishes "row gone" from "status changed" after a
// guarded update matched nothing.
func (r *JobRepository) classifyMissingUpdate(ctx context.Context, id uuid.UUID) error {
	var exists bool
	if err := r.pool.QueryRow(ctx, `SELECT true FROM jobs WHERE id = $1`, id).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("classify update: %w", err)
	}
	return ErrConflict
}

// BeginAttempt is the worker's claim: in one tx it flips QUEUED → PROCESSING,
// bumps attempt_count, and inserts the matching RUNNING attempt row. The QUEUED
// guard means only one worker can claim a given delivery.
func (r *JobRepository) BeginAttempt(ctx context.Context, jobID uuid.UUID) (domain.Job, domain.Attempt, error) {
	ctx, span := otel.Tracer("portrait/repository").Start(ctx, "db.begin_attempt")
	defer span.End()

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, domain.Attempt{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := tx.QueryRow(ctx,
		`UPDATE jobs SET status = $2, attempt_count = attempt_count + 1, updated_at = now()
		 WHERE id = $1 AND status = $3
		 RETURNING `+jobColumns,
		jobID, domain.StatusProcessing, domain.StatusQueued,
	)
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, domain.Attempt{}, r.classifyMissingUpdate(ctx, jobID)
	}
	if err != nil {
		return domain.Job{}, domain.Attempt{}, err
	}

	attempt := domain.Attempt{
		ID:     uuid.New(),
		JobID:  jobID,
		Number: job.AttemptCount,
		Status: domain.AttemptRunning,
	}
	arow := tx.QueryRow(ctx,
		`INSERT INTO attempts (id, job_id, number, status)
		 VALUES ($1, $2, $3, $4)
		 RETURNING started_at`,
		attempt.ID, attempt.JobID, attempt.Number, attempt.Status,
	)
	if err := arow.Scan(&attempt.StartedAt); err != nil {
		return domain.Job{}, domain.Attempt{}, fmt.Errorf("insert attempt: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.Job{}, domain.Attempt{}, fmt.Errorf("commit tx: %w", err)
	}
	return job, attempt, nil
}

// FinishAttempt records the terminal outcome of an attempt.
func (r *JobRepository) FinishAttempt(ctx context.Context, attemptID uuid.UUID, status domain.AttemptStatus, errMsg string) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE attempts SET status = $2, error = $3, finished_at = now()
		 WHERE id = $1`,
		attemptID, status, errMsg,
	)
	if err != nil {
		return fmt.Errorf("finish attempt: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// AddProcessingStep records a completed processor execution within an attempt.
func (r *JobRepository) AddProcessingStep(ctx context.Context, s domain.ProcessingStep) error {
	if s.ID == uuid.Nil {
		s.ID = uuid.New()
	}
	var finished any
	if s.FinishedAt != nil {
		finished = *s.FinishedAt
	}
	_, err := r.pool.Exec(ctx,
		`INSERT INTO processing_steps
			(id, attempt_id, name, status, duration_ms, error, finished_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		s.ID, s.AttemptID, s.Name, s.Status, s.Duration.Milliseconds(), s.Error, finished,
	)
	if err != nil {
		return fmt.Errorf("insert processing step: %w", err)
	}
	return nil
}

// AddArtifact upserts an output artifact, so re-processing with deterministic
// keys overwrites rather than duplicates.
func (r *JobRepository) AddArtifact(ctx context.Context, a domain.Artifact) error {
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	_, err := r.pool.Exec(ctx,
		`INSERT INTO artifacts (id, job_id, kind, object_key, content_type, size_bytes)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (job_id, object_key)
		 DO UPDATE SET kind = EXCLUDED.kind,
		               content_type = EXCLUDED.content_type,
		               size_bytes = EXCLUDED.size_bytes`,
		a.ID, a.JobID, a.Kind, a.ObjectKey, a.ContentType, a.SizeBytes,
	)
	if err != nil {
		return fmt.Errorf("insert artifact: %w", err)
	}
	return nil
}

// ListArtifacts returns a job's artifacts ordered by creation time.
func (r *JobRepository) ListArtifacts(ctx context.Context, jobID uuid.UUID) ([]domain.Artifact, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, job_id, kind, object_key, content_type, size_bytes, created_at
		 FROM artifacts WHERE job_id = $1 ORDER BY created_at, object_key`,
		jobID,
	)
	if err != nil {
		return nil, fmt.Errorf("query artifacts: %w", err)
	}
	defer rows.Close()

	var out []domain.Artifact
	for rows.Next() {
		var a domain.Artifact
		if err := rows.Scan(&a.ID, &a.JobID, &a.Kind, &a.ObjectKey,
			&a.ContentType, &a.SizeBytes, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan artifact: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate artifacts: %w", err)
	}
	return out, nil
}

// ListRequeueable returns QUEUED jobs untouched since before cutoff — candidates
// whose publish may have failed or whose message was lost. Oldest first, capped
// at limit.
func (r *JobRepository) ListRequeueable(ctx context.Context, cutoff time.Time, limit int) ([]domain.Job, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+jobColumns+`
		 FROM jobs
		 WHERE status = $1 AND updated_at < $2
		 ORDER BY updated_at
		 LIMIT $3`,
		domain.StatusQueued, cutoff, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query requeueable: %w", err)
	}
	defer rows.Close()

	var out []domain.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan requeueable: %w", err)
		}
		out = append(out, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate requeueable: %w", err)
	}
	return out, nil
}

func scanJob(row pgx.Row) (domain.Job, error) {
	var j domain.Job
	err := row.Scan(
		&j.ID, &j.Status, &j.Pipeline, &j.PipelineVersion, &j.InputObjectKey,
		&j.CorrelationID, &j.AttemptCount, &j.MaxAttempts, &j.LastError,
		&j.NextRetryAt, &j.CreatedAt, &j.UpdatedAt,
	)
	if err != nil {
		return domain.Job{}, err
	}
	return j, nil
}
