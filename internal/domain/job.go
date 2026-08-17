package domain

import (
	"time"

	"github.com/google/uuid"
)

// Job is the aggregate root. Postgres is the authoritative store; this is the
// in-memory view.
type Job struct {
	ID uuid.UUID

	Status          JobStatus
	Pipeline        string
	PipelineVersion string

	InputObjectKey string
	CorrelationID  string

	AttemptCount int
	MaxAttempts  int

	LastError   string
	NextRetryAt *time.Time // set while RETRYING

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (j Job) CanRetry() bool { return j.AttemptCount < j.MaxAttempts }

// Attempt is one processing attempt against a job.
type Attempt struct {
	ID     uuid.UUID
	JobID  uuid.UUID
	Number int
	Status AttemptStatus
	Error  string

	StartedAt  time.Time
	FinishedAt *time.Time
}

// ProcessingStep is one processor's execution within an attempt — what makes a
// run auditable.
type ProcessingStep struct {
	ID        uuid.UUID
	AttemptID uuid.UUID
	Name      string
	Status    StepStatus
	Duration  time.Duration
	Error     string

	StartedAt  time.Time
	FinishedAt *time.Time
}

// Artifact is an output object produced by a job.
type Artifact struct {
	ID          uuid.UUID
	JobID       uuid.UUID
	Kind        string // "output", "thumbnail", ...
	ObjectKey   string
	ContentType string
	SizeBytes   int64

	CreatedAt time.Time
}
