// Package domain holds the job model, its state machine, and the error
// taxonomy that drives retries. It depends on nothing else in the tree.
package domain

// JobStatus is a job's lifecycle state. Postgres is authoritative; transitions
// are checked by CanTransition and applied inside DB transactions.
type JobStatus string

const (
	StatusPending    JobStatus = "PENDING"
	StatusQueued     JobStatus = "QUEUED"
	StatusProcessing JobStatus = "PROCESSING"
	StatusCompleted  JobStatus = "COMPLETED"
	StatusFailed     JobStatus = "FAILED"
	StatusCancelled  JobStatus = "CANCELLED"
	StatusRetrying   JobStatus = "RETRYING"
)

func (s JobStatus) Valid() bool {
	switch s {
	case StatusPending, StatusQueued, StatusProcessing,
		StatusCompleted, StatusFailed, StatusCancelled, StatusRetrying:
		return true
	default:
		return false
	}
}

// IsTerminal reports an end state. FAILED counts as terminal: on its own it's an
// outcome, and it only continues if the retry logic moves it to RETRYING.
func (s JobStatus) IsTerminal() bool {
	switch s {
	case StatusCompleted, StatusCancelled, StatusFailed:
		return true
	default:
		return false
	}
}

// AttemptStatus is the outcome of one processing attempt.
type AttemptStatus string

const (
	AttemptRunning   AttemptStatus = "RUNNING"
	AttemptSucceeded AttemptStatus = "SUCCEEDED"
	AttemptFailed    AttemptStatus = "FAILED"
	AttemptCancelled AttemptStatus = "CANCELLED"
)

// StepStatus is the outcome of one processor within the pipeline.
type StepStatus string

const (
	StepRunning   StepStatus = "RUNNING"
	StepSucceeded StepStatus = "SUCCEEDED"
	StepFailed    StepStatus = "FAILED"
	StepSkipped   StepStatus = "SKIPPED"
)
