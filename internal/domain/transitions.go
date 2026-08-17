package domain

import (
	"errors"
	"fmt"
)

var ErrInvalidTransition = errors.New("invalid job state transition")

// The state machine:
//
//	PENDING    → QUEUED, CANCELLED
//	QUEUED     → PROCESSING, CANCELLED
//	PROCESSING → COMPLETED, FAILED, CANCELLED
//	FAILED     → RETRYING            (only while retries remain)
//	RETRYING   → QUEUED, CANCELLED
var allowedTransitions = map[JobStatus]map[JobStatus]struct{}{
	StatusPending: {
		StatusQueued:    {},
		StatusCancelled: {},
	},
	StatusQueued: {
		StatusProcessing: {},
		StatusCancelled:  {},
	},
	StatusProcessing: {
		StatusCompleted: {},
		StatusFailed:    {},
		StatusCancelled: {},
	},
	StatusFailed: {
		StatusRetrying: {},
	},
	StatusRetrying: {
		StatusQueued:    {},
		StatusCancelled: {},
	},
	StatusCompleted: {},
	StatusCancelled: {},
}

// CanTransition reports whether from → to is allowed. Unknown states and no-op
// self-transitions are not.
func CanTransition(from, to JobStatus) bool {
	targets, ok := allowedTransitions[from]
	if !ok {
		return false
	}
	_, ok = targets[to]
	return ok
}

// ValidateTransition is CanTransition with an error (wrapping
// ErrInvalidTransition) instead of a bool.
func ValidateTransition(from, to JobStatus) error {
	if !from.Valid() {
		return fmt.Errorf("%w: unknown source state %q", ErrInvalidTransition, from)
	}
	if !to.Valid() {
		return fmt.Errorf("%w: unknown target state %q", ErrInvalidTransition, to)
	}
	if !CanTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}
