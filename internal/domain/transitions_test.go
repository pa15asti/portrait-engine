package domain

import (
	"errors"
	"testing"
)

func TestCanTransition_AllowedPaths(t *testing.T) {
	allowed := []struct{ from, to JobStatus }{
		{StatusPending, StatusQueued},
		{StatusPending, StatusCancelled},
		{StatusQueued, StatusProcessing},
		{StatusQueued, StatusCancelled},
		{StatusProcessing, StatusCompleted},
		{StatusProcessing, StatusFailed},
		{StatusProcessing, StatusCancelled},
		{StatusFailed, StatusRetrying},
		{StatusRetrying, StatusQueued},
		{StatusRetrying, StatusCancelled},
	}
	for _, tc := range allowed {
		if !CanTransition(tc.from, tc.to) {
			t.Errorf("expected %s -> %s to be allowed", tc.from, tc.to)
		}
	}
}

func TestCanTransition_RejectedPaths(t *testing.T) {
	rejected := []struct{ from, to JobStatus }{
		// Skipping states.
		{StatusPending, StatusProcessing},
		{StatusPending, StatusCompleted},
		{StatusQueued, StatusCompleted},
		// From terminal states.
		{StatusCompleted, StatusQueued},
		{StatusCompleted, StatusProcessing},
		{StatusCancelled, StatusQueued},
		// FAILED may only retry, not complete or re-process directly.
		{StatusFailed, StatusCompleted},
		{StatusFailed, StatusProcessing},
		{StatusFailed, StatusCancelled},
		// No-op self transitions.
		{StatusProcessing, StatusProcessing},
		{StatusQueued, StatusQueued},
		// Backwards.
		{StatusProcessing, StatusQueued},
		{StatusProcessing, StatusPending},
		// Unknown states.
		{JobStatus("BOGUS"), StatusQueued},
		{StatusQueued, JobStatus("BOGUS")},
	}
	for _, tc := range rejected {
		if CanTransition(tc.from, tc.to) {
			t.Errorf("expected %s -> %s to be rejected", tc.from, tc.to)
		}
	}
}

func TestValidateTransition_WrapsSentinel(t *testing.T) {
	err := ValidateTransition(StatusCompleted, StatusQueued)
	if err == nil {
		t.Fatal("expected error for invalid transition")
	}
	if !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected error to wrap ErrInvalidTransition, got %v", err)
	}
}

func TestValidateTransition_AllowsValid(t *testing.T) {
	if err := ValidateTransition(StatusPending, StatusQueued); err != nil {
		t.Errorf("expected valid transition, got %v", err)
	}
}

func TestValidateTransition_UnknownStates(t *testing.T) {
	if err := ValidateTransition(JobStatus("X"), StatusQueued); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected invalid-transition error for unknown source, got %v", err)
	}
	if err := ValidateTransition(StatusPending, JobStatus("Y")); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("expected invalid-transition error for unknown target, got %v", err)
	}
}

func TestJobStatus_IsTerminal(t *testing.T) {
	terminal := []JobStatus{StatusCompleted, StatusCancelled, StatusFailed}
	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("expected %s to be terminal", s)
		}
	}
	nonTerminal := []JobStatus{StatusPending, StatusQueued, StatusProcessing, StatusRetrying}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("expected %s to be non-terminal", s)
		}
	}
}

func TestJobStatus_Valid(t *testing.T) {
	if JobStatus("NOPE").Valid() {
		t.Error("expected unknown status to be invalid")
	}
	for _, s := range []JobStatus{
		StatusPending, StatusQueued, StatusProcessing,
		StatusCompleted, StatusFailed, StatusCancelled, StatusRetrying,
	} {
		if !s.Valid() {
			t.Errorf("expected %s to be valid", s)
		}
	}
}

func TestJob_CanRetry(t *testing.T) {
	if !(Job{AttemptCount: 1, MaxAttempts: 3}).CanRetry() {
		t.Error("expected retry available when attempts remain")
	}
	if (Job{AttemptCount: 3, MaxAttempts: 3}).CanRetry() {
		t.Error("expected no retry when cap reached")
	}
	if (Job{AttemptCount: 4, MaxAttempts: 3}).CanRetry() {
		t.Error("expected no retry when over cap")
	}
}
