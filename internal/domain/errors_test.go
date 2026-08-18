package domain

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

var errBase = errors.New("boom")

func TestClassify_ExplicitKinds(t *testing.T) {
	if got := Classify(Permanent("decode", errBase)); got != KindPermanent {
		t.Errorf("expected permanent, got %s", got)
	}
	if got := Classify(Transient("upload", errBase)); got != KindTransient {
		t.Errorf("expected transient, got %s", got)
	}
}

func TestClassify_UnclassifiedDefaultsTransient(t *testing.T) {
	if got := Classify(errBase); got != KindTransient {
		t.Errorf("expected unclassified error to default to transient, got %s", got)
	}
}

func TestClassify_ThroughWrapping(t *testing.T) {
	// A permanent error wrapped by fmt.Errorf must still be detected via
	// errors.As inside Classify.
	wrapped := fmt.Errorf("pipeline failed: %w", Permanent("decode", errBase))
	if !IsPermanent(wrapped) {
		t.Errorf("expected wrapped permanent error to remain permanent")
	}
	if IsTransient(wrapped) {
		t.Errorf("did not expect wrapped permanent error to be transient")
	}
}

func TestIsPermanent_IsTransient_NilSafe(t *testing.T) {
	if IsPermanent(nil) {
		t.Error("nil must not be permanent")
	}
	if IsTransient(nil) {
		t.Error("nil must not be transient")
	}
}

func TestPermanentTransient_NilPassthrough(t *testing.T) {
	if Permanent("op", nil) != nil {
		t.Error("Permanent(nil) must be nil")
	}
	if Transient("op", nil) != nil {
		t.Error("Transient(nil) must be nil")
	}
}

func TestProcessingError_UnwrapAndMessage(t *testing.T) {
	err := Permanent("decode", errBase)
	if !errors.Is(err, errBase) {
		t.Error("expected Unwrap to expose the base error")
	}
	var pe *ProcessingError
	if !errors.As(err, &pe) {
		t.Fatal("expected errors.As to extract *ProcessingError")
	}
	if pe.Op != "decode" || pe.Kind != KindPermanent {
		t.Errorf("unexpected fields: %+v", pe)
	}
	want := "permanent: decode: boom"
	if pe.Error() != want {
		t.Errorf("message = %q, want %q", pe.Error(), want)
	}
}

func TestIsCancellation(t *testing.T) {
	if !IsCancellation(context.Canceled) {
		t.Error("context.Canceled must be cancellation")
	}
	if !IsCancellation(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded must be cancellation")
	}
	if !IsCancellation(fmt.Errorf("wrapped: %w", context.Canceled)) {
		t.Error("wrapped cancellation must be detected")
	}
	if IsCancellation(errBase) {
		t.Error("plain error is not cancellation")
	}
}

// A permanent error caused by cancellation should still be classifiable, but
// the worker checks IsCancellation first — verify the two predicates are
// independent and both observable.
func TestCancellation_IndependentFromClassification(t *testing.T) {
	err := Transient("upload", context.DeadlineExceeded)
	if !IsCancellation(err) {
		t.Error("expected cancellation to be detectable through the wrapper")
	}
	if Classify(err) != KindTransient {
		t.Error("expected declared kind to remain transient")
	}
}
