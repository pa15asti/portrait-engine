package domain

import (
	"context"
	"errors"
	"fmt"
)

// ErrorKind classifies a processing error for retry purposes.
type ErrorKind int

const (
	KindTransient ErrorKind = iota // temporary: safe to retry with backoff
	KindPermanent                  // won't succeed on retry (bad image, ...)
)

func (k ErrorKind) String() string {
	if k == KindPermanent {
		return "permanent"
	}
	return "transient"
}

// ProcessingError wraps an error with a retry classification and the operation
// it happened in.
type ProcessingError struct {
	Kind ErrorKind
	Op   string
	Err  error
}

func (e *ProcessingError) Error() string {
	if e.Op == "" {
		return fmt.Sprintf("%s: %v", e.Kind, e.Err)
	}
	return fmt.Sprintf("%s: %s: %v", e.Kind, e.Op, e.Err)
}

func (e *ProcessingError) Unwrap() error { return e.Err }

// Permanent / Transient tag err with a kind. A nil err stays nil.
func Permanent(op string, err error) error {
	if err == nil {
		return nil
	}
	return &ProcessingError{Kind: KindPermanent, Op: op, Err: err}
}

func Transient(op string, err error) error {
	if err == nil {
		return nil
	}
	return &ProcessingError{Kind: KindTransient, Op: op, Err: err}
}

// Classify returns the retry kind of err. Anything not explicitly tagged is
// treated as transient, so an unclassified failure retries up to the cap rather
// than dying on the first blip. Cancellation is handled separately (see
// IsCancellation) and must not be classified as a processing failure.
func Classify(err error) ErrorKind {
	var pe *ProcessingError
	if errors.As(err, &pe) {
		return pe.Kind
	}
	return KindTransient
}

func IsPermanent(err error) bool { return err != nil && Classify(err) == KindPermanent }
func IsTransient(err error) bool { return err != nil && Classify(err) == KindTransient }

func IsCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
