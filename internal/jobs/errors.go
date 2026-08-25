// Package jobs is the application service over the repository: uploads, job
// creation, enqueuing, cancellation, queries. HTTP handlers are thin adapters
// over it; no transport concerns live here.
package jobs

import "errors"

var (
	ErrValidation     = errors.New("validation failed") // surface as 400
	ErrNotCancellable = errors.New("job is not cancellable")
)
