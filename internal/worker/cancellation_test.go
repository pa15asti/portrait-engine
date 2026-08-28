package worker

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/pa15asti/portrait-engine/internal/domain"
	"github.com/pa15asti/portrait-engine/internal/pipeline"
)

// blockingProc blocks until its context is cancelled, then reports the
// cancellation. It records whether it was actually interrupted.
type blockingProc struct {
	interrupted *atomic.Bool
}

func (p blockingProc) Name() string { return "blocking" }
func (p blockingProc) Process(ctx context.Context, _ pipeline.ProcessingInput) (pipeline.ProcessingOutput, error) {
	<-ctx.Done()
	p.interrupted.Store(true)
	return pipeline.ProcessingOutput{}, ctx.Err()
}

func TestHandler_CancellationPropagatesIntoPipeline(t *testing.T) {
	job := queuedJob()
	proc := job
	proc.Status = domain.StatusProcessing
	proc.AttemptCount = 1

	// Initial GetJob (call 1) reports QUEUED; every later read — the watcher's
	// poll and the post-failure check — reports CANCELLED.
	store := &fakeJobStore{
		getFn: func(call int) (domain.Job, error) {
			if call == 1 {
				return job, nil
			}
			c := job
			c.Status = domain.StatusCancelled
			return c, nil
		},
		beginJob:     proc,
		beginAttempt: domain.Attempt{ID: uuid.New(), Number: 1},
	}
	blobs := &fakeBlobs{getFn: func() (io.ReadCloser, error) { return jpegReader(t), nil }}

	var interrupted atomic.Bool
	pipe := pipeline.New("portrait-enhance", "v1", blockingProc{interrupted: &interrupted})
	h := newHandler(store, blobs, fakeRegistry{p: pipe})
	h.cancelPollInterval = 5 * time.Millisecond // poll fast in the test

	done := make(chan Result, 1)
	go func() { done <- h.Handle(context.Background(), msgFor(job)) }()

	select {
	case res := <-done:
		if res.Decision != Complete {
			t.Fatalf("decision = %v, want Complete (cancelled)", res.Decision)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not observe cancellation in time")
	}

	if !interrupted.Load() {
		t.Error("processor was not interrupted by the cancellation")
	}
	if store.hasTransition(domain.StatusFailed, domain.StatusRetrying) {
		t.Error("a cancelled job must not be retried")
	}
}
