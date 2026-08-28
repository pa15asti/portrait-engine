package worker

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"io"
	"testing"

	"github.com/google/uuid"

	"github.com/pa15asti/portrait-engine/internal/domain"
	imgproc "github.com/pa15asti/portrait-engine/internal/image"
	"github.com/pa15asti/portrait-engine/internal/messaging"
	"github.com/pa15asti/portrait-engine/internal/pipeline"
	"github.com/pa15asti/portrait-engine/internal/repository"
	"github.com/pa15asti/portrait-engine/internal/storage"
)

type transition struct{ from, to domain.JobStatus }

type fakeJobStore struct {
	getFn func(call int) (domain.Job, error)
	getN  int

	beginJob     domain.Job
	beginAttempt domain.Attempt
	beginErr     error

	finishStatuses []domain.AttemptStatus
	transitions    []transition
	artifacts      []domain.Artifact
	steps          []domain.ProcessingStep
}

func (s *fakeJobStore) GetJob(_ context.Context, _ uuid.UUID) (domain.Job, error) {
	s.getN++
	return s.getFn(s.getN)
}
func (s *fakeJobStore) BeginAttempt(_ context.Context, _ uuid.UUID) (domain.Job, domain.Attempt, error) {
	if s.beginErr != nil {
		return domain.Job{}, domain.Attempt{}, s.beginErr
	}
	return s.beginJob, s.beginAttempt, nil
}
func (s *fakeJobStore) FinishAttempt(_ context.Context, _ uuid.UUID, status domain.AttemptStatus, _ string) error {
	s.finishStatuses = append(s.finishStatuses, status)
	return nil
}
func (s *fakeJobStore) UpdateJobStatus(_ context.Context, _ uuid.UUID, from domain.JobStatus, upd repository.JobUpdate) (domain.Job, error) {
	s.transitions = append(s.transitions, transition{from, upd.To})
	return domain.Job{Status: upd.To}, nil
}
func (s *fakeJobStore) AddProcessingStep(_ context.Context, st domain.ProcessingStep) error {
	s.steps = append(s.steps, st)
	return nil
}
func (s *fakeJobStore) AddArtifact(_ context.Context, a domain.Artifact) error {
	s.artifacts = append(s.artifacts, a)
	return nil
}

func (s *fakeJobStore) hasTransition(from, to domain.JobStatus) bool {
	for _, tr := range s.transitions {
		if tr.from == from && tr.to == to {
			return true
		}
	}
	return false
}

type fakeBlobs struct {
	getFn func() (io.ReadCloser, error)
	putFn func() error
}

func (b *fakeBlobs) Get(context.Context, string) (io.ReadCloser, error) { return b.getFn() }
func (b *fakeBlobs) Put(context.Context, string, io.Reader, storage.PutOptions) error {
	if b.putFn != nil {
		return b.putFn()
	}
	return nil
}

type fakeRegistry struct {
	p   *pipeline.Pipeline
	err error
}

func (r fakeRegistry) Get(string, string) (*pipeline.Pipeline, error) { return r.p, r.err }

type identityProc struct{}

func (identityProc) Name() string { return "identity" }
func (identityProc) Process(_ context.Context, in pipeline.ProcessingInput) (pipeline.ProcessingOutput, error) {
	return pipeline.ProcessingOutput{Image: in.Image}, nil
}

func jpegReader(t *testing.T) io.ReadCloser {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 16, 16))
	for y := 0; y < 16; y++ {
		for x := 0; x < 16; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x * 8), G: uint8(y * 8), B: 100, A: 255})
		}
	}
	var buf bytes.Buffer
	if err := imgproc.Encode(&buf, img, imgproc.JPEG, 90); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return io.NopCloser(&buf)
}

func queuedJob() domain.Job {
	return domain.Job{
		ID:              uuid.New(),
		Status:          domain.StatusQueued,
		Pipeline:        "portrait-enhance",
		PipelineVersion: "v1",
		InputObjectKey:  "uploads/2026/08/x.jpg",
		MaxAttempts:     5,
	}
}

func newHandler(store JobStore, blobs BlobStore, reg PipelineRegistry) *JobHandler {
	return NewJobHandler(store, blobs, reg, nil, discardLogger())
}

func passthroughPipeline() *pipeline.Pipeline {
	return pipeline.New("portrait-enhance", "v1", identityProc{})
}

func msgFor(j domain.Job) messaging.JobMessage {
	return messaging.JobMessage{JobID: j.ID.String(), Pipeline: j.Pipeline, PipelineVersion: j.PipelineVersion}
}

func TestHandler_HappyPath(t *testing.T) {
	job := queuedJob()
	proc := job
	proc.Status = domain.StatusProcessing
	proc.AttemptCount = 1

	store := &fakeJobStore{
		getFn:        func(int) (domain.Job, error) { return job, nil },
		beginJob:     proc,
		beginAttempt: domain.Attempt{ID: uuid.New(), JobID: job.ID, Number: 1, Status: domain.AttemptRunning},
	}
	blobs := &fakeBlobs{getFn: func() (io.ReadCloser, error) { return jpegReader(t), nil }}
	h := newHandler(store, blobs, fakeRegistry{p: passthroughPipeline()})

	res := h.Handle(context.Background(), msgFor(job))
	if res.Decision != Complete {
		t.Fatalf("decision = %v, want Complete", res.Decision)
	}
	if !store.hasTransition(domain.StatusProcessing, domain.StatusCompleted) {
		t.Errorf("expected PROCESSING->COMPLETED, got %v", store.transitions)
	}
	if len(store.artifacts) != 1 || store.artifacts[0].Kind != "output" {
		t.Errorf("expected one output artifact, got %+v", store.artifacts)
	}
	if len(store.finishStatuses) != 1 || store.finishStatuses[0] != domain.AttemptSucceeded {
		t.Errorf("expected attempt SUCCEEDED, got %v", store.finishStatuses)
	}
	if len(store.steps) == 0 {
		t.Error("expected processing steps to be recorded")
	}
}

func TestHandler_DuplicateTerminalIsAcked(t *testing.T) {
	job := queuedJob()
	job.Status = domain.StatusCompleted
	store := &fakeJobStore{
		getFn:    func(int) (domain.Job, error) { return job, nil },
		beginErr: errors.New("BeginAttempt must not be called"),
	}
	h := newHandler(store, &fakeBlobs{}, fakeRegistry{p: passthroughPipeline()})

	if res := h.Handle(context.Background(), msgFor(job)); res.Decision != Complete {
		t.Errorf("decision = %v, want Complete", res.Decision)
	}
	if len(store.transitions) != 0 {
		t.Errorf("terminal job must not transition, got %v", store.transitions)
	}
}

func TestHandler_NotClaimableIsAcked(t *testing.T) {
	job := queuedJob()
	store := &fakeJobStore{
		getFn:    func(int) (domain.Job, error) { return job, nil },
		beginErr: repository.ErrConflict,
	}
	h := newHandler(store, &fakeBlobs{}, fakeRegistry{p: passthroughPipeline()})

	if res := h.Handle(context.Background(), msgFor(job)); res.Decision != Complete {
		t.Errorf("decision = %v, want Complete (duplicate)", res.Decision)
	}
}

func TestHandler_JobNotFoundIsDiscarded(t *testing.T) {
	job := queuedJob()
	store := &fakeJobStore{getFn: func(int) (domain.Job, error) { return domain.Job{}, repository.ErrNotFound }}
	h := newHandler(store, &fakeBlobs{}, fakeRegistry{p: passthroughPipeline()})

	if res := h.Handle(context.Background(), msgFor(job)); res.Decision != Discard {
		t.Errorf("decision = %v, want Discard", res.Decision)
	}
}

func TestHandler_PermanentErrorFailsWithoutRetry(t *testing.T) {
	job := queuedJob()
	proc := job
	proc.Status = domain.StatusProcessing
	proc.AttemptCount = 1
	store := &fakeJobStore{
		getFn:        func(int) (domain.Job, error) { return job, nil },
		beginJob:     proc,
		beginAttempt: domain.Attempt{ID: uuid.New(), Number: 1},
	}
	// Invalid image bytes -> decode fails -> permanent error.
	blobs := &fakeBlobs{getFn: func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader([]byte("not an image"))), nil
	}}
	h := newHandler(store, blobs, fakeRegistry{p: passthroughPipeline()})

	res := h.Handle(context.Background(), msgFor(job))
	if res.Decision != Complete {
		t.Fatalf("decision = %v, want Complete (permanent failure acked)", res.Decision)
	}
	if !store.hasTransition(domain.StatusProcessing, domain.StatusFailed) {
		t.Errorf("expected PROCESSING->FAILED, got %v", store.transitions)
	}
	if store.hasTransition(domain.StatusFailed, domain.StatusRetrying) {
		t.Error("permanent error must not schedule a retry")
	}
}

func TestHandler_TransientErrorSchedulesRetry(t *testing.T) {
	job := queuedJob()
	proc := job
	proc.Status = domain.StatusProcessing
	proc.AttemptCount = 1 // 1 < MaxAttempts(5): retry allowed
	store := &fakeJobStore{
		getFn:        func(int) (domain.Job, error) { return job, nil }, // never CANCELLED
		beginJob:     proc,
		beginAttempt: domain.Attempt{ID: uuid.New(), Number: 1},
	}
	blobs := &fakeBlobs{
		getFn: func() (io.ReadCloser, error) { return jpegReader(t), nil },
		putFn: func() error { return errors.New("storage temporarily unavailable") },
	}
	h := newHandler(store, blobs, fakeRegistry{p: passthroughPipeline()})

	res := h.Handle(context.Background(), msgFor(job))
	if res.Decision != Retry {
		t.Fatalf("decision = %v, want Retry", res.Decision)
	}
	if res.RetryAfter <= 0 {
		t.Errorf("expected a positive backoff, got %v", res.RetryAfter)
	}
	// The retry path walks PROCESSING->FAILED->RETRYING->QUEUED.
	for _, want := range []transition{
		{domain.StatusProcessing, domain.StatusFailed},
		{domain.StatusFailed, domain.StatusRetrying},
		{domain.StatusRetrying, domain.StatusQueued},
	} {
		if !store.hasTransition(want.from, want.to) {
			t.Errorf("missing transition %s->%s; got %v", want.from, want.to, store.transitions)
		}
	}
}

func TestHandler_RetryExhaustedFails(t *testing.T) {
	job := queuedJob()
	proc := job
	proc.Status = domain.StatusProcessing
	proc.AttemptCount = 5 // == MaxAttempts: no retries left
	store := &fakeJobStore{
		getFn:        func(int) (domain.Job, error) { return job, nil },
		beginJob:     proc,
		beginAttempt: domain.Attempt{ID: uuid.New(), Number: 5},
	}
	blobs := &fakeBlobs{
		getFn: func() (io.ReadCloser, error) { return jpegReader(t), nil },
		putFn: func() error { return errors.New("still failing") },
	}
	h := newHandler(store, blobs, fakeRegistry{p: passthroughPipeline()})

	res := h.Handle(context.Background(), msgFor(job))
	if res.Decision != Complete {
		t.Fatalf("decision = %v, want Complete (exhausted -> failed & acked)", res.Decision)
	}
	if !store.hasTransition(domain.StatusProcessing, domain.StatusFailed) {
		t.Errorf("expected PROCESSING->FAILED, got %v", store.transitions)
	}
	if store.hasTransition(domain.StatusFailed, domain.StatusRetrying) {
		t.Error("exhausted job must not schedule another retry")
	}
}

func TestHandler_HonorsCancellationDuringProcessing(t *testing.T) {
	job := queuedJob()
	proc := job
	proc.Status = domain.StatusProcessing
	proc.AttemptCount = 1
	// First GetJob (initial) returns QUEUED; the post-failure check returns
	// CANCELLED, simulating an API cancel that landed while processing.
	store := &fakeJobStore{
		getFn: func(call int) (domain.Job, error) {
			if call == 1 {
				return job, nil
			}
			cancelled := job
			cancelled.Status = domain.StatusCancelled
			return cancelled, nil
		},
		beginJob:     proc,
		beginAttempt: domain.Attempt{ID: uuid.New(), Number: 1},
	}
	blobs := &fakeBlobs{
		getFn: func() (io.ReadCloser, error) { return jpegReader(t), nil },
		putFn: func() error { return errors.New("transient") },
	}
	h := newHandler(store, blobs, fakeRegistry{p: passthroughPipeline()})

	res := h.Handle(context.Background(), msgFor(job))
	if res.Decision != Complete {
		t.Fatalf("decision = %v, want Complete (cancelled)", res.Decision)
	}
	if store.hasTransition(domain.StatusFailed, domain.StatusRetrying) {
		t.Error("a cancelled job must not be retried")
	}
	if len(store.finishStatuses) == 0 || store.finishStatuses[0] != domain.AttemptFailed {
		t.Errorf("expected attempt recorded failed, got %v", store.finishStatuses)
	}
}
