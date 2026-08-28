package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/pa15asti/portrait-engine/internal/domain"
	imgproc "github.com/pa15asti/portrait-engine/internal/image"
	"github.com/pa15asti/portrait-engine/internal/messaging"
	"github.com/pa15asti/portrait-engine/internal/observability"
	"github.com/pa15asti/portrait-engine/internal/pipeline"
	"github.com/pa15asti/portrait-engine/internal/repository"
	"github.com/pa15asti/portrait-engine/internal/storage"
)

// finalizeTimeout bounds the detached context used to persist terminal state —
// detached so an outcome is recorded even after the job ctx was cancelled.
const finalizeTimeout = 10 * time.Second

type JobStore interface {
	GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error)
	BeginAttempt(ctx context.Context, id uuid.UUID) (domain.Job, domain.Attempt, error)
	FinishAttempt(ctx context.Context, attemptID uuid.UUID, status domain.AttemptStatus, errMsg string) error
	UpdateJobStatus(ctx context.Context, id uuid.UUID, from domain.JobStatus, upd repository.JobUpdate) (domain.Job, error)
	AddProcessingStep(ctx context.Context, s domain.ProcessingStep) error
	AddArtifact(ctx context.Context, a domain.Artifact) error
}

type BlobStore interface {
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Put(ctx context.Context, key string, r io.Reader, opts storage.PutOptions) error
}

type PipelineRegistry interface {
	Get(name, version string) (*pipeline.Pipeline, error)
}

// JobHandler drives one job message: claim it, run the resolved pipeline,
// persist artifacts and steps, and decide ack / retry / discard. It's the
// bridge between the pool's transport concerns and the domain.
type JobHandler struct {
	store    JobStore
	blobs    BlobStore
	registry PipelineRegistry
	log      *slog.Logger
	metrics  *observability.Metrics
	// how often to poll for an API cancellation while processing; tests lower it
	cancelPollInterval time.Duration
}

// NewJobHandler builds a handler. metrics may be nil.
func NewJobHandler(store JobStore, blobs BlobStore, registry PipelineRegistry, metrics *observability.Metrics, log *slog.Logger) *JobHandler {
	if log == nil {
		log = slog.Default()
	}
	return &JobHandler{
		store:              store,
		blobs:              blobs,
		registry:           registry,
		metrics:            metrics,
		log:                log,
		cancelPollInterval: 3 * time.Second,
	}
}

// Handle implements worker.Handler.
func (h *JobHandler) Handle(ctx context.Context, msg messaging.JobMessage) Result {
	ctx, span := observability.Tracer("portrait/worker").Start(ctx, "job.process")
	defer span.End()
	span.SetAttributes(
		attribute.String("job.id", msg.JobID),
		attribute.String("pipeline", msg.Pipeline),
		attribute.String("pipeline.version", msg.PipelineVersion),
	)

	log := observability.LoggerFrom(ctx)

	id, err := uuid.Parse(msg.JobID)
	if err != nil {
		log.Error("invalid job id in message", "error", err)
		return Result{Decision: Discard} // unparseable id will never succeed
	}

	job, err := h.store.GetJob(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			log.Warn("job not found, discarding message")
			return Result{Decision: Discard}
		}
		return Result{Decision: Retry} // transient read failure
	}
	if job.Status.IsTerminal() {
		// Duplicate delivery of an already-finished job: safe no-op.
		log.Info("job already terminal, acking duplicate", "status", string(job.Status))
		return Result{Decision: Complete}
	}

	// Only the winner of the QUEUED->PROCESSING guard proceeds; a conflict means
	// another delivery owns it.
	job, attempt, err := h.store.BeginAttempt(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrConflict) {
			log.Info("job not claimable, acking duplicate")
			return Result{Decision: Complete}
		}
		return Result{Decision: Retry}
	}

	h.metrics.ObserveQueueLatency(time.Since(job.CreatedAt))

	// Cancellable ctx + a watcher so an API cancel reaches the running pipeline.
	procCtx, procCancel := context.WithCancel(ctx)
	defer procCancel()
	stopWatch := h.watchCancellation(procCtx, id, procCancel)

	procStart := time.Now()
	steps, procErr := h.runPipeline(procCtx, job, attempt.ID)
	h.metrics.ObserveProcessing(time.Since(procStart))
	stopWatch()
	h.persistSteps(attempt.ID, steps)

	if procErr == nil {
		return h.finalizeSuccess(job, attempt)
	}
	return h.finalizeFailure(job, attempt, procErr)
}

// runPipeline loads the input, runs the pipeline, and writes the output.
// Errors are tagged permanent/transient for retry.
func (h *JobHandler) runPipeline(ctx context.Context, job domain.Job, attemptID uuid.UUID) ([]pipeline.StepResult, error) {
	pipe, err := h.registry.Get(job.Pipeline, job.PipelineVersion)
	if err != nil {
		return nil, domain.Permanent("resolve-pipeline", err) // config problem, not transient
	}

	rc, err := h.blobs.Get(ctx, job.InputObjectKey)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, domain.Permanent("load-input", err)
		}
		h.metrics.IncStorageError()
		return nil, domain.Transient("load-input", err)
	}
	defer rc.Close()

	srcImg, err := imgproc.Decode(rc)
	if err != nil {
		return nil, domain.Permanent("decode", err)
	}

	var steps []pipeline.StepResult
	out, err := pipe.Run(ctx, pipeline.ProcessingInput{Image: srcImg}, func(s pipeline.StepResult) {
		steps = append(steps, s)
	})
	if err != nil {
		if domain.IsCancellation(err) {
			return steps, err // propagate cancellation as-is
		}
		return steps, domain.Transient("pipeline", err)
	}

	var buf bytes.Buffer
	if err := imgproc.Encode(&buf, out.Image, imgproc.JPEG, 90); err != nil {
		return steps, domain.Transient("encode", err)
	}

	outKey := storage.GenerateOutputKey(job.ID, "result.jpg")
	size := int64(buf.Len())
	if err := h.blobs.Put(ctx, outKey, &buf, storage.PutOptions{ContentType: "image/jpeg", Size: size}); err != nil {
		h.metrics.IncStorageError()
		return steps, domain.Transient("store-output", err)
	}

	// Detached ctx so a late cancel doesn't lose an output we already produced.
	fctx, cancel := context.WithTimeout(context.Background(), finalizeTimeout)
	defer cancel()
	if err := h.store.AddArtifact(fctx, domain.Artifact{
		JobID:       job.ID,
		Kind:        "output",
		ObjectKey:   outKey,
		ContentType: "image/jpeg",
		SizeBytes:   size,
	}); err != nil {
		return steps, domain.Transient("record-artifact", err)
	}
	return steps, nil
}

func (h *JobHandler) finalizeSuccess(job domain.Job, attempt domain.Attempt) Result {
	log := h.log.With(slog.String("job_id", job.ID.String()))
	fctx, cancel := context.WithTimeout(context.Background(), finalizeTimeout)
	defer cancel()

	if err := h.store.FinishAttempt(fctx, attempt.ID, domain.AttemptSucceeded, ""); err != nil {
		log.Error("finish attempt (success)", "error", err)
		return Result{Decision: Retry}
	}
	if _, err := h.store.UpdateJobStatus(fctx, job.ID, domain.StatusProcessing, repository.JobUpdate{To: domain.StatusCompleted}); err != nil {
		if errors.Is(err, repository.ErrConflict) {
			// The job was cancelled while we processed; accept that outcome.
			log.Info("job changed during processing, not marking completed")
			return Result{Decision: Complete}
		}
		log.Error("mark completed", "error", err)
		return Result{Decision: Retry}
	}
	h.metrics.IncJobsCompleted()
	log.Info("job completed")
	return Result{Decision: Complete}
}

func (h *JobHandler) finalizeFailure(job domain.Job, attempt domain.Attempt, procErr error) Result {
	log := h.log.With(slog.String("job_id", job.ID.String()))
	fctx, cancel := context.WithTimeout(context.Background(), finalizeTimeout)
	defer cancel()

	h.metrics.IncProcessorError(domain.Classify(procErr).String())

	cancelled := domain.IsCancellation(procErr)
	attemptStatus := domain.AttemptFailed
	if cancelled {
		attemptStatus = domain.AttemptCancelled
	}
	if err := h.store.FinishAttempt(fctx, attempt.ID, attemptStatus, procErr.Error()); err != nil {
		log.Error("finish attempt (failure)", "error", err)
	}

	// Honor an API cancel that landed while we processed.
	if cur, err := h.store.GetJob(fctx, job.ID); err == nil && cur.Status == domain.StatusCancelled {
		log.Info("job was cancelled during processing")
		return Result{Decision: Complete}
	}

	// Permanent errors fail immediately; retrying cannot help.
	if domain.IsPermanent(procErr) {
		h.markFailed(fctx, log, job.ID, procErr)
		h.metrics.IncJobsFailed()
		return Result{Decision: Complete}
	}

	// Transient / cancellation: retry while attempts remain. DB attempt count is
	// the authority (MaxDeliver is set higher, so it never trips first).
	if job.AttemptCount < job.MaxAttempts {
		delay := backoff(job.AttemptCount)
		if err := h.scheduleRetry(fctx, job.ID, procErr, delay); err != nil {
			log.Error("schedule retry", "error", err)
			return Result{Decision: Retry, RetryAfter: delay}
		}
		h.metrics.IncJobsRetried()
		log.Warn("job scheduled for retry", "attempt", job.AttemptCount, "retry_after", delay.String())
		return Result{Decision: Retry, RetryAfter: delay}
	}

	log.Warn("job failed permanently after exhausting retries")
	h.markFailed(fctx, log, job.ID, procErr)
	h.metrics.IncJobsFailed()
	return Result{Decision: Complete}
}

// scheduleRetry walks PROCESSING -> FAILED -> RETRYING -> QUEUED so the
// redelivered message can re-claim, recording the backoff deadline.
func (h *JobHandler) scheduleRetry(ctx context.Context, id uuid.UUID, cause error, delay time.Duration) error {
	if _, err := h.store.UpdateJobStatus(ctx, id, domain.StatusProcessing, repository.JobUpdate{
		To:           domain.StatusFailed,
		SetLastError: true,
		LastError:    cause.Error(),
	}); err != nil {
		return err
	}
	if _, err := h.store.UpdateJobStatus(ctx, id, domain.StatusFailed, repository.JobUpdate{
		To:             domain.StatusRetrying,
		SetNextRetryAt: true,
		NextRetryAt:    time.Now().Add(delay),
	}); err != nil {
		return err
	}
	_, err := h.store.UpdateJobStatus(ctx, id, domain.StatusRetrying, repository.JobUpdate{
		To:             domain.StatusQueued,
		ClearNextRetry: true,
	})
	return err
}

func (h *JobHandler) markFailed(ctx context.Context, log *slog.Logger, id uuid.UUID, cause error) {
	if _, err := h.store.UpdateJobStatus(ctx, id, domain.StatusProcessing, repository.JobUpdate{
		To:           domain.StatusFailed,
		SetLastError: true,
		LastError:    cause.Error(),
	}); err != nil && !errors.Is(err, repository.ErrConflict) {
		log.Error("mark failed", "error", err)
	}
}

func (h *JobHandler) persistSteps(attemptID uuid.UUID, steps []pipeline.StepResult) {
	if len(steps) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), finalizeTimeout)
	defer cancel()
	for _, s := range steps {
		errMsg := ""
		if s.Err != nil {
			errMsg = s.Err.Error()
		}
		now := time.Now()
		_ = h.store.AddProcessingStep(ctx, domain.ProcessingStep{
			AttemptID:  attemptID,
			Name:       s.Name,
			Status:     s.Status,
			Duration:   s.Duration,
			Error:      errMsg,
			FinishedAt: &now,
		})
	}
}

// watchCancellation polls the job while it processes and cancels the ctx if it
// becomes CANCELLED, so a cancel reaches the running pipeline instead of being
// discarded only at the final guard. The returned stop func must run before
// Handle returns (no leak). Polling is crude but needs no extra infra.
func (h *JobHandler) watchCancellation(ctx context.Context, id uuid.UUID, cancel context.CancelFunc) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(h.cancelPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				job, err := h.store.GetJob(ctx, id)
				if err == nil && job.Status == domain.StatusCancelled {
					cancel()
					return
				}
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}
