package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/pa15asti/portrait-engine/internal/domain"
	"github.com/pa15asti/portrait-engine/internal/messaging"
)

type RequeueLister interface {
	ListRequeueable(ctx context.Context, cutoff time.Time, limit int) ([]domain.Job, error)
}

type RequeuePublisher interface {
	Publish(ctx context.Context, m messaging.JobMessage) error
}

// Requeuer republishes stuck QUEUED jobs. Safe against duplicates: the publisher
// dedups by job id and the worker re-reads state, so a job whose message is
// merely in-flight isn't processed twice.
type Requeuer struct {
	lister    RequeueLister
	publisher RequeuePublisher
	log       *slog.Logger
}

// NewRequeuer constructs a Requeuer.
func NewRequeuer(lister RequeueLister, publisher RequeuePublisher, log *slog.Logger) *Requeuer {
	if log == nil {
		log = slog.Default()
	}
	return &Requeuer{lister: lister, publisher: publisher, log: log}
}

// Sweep republishes QUEUED jobs older than minAge, up to limit. A single
// publish error is logged and skipped so one bad job doesn't stop the sweep.
func (r *Requeuer) Sweep(ctx context.Context, minAge time.Duration, limit int) (int, error) {
	cutoff := time.Now().Add(-minAge)
	jobs, err := r.lister.ListRequeueable(ctx, cutoff, limit)
	if err != nil {
		return 0, err
	}

	requeued := 0
	for _, job := range jobs {
		msg := messaging.JobMessage{
			JobID:           job.ID.String(),
			Pipeline:        job.Pipeline,
			PipelineVersion: job.PipelineVersion,
			CorrelationID:   job.CorrelationID,
		}
		if err := r.publisher.Publish(ctx, msg); err != nil {
			r.log.Error("requeue publish failed", "job_id", job.ID.String(), "error", err)
			continue
		}
		requeued++
	}
	if requeued > 0 {
		r.log.Info("requeued stuck jobs", "count", requeued)
	}
	return requeued, nil
}
