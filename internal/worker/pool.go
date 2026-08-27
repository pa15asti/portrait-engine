// Package worker is the bounded pool that consumes job messages. It owns the
// tricky parts — concurrency, per-job timeouts, panic isolation, graceful
// shutdown — and hands the actual per-job work to a Handler.
package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/pa15asti/portrait-engine/internal/messaging"
	"github.com/pa15asti/portrait-engine/internal/observability"
)

// Decision is what the pool should do with a delivery after the handler runs.
type Decision int

const (
	Complete Decision = iota // ack: done (success or terminal failure recorded)
	Retry                    // nak for redelivery after RetryAfter
	Discard                  // term: poison message, never redeliver
)

// Result is the handler's decision plus an optional redelivery delay.
type Result struct {
	Decision   Decision
	RetryAfter time.Duration
}

// Handler processes one job message. Must be concurrency-safe and respect ctx.
// It must not ack/nak — the pool settles the delivery from the returned Result.
type Handler interface {
	Handle(ctx context.Context, msg messaging.JobMessage) Result
}

// Consumer is the pool's message source (real: *messaging.Consumer).
type Consumer interface {
	Start(maxInFlight int) error
	Next() (messaging.Delivery, error)
	Stop()
}

// Pool runs a fixed number of goroutines (never one-per-message) fed by a single
// dispatcher — predictable resource use and natural backpressure.
type Pool struct {
	consumer        Consumer
	handler         Handler
	concurrency     int
	jobTimeout      time.Duration
	shutdownTimeout time.Duration
	log             *slog.Logger
	metrics         *observability.Metrics
}

type Options struct {
	Concurrency     int
	JobTimeout      time.Duration
	ShutdownTimeout time.Duration
	Logger          *slog.Logger
	Metrics         *observability.Metrics // may be nil
}

// NewPool constructs a Pool.
func NewPool(consumer Consumer, handler Handler, opts Options) *Pool {
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Pool{
		consumer:        consumer,
		handler:         handler,
		concurrency:     opts.Concurrency,
		jobTimeout:      opts.JobTimeout,
		shutdownTimeout: opts.ShutdownTimeout,
		log:             opts.Logger,
		metrics:         opts.Metrics,
	}
}

// Run blocks until ctx is cancelled, then drains: stop pulling, let in-flight
// jobs finish within ShutdownTimeout, and if that passes, cancel them (their
// messages nak and redeliver later). Returns only after every worker exits.
func (p *Pool) Run(ctx context.Context) error {
	if err := p.consumer.Start(p.concurrency); err != nil {
		return err
	}

	// Independent of ctx so a shutdown signal doesn't kill running jobs
	// immediately — they get ShutdownTimeout. Cancelled only on forced drain.
	procCtx, procCancel := context.WithCancel(context.Background())
	defer procCancel()

	work := make(chan messaging.Delivery)
	var workers sync.WaitGroup
	for i := 0; i < p.concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for del := range work {
				p.process(procCtx, del)
			}
		}()
	}

	// Dispatcher: pull and hand off to a free worker. Exits when the consumer is
	// stopped or processing is force-drained.
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		for {
			del, err := p.consumer.Next()
			if err != nil {
				return // consumer stopped
			}
			select {
			case work <- del:
			case <-procCtx.Done():
				// Forced drain: return the message for redelivery.
				_ = del.Nak(0)
				return
			}
		}
	}()

	<-ctx.Done()
	p.log.Info("worker pool draining", "shutdown_timeout", p.shutdownTimeout.String())

	// Stop pulling; the dispatcher will observe the stopped consumer and exit.
	p.consumer.Stop()
	<-dispatchDone
	close(work) // no more work will be dispatched; workers drain and exit

	drained := make(chan struct{})
	go func() {
		workers.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		p.log.Info("worker pool drained cleanly")
	case <-time.After(p.shutdownTimeout):
		p.log.Warn("worker pool drain timed out, cancelling in-flight jobs")
		procCancel()
		<-drained
	}
	return nil
}

// process runs one delivery with panic isolation and a per-job timeout, then
// settles it (ack/nak/term).
func (p *Pool) process(procCtx context.Context, del messaging.Delivery) {
	var msg messaging.JobMessage
	if err := json.Unmarshal(del.Data(), &msg); err != nil {
		p.log.Error("discarding unparseable job message", "error", err)
		_ = del.Term()
		return
	}

	log := p.log.With(
		slog.String("job_id", msg.JobID),
		slog.String("correlation_id", msg.CorrelationID),
		slog.Int("delivery", del.Deliveries()),
	)

	// Join the producer's trace via the headers.
	tracedCtx := observability.ExtractTrace(procCtx, del.Headers())

	p.metrics.WorkerStarted()
	res := p.handleWithRecovery(tracedCtx, log, msg)
	p.metrics.WorkerFinished()

	switch res.Decision {
	case Complete:
		p.ackOrLog(log, del.Ack)
	case Discard:
		p.ackOrLog(log, del.Term)
	case Retry:
		p.ackOrLog(log, func() error { return del.Nak(res.RetryAfter) })
	}
}

// handleWithRecovery runs the handler under a per-job timeout and turns a panic
// into a Retry, so one bad job can't crash the pool or lose the message.
func (p *Pool) handleWithRecovery(procCtx context.Context, log *slog.Logger, msg messaging.JobMessage) (res Result) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered panic in job handler",
				slog.Any("panic", r),
				slog.String("stack", string(debug.Stack())),
			)
			res = Result{Decision: Retry}
		}
	}()

	ctx := procCtx
	if p.jobTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(procCtx, p.jobTimeout)
		defer cancel()
	}
	ctx = observability.WithLogger(ctx, log)
	return p.handler.Handle(ctx, msg)
}

func (p *Pool) ackOrLog(log *slog.Logger, fn func() error) {
	if err := fn(); err != nil {
		log.Error("failed to settle message", "error", err)
	}
}
