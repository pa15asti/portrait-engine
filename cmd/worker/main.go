// Command worker consumes jobs from JetStream, loads state from Postgres, runs
// the pipeline, and writes artifacts. Delivery is at-least-once, so duplicate
// deliveries are made safe.
package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/pa15asti/portrait-engine/internal/config"
	imgproc "github.com/pa15asti/portrait-engine/internal/image"
	"github.com/pa15asti/portrait-engine/internal/messaging"
	"github.com/pa15asti/portrait-engine/internal/observability"
	"github.com/pa15asti/portrait-engine/internal/pipeline"
	"github.com/pa15asti/portrait-engine/internal/repository"
	"github.com/pa15asti/portrait-engine/internal/storage"
	"github.com/pa15asti/portrait-engine/internal/worker"
)

func main() {
	// Self-contained health probe for the container runtime: `worker healthcheck`.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := observability.Probe(os.Getenv("WORKER_METRICS_ADDR"), "/health"); err != nil {
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("portrait-worker")
	if err != nil {
		observability.NewLogger("error", "portrait-worker").Error("load config", "error", err)
		return err
	}
	log := observability.NewLogger(cfg.LogLevel, cfg.Telemetry.ServiceName)

	ctx, stop := observability.SignalContext(context.Background())
	defer stop()

	shutdownTracing, err := observability.SetupTracing(ctx, cfg.Telemetry.OTLPEndpoint, cfg.Telemetry.ServiceName)
	if err != nil {
		log.Error("setup tracing", "error", err)
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(sctx)
	}()
	metrics := observability.NewMetrics()

	// /metrics + /health so the worker is scrapeable.
	metricsSrv := &http.Server{Addr: cfg.Worker.MetricsAddr, Handler: metricsMux(metrics)}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server failed", "error", err)
		}
	}()
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(sctx)
	}()

	dbPool, err := repository.NewPool(ctx, cfg.Postgres)
	if err != nil {
		log.Error("connect postgres", "error", err)
		return err
	}
	defer dbPool.Close()
	repo := repository.NewJobRepository(dbPool)

	store, err := storage.NewMinioStore(ctx, cfg.Storage)
	if err != nil {
		log.Error("connect object store", "error", err)
		return err
	}

	broker, err := messaging.Connect(cfg.NATS)
	if err != nil {
		log.Error("connect nats", "error", err)
		return err
	}
	defer broker.Close()
	if err := broker.EnsureStream(ctx); err != nil {
		log.Error("ensure jetstream stream", "error", err)
		return err
	}
	consumer, err := broker.NewConsumer(ctx)
	if err != nil {
		log.Error("create consumer", "error", err)
		return err
	}

	detector, err := imgproc.NewFaceDetector()
	if err != nil {
		log.Error("load face detector", "error", err)
		return err
	}
	registry := pipeline.DefaultRegistry(detector)
	log.Info("pipelines registered", "pipelines", registry.Keys())

	// Recover jobs left QUEUED without a live message (failed publish, prior crash).
	requeuer := worker.NewRequeuer(repo, broker.Publisher(), log)
	if n, err := requeuer.Sweep(ctx, time.Minute, 500); err != nil {
		log.Warn("startup requeue sweep failed", "error", err)
	} else if n > 0 {
		log.Info("startup requeue sweep republished jobs", "count", n)
	}

	handler := worker.NewJobHandler(repo, store, registry, metrics, log)
	pool := worker.NewPool(consumer, handler, worker.Options{
		Concurrency:     cfg.Worker.Concurrency,
		JobTimeout:      cfg.Worker.JobTimeout,
		ShutdownTimeout: cfg.Worker.ShutdownTimeout,
		Logger:          log,
		Metrics:         metrics,
	})

	log.Info("worker started",
		"concurrency", cfg.Worker.Concurrency,
		"job_timeout", cfg.Worker.JobTimeout.String(),
	)

	// Run blocks until a signal cancels ctx, then drains gracefully.
	if err := pool.Run(ctx); err != nil {
		log.Error("worker pool error", "error", err)
		return err
	}
	log.Info("shutdown complete")
	return nil
}

// metricsMux serves Prometheus metrics and a liveness probe for the worker.
func metricsMux(m *observability.Metrics) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	return mux
}
