// Command api runs the REST API: accepts upload/job requests, persists state to
// Postgres, publishes work to JetStream. Never proxies image bytes.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/pa15asti/portrait-engine/internal/api"
	"github.com/pa15asti/portrait-engine/internal/config"
	"github.com/pa15asti/portrait-engine/internal/jobs"
	"github.com/pa15asti/portrait-engine/internal/messaging"
	"github.com/pa15asti/portrait-engine/internal/observability"
	"github.com/pa15asti/portrait-engine/internal/repository"
	"github.com/pa15asti/portrait-engine/internal/storage"
)

func main() {
	// `api healthcheck` — self-probe for the container runtime (distroless has
	// no shell/curl).
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := observability.Probe(os.Getenv("HTTP_ADDR"), "/ready"); err != nil {
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		os.Exit(1) // run() already logged
	}
}

func run() error {
	cfg, err := config.Load("portrait-api")
	if err != nil {
		observability.NewLogger("error", "portrait-api").
			Error("load config", "error", err)
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(shutdownCtx)
	}()
	metrics := observability.NewMetrics()

	pool, err := repository.NewPool(ctx, cfg.Postgres)
	if err != nil {
		log.Error("connect postgres", "error", err)
		return err
	}
	defer pool.Close()

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

	repo := repository.NewJobRepository(pool)
	jobSvc := jobs.NewService(repo, store, broker.Publisher(), jobs.Config{
		MaxAttempts:    cfg.Worker.MaxAttempts,
		PresignExpiry:  cfg.Storage.PresignExpiry,
		MaxUploadBytes: cfg.Storage.MaxUploadBytes,
	})

	srv := &http.Server{
		Addr:         cfg.HTTP.Addr,
		Handler:      api.NewServer(cfg.HTTP, log, jobSvc, metrics).Handler(),
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
	}

	// Serve in the background; funnel a fatal listen error back to main.
	serveErr := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", cfg.HTTP.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	// Block until a signal cancels ctx or the server fails to start.
	select {
	case err := <-serveErr:
		if err != nil {
			log.Error("http server failed", "error", err)
			return err
		}
		return nil
	case <-ctx.Done():
		log.Info("shutdown signal received, draining connections")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed, forcing close", "error", err)
		_ = srv.Close()
		return err
	}

	log.Info("shutdown complete")
	return nil
}
