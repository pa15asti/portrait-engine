// Package api is the HTTP surface: routing, middleware, handlers. It never
// proxies image bytes (those go straight to storage via presigned URLs), and
// handlers are thin adapters over internal/jobs.
package api

import (
	"log/slog"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/pa15asti/portrait-engine/internal/config"
	"github.com/pa15asti/portrait-engine/internal/observability"
)

// Server holds the HTTP dependencies.
type Server struct {
	cfg     config.HTTPConfig
	log     *slog.Logger
	jobs    JobService
	metrics *observability.Metrics
}

// NewServer constructs a Server backed by the given job service. metrics may be
// nil (instrumentation becomes a no-op).
func NewServer(cfg config.HTTPConfig, log *slog.Logger, jobsSvc JobService, metrics *observability.Metrics) *Server {
	return &Server{cfg: cfg, log: log, jobs: jobsSvc, metrics: metrics}
}

// Handler builds the fully-configured http.Handler with routes and middleware.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Operational endpoints (no request-scoped middleware needed, but harmless).
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	if s.metrics != nil {
		mux.Handle("GET /metrics", s.metrics.Handler())
	}

	// v1 API.
	mux.HandleFunc("POST /v1/uploads", s.handleCreateUpload)
	mux.HandleFunc("POST /v1/jobs", s.handleCreateJob)
	mux.HandleFunc("GET /v1/jobs/{id}", s.handleGetJob)
	mux.HandleFunc("POST /v1/jobs/{id}/cancel", s.handleCancelJob)
	mux.HandleFunc("GET /v1/jobs/{id}/artifacts", s.handleListArtifacts)

	return s.withMiddleware(mux)
}

// Order (outermost first): otelhttp → request-id → recovery → metrics →
// logging → handler. Request-id is early so recovery/logging have the logger;
// recovery wraps the rest so a panic still becomes a logged 500.
func (s *Server) withMiddleware(next http.Handler) http.Handler {
	chain := s.withRequestID(s.withRecovery(s.withMetrics(s.withLogging(next))))
	// otelhttp derives the span name from the matched route pattern.
	return otelhttp.NewHandler(chain, "http.server",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			if r.Pattern != "" {
				return r.Pattern
			}
			return r.Method + " " + r.URL.Path
		}),
	)
}
