package api

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/pa15asti/portrait-engine/internal/observability"
)

// requestIDHeader carries a client- or server-assigned request identifier.
const requestIDHeader = "X-Request-ID"

type ctxKey int

const requestIDKey ctxKey = iota

// requestIDFrom returns the request ID stored in the context, or "".
func requestIDFrom(r *http.Request) string {
	if v, ok := r.Context().Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// statusRecorder captures the response status code for logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// withRequestID assigns/propagates a request ID and binds a request-scoped
// logger carrying it.
func (s *Server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(requestIDHeader)
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set(requestIDHeader, id)

		ctx := context.WithValue(r.Context(), requestIDKey, id)
		reqLog := s.log.With(slog.String("request_id", id))
		ctx = observability.WithLogger(ctx, reqLog)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// withLogging logs one structured line per request after it completes.
func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		observability.LoggerFrom(r.Context()).Info("http request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Duration("duration", time.Since(start)),
		)
	})
}

// withMetrics records count/latency labelled by matched route (not the raw
// path, to keep cardinality low).
func (s *Server) withMetrics(next http.Handler) http.Handler {
	if s.metrics == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		s.metrics.ObserveHTTP(r.Method, route, strconv.Itoa(rec.status), time.Since(start))
	})
}

// withRecovery converts a panic in a handler into a 500 rather than crashing
// the server, logging the recovered value.
func (s *Server) withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				observability.LoggerFrom(r.Context()).Error("panic recovered",
					slog.Any("panic", rec),
					slog.String("path", r.URL.Path),
				)
				writeError(w, http.StatusInternalServerError, "internal", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
