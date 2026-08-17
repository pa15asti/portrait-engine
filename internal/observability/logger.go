// Package observability provides logging, metrics, tracing, and lifecycle
// helpers. Logs carry identifiers only — never image bytes, credentials, or
// presigned URLs.
package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type contextKey int

const loggerKey contextKey = iota

// NewLogger builds a JSON logger at the given level, tagged with the service.
// Unknown levels fall back to info.
func NewLogger(level, service string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(level)})
	return slog.New(h).With(slog.String("service", service))
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

func LoggerFrom(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}
