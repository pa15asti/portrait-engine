package observability

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TestTracePropagation_AcrossHeaders proves the trace context survives an
// inject-on-publish / extract-on-consume round trip through a header map, so a
// consumer's spans join the producer's trace across the queue.
func TestTracePropagation_AcrossHeaders(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	// Producer side: start a span and inject its context into headers.
	ctx, span := otel.Tracer("producer").Start(context.Background(), "publish")
	defer span.End()
	wantTrace := span.SpanContext().TraceID()

	headers := map[string][]string{}
	InjectTrace(ctx, headers)
	if len(headers["traceparent"]) == 0 {
		t.Fatal("expected a traceparent header to be injected")
	}

	// Consumer side: extract into a fresh context and confirm the trace matches.
	extracted := ExtractTrace(context.Background(), headers)
	got := trace.SpanContextFromContext(extracted)
	if !got.IsValid() {
		t.Fatal("extracted span context is not valid")
	}
	if got.TraceID() != wantTrace {
		t.Errorf("trace id not propagated: got %s want %s", got.TraceID(), wantTrace)
	}
}

func TestExtractTrace_NoHeadersIsSafe(t *testing.T) {
	// Extracting from empty headers must not panic and yields no valid parent.
	ctx := ExtractTrace(context.Background(), map[string][]string{})
	if trace.SpanContextFromContext(ctx).IsValid() {
		t.Error("did not expect a valid span context from empty headers")
	}
}
