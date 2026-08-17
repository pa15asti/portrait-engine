package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// SetupTracing configures the global tracer provider. Empty endpoint = no-op
// (default provider), so local runs need no collector. The returned shutdown
// flushes pending spans.
func SetupTracing(ctx context.Context, endpoint, serviceName string) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if endpoint == "" {
		return noop, nil
	}

	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithInsecure())
	if err != nil {
		return noop, fmt.Errorf("create otlp exporter: %w", err)
	}

	res := resource.NewSchemaless(attribute.String("service.name", serviceName))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return tp.Shutdown, nil
}

// Tracer returns a named tracer from the global provider.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// mapCarrier adapts a NATS-style header map to otel's TextMapCarrier.
type mapCarrier map[string][]string

func (c mapCarrier) Get(key string) string {
	if v := c[key]; len(v) > 0 {
		return v[0]
	}
	return ""
}
func (c mapCarrier) Set(key, value string) { c[key] = []string{value} }
func (c mapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectTrace / ExtractTrace carry the trace context in message headers so a
// consumer's spans join the producer's trace across the queue.
func InjectTrace(ctx context.Context, headers map[string][]string) {
	otel.GetTextMapPropagator().Inject(ctx, mapCarrier(headers))
}

func ExtractTrace(ctx context.Context, headers map[string][]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, mapCarrier(headers))
}
