package messaging

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/pa15asti/portrait-engine/internal/observability"
)

// Publisher publishes job messages to JetStream.
type Publisher struct {
	js      jetstream.JetStream
	subject string
}

// Publish sends a job message, using JobID as the JetStream message ID so
// duplicate publishes within the dedup window collapse (belt-and-suspenders on
// top of the consumer-side idempotency).
func (p *Publisher) Publish(ctx context.Context, m JobMessage) error {
	ctx, span := otel.Tracer("portrait/messaging").Start(ctx, "jetstream.publish")
	span.SetAttributes(attribute.String("job.id", m.JobID), attribute.String("subject", p.subject))
	defer span.End()

	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal job message: %w", err)
	}

	// Trace context in headers so consumer spans join this trace.
	header := nats.Header{}
	observability.InjectTrace(ctx, header)

	msg := &nats.Msg{Subject: p.subject, Data: data, Header: header}
	if _, err := p.js.PublishMsg(ctx, msg, jetstream.WithMsgID(m.JobID)); err != nil {
		return fmt.Errorf("publish job %s: %w", m.JobID, err)
	}
	return nil
}
