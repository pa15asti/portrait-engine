package messaging

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pa15asti/portrait-engine/internal/config"
)

// Client owns the NATS connection and JetStream context.
type Client struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	cfg config.NATSConfig
}

// Connect dials NATS and initializes JetStream.
func Connect(cfg config.NATSConfig) (*Client, error) {
	nc, err := nats.Connect(cfg.URL,
		nats.Name("portrait-engine"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("init jetstream: %w", err)
	}
	return &Client{nc: nc, js: js, cfg: cfg}, nil
}

// EnsureStream creates/updates the work-queue stream. WorkQueue retention drops
// a message once acked — the right fit for a job queue with one durable consumer.
func (c *Client) EnsureStream(ctx context.Context) error {
	_, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        c.cfg.Stream,
		Subjects:    []string{c.cfg.Subject},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.WorkQueuePolicy,
		Discard:     jetstream.DiscardOld,
		MaxMsgs:     -1,
		Duplicates:  2 * time.Minute, // dedup window for WithMsgID publishes
		Description: "Portrait Engine job work queue",
	})
	if err != nil {
		return fmt.Errorf("ensure stream %q: %w", c.cfg.Stream, err)
	}
	return nil
}

// Publisher returns a publisher bound to the configured subject.
func (c *Client) Publisher() *Publisher {
	return &Publisher{js: c.js, subject: c.cfg.Subject}
}

// NewConsumer creates (or updates) the shared durable pull consumer.
func (c *Client) NewConsumer(ctx context.Context) (*Consumer, error) {
	cons, err := c.js.CreateOrUpdateConsumer(ctx, c.cfg.Stream, jetstream.ConsumerConfig{
		Durable:       c.cfg.Durable,
		FilterSubject: c.cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		// MaxDeliver bounds redelivery; once exceeded JetStream stops
		// redelivering the message (it becomes a dead message).
		MaxDeliver: c.cfg.MaxDeliver,
		AckWait:    c.cfg.AckWait,
	})
	if err != nil {
		return nil, fmt.Errorf("ensure consumer %q: %w", c.cfg.Durable, err)
	}
	return &Consumer{cons: cons}, nil
}

// Ping reports whether the NATS connection is currently usable (for readiness).
func (c *Client) Ping() error {
	if !c.nc.IsConnected() {
		return fmt.Errorf("nats not connected (status %v)", c.nc.Status())
	}
	return nil
}

// Close drains and closes the NATS connection.
func (c *Client) Close() {
	// Drain flushes pending messages and unsubscribes gracefully.
	_ = c.nc.Drain()
}
