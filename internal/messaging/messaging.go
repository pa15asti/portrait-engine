// Package messaging is the NATS JetStream transport. Messages are pointers to
// work, not snapshots — the worker loads authoritative state from Postgres by
// JobID. Delivery is at-least-once, so consumers must be idempotent.
package messaging

import "time"

// JobMessage is the payload published per unit of work.
type JobMessage struct {
	JobID string `json:"job_id"`
	// Empty in the initial enqueue (assigned at claim); kept for trace correlation.
	AttemptID       string `json:"attempt_id,omitempty"`
	Pipeline        string `json:"pipeline"`
	PipelineVersion string `json:"pipeline_version"`
	CorrelationID   string `json:"correlation_id"`
}

// Delivery is one received message, decoupled from the SDK. Take exactly one of
// Ack/Nak/Term per delivery.
type Delivery interface {
	Data() []byte
	Headers() map[string][]string // carries the propagated trace context
	Ack() error
	Nak(delay time.Duration) error // redeliver after delay
	Term() error                   // poison message, stop redelivery
	Deliveries() int               // times delivered so far (>= 1)
}
