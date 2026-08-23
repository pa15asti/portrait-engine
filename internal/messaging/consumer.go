package messaging

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// errNotStarted is returned when Next is called before Start.
var errNotStarted = errors.New("consumer not started")

// Consumer is the durable pull consumer shared by the pool. The in-flight limit
// gives natural backpressure — at most maxInFlight outstanding at once.
type Consumer struct {
	cons jetstream.Consumer
	iter jetstream.MessagesContext
}

// Start begins pulling messages, buffering at most maxInFlight unacknowledged
// messages at a time. Call Stop to end consumption.
func (c *Consumer) Start(maxInFlight int) error {
	it, err := c.cons.Messages(jetstream.PullMaxMessages(maxInFlight))
	if err != nil {
		return err
	}
	c.iter = it
	return nil
}

// Next blocks until the next message is available or the consumer is stopped.
// After Stop, Next returns an error (jetstream.ErrMsgIteratorClosed).
func (c *Consumer) Next() (Delivery, error) {
	if c.iter == nil {
		return nil, errNotStarted
	}
	msg, err := c.iter.Next()
	if err != nil {
		return nil, err
	}
	return &jsDelivery{msg: msg}, nil
}

// Stop ends message consumption, unblocking any in-flight Next call. Messages
// already delivered but not yet acked are left for redelivery.
func (c *Consumer) Stop() {
	if c.iter != nil {
		c.iter.Stop()
	}
}

// jsDelivery adapts a JetStream message to the Delivery interface.
type jsDelivery struct {
	msg jetstream.Msg
}

func (d *jsDelivery) Data() []byte { return d.msg.Data() }

func (d *jsDelivery) Headers() map[string][]string { return d.msg.Headers() }

func (d *jsDelivery) Ack() error { return d.msg.Ack() }

func (d *jsDelivery) Nak(delay time.Duration) error {
	if delay <= 0 {
		return d.msg.Nak()
	}
	return d.msg.NakWithDelay(delay)
}

func (d *jsDelivery) Term() error { return d.msg.Term() }

func (d *jsDelivery) Deliveries() int {
	md, err := d.msg.Metadata()
	if err != nil {
		return 0
	}
	return int(md.NumDelivered)
}
