//go:build integration

package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/pa15asti/portrait-engine/internal/config"
)

var natsURL string

func TestMain(m *testing.M) {
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "nats:2.10-alpine",
			ExposedPorts: []string{"4222/tcp"},
			Cmd:          []string{"-js", "-m", "8222"},
			WaitingFor:   wait.ForListeningPort("4222/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start nats container: %v\n", err)
		os.Exit(1)
	}
	host, err := container.Host(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "nats host: %v\n", err)
		os.Exit(1)
	}
	port, err := container.MappedPort(ctx, "4222/tcp")
	if err != nil {
		fmt.Fprintf(os.Stderr, "nats port: %v\n", err)
		os.Exit(1)
	}
	natsURL = fmt.Sprintf("nats://%s:%s", host, port.Port())

	code := m.Run()
	_ = container.Terminate(context.Background())
	os.Exit(code)
}

// setup connects a client with a test-unique stream/subject/consumer and
// returns a ready publisher and started consumer.
func setup(t *testing.T, maxDeliver int) (*Publisher, *Consumer) {
	t.Helper()
	ctx := context.Background()
	name := t.Name()
	cfg := config.NATSConfig{
		URL:        natsURL,
		Stream:     "S_" + name,
		Subject:    "test." + name,
		Durable:    "d_" + name,
		MaxDeliver: maxDeliver,
		AckWait:    2 * time.Second,
	}
	client, err := Connect(cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(client.Close)
	if err := client.EnsureStream(ctx); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}
	consumer, err := client.NewConsumer(ctx)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Start(10); err != nil {
		t.Fatalf("start consumer: %v", err)
	}
	t.Cleanup(consumer.Stop)
	return client.Publisher(), consumer
}

func nextOrTimeout(t *testing.T, c *Consumer, d time.Duration) (Delivery, bool) {
	t.Helper()
	type res struct {
		del Delivery
		err error
	}
	ch := make(chan res, 1)
	go func() {
		del, err := c.Next()
		ch <- res{del, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("Next: %v", r.err)
		}
		return r.del, false
	case <-time.After(d):
		return nil, true
	}
}

func TestPublishConsumeAck(t *testing.T) {
	pub, cons := setup(t, 5)
	ctx := context.Background()

	want := JobMessage{JobID: "job-1", Pipeline: "portrait-enhance", PipelineVersion: "v1", CorrelationID: "corr-1"}
	if err := pub.Publish(ctx, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	del, timedOut := nextOrTimeout(t, cons, 5*time.Second)
	if timedOut {
		t.Fatal("expected a delivery")
	}
	var got JobMessage
	if err := json.Unmarshal(del.Data(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.JobID != want.JobID || got.Pipeline != want.Pipeline {
		t.Errorf("payload mismatch: %+v", got)
	}
	if del.Deliveries() != 1 {
		t.Errorf("first delivery count = %d, want 1", del.Deliveries())
	}
	if err := del.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// After ack there must be no redelivery.
	if _, timedOut := nextOrTimeout(t, cons, 2*time.Second); !timedOut {
		t.Error("expected no redelivery after ack")
	}
}

func TestRedeliveryOnNak(t *testing.T) {
	pub, cons := setup(t, 5)
	ctx := context.Background()

	if err := pub.Publish(ctx, JobMessage{JobID: "job-nak"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	del, _ := nextOrTimeout(t, cons, 5*time.Second)
	if del.Deliveries() != 1 {
		t.Errorf("delivery = %d, want 1", del.Deliveries())
	}
	// Negative-ack with immediate redelivery.
	if err := del.Nak(0); err != nil {
		t.Fatalf("Nak: %v", err)
	}

	redel, timedOut := nextOrTimeout(t, cons, 5*time.Second)
	if timedOut {
		t.Fatal("expected redelivery after nak")
	}
	if redel.Deliveries() != 2 {
		t.Errorf("redelivery count = %d, want 2", redel.Deliveries())
	}
	if err := redel.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestMaxDeliverExhaustion(t *testing.T) {
	const maxDeliver = 3
	pub, cons := setup(t, maxDeliver)
	ctx := context.Background()

	if err := pub.Publish(ctx, JobMessage{JobID: "job-poison"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// The message should be delivered exactly maxDeliver times when we keep
	// nak-ing it, then never again.
	for i := 1; i <= maxDeliver; i++ {
		del, timedOut := nextOrTimeout(t, cons, 5*time.Second)
		if timedOut {
			t.Fatalf("expected delivery %d of %d", i, maxDeliver)
		}
		if del.Deliveries() != i {
			t.Errorf("delivery count = %d, want %d", del.Deliveries(), i)
		}
		if err := del.Nak(0); err != nil {
			t.Fatalf("Nak: %v", err)
		}
	}

	// No further redelivery past the max-deliver cap.
	if _, timedOut := nextOrTimeout(t, cons, 3*time.Second); !timedOut {
		t.Error("expected no delivery beyond max-deliver cap")
	}
}
