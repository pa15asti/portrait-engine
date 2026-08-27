package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/pa15asti/portrait-engine/internal/messaging"
)

// TestMain fails the package if any test leaks a goroutine.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

var errConsumerStopped = errors.New("consumer stopped")

type fakeConsumer struct {
	ch       chan messaging.Delivery
	done     chan struct{}
	stopOnce sync.Once
}

func newFakeConsumer(capacity int) *fakeConsumer {
	return &fakeConsumer{ch: make(chan messaging.Delivery, capacity), done: make(chan struct{})}
}
func (c *fakeConsumer) Start(int) error { return nil }
func (c *fakeConsumer) Next() (messaging.Delivery, error) {
	select {
	case d := <-c.ch:
		return d, nil
	case <-c.done:
		return nil, errConsumerStopped
	}
}
func (c *fakeConsumer) Stop()                     { c.stopOnce.Do(func() { close(c.done) }) }
func (c *fakeConsumer) push(d messaging.Delivery) { c.ch <- d }

type fakeDelivery struct {
	data       []byte
	deliveries int
	mu         sync.Mutex
	acked      int
	naked      int
	termed     int
	nakDelay   time.Duration
}

func newDelivery(t *testing.T, m messaging.JobMessage) *fakeDelivery {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &fakeDelivery{data: data, deliveries: 1}
}

func (d *fakeDelivery) Data() []byte                 { return d.data }
func (d *fakeDelivery) Headers() map[string][]string { return nil }
func (d *fakeDelivery) Ack() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.acked++
	return nil
}
func (d *fakeDelivery) Nak(delay time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.naked++
	d.nakDelay = delay
	return nil
}
func (d *fakeDelivery) Term() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.termed++
	return nil
}
func (d *fakeDelivery) Deliveries() int { return d.deliveries }
func (d *fakeDelivery) counts() (ack, nak, term int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.acked, d.naked, d.termed
}

type fakeHandler struct {
	fn func(ctx context.Context, msg messaging.JobMessage) Result
}

func (h fakeHandler) Handle(ctx context.Context, msg messaging.JobMessage) Result {
	return h.fn(ctx, msg)
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// runPool starts a pool and returns a cancel func plus a channel closed when
// Run returns.
func runPool(p *Pool) (context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = p.Run(ctx)
		close(done)
	}()
	return cancel, done
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestPool_ProcessesAndAcks(t *testing.T) {
	const n = 5
	var wg sync.WaitGroup
	wg.Add(n)
	handler := fakeHandler{fn: func(context.Context, messaging.JobMessage) Result {
		wg.Done()
		return Result{Decision: Complete}
	}}
	cons := newFakeConsumer(n)
	dels := make([]*fakeDelivery, n)
	for i := 0; i < n; i++ {
		dels[i] = newDelivery(t, messaging.JobMessage{JobID: "j"})
		cons.push(dels[i])
	}
	pool := NewPool(cons, handler, Options{Concurrency: 3, ShutdownTimeout: time.Second, Logger: discardLogger()})

	cancel, done := runPool(pool)
	wg.Wait()
	cancel()
	<-done

	for i, d := range dels {
		if ack, _, _ := d.counts(); ack != 1 {
			t.Errorf("delivery %d acked %d times, want 1", i, ack)
		}
	}
}

func TestPool_BoundedConcurrency(t *testing.T) {
	const (
		concurrency = 3
		n           = 12
	)
	var active, maxActive int32
	var wg sync.WaitGroup
	wg.Add(n)
	handler := fakeHandler{fn: func(context.Context, messaging.JobMessage) Result {
		defer wg.Done()
		cur := atomic.AddInt32(&active, 1)
		for {
			m := atomic.LoadInt32(&maxActive)
			if cur <= m || atomic.CompareAndSwapInt32(&maxActive, m, cur) {
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return Result{Decision: Complete}
	}}
	cons := newFakeConsumer(n)
	for i := 0; i < n; i++ {
		cons.push(newDelivery(t, messaging.JobMessage{JobID: "j"}))
	}
	pool := NewPool(cons, handler, Options{Concurrency: concurrency, ShutdownTimeout: time.Second, Logger: discardLogger()})

	cancel, done := runPool(pool)
	wg.Wait()
	cancel()
	<-done

	peak := atomic.LoadInt32(&maxActive)
	if peak > concurrency {
		t.Errorf("peak concurrency %d exceeded bound %d", peak, concurrency)
	}
	if peak < 2 {
		t.Errorf("expected parallel processing, peak was only %d", peak)
	}
}

func TestPool_PanicRecovery(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	handler := fakeHandler{fn: func(_ context.Context, msg messaging.JobMessage) Result {
		defer wg.Done()
		if msg.JobID == "panic" {
			panic("boom")
		}
		return Result{Decision: Complete}
	}}
	cons := newFakeConsumer(2)
	bad := newDelivery(t, messaging.JobMessage{JobID: "panic"})
	good := newDelivery(t, messaging.JobMessage{JobID: "ok"})
	cons.push(bad)
	cons.push(good)
	pool := NewPool(cons, handler, Options{Concurrency: 1, ShutdownTimeout: time.Second, Logger: discardLogger()})

	cancel, done := runPool(pool)
	wg.Wait()
	cancel()
	<-done

	// The panicking message is nak'd for redelivery; the pool survives and the
	// following message is processed normally.
	if _, nak, _ := bad.counts(); nak != 1 {
		t.Errorf("panicking delivery nak = %d, want 1", nak)
	}
	if ack, _, _ := good.counts(); ack != 1 {
		t.Errorf("good delivery ack = %d, want 1", ack)
	}
}

func TestPool_UnparseableIsTerminated(t *testing.T) {
	var called atomic.Bool
	handler := fakeHandler{fn: func(context.Context, messaging.JobMessage) Result {
		called.Store(true)
		return Result{Decision: Complete}
	}}
	cons := newFakeConsumer(1)
	bad := &fakeDelivery{data: []byte("{not json"), deliveries: 1}
	cons.push(bad)
	pool := NewPool(cons, handler, Options{Concurrency: 1, ShutdownTimeout: time.Second, Logger: discardLogger()})

	cancel, done := runPool(pool)
	waitFor(t, func() bool { _, _, term := bad.counts(); return term == 1 }, time.Second)
	cancel()
	<-done

	if called.Load() {
		t.Error("handler must not be called for an unparseable message")
	}
}

func TestPool_PerJobTimeout(t *testing.T) {
	var sawDeadline atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	handler := fakeHandler{fn: func(ctx context.Context, _ messaging.JobMessage) Result {
		defer wg.Done()
		<-ctx.Done()
		sawDeadline.Store(errors.Is(ctx.Err(), context.DeadlineExceeded))
		return Result{Decision: Retry}
	}}
	cons := newFakeConsumer(1)
	del := newDelivery(t, messaging.JobMessage{JobID: "slow"})
	cons.push(del)
	pool := NewPool(cons, handler, Options{Concurrency: 1, JobTimeout: 40 * time.Millisecond, ShutdownTimeout: time.Second, Logger: discardLogger()})

	cancel, done := runPool(pool)
	wg.Wait()
	cancel()
	<-done

	if !sawDeadline.Load() {
		t.Error("handler should observe a deadline-exceeded context")
	}
	if _, nak, _ := del.counts(); nak != 1 {
		t.Errorf("timed-out delivery nak = %d, want 1", nak)
	}
}

func TestPool_GracefulDrainFinishesInFlight(t *testing.T) {
	started := make(chan struct{})
	handler := fakeHandler{fn: func(context.Context, messaging.JobMessage) Result {
		close(started)
		time.Sleep(80 * time.Millisecond) // still running when shutdown begins
		return Result{Decision: Complete}
	}}
	cons := newFakeConsumer(1)
	del := newDelivery(t, messaging.JobMessage{JobID: "inflight"})
	cons.push(del)
	pool := NewPool(cons, handler, Options{Concurrency: 1, ShutdownTimeout: 2 * time.Second, Logger: discardLogger()})

	cancel, done := runPool(pool)
	<-started
	cancel() // shutdown while the job is in-flight
	<-done

	// A generous shutdown timeout means the in-flight job completes and acks.
	if ack, _, _ := del.counts(); ack != 1 {
		t.Errorf("in-flight delivery ack = %d, want 1", ack)
	}
}

func TestPool_ForceCancelOnTimeout(t *testing.T) {
	started := make(chan struct{})
	handler := fakeHandler{fn: func(ctx context.Context, _ messaging.JobMessage) Result {
		close(started)
		<-ctx.Done() // only returns once forcibly cancelled
		return Result{Decision: Retry}
	}}
	cons := newFakeConsumer(1)
	del := newDelivery(t, messaging.JobMessage{JobID: "stuck"})
	cons.push(del)
	pool := NewPool(cons, handler, Options{Concurrency: 1, ShutdownTimeout: 20 * time.Millisecond, Logger: discardLogger()})

	cancel, done := runPool(pool)
	<-started
	cancel()
	<-done

	// The drain deadline passes, in-flight work is cancelled, and the message
	// is nak'd so it is redelivered later.
	if _, nak, _ := del.counts(); nak != 1 {
		t.Errorf("force-cancelled delivery nak = %d, want 1", nak)
	}
}
