package pipeline

import (
	"context"
	"errors"
	"image"
	"testing"

	"github.com/pa15asti/portrait-engine/internal/domain"
)

type fakeProc struct {
	name string
	fn   func(ctx context.Context, in ProcessingInput) (ProcessingOutput, error)
}

func (p fakeProc) Name() string { return p.name }
func (p fakeProc) Process(ctx context.Context, in ProcessingInput) (ProcessingOutput, error) {
	return p.fn(ctx, in)
}

func passthrough(name string, log *[]string) fakeProc {
	return fakeProc{name: name, fn: func(_ context.Context, in ProcessingInput) (ProcessingOutput, error) {
		*log = append(*log, name)
		return ProcessingOutput(in), nil
	}}
}

func TestPipeline_RunsInOrderRecordsSteps(t *testing.T) {
	var order []string
	p := New("portrait", "v1", passthrough("a", &order), passthrough("b", &order), passthrough("c", &order))

	var steps []StepResult
	_, err := p.Run(context.Background(), ProcessingInput{}, func(s StepResult) { steps = append(steps, s) })
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := len(order); got != 3 {
		t.Fatalf("ran %d processors, want 3", got)
	}
	if order[0] != "a" || order[1] != "b" || order[2] != "c" {
		t.Errorf("processors ran out of order: %v", order)
	}
	if len(steps) != 3 {
		t.Fatalf("recorded %d steps, want 3", len(steps))
	}
	for _, s := range steps {
		if s.Status != domain.StepSucceeded {
			t.Errorf("step %q status = %s, want SUCCEEDED", s.Name, s.Status)
		}
	}
}

func TestPipeline_StopsOnError(t *testing.T) {
	var order []string
	boom := errors.New("boom")
	failing := fakeProc{name: "b", fn: func(context.Context, ProcessingInput) (ProcessingOutput, error) {
		order = append(order, "b")
		return ProcessingOutput{}, boom
	}}
	p := New("portrait", "v1", passthrough("a", &order), failing, passthrough("c", &order))

	var steps []StepResult
	_, err := p.Run(context.Background(), ProcessingInput{}, func(s StepResult) { steps = append(steps, s) })
	if !errors.Is(err, boom) {
		t.Fatalf("expected wrapped boom, got %v", err)
	}
	if len(order) != 2 { // "c" must not run
		t.Errorf("expected 2 processors to run, got %v", order)
	}
	if len(steps) != 2 || steps[1].Status != domain.StepFailed {
		t.Errorf("expected the failing step recorded as FAILED, got %+v", steps)
	}
}

func TestPipeline_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	ran := false
	p := New("portrait", "v1", fakeProc{name: "a", fn: func(context.Context, ProcessingInput) (ProcessingOutput, error) {
		ran = true
		return ProcessingOutput{}, nil
	}})
	_, err := p.Run(ctx, ProcessingInput{}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if ran {
		t.Error("processor must not run under a cancelled context")
	}
}

func TestPipeline_PassesImageThrough(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 2, 2))
	proc := fakeProc{name: "identity", fn: func(_ context.Context, in ProcessingInput) (ProcessingOutput, error) {
		return ProcessingOutput{Image: in.Image}, nil
	}}
	p := New("portrait", "v1", proc)
	out, err := p.Run(context.Background(), ProcessingInput{Image: img}, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Image != img {
		t.Error("expected the image to pass through unchanged")
	}
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	p := New("portrait", "v1", fakeProc{name: "a", fn: func(context.Context, ProcessingInput) (ProcessingOutput, error) {
		return ProcessingOutput{}, nil
	}})
	r.Register(p)

	got, err := r.Get("portrait", "v1")
	if err != nil || got != p {
		t.Fatalf("Get failed: got=%v err=%v", got, err)
	}
	if _, err := r.Get("portrait", "v2"); !errors.Is(err, ErrUnknownPipeline) {
		t.Errorf("expected ErrUnknownPipeline, got %v", err)
	}
}

func TestRegistry_DuplicatePanics(t *testing.T) {
	r := NewRegistry()
	p := New("portrait", "v1")
	r.Register(p)
	defer func() {
		if recover() == nil {
			t.Error("expected a panic on duplicate registration")
		}
	}()
	r.Register(New("portrait", "v1"))
}
