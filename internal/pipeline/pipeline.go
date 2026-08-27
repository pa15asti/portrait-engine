// Package pipeline is the pluggable processing abstraction and the versioned
// registry. A Pipeline is an ordered list of Processors run under an explicit
// version; jobs record their version and the worker resolves that exact one, so
// shipping a new version doesn't change how existing jobs process.
package pipeline

import (
	"context"
	"fmt"
	"image"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/pa15asti/portrait-engine/internal/domain"
)

// Face is a detected face region with a detection score.
type Face struct {
	Rect  image.Rectangle
	Score float32
}

// ProcessingInput flows into a processor: the current image plus any faces
// detected by earlier stages.
type ProcessingInput struct {
	Image image.Image
	Faces []Face
}

// ProcessingOutput flows out of a processor and becomes the next stage's input.
type ProcessingOutput struct {
	Image image.Image
	Faces []Face
}

// Processor is one stage. Must respect ctx, and be deterministic given its
// config — that's what makes a version reproducible.
type Processor interface {
	Name() string
	Process(ctx context.Context, in ProcessingInput) (ProcessingOutput, error)
}

// StepResult records one processor's execution.
type StepResult struct {
	Name     string
	Status   domain.StepStatus
	Duration time.Duration
	Err      error
}

// Pipeline is an ordered, versioned sequence of processors.
type Pipeline struct {
	name       string
	version    string
	processors []Processor
}

// New builds a pipeline from an ordered list of processors.
func New(name, version string, processors ...Processor) *Pipeline {
	return &Pipeline{name: name, version: version, processors: processors}
}

// Name returns the pipeline name.
func (p *Pipeline) Name() string { return p.name }

// Version returns the pipeline version.
func (p *Pipeline) Version() string { return p.version }

// Run executes the processors in order, feeding each output into the next.
// onStep (may be nil) fires after every processor including failures, so a
// failed run is still auditable. Stops at the first error or on cancellation.
func (p *Pipeline) Run(ctx context.Context, input ProcessingInput, onStep func(StepResult)) (ProcessingOutput, error) {
	tracer := otel.Tracer("portrait/pipeline")
	ctx, span := tracer.Start(ctx, "pipeline.run")
	defer span.End()
	span.SetAttributes(
		attribute.String("pipeline.name", p.name),
		attribute.String("pipeline.version", p.version),
	)

	cur := ProcessingOutput(input)

	for _, proc := range p.processors {
		if err := ctx.Err(); err != nil { // cancel between stages, before heavy work
			return cur, err
		}

		procCtx, procSpan := tracer.Start(ctx, "processor."+proc.Name())
		start := time.Now()
		out, err := proc.Process(procCtx, ProcessingInput(cur))
		dur := time.Since(start)
		if err != nil {
			procSpan.RecordError(err)
		}
		procSpan.End()

		step := StepResult{Name: proc.Name(), Duration: dur}
		if err != nil {
			step.Status = domain.StepFailed
			step.Err = err
			if onStep != nil {
				onStep(step)
			}
			return cur, fmt.Errorf("processor %q: %w", proc.Name(), err)
		}

		step.Status = domain.StepSucceeded
		if onStep != nil {
			onStep(step)
		}
		cur = out
	}

	return cur, nil
}
