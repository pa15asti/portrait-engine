package pipeline

import (
	"context"
	stdimage "image"

	imgproc "github.com/pa15asti/portrait-engine/internal/image"
)

// FaceDetectionProcessor annotates the input with detected faces; later stages
// (background blur) use them. Doesn't touch the image.
type FaceDetectionProcessor struct {
	detector *imgproc.FaceDetector
	minSize  int
}

func NewFaceDetectionProcessor(d *imgproc.FaceDetector, minSize int) *FaceDetectionProcessor {
	return &FaceDetectionProcessor{detector: d, minSize: minSize}
}

func (p *FaceDetectionProcessor) Name() string { return "face-detection" }

func (p *FaceDetectionProcessor) Process(ctx context.Context, in ProcessingInput) (ProcessingOutput, error) {
	if err := ctx.Err(); err != nil {
		return ProcessingOutput{}, err
	}
	detected := p.detector.Detect(in.Image, p.minSize)
	faces := make([]Face, len(detected))
	for i, f := range detected {
		faces[i] = Face{Rect: f.Rect, Score: f.Score}
	}
	return ProcessingOutput{Image: in.Image, Faces: faces}, nil
}

// ResizeProcessor fits the image into a box. Runs before face detection so face
// coordinates match the final image.
type ResizeProcessor struct {
	maxW, maxH int
}

func NewResizeProcessor(maxW, maxH int) *ResizeProcessor {
	return &ResizeProcessor{maxW: maxW, maxH: maxH}
}

func (p *ResizeProcessor) Name() string { return "resize" }

func (p *ResizeProcessor) Process(ctx context.Context, in ProcessingInput) (ProcessingOutput, error) {
	if err := ctx.Err(); err != nil {
		return ProcessingOutput{}, err
	}
	return ProcessingOutput{Image: imgproc.Fit(in.Image, p.maxW, p.maxH), Faces: in.Faces}, nil
}

// ColorProcessor applies brightness/contrast/saturation adjustments.
type ColorProcessor struct {
	brightness, contrast, saturation float64
}

func NewColorProcessor(brightness, contrast, saturation float64) *ColorProcessor {
	return &ColorProcessor{brightness: brightness, contrast: contrast, saturation: saturation}
}

func (p *ColorProcessor) Name() string { return "color" }

func (p *ColorProcessor) Process(ctx context.Context, in ProcessingInput) (ProcessingOutput, error) {
	if err := ctx.Err(); err != nil {
		return ProcessingOutput{}, err
	}
	out := imgproc.AdjustColors(in.Image, p.brightness, p.contrast, p.saturation)
	return ProcessingOutput{Image: out, Faces: in.Faces}, nil
}

// BeautyProcessor is a light Gaussian smoothing blend — a blur, not ML retouch.
type BeautyProcessor struct {
	sigma, opacity float64
}

func NewBeautyProcessor(sigma, opacity float64) *BeautyProcessor {
	return &BeautyProcessor{sigma: sigma, opacity: opacity}
}

func (p *BeautyProcessor) Name() string { return "beauty" }

func (p *BeautyProcessor) Process(ctx context.Context, in ProcessingInput) (ProcessingOutput, error) {
	if err := ctx.Err(); err != nil {
		return ProcessingOutput{}, err
	}
	out := imgproc.Smooth(in.Image, p.sigma, p.opacity)
	return ProcessingOutput{Image: out, Faces: in.Faces}, nil
}

// BackgroundProcessor blurs the background, keeping detected faces sharp
// (portrait mode). No faces → no-op.
type BackgroundProcessor struct {
	sigma float64
}

func NewBackgroundProcessor(sigma float64) *BackgroundProcessor {
	return &BackgroundProcessor{sigma: sigma}
}

func (p *BackgroundProcessor) Name() string { return "background" }

func (p *BackgroundProcessor) Process(ctx context.Context, in ProcessingInput) (ProcessingOutput, error) {
	if err := ctx.Err(); err != nil {
		return ProcessingOutput{}, err
	}
	rects := make([]stdimage.Rectangle, len(in.Faces))
	for i, f := range in.Faces {
		rects[i] = f.Rect
	}
	out := imgproc.BlurBackground(in.Image, rects, p.sigma)
	return ProcessingOutput{Image: out, Faces: in.Faces}, nil
}

// DefaultRegistry builds the shipped pipelines. Add new versions here rather
// than mutating existing ones, so old jobs stay reproducible.
func DefaultRegistry(detector *imgproc.FaceDetector) *Registry {
	r := NewRegistry()
	r.Register(New("portrait-enhance", "v1",
		NewResizeProcessor(1600, 1600),
		NewFaceDetectionProcessor(detector, 60),
		NewColorProcessor(5, 8, 6),
		NewBeautyProcessor(2.0, 0.35),
		NewBackgroundProcessor(6.0),
	))
	return r
}
