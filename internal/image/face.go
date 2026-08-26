package image

import (
	_ "embed"
	"fmt"
	stdimage "image"

	pigo "github.com/esimov/pigo/core"
)

// Pigo cascade, embedded so the binary is self-contained.
//
//go:embed cascade/facefinder
var cascadeData []byte

type DetectedFace struct {
	Rect  stdimage.Rectangle
	Score float32
}

// FaceDetector wraps a loaded Pigo classifier. Concurrency-safe: RunCascade
// doesn't mutate it.
type FaceDetector struct {
	classifier *pigo.Pigo
	minQuality float32 // Pigo scores run ~0..30+
}

// NewFaceDetector loads the embedded cascade.
func NewFaceDetector() (*FaceDetector, error) {
	classifier, err := pigo.NewPigo().Unpack(cascadeData)
	if err != nil {
		return nil, fmt.Errorf("unpack face cascade: %w", err)
	}
	return &FaceDetector{classifier: classifier, minQuality: 5.0}, nil
}

// Detect finds faces at least minSize px across, clustered and quality-filtered.
func (d *FaceDetector) Detect(img stdimage.Image, minSize int) []DetectedFace {
	pixels, w, h := grayscale(img)
	if w == 0 || h == 0 {
		return nil
	}
	maxSize := h
	if w < h {
		maxSize = w
	}
	if minSize < 20 {
		minSize = 20
	}

	params := pigo.CascadeParams{
		MinSize:     minSize,
		MaxSize:     maxSize,
		ShiftFactor: 0.1,
		ScaleFactor: 1.1,
		ImageParams: pigo.ImageParams{
			Pixels: pixels,
			Rows:   h,
			Cols:   w,
			Dim:    w,
		},
	}

	dets := d.classifier.RunCascade(params, 0.0)
	dets = d.classifier.ClusterDetections(dets, 0.2)

	faces := make([]DetectedFace, 0, len(dets))
	for _, det := range dets {
		if det.Q < d.minQuality {
			continue
		}
		half := det.Scale / 2
		faces = append(faces, DetectedFace{
			Rect:  stdimage.Rect(det.Col-half, det.Row-half, det.Col+half, det.Row+half),
			Score: det.Q,
		})
	}
	return faces
}

// grayscale converts to the row-major 8-bit luminance buffer Pigo wants.
func grayscale(img stdimage.Image) (pixels []uint8, w, h int) {
	b := img.Bounds()
	w, h = b.Dx(), b.Dy()
	if w == 0 || h == 0 {
		return nil, 0, 0
	}
	pixels = make([]uint8, w*h)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			r, g, bl, _ := img.At(b.Min.X+x, b.Min.Y+y).RGBA()
			// Rec. 601 luma; RGBA() returns 16-bit channels, shift to 8-bit.
			lum := (299*(r>>8) + 587*(g>>8) + 114*(bl>>8)) / 1000
			pixels[y*w+x] = uint8(lum)
		}
	}
	return pixels, w, h
}
