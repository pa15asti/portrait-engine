package image

import (
	"bytes"
	"errors"
	"image"
	"image/color"
	"testing"
)

func synthetic(w, h int) image.Image {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x % 256), G: uint8(y % 256), B: 128, A: 255})
		}
	}
	return img
}

func TestEncodeDecode_RoundTrip(t *testing.T) {
	src := synthetic(32, 24)

	var buf bytes.Buffer
	if err := Encode(&buf, src, JPEG, 90); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if buf.Len() == 0 {
		t.Fatal("encoded output is empty")
	}
	got, err := Decode(&buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Bounds().Dx() != 32 || got.Bounds().Dy() != 24 {
		t.Errorf("decoded bounds = %v, want 32x24", got.Bounds())
	}
}

func TestDecode_InvalidBytes(t *testing.T) {
	if _, err := Decode(bytes.NewReader([]byte("not an image"))); err == nil {
		t.Error("expected decode error for non-image bytes")
	}
}

func TestDecode_RejectsHugeDimensions(t *testing.T) {
	old := MaxPixels
	MaxPixels = 100 // 10x10
	defer func() { MaxPixels = old }()

	var buf bytes.Buffer
	if err := Encode(&buf, synthetic(64, 64), PNG, 0); err != nil { // 4096 pixels > 100
		t.Fatalf("encode: %v", err)
	}
	_, err := Decode(&buf)
	if !errors.Is(err, ErrImageTooLarge) {
		t.Errorf("expected ErrImageTooLarge for oversize dimensions, got %v", err)
	}
}

func TestDecode_RejectsHugeByteStream(t *testing.T) {
	old := MaxDecodeBytes
	MaxDecodeBytes = 16
	defer func() { MaxDecodeBytes = old }()

	var buf bytes.Buffer
	if err := Encode(&buf, synthetic(32, 32), JPEG, 90); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if buf.Len() <= 16 {
		t.Skip("encoded image unexpectedly small")
	}
	_, err := Decode(&buf)
	if !errors.Is(err, ErrImageTooLarge) {
		t.Errorf("expected ErrImageTooLarge for oversize byte stream, got %v", err)
	}
}

func TestFit_DownscalesOnly(t *testing.T) {
	big := synthetic(400, 200)
	fit := Fit(big, 100, 100)
	if fit.Bounds().Dx() > 100 || fit.Bounds().Dy() > 100 {
		t.Errorf("Fit did not constrain size: %v", fit.Bounds())
	}
	// Already-small images are returned unchanged.
	small := synthetic(50, 50)
	if got := Fit(small, 100, 100); got.Bounds() != small.Bounds() {
		t.Errorf("Fit enlarged a small image: %v", got.Bounds())
	}
}

func TestTransforms_ProduceImages(t *testing.T) {
	src := synthetic(40, 40)
	if out := AdjustColors(src, 5, 5, 5); out.Bounds().Empty() {
		t.Error("AdjustColors produced an empty image")
	}
	if out := Smooth(src, 2, 0.3); out.Bounds().Empty() {
		t.Error("Smooth produced an empty image")
	}
	faces := []image.Rectangle{image.Rect(10, 10, 20, 20)}
	if out := BlurBackground(src, faces, 4); out.Bounds().Empty() {
		t.Error("BlurBackground produced an empty image")
	}
	// No faces -> unchanged.
	if out := BlurBackground(src, nil, 4); out != src {
		t.Error("BlurBackground with no faces should return the input unchanged")
	}
}

func TestFaceDetector_RunsWithoutError(t *testing.T) {
	det, err := NewFaceDetector()
	if err != nil {
		t.Fatalf("NewFaceDetector: %v", err)
	}
	// A synthetic gradient contains no real faces; we only assert the detector
	// runs and returns a (possibly empty) result without panicking.
	faces := det.Detect(synthetic(120, 120), 40)
	if faces == nil {
		faces = []DetectedFace{}
	}
	for _, f := range faces {
		if f.Score < 0 {
			t.Errorf("unexpected negative score: %v", f.Score)
		}
	}
}
