// Package image holds the pure-Go image ops: decode/encode, transforms, and
// Pigo face detection. No cgo, so binaries stay static and processing is
// deterministic. (Inside this file, "image" is the stdlib package.)
package image

import (
	"bytes"
	"errors"
	"fmt"
	stdimage "image"
	"io"

	"github.com/disintegration/imaging"
)

// Format identifies an encodable output format.
type Format int

const (
	JPEG Format = iota
	PNG
)

var ErrImageTooLarge = errors.New("image too large")

// Decompression-bomb guards (vars so tests can lower them): a small file can
// expand into a huge bitmap. Bound both the encoded bytes and the pixel count.
var (
	MaxDecodeBytes int64 = 64 << 20   // 64 MiB encoded
	MaxPixels      int64 = 30_000_000 // 30 MP (~120 MB as RGBA)
)

// Decode reads an image (EXIF auto-orient) after bounding its encoded size and
// pixel dimensions.
func Decode(r io.Reader) (stdimage.Image, error) {
	data, err := io.ReadAll(io.LimitReader(r, MaxDecodeBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read image: %w", err)
	}
	if int64(len(data)) > MaxDecodeBytes {
		return nil, fmt.Errorf("%w: encoded size exceeds %d bytes", ErrImageTooLarge, MaxDecodeBytes)
	}

	// Check dimensions before allocating the bitmap.
	cfg, _, err := stdimage.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("decode image header: %w", err)
	}
	if int64(cfg.Width)*int64(cfg.Height) > MaxPixels {
		return nil, fmt.Errorf("%w: %dx%d exceeds %d pixels", ErrImageTooLarge, cfg.Width, cfg.Height, MaxPixels)
	}

	img, err := imaging.Decode(bytes.NewReader(data), imaging.AutoOrientation(true))
	if err != nil {
		return nil, fmt.Errorf("decode image: %w", err)
	}
	return img, nil
}

// Encode writes img in the given format. quality applies to JPEG (1-100).
func Encode(w io.Writer, img stdimage.Image, format Format, quality int) error {
	switch format {
	case PNG:
		return imaging.Encode(w, img, imaging.PNG)
	default:
		if quality <= 0 || quality > 100 {
			quality = 90
		}
		return imaging.Encode(w, img, imaging.JPEG, imaging.JPEGQuality(quality))
	}
}
