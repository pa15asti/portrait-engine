package image

import (
	stdimage "image"

	"github.com/disintegration/imaging"
)

// Fit scales img down to fit within maxW x maxH, preserving aspect ratio. It
// never enlarges beyond the original.
func Fit(img stdimage.Image, maxW, maxH int) stdimage.Image {
	b := img.Bounds()
	if b.Dx() <= maxW && b.Dy() <= maxH {
		return img
	}
	return imaging.Fit(img, maxW, maxH, imaging.Lanczos)
}

// AdjustColors applies brightness, contrast, and saturation deltas (percent,
// -100..100). Zero values are no-ops.
func AdjustColors(img stdimage.Image, brightness, contrast, saturation float64) stdimage.Image {
	out := imaging.AdjustBrightness(img, brightness)
	out = imaging.AdjustContrast(out, contrast)
	out = imaging.AdjustSaturation(out, saturation)
	return out
}

// Smooth blends a blurred copy over the original at opacity (0..1) — a blur
// blend, not ML retouch.
func Smooth(img stdimage.Image, sigma, opacity float64) stdimage.Image {
	blurred := imaging.Blur(img, sigma)
	return imaging.Overlay(img, blurred, stdimage.Pt(0, 0), opacity)
}

// BlurBackground blurs the whole image, then pastes the sharp face regions back
// on top — portrait-style blur keyed on the faces. No faces → unchanged.
func BlurBackground(img stdimage.Image, faces []stdimage.Rectangle, sigma float64) stdimage.Image {
	if len(faces) == 0 {
		return img
	}
	bounds := img.Bounds()
	result := imaging.Blur(img, sigma)
	for _, face := range faces {
		region := expand(face, 0.35).Intersect(bounds)
		if region.Empty() {
			continue
		}
		sharp := imaging.Crop(img, region)
		result = imaging.Paste(result, sharp, region.Min)
	}
	return result
}

// expand grows r by frac of its size on each side.
func expand(r stdimage.Rectangle, frac float64) stdimage.Rectangle {
	dx := int(float64(r.Dx()) * frac)
	dy := int(float64(r.Dy()) * frac)
	return stdimage.Rect(r.Min.X-dx, r.Min.Y-dy, r.Max.X+dx, r.Max.Y+dy)
}
