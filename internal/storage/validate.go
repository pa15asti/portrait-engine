package storage

import (
	"errors"
	"fmt"
)

// ErrUnsupportedMedia means the content type is not an accepted image format.
var ErrUnsupportedMedia = errors.New("unsupported media type")

// ErrTooLarge means an object exceeds the configured size limit.
var ErrTooLarge = errors.New("object too large")

// contentTypeExt maps accepted upload content types to a file extension.
var contentTypeExt = map[string]string{
	"image/jpeg": ".jpg",
	"image/png":  ".png",
}

// ExtensionForContentType validates that contentType is a supported image
// format and returns its canonical extension.
func ExtensionForContentType(contentType string) (string, error) {
	ext, ok := contentTypeExt[contentType]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnsupportedMedia, contentType)
	}
	return ext, nil
}

// SupportedContentType reports whether contentType is an accepted image format.
func SupportedContentType(contentType string) bool {
	_, ok := contentTypeExt[contentType]
	return ok
}

// ValidateUploadRequest checks a requested upload's declared type and size
// before a presigned URL is issued.
func ValidateUploadRequest(contentType string, sizeBytes, maxBytes int64) error {
	if !SupportedContentType(contentType) {
		return fmt.Errorf("%w: %q", ErrUnsupportedMedia, contentType)
	}
	if sizeBytes > 0 && sizeBytes > maxBytes {
		return fmt.Errorf("%w: %d > %d", ErrTooLarge, sizeBytes, maxBytes)
	}
	return nil
}

// ValidateObject checks a stored object's actual metadata (as reported by the
// store) before it is accepted for processing.
func ValidateObject(info ObjectInfo, maxBytes int64) error {
	if info.Size <= 0 {
		return fmt.Errorf("%w: empty object", ErrUnsupportedMedia)
	}
	if info.Size > maxBytes {
		return fmt.Errorf("%w: %d > %d", ErrTooLarge, info.Size, maxBytes)
	}
	// Some clients omit content type on PUT; accept when absent, but reject an
	// explicit unsupported type.
	if info.ContentType != "" && info.ContentType != "application/octet-stream" &&
		!SupportedContentType(info.ContentType) {
		return fmt.Errorf("%w: %q", ErrUnsupportedMedia, info.ContentType)
	}
	return nil
}
