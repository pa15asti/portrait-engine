// Package storage names and stores objects. Object keys are server-minted under
// a fixed prefix and validated on the way back in — client filenames never
// become storage paths (no traversal, no writing outside our namespace).
package storage

import (
	"encoding/base64"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
)

// InputPrefix is the namespace for uploaded source images.
const InputPrefix = "uploads/"

// OutputPrefix is the namespace for produced artifacts.
const OutputPrefix = "outputs/"

// ErrInvalidKey means an object key is malformed or escapes its namespace.
var ErrInvalidKey = errors.New("invalid object key")

// supportedExtensions maps accepted upload extensions to a canonical form.
var supportedExtensions = map[string]string{
	".jpg":  ".jpg",
	".jpeg": ".jpg",
	".png":  ".png",
}

// GenerateInputKey mints a fresh, collision-resistant object key for an upload
// with the given (dotted) extension, e.g. "uploads/2026/08/<uuid>.jpg".
// Unknown extensions default to .jpg.
func GenerateInputKey(ext string) string {
	canonical, ok := supportedExtensions[strings.ToLower(ext)]
	if !ok {
		canonical = ".jpg"
	}
	now := time.Now().UTC()
	return fmt.Sprintf("%s%04d/%02d/%s%s", InputPrefix, now.Year(), int(now.Month()), uuid.NewString(), canonical)
}

// GenerateOutputKey mints a deterministic key for a job's artifact so that
// re-processing overwrites rather than duplicating.
func GenerateOutputKey(jobID uuid.UUID, name string) string {
	return fmt.Sprintf("%s%s/%s", OutputPrefix, jobID.String(), name)
}

// EncodeUploadID returns an opaque, URL-safe token for an object key. Clients
// treat it as opaque; the server decodes it back to the key.
func EncodeUploadID(key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(key))
}

// DecodeUploadID recovers and validates the object key from an upload token.
func DecodeUploadID(token string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", fmt.Errorf("%w: undecodable upload id", ErrInvalidKey)
	}
	key := string(raw)
	if err := ValidateInputKey(key); err != nil {
		return "", err
	}
	return key, nil
}

// ValidateInputKey ensures key is a clean path within the input namespace.
func ValidateInputKey(key string) error {
	if !strings.HasPrefix(key, InputPrefix) {
		return fmt.Errorf("%w: not in input namespace", ErrInvalidKey)
	}
	if strings.Contains(key, "..") || strings.HasPrefix(key, "/") || strings.Contains(key, "//") {
		return fmt.Errorf("%w: path traversal", ErrInvalidKey)
	}
	// Reject control characters and ensure the path is already clean.
	if key != path.Clean(key) {
		return fmt.Errorf("%w: not a clean path", ErrInvalidKey)
	}
	for _, r := range key {
		if r < 0x20 {
			return fmt.Errorf("%w: control character", ErrInvalidKey)
		}
	}
	return nil
}
