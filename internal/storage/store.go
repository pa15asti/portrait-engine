package storage

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrObjectNotFound is returned by Get/Stat when the object does not exist.
var ErrObjectNotFound = errors.New("object not found")

// PutOptions carries metadata for an upload.
type PutOptions struct {
	ContentType string
	// Size is the content length in bytes. -1 means unknown (streamed).
	Size int64
}

// ObjectInfo describes a stored object.
type ObjectInfo struct {
	Key          string
	Size         int64
	ContentType  string
	ETag         string
	LastModified time.Time
}

// ObjectStore abstracts blob storage (real: S3/MinIO). Keeps callers off the SDK
// and testable with fakes.
type ObjectStore interface {
	Put(ctx context.Context, key string, r io.Reader, opts PutOptions) error
	Get(ctx context.Context, key string) (io.ReadCloser, error) // caller closes
	Stat(ctx context.Context, key string) (ObjectInfo, error)   // ErrObjectNotFound if absent
	Delete(ctx context.Context, key string) error               // missing key is not an error
	PresignUpload(ctx context.Context, key string, expiry time.Duration) (string, error)
	PresignDownload(ctx context.Context, key string, expiry time.Duration) (string, error)
}
