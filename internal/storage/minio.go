package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/pa15asti/portrait-engine/internal/config"
)

// span starts an S3 operation span.
func span(ctx context.Context, op, key string) (context.Context, func()) {
	ctx, s := otel.Tracer("portrait/storage").Start(ctx, "s3."+op)
	s.SetAttributes(attribute.String("s3.key", key))
	return ctx, func() { s.End() }
}

// MinioStore is an ObjectStore over any S3-compatible service. Two clients: one
// in-network, one for signing presigned URLs against a public endpoint so those
// URLs are reachable outside the container network.
type MinioStore struct {
	client        *minio.Client
	presignClient *minio.Client
	bucket        string
}

// NewMinioStore connects to the object store and verifies the bucket exists.
func NewMinioStore(ctx context.Context, cfg config.StorageConfig) (*MinioStore, error) {
	creds := credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, "")

	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  creds,
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("create minio client: %w", err)
	}

	presignClient := client
	if cfg.PublicEndpoint != "" && cfg.PublicEndpoint != cfg.Endpoint {
		presignClient, err = minio.New(cfg.PublicEndpoint, &minio.Options{
			Creds:  creds,
			Secure: cfg.UseSSL,
			Region: cfg.Region,
		})
		if err != nil {
			return nil, fmt.Errorf("create presign client: %w", err)
		}
	}

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("check bucket %q: %w", cfg.Bucket, err)
	}
	if !exists {
		return nil, fmt.Errorf("bucket %q does not exist", cfg.Bucket)
	}

	return &MinioStore{client: client, presignClient: presignClient, bucket: cfg.Bucket}, nil
}

func (s *MinioStore) Put(ctx context.Context, key string, r io.Reader, opts PutOptions) error {
	ctx, end := span(ctx, "put", key)
	defer end()
	_, err := s.client.PutObject(ctx, s.bucket, key, r, opts.Size, minio.PutObjectOptions{
		ContentType: opts.ContentType,
	})
	if err != nil {
		return fmt.Errorf("put %q: %w", key, err)
	}
	return nil
}

func (s *MinioStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	ctx, end := span(ctx, "get", key)
	defer end()
	// Stat first so a missing object is ErrObjectNotFound now, not lazily on Read.
	if _, err := s.Stat(ctx, key); err != nil {
		return nil, err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get %q: %w", key, err)
	}
	return obj, nil
}

func (s *MinioStore) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	ctx, end := span(ctx, "stat", key)
	defer end()
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if isNotFound(err) {
			return ObjectInfo{}, ErrObjectNotFound
		}
		return ObjectInfo{}, fmt.Errorf("stat %q: %w", key, err)
	}
	return ObjectInfo{
		Key:          key,
		Size:         info.Size,
		ContentType:  info.ContentType,
		ETag:         info.ETag,
		LastModified: info.LastModified,
	}, nil
}

func (s *MinioStore) Delete(ctx context.Context, key string) error {
	if err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("delete %q: %w", key, err)
	}
	return nil
}

func (s *MinioStore) PresignUpload(ctx context.Context, key string, expiry time.Duration) (string, error) {
	u, err := s.presignClient.PresignedPutObject(ctx, s.bucket, key, expiry)
	if err != nil {
		return "", fmt.Errorf("presign upload %q: %w", key, err)
	}
	return u.String(), nil
}

func (s *MinioStore) PresignDownload(ctx context.Context, key string, expiry time.Duration) (string, error) {
	u, err := s.presignClient.PresignedGetObject(ctx, s.bucket, key, expiry, nil)
	if err != nil {
		return "", fmt.Errorf("presign download %q: %w", key, err)
	}
	return u.String(), nil
}

// isNotFound reports whether err is an S3 "no such key" error.
func isNotFound(err error) bool {
	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		return resp.Code == "NoSuchKey" || resp.StatusCode == 404
	}
	return false
}
