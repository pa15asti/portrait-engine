//go:build integration

package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/pa15asti/portrait-engine/internal/config"
)

func newTestStore(t *testing.T) *MinioStore {
	t.Helper()
	ctx := context.Background()

	container, err := tcminio.Run(ctx, "minio/minio:latest")
	if err != nil {
		t.Fatalf("start minio container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	cfg := config.StorageConfig{
		Endpoint:       endpoint,
		AccessKey:      container.Username,
		SecretKey:      container.Password,
		Bucket:         "portraits",
		UseSSL:         false,
		PresignExpiry:  15 * time.Minute,
		MaxUploadBytes: 15 << 20,
	}

	// Create the bucket before constructing the store (which asserts it exists).
	admin, err := miniogo.New(endpoint, &miniogo.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	if err := admin.MakeBucket(ctx, cfg.Bucket, miniogo.MakeBucketOptions{}); err != nil {
		t.Fatalf("make bucket: %v", err)
	}

	store, err := NewMinioStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewMinioStore: %v", err)
	}
	return store
}

func TestMinioStore_RoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	key := GenerateInputKey(".jpg")
	payload := []byte("not-really-an-image-but-bytes")

	if err := store.Put(ctx, key, bytes.NewReader(payload), PutOptions{
		ContentType: "image/jpeg",
		Size:        int64(len(payload)),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	info, err := store.Stat(ctx, key)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", info.Size, len(payload))
	}
	if info.ContentType != "image/jpeg" {
		t.Errorf("content type = %q, want image/jpeg", info.ContentType)
	}

	rc, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload round-trip mismatch")
	}

	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Stat(ctx, key); !errors.Is(err, ErrObjectNotFound) {
		t.Errorf("expected ErrObjectNotFound after delete, got %v", err)
	}
	if _, err := store.Get(ctx, key); !errors.Is(err, ErrObjectNotFound) {
		t.Errorf("expected ErrObjectNotFound on Get after delete, got %v", err)
	}
}

func TestMinioStore_PresignUploadWorks(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	key := GenerateInputKey(".png")
	url, err := store.PresignUpload(ctx, key, 10*time.Minute)
	if err != nil {
		t.Fatalf("PresignUpload: %v", err)
	}
	if !strings.HasPrefix(url, "http") {
		t.Fatalf("unexpected presigned url: %q", url)
	}

	// A client can PUT bytes directly to the presigned URL.
	payload := []byte("presigned-bytes")
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT to presigned url: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("presigned PUT status = %d", resp.StatusCode)
	}

	info, err := store.Stat(ctx, key)
	if err != nil {
		t.Fatalf("Stat after presigned PUT: %v", err)
	}
	if info.Size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", info.Size, len(payload))
	}
}
