//go:build integration

// Package e2e wires the real components (PostgreSQL, NATS JetStream, MinIO) and
// exercises the whole system end to end. Run with:
//
//	go test -tags=integration ./test/...
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/color"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/pa15asti/portrait-engine/internal/config"
	imgproc "github.com/pa15asti/portrait-engine/internal/image"
	"github.com/pa15asti/portrait-engine/internal/jobs"
	"github.com/pa15asti/portrait-engine/internal/messaging"
	"github.com/pa15asti/portrait-engine/internal/pipeline"
	"github.com/pa15asti/portrait-engine/internal/repository"
	"github.com/pa15asti/portrait-engine/internal/storage"
	"github.com/pa15asti/portrait-engine/internal/worker"
)

// harness holds the wired system for an e2e test.
type harness struct {
	repo    *repository.JobRepository
	store   *storage.MinioStore
	svc     *jobs.Service
	handler *worker.JobHandler
	broker  *messaging.Client

	poolDone chan struct{}
	poolStop context.CancelFunc
}

const (
	maxAttempts = 2
	failVersion = "fail"
	slowVersion = "slow"
)

// newHarness starts the containers, applies migrations, wires every component,
// and runs the worker pool. Everything is torn down via t.Cleanup.
func newHarness(t *testing.T) *harness {
	t.Helper()
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	pool := startPostgres(t)
	repo := repository.NewJobRepository(pool)
	store := startMinio(t)
	natsURL := startNATS(t)

	natsCfg := config.NATSConfig{
		URL: natsURL, Stream: "JOBS", Subject: "jobs.process",
		Durable: "e2e-workers", MaxDeliver: 10, AckWait: 30 * time.Second,
	}
	broker, err := messaging.Connect(natsCfg)
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	t.Cleanup(broker.Close)
	if err := broker.EnsureStream(ctx); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	svc := jobs.NewService(repo, store, broker.Publisher(), jobs.Config{
		MaxAttempts: maxAttempts, PresignExpiry: 15 * time.Minute, MaxUploadBytes: 15 << 20,
	})

	detector, err := imgproc.NewFaceDetector()
	if err != nil {
		t.Fatalf("face detector: %v", err)
	}
	registry := pipeline.DefaultRegistry(detector)
	// Extra pipelines for the failure and cancellation scenarios.
	registry.Register(pipeline.New("portrait-enhance", failVersion, failingProcessor{}))
	registry.Register(pipeline.New("portrait-enhance", slowVersion, slowProcessor{}))

	handler := worker.NewJobHandler(repo, store, registry, nil, log)
	consumer, err := broker.NewConsumer(ctx)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	pool2 := worker.NewPool(consumer, handler, worker.Options{
		Concurrency: 4, JobTimeout: 30 * time.Second, ShutdownTimeout: 10 * time.Second, Logger: log,
	})

	poolCtx, poolStop := context.WithCancel(context.Background())
	poolDone := make(chan struct{})
	go func() {
		_ = pool2.Run(poolCtx)
		close(poolDone)
	}()

	h := &harness{repo: repo, store: store, svc: svc, handler: handler, broker: broker, poolDone: poolDone, poolStop: poolStop}
	t.Cleanup(h.stopPool)
	return h
}

// stopPool triggers graceful shutdown and waits for the pool to drain — this
// doubles as the graceful-shutdown assertion.
func (h *harness) stopPool() {
	h.poolStop()
	select {
	case <-h.poolDone:
	case <-time.After(15 * time.Second):
		panic("worker pool did not shut down within timeout")
	}
}

// createAndUpload runs the real upload flow: presign, PUT the image, return the
// upload id.
func (h *harness) createAndUpload(t *testing.T) string {
	t.Helper()
	up, err := h.svc.CreateUpload(context.Background(), jobs.CreateUploadInput{ContentType: "image/jpeg", SizeBytes: 0})
	if err != nil {
		t.Fatalf("CreateUpload: %v", err)
	}
	body := jpegBytes(t)
	req, err := http.NewRequest(http.MethodPut, up.UploadURL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build PUT: %v", err)
	}
	req.Header.Set("Content-Type", "image/jpeg")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT to presigned url: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("presigned PUT status = %d", resp.StatusCode)
	}
	return up.UploadID
}

// waitStatus polls until the job reaches want, or fails after timeout.
func (h *harness) waitStatus(t *testing.T, id uuid.UUID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		job, err := h.repo.GetJob(context.Background(), id)
		if err == nil {
			last = string(job.Status)
			if last == want {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach %s within %s (last: %s)", id, want, timeout, last)
}

// --- test processors ---

type failingProcessor struct{}

func (failingProcessor) Name() string { return "always-fail" }
func (failingProcessor) Process(context.Context, pipeline.ProcessingInput) (pipeline.ProcessingOutput, error) {
	return pipeline.ProcessingOutput{}, fmt.Errorf("simulated transient failure")
}

type slowProcessor struct{}

func (slowProcessor) Name() string { return "slow" }
func (slowProcessor) Process(ctx context.Context, in pipeline.ProcessingInput) (pipeline.ProcessingOutput, error) {
	select {
	case <-time.After(10 * time.Second):
		return pipeline.ProcessingOutput(in), nil
	case <-ctx.Done():
		return pipeline.ProcessingOutput{}, ctx.Err()
	}
}

// --- helpers ---

func jpegBytes(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 200, 200))
	for y := 0; y < 200; y++ {
		for x := 0; x < 200; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x), G: uint8(y), B: 120, A: 255})
		}
	}
	var buf bytes.Buffer
	if err := imgproc.Encode(&buf, img, imgproc.JPEG, 90); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return buf.Bytes()
}

func startPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	c, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("portrait"), postgres.WithUsername("portrait"), postgres.WithPassword("portrait"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp").WithStartupTimeout(90*time.Second)),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	sql, err := os.ReadFile("../migrations/000001_init.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(sql)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func startMinio(t *testing.T) *storage.MinioStore {
	t.Helper()
	ctx := context.Background()
	c, err := tcminio.Run(ctx, "minio/minio:latest")
	if err != nil {
		t.Fatalf("start minio: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	endpoint, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("minio endpoint: %v", err)
	}
	cfg := config.StorageConfig{
		Endpoint: endpoint, AccessKey: c.Username, SecretKey: c.Password,
		Bucket: "portraits", UseSSL: false, PresignExpiry: 15 * time.Minute, MaxUploadBytes: 15 << 20,
	}
	admin, err := miniogo.New(endpoint, &miniogo.Options{
		Creds: credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""), Secure: false,
	})
	if err != nil {
		t.Fatalf("minio admin: %v", err)
	}
	if err := admin.MakeBucket(ctx, cfg.Bucket, miniogo.MakeBucketOptions{}); err != nil {
		t.Fatalf("make bucket: %v", err)
	}
	store, err := storage.NewMinioStore(ctx, cfg)
	if err != nil {
		t.Fatalf("minio store: %v", err)
	}
	return store
}

func startNATS(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "nats:2.10-alpine",
			ExposedPorts: []string{"4222/tcp"},
			Cmd:          []string{"-js", "-m", "8222"},
			WaitingFor:   wait.ForListeningPort("4222/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start nats: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("nats host: %v", err)
	}
	port, err := c.MappedPort(ctx, "4222/tcp")
	if err != nil {
		t.Fatalf("nats port: %v", err)
	}
	return fmt.Sprintf("nats://%s:%s", host, port.Port())
}
