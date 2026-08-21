//go:build integration

package repository

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/pa15asti/portrait-engine/internal/domain"
)

// newTestPool starts a throwaway PostgreSQL container, applies the schema, and
// returns a ready pool. Cleanup is registered on t.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("portrait"),
		postgres.WithUsername("portrait"),
		postgres.WithPassword("portrait"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5432/tcp").WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	migration, err := os.ReadFile("../../migrations/000001_init.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	// No bind args -> pgx uses the simple protocol, which runs the multi-
	// statement migration file in one call.
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	return pool
}

func sampleParams() NewJobParams {
	return NewJobParams{
		ID:              uuid.New(),
		Pipeline:        "portrait-enhance",
		PipelineVersion: "v1",
		InputObjectKey:  "uploads/2026/08/abc.jpg",
		CorrelationID:   "corr-1",
		MaxAttempts:     5,
	}
}

func TestCreateAndGetJob(t *testing.T) {
	ctx := context.Background()
	repo := NewJobRepository(newTestPool(t))

	p := sampleParams()
	created, isNew, err := repo.CreateJob(ctx, p, "", "")
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if !isNew {
		t.Fatal("expected created=true")
	}
	if created.Status != domain.StatusPending {
		t.Errorf("status = %s, want PENDING", created.Status)
	}

	got, err := repo.GetJob(ctx, p.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.ID != p.ID || got.Pipeline != p.Pipeline || got.MaxAttempts != 5 {
		t.Errorf("unexpected job: %+v", got)
	}

	if _, err := repo.GetJob(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for missing job, got %v", err)
	}
}

func TestUpdateJobStatus_GuardedAndInvalid(t *testing.T) {
	ctx := context.Background()
	repo := NewJobRepository(newTestPool(t))

	p := sampleParams()
	if _, _, err := repo.CreateJob(ctx, p, "", ""); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	// Valid transition PENDING -> QUEUED.
	job, err := repo.UpdateJobStatus(ctx, p.ID, domain.StatusPending, JobUpdate{To: domain.StatusQueued})
	if err != nil {
		t.Fatalf("PENDING->QUEUED: %v", err)
	}
	if job.Status != domain.StatusQueued {
		t.Errorf("status = %s, want QUEUED", job.Status)
	}

	// Invalid transition QUEUED -> COMPLETED is rejected by the state machine.
	if _, err := repo.UpdateJobStatus(ctx, p.ID, domain.StatusQueued, JobUpdate{To: domain.StatusCompleted}); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("expected ErrInvalidTransition, got %v", err)
	}

	// Guarded conflict: claim from a stale 'from' state. The job is QUEUED, but
	// we pass from=PENDING for a state-machine-valid transition (PENDING->
	// CANCELLED) — the guard must reject it as a conflict.
	if _, err := repo.UpdateJobStatus(ctx, p.ID, domain.StatusPending, JobUpdate{To: domain.StatusCancelled}); !errors.Is(err, ErrConflict) {
		t.Errorf("expected ErrConflict for stale from-state, got %v", err)
	}

	// Missing job.
	if _, err := repo.UpdateJobStatus(ctx, uuid.New(), domain.StatusPending, JobUpdate{To: domain.StatusQueued}); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestConcurrentIdempotentCreate(t *testing.T) {
	ctx := context.Background()
	repo := NewJobRepository(newTestPool(t))

	const n = 12
	const key = "idem-key-1"
	const hash = "hash-1"

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		ids      = map[uuid.UUID]struct{}{}
		newCount int
		errCount int
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			job, isNew, err := repo.CreateJob(ctx, sampleParams(), key, hash)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errCount++
				return
			}
			ids[job.ID] = struct{}{}
			if isNew {
				newCount++
			}
		}()
	}
	wg.Wait()

	if errCount != 0 {
		t.Fatalf("unexpected errors: %d", errCount)
	}
	if len(ids) != 1 {
		t.Errorf("expected all requests to resolve to one job, got %d distinct ids", len(ids))
	}
	if newCount != 1 {
		t.Errorf("expected exactly one creator, got %d", newCount)
	}
}

func TestIdempotencyConflict(t *testing.T) {
	ctx := context.Background()
	repo := NewJobRepository(newTestPool(t))

	if _, _, err := repo.CreateJob(ctx, sampleParams(), "k", "hash-A"); err != nil {
		t.Fatalf("first create: %v", err)
	}
	// Same key, different request body hash -> conflict.
	if _, _, err := repo.CreateJob(ctx, sampleParams(), "k", "hash-B"); !errors.Is(err, ErrIdempotencyConflict) {
		t.Errorf("expected ErrIdempotencyConflict, got %v", err)
	}
	// Same key, same hash -> returns original, created=false.
	_, isNew, err := repo.CreateJob(ctx, sampleParams(), "k", "hash-A")
	if err != nil {
		t.Fatalf("replay create: %v", err)
	}
	if isNew {
		t.Error("expected created=false on idempotent replay")
	}
}

func TestBeginAttempt_Guard(t *testing.T) {
	ctx := context.Background()
	repo := NewJobRepository(newTestPool(t))

	p := sampleParams()
	if _, _, err := repo.CreateJob(ctx, p, "", ""); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	// BeginAttempt only claims QUEUED jobs; PENDING must fail as a conflict.
	if _, _, err := repo.BeginAttempt(ctx, p.ID); !errors.Is(err, ErrConflict) {
		t.Errorf("expected ErrConflict claiming PENDING job, got %v", err)
	}

	if _, err := repo.UpdateJobStatus(ctx, p.ID, domain.StatusPending, JobUpdate{To: domain.StatusQueued}); err != nil {
		t.Fatalf("queue job: %v", err)
	}

	job, attempt, err := repo.BeginAttempt(ctx, p.ID)
	if err != nil {
		t.Fatalf("BeginAttempt: %v", err)
	}
	if job.Status != domain.StatusProcessing || job.AttemptCount != 1 {
		t.Errorf("unexpected job after claim: status=%s attempts=%d", job.Status, job.AttemptCount)
	}
	if attempt.Number != 1 || attempt.Status != domain.AttemptRunning {
		t.Errorf("unexpected attempt: %+v", attempt)
	}

	// A second claim on the now-PROCESSING job must conflict (no double claim).
	if _, _, err := repo.BeginAttempt(ctx, p.ID); !errors.Is(err, ErrConflict) {
		t.Errorf("expected ErrConflict on double claim, got %v", err)
	}

	// Finish the attempt.
	if err := repo.FinishAttempt(ctx, attempt.ID, domain.AttemptSucceeded, ""); err != nil {
		t.Fatalf("FinishAttempt: %v", err)
	}
}

func TestListRequeueable(t *testing.T) {
	ctx := context.Background()
	repo := NewJobRepository(newTestPool(t))

	// A QUEUED job is a requeue candidate.
	p := sampleParams()
	if _, _, err := repo.CreateJob(ctx, p, "", ""); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := repo.UpdateJobStatus(ctx, p.ID, domain.StatusPending, JobUpdate{To: domain.StatusQueued}); err != nil {
		t.Fatalf("queue job: %v", err)
	}

	// A still-PENDING job must never be requeued.
	other := sampleParams()
	if _, _, err := repo.CreateJob(ctx, other, "", ""); err != nil {
		t.Fatalf("CreateJob other: %v", err)
	}

	// Future cutoff includes the QUEUED job (updated_at < cutoff).
	got, err := repo.ListRequeueable(ctx, time.Now().Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("ListRequeueable: %v", err)
	}
	if len(got) != 1 || got[0].ID != p.ID {
		t.Fatalf("expected only the QUEUED job, got %d rows", len(got))
	}

	// Past cutoff excludes it (updated_at is newer than the cutoff).
	got, err = repo.ListRequeueable(ctx, time.Now().Add(-time.Hour), 100)
	if err != nil {
		t.Fatalf("ListRequeueable past: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected no rows for a past cutoff, got %d", len(got))
	}
}

func TestArtifacts_AddListOverwrite(t *testing.T) {
	ctx := context.Background()
	repo := NewJobRepository(newTestPool(t))

	p := sampleParams()
	if _, _, err := repo.CreateJob(ctx, p, "", ""); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	a := domain.Artifact{
		JobID:       p.ID,
		Kind:        "output",
		ObjectKey:   "outputs/" + p.ID.String() + "/result.jpg",
		ContentType: "image/jpeg",
		SizeBytes:   1234,
	}
	if err := repo.AddArtifact(ctx, a); err != nil {
		t.Fatalf("AddArtifact: %v", err)
	}
	// Re-processing writes the same key with a new size -> overwrite, not dup.
	a.SizeBytes = 5678
	if err := repo.AddArtifact(ctx, a); err != nil {
		t.Fatalf("AddArtifact overwrite: %v", err)
	}

	list, err := repo.ListArtifacts(ctx, p.ID)
	if err != nil {
		t.Fatalf("ListArtifacts: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 artifact after overwrite, got %d", len(list))
	}
	if list[0].SizeBytes != 5678 {
		t.Errorf("expected overwritten size 5678, got %d", list[0].SizeBytes)
	}
}
