package jobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/pa15asti/portrait-engine/internal/domain"
	"github.com/pa15asti/portrait-engine/internal/messaging"
	"github.com/pa15asti/portrait-engine/internal/repository"
	"github.com/pa15asti/portrait-engine/internal/storage"
)

type fakeRepo struct {
	createFn func(ctx context.Context, p repository.NewJobParams, key, hash string) (domain.Job, bool, error)
	getFn    func(ctx context.Context, id uuid.UUID) (domain.Job, error)
	updateFn func(ctx context.Context, id uuid.UUID, from domain.JobStatus, upd repository.JobUpdate) (domain.Job, error)
	listFn   func(ctx context.Context, id uuid.UUID) ([]domain.Artifact, error)
}

func (f *fakeRepo) CreateJob(ctx context.Context, p repository.NewJobParams, key, hash string) (domain.Job, bool, error) {
	return f.createFn(ctx, p, key, hash)
}
func (f *fakeRepo) GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	return f.getFn(ctx, id)
}
func (f *fakeRepo) UpdateJobStatus(ctx context.Context, id uuid.UUID, from domain.JobStatus, upd repository.JobUpdate) (domain.Job, error) {
	return f.updateFn(ctx, id, from, upd)
}
func (f *fakeRepo) ListArtifacts(ctx context.Context, id uuid.UUID) ([]domain.Artifact, error) {
	return f.listFn(ctx, id)
}

type fakeStore struct {
	presignFn func(ctx context.Context, key string, expiry time.Duration) (string, error)
	statFn    func(ctx context.Context, key string) (storage.ObjectInfo, error)
}

func (f *fakeStore) PresignUpload(ctx context.Context, key string, expiry time.Duration) (string, error) {
	return f.presignFn(ctx, key, expiry)
}
func (f *fakeStore) Stat(ctx context.Context, key string) (storage.ObjectInfo, error) {
	return f.statFn(ctx, key)
}

// okStore returns a store whose Stat reports a valid image object.
func okStore() *fakeStore {
	return &fakeStore{
		presignFn: func(_ context.Context, key string, _ time.Duration) (string, error) {
			return "https://example/" + key, nil
		},
		statFn: func(_ context.Context, key string) (storage.ObjectInfo, error) {
			return storage.ObjectInfo{Key: key, Size: 2048, ContentType: "image/jpeg"}, nil
		},
	}
}

type fakePublisher struct {
	publishFn func(ctx context.Context, m messaging.JobMessage) error
}

func (f *fakePublisher) Publish(ctx context.Context, m messaging.JobMessage) error {
	return f.publishFn(ctx, m)
}

// okPublisher accepts every publish.
func okPublisher() *fakePublisher {
	return &fakePublisher{publishFn: func(context.Context, messaging.JobMessage) error { return nil }}
}

func newSvc(repo Repository, store ObjectStore, pub Publisher, maxAttempts int) *Service {
	return NewService(repo, store, pub, Config{
		MaxAttempts:    maxAttempts,
		PresignExpiry:  15 * time.Minute,
		MaxUploadBytes: 1 << 20,
	})
}

func validUploadID(t *testing.T) (token, key string) {
	t.Helper()
	key = storage.GenerateInputKey(".jpg")
	return storage.EncodeUploadID(key), key
}

func TestCreateUpload(t *testing.T) {
	var presignedKey string
	store := &fakeStore{
		presignFn: func(_ context.Context, key string, _ time.Duration) (string, error) {
			presignedKey = key
			return "https://example/" + key, nil
		},
	}
	svc := newSvc(&fakeRepo{}, store, okPublisher(), 5)

	up, err := svc.CreateUpload(context.Background(), CreateUploadInput{ContentType: "image/png", SizeBytes: 1000})
	if err != nil {
		t.Fatalf("CreateUpload: %v", err)
	}
	if up.ObjectKey != presignedKey {
		t.Errorf("presigned key %q != returned object key %q", presignedKey, up.ObjectKey)
	}
	// The upload id must decode back to the object key.
	decoded, err := storage.DecodeUploadID(up.UploadID)
	if err != nil || decoded != up.ObjectKey {
		t.Errorf("upload id does not round-trip: %v (%q)", err, decoded)
	}

	// Unsupported media type is rejected before presigning.
	if _, err := svc.CreateUpload(context.Background(), CreateUploadInput{ContentType: "image/gif"}); !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation for gif, got %v", err)
	}
	// Oversized request is rejected.
	if _, err := svc.CreateUpload(context.Background(), CreateUploadInput{ContentType: "image/jpeg", SizeBytes: 1 << 30}); !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation for oversized, got %v", err)
	}
}

func TestCreateJob_ValidationErrors(t *testing.T) {
	token, _ := validUploadID(t)
	repo := &fakeRepo{createFn: func(context.Context, repository.NewJobParams, string, string) (domain.Job, bool, error) {
		t.Fatal("repo.CreateJob must not be called on validation failure")
		return domain.Job{}, false, nil
	}}
	svc := newSvc(repo, okStore(), okPublisher(), 5)

	cases := []CreateJobInput{
		{UploadID: token, Pipeline: "", PipelineVersion: "v1"},
		{UploadID: token, Pipeline: "p", PipelineVersion: ""},
		{UploadID: "", Pipeline: "p", PipelineVersion: "v1"},
		{UploadID: "!!bad!!", Pipeline: "p", PipelineVersion: "v1"},
	}
	for i, in := range cases {
		if _, _, err := svc.CreateJob(context.Background(), in); !errors.Is(err, ErrValidation) {
			t.Errorf("case %d: expected ErrValidation, got %v", i, err)
		}
	}
}

func TestCreateJob_UploadMustExist(t *testing.T) {
	token, _ := validUploadID(t)
	store := okStore()
	store.statFn = func(context.Context, string) (storage.ObjectInfo, error) {
		return storage.ObjectInfo{}, storage.ErrObjectNotFound
	}
	repo := &fakeRepo{createFn: func(context.Context, repository.NewJobParams, string, string) (domain.Job, bool, error) {
		t.Fatal("must not create a job for a missing upload")
		return domain.Job{}, false, nil
	}}
	svc := newSvc(repo, store, okPublisher(), 5)

	in := CreateJobInput{UploadID: token, Pipeline: "p", PipelineVersion: "v1"}
	if _, _, err := svc.CreateJob(context.Background(), in); !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation for missing upload, got %v", err)
	}
}

func TestCreateJob_RejectsOversizedObject(t *testing.T) {
	token, _ := validUploadID(t)
	store := okStore()
	store.statFn = func(_ context.Context, key string) (storage.ObjectInfo, error) {
		return storage.ObjectInfo{Key: key, Size: 100 << 20, ContentType: "image/jpeg"}, nil
	}
	svc := newSvc(&fakeRepo{}, store, okPublisher(), 5)

	in := CreateJobInput{UploadID: token, Pipeline: "p", PipelineVersion: "v1"}
	if _, _, err := svc.CreateJob(context.Background(), in); !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation for oversized object, got %v", err)
	}
}

func TestCreateJob_PassesDecodedKeyAndStableHash(t *testing.T) {
	token, key := validUploadID(t)

	var gotParams repository.NewJobParams
	var gotKey, gotHash string
	newJobID := uuid.New()
	repo := &fakeRepo{
		createFn: func(_ context.Context, p repository.NewJobParams, k, h string) (domain.Job, bool, error) {
			gotParams, gotKey, gotHash = p, k, h
			return domain.Job{ID: newJobID, Status: domain.StatusPending, Pipeline: p.Pipeline, PipelineVersion: p.PipelineVersion}, true, nil
		},
		updateFn: func(_ context.Context, id uuid.UUID, from domain.JobStatus, upd repository.JobUpdate) (domain.Job, error) {
			if from != domain.StatusPending || upd.To != domain.StatusQueued {
				t.Fatalf("enqueue must transition PENDING->QUEUED, got %s->%s", from, upd.To)
			}
			return domain.Job{ID: id, Status: domain.StatusQueued, Pipeline: "portrait-enhance", PipelineVersion: "v1"}, nil
		},
	}
	var publishedID string
	pub := &fakePublisher{publishFn: func(_ context.Context, m messaging.JobMessage) error {
		publishedID = m.JobID
		return nil
	}}
	svc := newSvc(repo, okStore(), pub, 7)

	in := CreateJobInput{UploadID: token, Pipeline: "portrait-enhance", PipelineVersion: "v1", IdempotencyKey: "idem-1"}
	job, created, err := svc.CreateJob(context.Background(), in)
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	// After enqueue the job is QUEUED and the message was published for it.
	if !created || job.Status != domain.StatusQueued {
		t.Errorf("unexpected result: created=%v job=%+v", created, job)
	}
	if publishedID != newJobID.String() {
		t.Errorf("published job id = %q, want %q", publishedID, newJobID)
	}
	if gotParams.InputObjectKey != key {
		t.Errorf("object key = %q, want decoded %q", gotParams.InputObjectKey, key)
	}
	if gotParams.MaxAttempts != 7 {
		t.Errorf("max attempts = %d, want 7", gotParams.MaxAttempts)
	}
	if gotKey != "idem-1" {
		t.Errorf("idempotency key = %q, want idem-1", gotKey)
	}
	if gotHash == "" {
		t.Error("request hash must be non-empty")
	}
	if h2 := requestHash(key, "portrait-enhance", "v1"); h2 != gotHash {
		t.Error("request hash is not stable for identical input")
	}
	if requestHash(key, "other", "v1") == gotHash {
		t.Error("request hash must change when the request changes")
	}
}

func TestCreateJob_ReplaySkipsEnqueue(t *testing.T) {
	token, _ := validUploadID(t)
	id := uuid.New()
	repo := &fakeRepo{
		createFn: func(context.Context, repository.NewJobParams, string, string) (domain.Job, bool, error) {
			// Idempotent replay: the job already exists and is past PENDING.
			return domain.Job{ID: id, Status: domain.StatusQueued}, false, nil
		},
		updateFn: func(context.Context, uuid.UUID, domain.JobStatus, repository.JobUpdate) (domain.Job, error) {
			t.Fatal("must not re-enqueue an already-queued job")
			return domain.Job{}, nil
		},
	}
	pub := &fakePublisher{publishFn: func(context.Context, messaging.JobMessage) error {
		t.Fatal("must not publish on idempotent replay")
		return nil
	}}
	svc := newSvc(repo, okStore(), pub, 5)

	job, created, err := svc.CreateJob(context.Background(), CreateJobInput{UploadID: token, Pipeline: "p", PipelineVersion: "v1"})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if created || job.Status != domain.StatusQueued {
		t.Errorf("unexpected replay result: created=%v status=%s", created, job.Status)
	}
}

func TestCreateJob_PublishFailureLeavesJobQueued(t *testing.T) {
	token, _ := validUploadID(t)
	id := uuid.New()
	repo := &fakeRepo{
		createFn: func(context.Context, repository.NewJobParams, string, string) (domain.Job, bool, error) {
			return domain.Job{ID: id, Status: domain.StatusPending}, true, nil
		},
		updateFn: func(_ context.Context, jid uuid.UUID, _ domain.JobStatus, _ repository.JobUpdate) (domain.Job, error) {
			return domain.Job{ID: jid, Status: domain.StatusQueued}, nil
		},
	}
	pub := &fakePublisher{publishFn: func(context.Context, messaging.JobMessage) error {
		return errors.New("broker down")
	}}
	svc := newSvc(repo, okStore(), pub, 5)

	job, created, err := svc.CreateJob(context.Background(), CreateJobInput{UploadID: token, Pipeline: "p", PipelineVersion: "v1"})
	if err == nil {
		t.Fatal("expected an enqueue error when publishing fails")
	}
	// State-first: the DB already reflects QUEUED even though publish failed.
	if !created || job.Status != domain.StatusQueued {
		t.Errorf("expected created QUEUED job despite publish failure, got created=%v status=%s", created, job.Status)
	}
}

func TestCreateJob_ConcurrentEnqueueConflictResolves(t *testing.T) {
	token, _ := validUploadID(t)
	id := uuid.New()
	repo := &fakeRepo{
		createFn: func(context.Context, repository.NewJobParams, string, string) (domain.Job, bool, error) {
			// A replay: the job already exists and is still PENDING.
			return domain.Job{ID: id, Status: domain.StatusPending}, false, nil
		},
		updateFn: func(context.Context, uuid.UUID, domain.JobStatus, repository.JobUpdate) (domain.Job, error) {
			// The winner advanced it first; our guarded enqueue conflicts.
			return domain.Job{}, repository.ErrConflict
		},
		getFn: func(context.Context, uuid.UUID) (domain.Job, error) {
			return domain.Job{ID: id, Status: domain.StatusQueued}, nil
		},
	}
	pub := &fakePublisher{publishFn: func(context.Context, messaging.JobMessage) error {
		t.Fatal("publish must not be reached when the enqueue update conflicts")
		return nil
	}}
	svc := newSvc(repo, okStore(), pub, 5)

	job, created, err := svc.CreateJob(context.Background(), CreateJobInput{UploadID: token, Pipeline: "p", PipelineVersion: "v1"})
	if err != nil {
		t.Fatalf("expected conflict to resolve without error, got %v", err)
	}
	if created || job.Status != domain.StatusQueued {
		t.Errorf("expected existing QUEUED job, created=%v status=%s", created, job.Status)
	}
}

func TestCancelJob_Terminal(t *testing.T) {
	id := uuid.New()
	repo := &fakeRepo{
		getFn: func(context.Context, uuid.UUID) (domain.Job, error) {
			return domain.Job{ID: id, Status: domain.StatusCompleted}, nil
		},
		updateFn: func(context.Context, uuid.UUID, domain.JobStatus, repository.JobUpdate) (domain.Job, error) {
			t.Fatal("must not attempt to update a terminal job")
			return domain.Job{}, nil
		},
	}
	svc := newSvc(repo, okStore(), okPublisher(), 5)
	if _, err := svc.CancelJob(context.Background(), id); !errors.Is(err, ErrNotCancellable) {
		t.Errorf("expected ErrNotCancellable, got %v", err)
	}
}

func TestCancelJob_Success(t *testing.T) {
	id := uuid.New()
	repo := &fakeRepo{
		getFn: func(context.Context, uuid.UUID) (domain.Job, error) {
			return domain.Job{ID: id, Status: domain.StatusQueued}, nil
		},
		updateFn: func(_ context.Context, _ uuid.UUID, from domain.JobStatus, upd repository.JobUpdate) (domain.Job, error) {
			if from != domain.StatusQueued || upd.To != domain.StatusCancelled {
				t.Fatalf("unexpected transition %s->%s", from, upd.To)
			}
			return domain.Job{ID: id, Status: domain.StatusCancelled}, nil
		},
	}
	svc := newSvc(repo, okStore(), okPublisher(), 5)
	job, err := svc.CancelJob(context.Background(), id)
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if job.Status != domain.StatusCancelled {
		t.Errorf("status = %s, want CANCELLED", job.Status)
	}
}

func TestCancelJob_ConflictThenReload(t *testing.T) {
	id := uuid.New()
	getCalls := 0
	repo := &fakeRepo{
		getFn: func(context.Context, uuid.UUID) (domain.Job, error) {
			getCalls++
			if getCalls == 1 {
				return domain.Job{ID: id, Status: domain.StatusProcessing}, nil
			}
			return domain.Job{ID: id, Status: domain.StatusCompleted}, nil
		},
		updateFn: func(context.Context, uuid.UUID, domain.JobStatus, repository.JobUpdate) (domain.Job, error) {
			return domain.Job{}, repository.ErrConflict
		},
	}
	svc := newSvc(repo, okStore(), okPublisher(), 5)
	if _, err := svc.CancelJob(context.Background(), id); !errors.Is(err, ErrNotCancellable) {
		t.Errorf("expected ErrNotCancellable after reload, got %v", err)
	}
	if getCalls != 2 {
		t.Errorf("expected a reload after conflict, got %d GetJob calls", getCalls)
	}
}

func TestCancelJob_NotFound(t *testing.T) {
	repo := &fakeRepo{getFn: func(context.Context, uuid.UUID) (domain.Job, error) {
		return domain.Job{}, repository.ErrNotFound
	}}
	svc := newSvc(repo, okStore(), okPublisher(), 5)
	if _, err := svc.CancelJob(context.Background(), uuid.New()); !errors.Is(err, repository.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}
