package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/pa15asti/portrait-engine/internal/domain"
	"github.com/pa15asti/portrait-engine/internal/messaging"
	"github.com/pa15asti/portrait-engine/internal/repository"
	"github.com/pa15asti/portrait-engine/internal/storage"
)

// Repository is the persistence the service needs (real: *repository.JobRepository).
type Repository interface {
	CreateJob(ctx context.Context, p repository.NewJobParams, idempotencyKey, requestHash string) (domain.Job, bool, error)
	GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error)
	UpdateJobStatus(ctx context.Context, id uuid.UUID, from domain.JobStatus, upd repository.JobUpdate) (domain.Job, error)
	ListArtifacts(ctx context.Context, jobID uuid.UUID) ([]domain.Artifact, error)
}

// ObjectStore is the storage the service needs: presign uploads and confirm an
// object exists before enqueuing.
type ObjectStore interface {
	PresignUpload(ctx context.Context, key string, expiry time.Duration) (string, error)
	Stat(ctx context.Context, key string) (storage.ObjectInfo, error)
}

type Publisher interface {
	Publish(ctx context.Context, m messaging.JobMessage) error
}

type Config struct {
	MaxAttempts    int
	PresignExpiry  time.Duration
	MaxUploadBytes int64
}

type Service struct {
	repo           Repository
	store          ObjectStore
	publisher      Publisher
	maxAttempts    int
	presignExpiry  time.Duration
	maxUploadBytes int64
}

// NewService constructs a Service.
func NewService(repo Repository, store ObjectStore, publisher Publisher, cfg Config) *Service {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	return &Service{
		repo:           repo,
		store:          store,
		publisher:      publisher,
		maxAttempts:    cfg.MaxAttempts,
		presignExpiry:  cfg.PresignExpiry,
		maxUploadBytes: cfg.MaxUploadBytes,
	}
}

// CreateUploadInput is the validated input to CreateUpload.
type CreateUploadInput struct {
	ContentType string
	SizeBytes   int64
}

// Upload is the result of requesting an upload slot.
type Upload struct {
	UploadID  string
	ObjectKey string
	UploadURL string
	ExpiresIn time.Duration
}

// CreateUpload validates the media type/size and returns a presigned PUT URL.
func (s *Service) CreateUpload(ctx context.Context, in CreateUploadInput) (Upload, error) {
	if err := storage.ValidateUploadRequest(in.ContentType, in.SizeBytes, s.maxUploadBytes); err != nil {
		return Upload{}, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	ext, err := storage.ExtensionForContentType(in.ContentType)
	if err != nil {
		return Upload{}, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	key := storage.GenerateInputKey(ext)
	url, err := s.store.PresignUpload(ctx, key, s.presignExpiry)
	if err != nil {
		return Upload{}, fmt.Errorf("presign upload: %w", err)
	}
	return Upload{
		UploadID:  storage.EncodeUploadID(key),
		ObjectKey: key,
		UploadURL: url,
		ExpiresIn: s.presignExpiry,
	}, nil
}

// CreateJobInput is the validated input to CreateJob.
type CreateJobInput struct {
	UploadID        string
	Pipeline        string
	PipelineVersion string
	IdempotencyKey  string
	CorrelationID   string
}

// CreateJob validates the request, confirms the upload exists, creates the job
// (idempotently when a key is given), and enqueues it. Returns the job and
// whether it was newly created.
func (s *Service) CreateJob(ctx context.Context, in CreateJobInput) (domain.Job, bool, error) {
	pipeline := strings.TrimSpace(in.Pipeline)
	version := strings.TrimSpace(in.PipelineVersion)
	if pipeline == "" || version == "" {
		return domain.Job{}, false, fmt.Errorf("%w: pipeline and pipeline_version are required", ErrValidation)
	}
	if in.UploadID == "" {
		return domain.Job{}, false, fmt.Errorf("%w: upload_id is required", ErrValidation)
	}

	objectKey, err := storage.DecodeUploadID(in.UploadID)
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("%w: invalid upload_id", ErrValidation)
	}

	// Don't create a job for an image that isn't there / is the wrong size.
	info, err := s.store.Stat(ctx, objectKey)
	if errors.Is(err, storage.ErrObjectNotFound) {
		return domain.Job{}, false, fmt.Errorf("%w: upload not found", ErrValidation)
	}
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("stat upload: %w", err)
	}
	if err := storage.ValidateObject(info, s.maxUploadBytes); err != nil {
		return domain.Job{}, false, fmt.Errorf("%w: %w", ErrValidation, err)
	}

	params := repository.NewJobParams{
		ID:              uuid.New(),
		Pipeline:        pipeline,
		PipelineVersion: version,
		InputObjectKey:  objectKey,
		CorrelationID:   in.CorrelationID,
		MaxAttempts:     s.maxAttempts,
	}

	// Binds the idempotency key to this request body; a replay with a different
	// body is then a conflict.
	hash := requestHash(objectKey, pipeline, version)

	job, created, err := s.repo.CreateJob(ctx, params, in.IdempotencyKey, hash)
	if err != nil {
		return domain.Job{}, false, err
	}

	// Enqueue if not yet published — covers a fresh create and self-heals a
	// replay whose earlier enqueue failed after the row was committed.
	if job.Status == domain.StatusPending {
		queued, err := s.enqueue(ctx, job)
		if err != nil {
			// A concurrent duplicate request may have advanced the job past
			// PENDING between our read and our guarded update. That's not a
			// failure — return the job's current state.
			if errors.Is(err, repository.ErrConflict) {
				if cur, gerr := s.repo.GetJob(ctx, job.ID); gerr == nil {
					return cur, created, nil
				}
			}
			return queued, created, fmt.Errorf("enqueue job: %w", err)
		}
		return queued, created, nil
	}
	return job, created, nil
}

// enqueue moves PENDING → QUEUED, then publishes. State-first: if publish fails,
// the DB still shows the intent and the worker's startup requeue sweep recovers
// the stuck QUEUED job.
func (s *Service) enqueue(ctx context.Context, job domain.Job) (domain.Job, error) {
	queued, err := s.repo.UpdateJobStatus(ctx, job.ID, domain.StatusPending, repository.JobUpdate{
		To: domain.StatusQueued,
	})
	if err != nil {
		return job, err
	}
	msg := messaging.JobMessage{
		JobID:           queued.ID.String(),
		Pipeline:        queued.Pipeline,
		PipelineVersion: queued.PipelineVersion,
		CorrelationID:   queued.CorrelationID,
	}
	if err := s.publisher.Publish(ctx, msg); err != nil {
		return queued, err
	}
	return queued, nil
}

// GetJob returns a job by ID.
func (s *Service) GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	return s.repo.GetJob(ctx, id)
}

// ListArtifacts returns a job's artifacts.
func (s *Service) ListArtifacts(ctx context.Context, id uuid.UUID) ([]domain.Artifact, error) {
	return s.repo.ListArtifacts(ctx, id)
}

// CancelJob does a guarded transition to CANCELLED. Terminal jobs return
// ErrNotCancellable; a concurrent change is retried once against fresh state.
func (s *Service) CancelJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	job, err := s.repo.GetJob(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}

	const maxTries = 2
	for try := 0; try < maxTries; try++ {
		if !domain.CanTransition(job.Status, domain.StatusCancelled) {
			return domain.Job{}, ErrNotCancellable
		}
		updated, err := s.repo.UpdateJobStatus(ctx, id, job.Status, repository.JobUpdate{To: domain.StatusCancelled})
		switch {
		case err == nil:
			return updated, nil
		case errors.Is(err, repository.ErrConflict):
			// State moved under us; reload and re-evaluate cancellability.
			job, err = s.repo.GetJob(ctx, id)
			if err != nil {
				return domain.Job{}, err
			}
		default:
			return domain.Job{}, err
		}
	}
	return domain.Job{}, ErrNotCancellable
}

func requestHash(objectKey, pipeline, version string) string {
	h := sha256.New()
	// Length-prefixed to avoid ambiguity between field boundaries.
	for _, f := range []string{objectKey, pipeline, version} {
		fmt.Fprintf(h, "%d:%s", len(f), f)
	}
	return hex.EncodeToString(h.Sum(nil))
}
