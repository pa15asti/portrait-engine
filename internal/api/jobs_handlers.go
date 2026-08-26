package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/pa15asti/portrait-engine/internal/domain"
	"github.com/pa15asti/portrait-engine/internal/jobs"
	"github.com/pa15asti/portrait-engine/internal/repository"
	"github.com/pa15asti/portrait-engine/internal/storage"
)

// JobService is what the API needs (real: *jobs.Service).
type JobService interface {
	CreateUpload(ctx context.Context, in jobs.CreateUploadInput) (jobs.Upload, error)
	CreateJob(ctx context.Context, in jobs.CreateJobInput) (domain.Job, bool, error)
	GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error)
	CancelJob(ctx context.Context, id uuid.UUID) (domain.Job, error)
	ListArtifacts(ctx context.Context, id uuid.UUID) ([]domain.Artifact, error)
}

// idempotencyHeader lets clients make POST /v1/jobs safe to retry.
const idempotencyHeader = "Idempotency-Key"

// The API never receives image bytes, so a small body cap is fine.
const maxJobBodyBytes = 16 << 10

type createUploadRequest struct {
	ContentType string `json:"content_type"`
	SizeBytes   int64  `json:"size_bytes"`
}

type uploadResponse struct {
	UploadID         string `json:"upload_id"`
	ObjectKey        string `json:"object_key"`
	UploadURL        string `json:"upload_url"`
	ExpiresInSeconds int    `json:"expires_in_seconds"`
}

type createJobRequest struct {
	UploadID        string `json:"upload_id"`
	Pipeline        string `json:"pipeline"`
	PipelineVersion string `json:"pipeline_version"`
}

func (s *Server) handleCreateUpload(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxJobBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req createUploadRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "request body is not valid JSON")
		return
	}

	up, err := s.jobs.CreateUpload(r.Context(), jobs.CreateUploadInput{
		ContentType: req.ContentType,
		SizeBytes:   req.SizeBytes,
	})
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	// The presigned URL is returned to the caller but never logged.
	writeJSON(w, http.StatusCreated, uploadResponse{
		UploadID:         up.UploadID,
		ObjectKey:        up.ObjectKey,
		UploadURL:        up.UploadURL,
		ExpiresInSeconds: int(up.ExpiresIn.Seconds()),
	})
}

type jobResponse struct {
	ID              uuid.UUID `json:"id"`
	Status          string    `json:"status"`
	Pipeline        string    `json:"pipeline"`
	PipelineVersion string    `json:"pipeline_version"`
	AttemptCount    int       `json:"attempt_count"`
	MaxAttempts     int       `json:"max_attempts"`
	LastError       string    `json:"last_error,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func toJobResponse(j domain.Job) jobResponse {
	return jobResponse{
		ID:              j.ID,
		Status:          string(j.Status),
		Pipeline:        j.Pipeline,
		PipelineVersion: j.PipelineVersion,
		AttemptCount:    j.AttemptCount,
		MaxAttempts:     j.MaxAttempts,
		LastError:       j.LastError,
		CreatedAt:       j.CreatedAt,
		UpdatedAt:       j.UpdatedAt,
	}
}

type artifactResponse struct {
	Kind        string    `json:"kind"`
	ObjectKey   string    `json:"object_key"`
	ContentType string    `json:"content_type"`
	SizeBytes   int64     `json:"size_bytes"`
	CreatedAt   time.Time `json:"created_at"`
}

type artifactsResponse struct {
	JobID     uuid.UUID          `json:"job_id"`
	Artifacts []artifactResponse `json:"artifacts"`
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxJobBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req createJobRequest
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "request body is not valid JSON")
		return
	}

	in := jobs.CreateJobInput{
		UploadID:        req.UploadID,
		Pipeline:        req.Pipeline,
		PipelineVersion: req.PipelineVersion,
		IdempotencyKey:  r.Header.Get(idempotencyHeader),
		CorrelationID:   requestIDFrom(r),
	}

	job, created, err := s.jobs.CreateJob(r.Context(), in)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}

	status := http.StatusOK // idempotent replay
	if created {
		status = http.StatusCreated
		s.metrics.IncJobsCreated()
	}
	writeJSON(w, status, toJobResponse(job))
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	job, err := s.jobs.GetJob(r.Context(), id)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toJobResponse(job))
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	job, err := s.jobs.CancelJob(r.Context(), id)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toJobResponse(job))
}

func (s *Server) handleListArtifacts(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	// 404 for a missing job rather than an empty list.
	if _, err := s.jobs.GetJob(r.Context(), id); err != nil {
		s.writeServiceError(w, err)
		return
	}
	artifacts, err := s.jobs.ListArtifacts(r.Context(), id)
	if err != nil {
		s.writeServiceError(w, err)
		return
	}
	out := artifactsResponse{JobID: id, Artifacts: make([]artifactResponse, 0, len(artifacts))}
	for _, a := range artifacts {
		out.Artifacts = append(out.Artifacts, artifactResponse{
			Kind:        a.Kind,
			ObjectKey:   a.ObjectKey,
			ContentType: a.ContentType,
			SizeBytes:   a.SizeBytes,
			CreatedAt:   a.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// parseID extracts and validates the {id} path value, writing a 400 on failure.
func parseID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", "job id must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

// writeServiceError maps service/repository errors to HTTP responses without
// leaking internal detail on unexpected failures.
func (s *Server) writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, jobs.ErrValidation),
		errors.Is(err, storage.ErrUnsupportedMedia),
		errors.Is(err, storage.ErrTooLarge):
		writeError(w, http.StatusBadRequest, "validation", err.Error())
	case errors.Is(err, repository.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "job not found")
	case errors.Is(err, repository.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict",
			"idempotency key was reused with a different request")
	case errors.Is(err, jobs.ErrNotCancellable):
		writeError(w, http.StatusConflict, "not_cancellable",
			"job is already in a terminal state")
	default:
		writeError(w, http.StatusInternalServerError, "internal", "internal server error")
	}
}
