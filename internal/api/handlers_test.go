package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/pa15asti/portrait-engine/internal/config"
	"github.com/pa15asti/portrait-engine/internal/domain"
	"github.com/pa15asti/portrait-engine/internal/jobs"
	"github.com/pa15asti/portrait-engine/internal/repository"
)

// fakeJobService implements JobService with configurable behavior.
type fakeJobService struct {
	uploadFn func(ctx context.Context, in jobs.CreateUploadInput) (jobs.Upload, error)
	createFn func(ctx context.Context, in jobs.CreateJobInput) (domain.Job, bool, error)
	getFn    func(ctx context.Context, id uuid.UUID) (domain.Job, error)
	cancelFn func(ctx context.Context, id uuid.UUID) (domain.Job, error)
	listFn   func(ctx context.Context, id uuid.UUID) ([]domain.Artifact, error)
}

func (f *fakeJobService) CreateUpload(ctx context.Context, in jobs.CreateUploadInput) (jobs.Upload, error) {
	return f.uploadFn(ctx, in)
}
func (f *fakeJobService) CreateJob(ctx context.Context, in jobs.CreateJobInput) (domain.Job, bool, error) {
	return f.createFn(ctx, in)
}
func (f *fakeJobService) GetJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	return f.getFn(ctx, id)
}
func (f *fakeJobService) CancelJob(ctx context.Context, id uuid.UUID) (domain.Job, error) {
	return f.cancelFn(ctx, id)
}
func (f *fakeJobService) ListArtifacts(ctx context.Context, id uuid.UUID) ([]domain.Artifact, error) {
	return f.listFn(ctx, id)
}

func newTestServer(svc JobService) http.Handler {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewServer(config.HTTPConfig{}, log, svc, nil).Handler()
}

func do(h http.Handler, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, r)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreateUpload(t *testing.T) {
	svc := &fakeJobService{uploadFn: func(_ context.Context, in jobs.CreateUploadInput) (jobs.Upload, error) {
		if in.ContentType != "image/jpeg" {
			t.Errorf("content type = %q", in.ContentType)
		}
		return jobs.Upload{UploadID: "uid", ObjectKey: "uploads/x.jpg", UploadURL: "https://minio/put", ExpiresIn: 900 * 1e9}, nil
	}}
	rec := do(newTestServer(svc), http.MethodPost, "/v1/uploads",
		`{"content_type":"image/jpeg","size_bytes":1000}`, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body)
	}
	var resp uploadResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.UploadID != "uid" || resp.UploadURL != "https://minio/put" || resp.ExpiresInSeconds != 900 {
		t.Errorf("unexpected upload response: %+v", resp)
	}
}

func TestCreateUpload_UnsupportedType(t *testing.T) {
	svc := &fakeJobService{uploadFn: func(context.Context, jobs.CreateUploadInput) (jobs.Upload, error) {
		return jobs.Upload{}, jobs.ErrValidation
	}}
	rec := do(newTestServer(svc), http.MethodPost, "/v1/uploads", `{"content_type":"image/gif"}`, nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestCreateJob_Created(t *testing.T) {
	id := uuid.New()
	var gotInput jobs.CreateJobInput
	svc := &fakeJobService{createFn: func(_ context.Context, in jobs.CreateJobInput) (domain.Job, bool, error) {
		gotInput = in
		return domain.Job{ID: id, Status: domain.StatusPending, Pipeline: "p", PipelineVersion: "v1", MaxAttempts: 5}, true, nil
	}}
	h := newTestServer(svc)

	rec := do(h, http.MethodPost, "/v1/jobs",
		`{"upload_id":"tok","pipeline":"p","pipeline_version":"v1"}`,
		map[string]string{"Idempotency-Key": "k1", "X-Request-ID": "req-123"})

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body)
	}
	if gotInput.IdempotencyKey != "k1" {
		t.Errorf("idempotency key not forwarded: %q", gotInput.IdempotencyKey)
	}
	if gotInput.CorrelationID != "req-123" {
		t.Errorf("correlation id = %q, want the request id", gotInput.CorrelationID)
	}
	if rec.Header().Get("X-Request-ID") != "req-123" {
		t.Errorf("response should echo request id, got %q", rec.Header().Get("X-Request-ID"))
	}
	var resp jobResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.ID != id || resp.Status != "PENDING" {
		t.Errorf("unexpected body: %+v", resp)
	}
}

func TestCreateJob_IdempotentReplayIs200(t *testing.T) {
	svc := &fakeJobService{createFn: func(context.Context, jobs.CreateJobInput) (domain.Job, bool, error) {
		return domain.Job{ID: uuid.New(), Status: domain.StatusPending}, false, nil
	}}
	rec := do(newTestServer(svc), http.MethodPost, "/v1/jobs",
		`{"upload_id":"tok","pipeline":"p","pipeline_version":"v1"}`, nil)
	if rec.Code != http.StatusOK {
		t.Errorf("replay status = %d, want 200", rec.Code)
	}
}

func TestCreateJob_BadRequests(t *testing.T) {
	svc := &fakeJobService{createFn: func(context.Context, jobs.CreateJobInput) (domain.Job, bool, error) {
		return domain.Job{}, false, jobs.ErrValidation
	}}
	h := newTestServer(svc)

	// Malformed JSON.
	if rec := do(h, http.MethodPost, "/v1/jobs", `{not json`, nil); rec.Code != http.StatusBadRequest {
		t.Errorf("malformed body status = %d, want 400", rec.Code)
	}
	// Unknown field is rejected by DisallowUnknownFields.
	if rec := do(h, http.MethodPost, "/v1/jobs", `{"surprise":1}`, nil); rec.Code != http.StatusBadRequest {
		t.Errorf("unknown field status = %d, want 400", rec.Code)
	}
	// Service validation error.
	if rec := do(h, http.MethodPost, "/v1/jobs", `{"upload_id":"","pipeline":"","pipeline_version":""}`, nil); rec.Code != http.StatusBadRequest {
		t.Errorf("validation status = %d, want 400", rec.Code)
	}
}

func TestCreateJob_IdempotencyConflict(t *testing.T) {
	svc := &fakeJobService{createFn: func(context.Context, jobs.CreateJobInput) (domain.Job, bool, error) {
		return domain.Job{}, false, repository.ErrIdempotencyConflict
	}}
	rec := do(newTestServer(svc), http.MethodPost, "/v1/jobs",
		`{"upload_id":"tok","pipeline":"p","pipeline_version":"v1"}`, nil)
	if rec.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", rec.Code)
	}
}

func TestGetJob(t *testing.T) {
	id := uuid.New()
	svc := &fakeJobService{getFn: func(_ context.Context, gid uuid.UUID) (domain.Job, error) {
		if gid != id {
			return domain.Job{}, repository.ErrNotFound
		}
		return domain.Job{ID: id, Status: domain.StatusProcessing}, nil
	}}
	h := newTestServer(svc)

	if rec := do(h, http.MethodGet, "/v1/jobs/"+id.String(), "", nil); rec.Code != http.StatusOK {
		t.Errorf("found status = %d, want 200", rec.Code)
	}
	if rec := do(h, http.MethodGet, "/v1/jobs/"+uuid.New().String(), "", nil); rec.Code != http.StatusNotFound {
		t.Errorf("missing status = %d, want 404", rec.Code)
	}
	if rec := do(h, http.MethodGet, "/v1/jobs/not-a-uuid", "", nil); rec.Code != http.StatusBadRequest {
		t.Errorf("bad id status = %d, want 400", rec.Code)
	}
}

func TestCancelJob(t *testing.T) {
	id := uuid.New()
	svc := &fakeJobService{cancelFn: func(_ context.Context, cid uuid.UUID) (domain.Job, error) {
		return domain.Job{ID: cid, Status: domain.StatusCancelled}, nil
	}}
	if rec := do(newTestServer(svc), http.MethodPost, "/v1/jobs/"+id.String()+"/cancel", "", nil); rec.Code != http.StatusOK {
		t.Errorf("cancel status = %d, want 200", rec.Code)
	}

	svc.cancelFn = func(context.Context, uuid.UUID) (domain.Job, error) {
		return domain.Job{}, jobs.ErrNotCancellable
	}
	if rec := do(newTestServer(svc), http.MethodPost, "/v1/jobs/"+id.String()+"/cancel", "", nil); rec.Code != http.StatusConflict {
		t.Errorf("not-cancellable status = %d, want 409", rec.Code)
	}
}

func TestListArtifacts(t *testing.T) {
	id := uuid.New()
	svc := &fakeJobService{
		getFn: func(context.Context, uuid.UUID) (domain.Job, error) {
			return domain.Job{ID: id, Status: domain.StatusCompleted}, nil
		},
		listFn: func(_ context.Context, aid uuid.UUID) ([]domain.Artifact, error) {
			return []domain.Artifact{{JobID: aid, Kind: "output", ObjectKey: "outputs/x.jpg"}}, nil
		},
	}
	rec := do(newTestServer(svc), http.MethodGet, "/v1/jobs/"+id.String()+"/artifacts", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp artifactsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Artifacts) != 1 || resp.Artifacts[0].Kind != "output" {
		t.Errorf("unexpected artifacts: %+v", resp)
	}
}

func TestListArtifacts_MissingJobIs404(t *testing.T) {
	svc := &fakeJobService{
		getFn: func(context.Context, uuid.UUID) (domain.Job, error) {
			return domain.Job{}, repository.ErrNotFound
		},
		listFn: func(context.Context, uuid.UUID) ([]domain.Artifact, error) {
			t.Fatal("must not list artifacts for a missing job")
			return nil, nil
		},
	}
	rec := do(newTestServer(svc), http.MethodGet, "/v1/jobs/"+uuid.New().String()+"/artifacts", "", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestMiddleware_PanicRecovery(t *testing.T) {
	svc := &fakeJobService{getFn: func(context.Context, uuid.UUID) (domain.Job, error) {
		panic("boom")
	}}
	rec := do(newTestServer(svc), http.MethodGet, "/v1/jobs/"+uuid.New().String(), "", nil)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("panic should map to 500, got %d", rec.Code)
	}
}

func TestMiddleware_GeneratesRequestID(t *testing.T) {
	svc := &fakeJobService{getFn: func(context.Context, uuid.UUID) (domain.Job, error) {
		return domain.Job{ID: uuid.New(), Status: domain.StatusPending}, nil
	}}
	rec := do(newTestServer(svc), http.MethodGet, "/v1/jobs/"+uuid.New().String(), "", nil)
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("expected a generated X-Request-ID when none supplied")
	}
}

func TestHealthAndReady(t *testing.T) {
	h := newTestServer(&fakeJobService{})
	if rec := do(h, http.MethodGet, "/health", "", nil); rec.Code != http.StatusOK {
		t.Errorf("health status = %d", rec.Code)
	}
	if rec := do(h, http.MethodGet, "/ready", "", nil); rec.Code != http.StatusOK {
		t.Errorf("ready status = %d", rec.Code)
	}
}
