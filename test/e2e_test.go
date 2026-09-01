//go:build integration

package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/pa15asti/portrait-engine/internal/jobs"
	"github.com/pa15asti/portrait-engine/internal/messaging"
	"github.com/pa15asti/portrait-engine/internal/worker"
)

// TestE2E wires the whole system against real Postgres, NATS, and MinIO and
// runs the scenarios as subtests sharing one set of containers.
func TestE2E(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	t.Run("happy path: upload -> job -> worker -> artifact -> completed", func(t *testing.T) {
		uploadID := h.createAndUpload(t)
		job, created, err := h.svc.CreateJob(ctx, jobs.CreateJobInput{
			UploadID: uploadID, Pipeline: "portrait-enhance", PipelineVersion: "v1",
		})
		if err != nil || !created {
			t.Fatalf("CreateJob: created=%v err=%v", created, err)
		}

		h.waitStatus(t, job.ID, "COMPLETED", 30*time.Second)

		artifacts, err := h.repo.ListArtifacts(ctx, job.ID)
		if err != nil {
			t.Fatalf("ListArtifacts: %v", err)
		}
		if len(artifacts) != 1 || artifacts[0].Kind != "output" {
			t.Fatalf("expected one output artifact, got %+v", artifacts)
		}
		info, err := h.store.Stat(ctx, artifacts[0].ObjectKey)
		if err != nil {
			t.Fatalf("output object missing in storage: %v", err)
		}
		if info.Size == 0 {
			t.Error("output artifact is empty")
		}
	})

	t.Run("duplicate delivery is idempotent", func(t *testing.T) {
		uploadID := h.createAndUpload(t)
		job, _, err := h.svc.CreateJob(ctx, jobs.CreateJobInput{
			UploadID: uploadID, Pipeline: "portrait-enhance", PipelineVersion: "v1",
		})
		if err != nil {
			t.Fatalf("CreateJob: %v", err)
		}
		h.waitStatus(t, job.ID, "COMPLETED", 30*time.Second)

		before, _ := h.repo.ListArtifacts(ctx, job.ID)

		// Simulate a redelivery of the same message after completion.
		res := h.handler.Handle(ctx, messaging.JobMessage{
			JobID: job.ID.String(), Pipeline: job.Pipeline, PipelineVersion: job.PipelineVersion,
		})
		if res.Decision != worker.Complete {
			t.Errorf("duplicate delivery decision = %v, want Complete", res.Decision)
		}

		after, _ := h.repo.ListArtifacts(ctx, job.ID)
		if len(after) != len(before) {
			t.Errorf("duplicate processing created extra artifacts: %d -> %d", len(before), len(after))
		}
		final, _ := h.repo.GetJob(ctx, job.ID)
		if string(final.Status) != "COMPLETED" {
			t.Errorf("job status after duplicate = %s, want COMPLETED", final.Status)
		}
	})

	t.Run("concurrent idempotent requests create one job", func(t *testing.T) {
		uploadID := h.createAndUpload(t)
		const n = 8
		const key = "concurrent-idem-key"

		var (
			wg       sync.WaitGroup
			mu       sync.Mutex
			ids      = map[uuid.UUID]struct{}{}
			newCount int
		)
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				job, created, err := h.svc.CreateJob(ctx, jobs.CreateJobInput{
					UploadID: uploadID, Pipeline: "portrait-enhance", PipelineVersion: "v1", IdempotencyKey: key,
				})
				if err != nil {
					return
				}
				mu.Lock()
				ids[job.ID] = struct{}{}
				if created {
					newCount++
				}
				mu.Unlock()
			}()
		}
		wg.Wait()

		if len(ids) != 1 {
			t.Errorf("expected 1 distinct job, got %d", len(ids))
		}
		if newCount != 1 {
			t.Errorf("expected exactly one creator, got %d", newCount)
		}
		for id := range ids {
			h.waitStatus(t, id, "COMPLETED", 30*time.Second)
		}
	})

	t.Run("cancellation stops an in-flight job", func(t *testing.T) {
		uploadID := h.createAndUpload(t)
		job, _, err := h.svc.CreateJob(ctx, jobs.CreateJobInput{
			UploadID: uploadID, Pipeline: "portrait-enhance", PipelineVersion: slowVersion,
		})
		if err != nil {
			t.Fatalf("CreateJob: %v", err)
		}

		// Wait until the worker has claimed it, then cancel.
		h.waitStatus(t, job.ID, "PROCESSING", 15*time.Second)
		if _, err := h.svc.CancelJob(ctx, job.ID); err != nil {
			t.Fatalf("CancelJob: %v", err)
		}

		h.waitStatus(t, job.ID, "CANCELLED", 15*time.Second)
	})

	t.Run("retry exhaustion ends in FAILED", func(t *testing.T) {
		uploadID := h.createAndUpload(t)
		job, _, err := h.svc.CreateJob(ctx, jobs.CreateJobInput{
			UploadID: uploadID, Pipeline: "portrait-enhance", PipelineVersion: failVersion,
		})
		if err != nil {
			t.Fatalf("CreateJob: %v", err)
		}

		h.waitStatus(t, job.ID, "FAILED", 30*time.Second)

		final, _ := h.repo.GetJob(ctx, job.ID)
		if final.AttemptCount != maxAttempts {
			t.Errorf("attempt_count = %d, want %d (exhausted)", final.AttemptCount, maxAttempts)
		}
	})
}
