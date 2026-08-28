package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/pa15asti/portrait-engine/internal/domain"
	"github.com/pa15asti/portrait-engine/internal/messaging"
)

type fakeLister struct {
	jobs   []domain.Job
	err    error
	cutoff time.Time
}

func (l *fakeLister) ListRequeueable(_ context.Context, cutoff time.Time, _ int) ([]domain.Job, error) {
	l.cutoff = cutoff
	return l.jobs, l.err
}

type recordingPublisher struct {
	ids    []string
	failOn map[string]bool
}

func (p *recordingPublisher) Publish(_ context.Context, m messaging.JobMessage) error {
	if p.failOn[m.JobID] {
		return errors.New("publish failed")
	}
	p.ids = append(p.ids, m.JobID)
	return nil
}

func TestRequeuer_RepublishesStuckJobs(t *testing.T) {
	j1 := domain.Job{ID: uuid.New(), Status: domain.StatusQueued, Pipeline: "p", PipelineVersion: "v1"}
	j2 := domain.Job{ID: uuid.New(), Status: domain.StatusQueued, Pipeline: "p", PipelineVersion: "v1"}
	lister := &fakeLister{jobs: []domain.Job{j1, j2}}
	pub := &recordingPublisher{}

	r := NewRequeuer(lister, pub, discardLogger())
	n, err := r.Sweep(context.Background(), time.Minute, 100)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if n != 2 || len(pub.ids) != 2 {
		t.Fatalf("requeued %d (published %d), want 2", n, len(pub.ids))
	}
	if pub.ids[0] != j1.ID.String() || pub.ids[1] != j2.ID.String() {
		t.Errorf("unexpected published ids: %v", pub.ids)
	}
	// The cutoff must be roughly now-minAge.
	if time.Since(lister.cutoff) < 30*time.Second {
		t.Errorf("cutoff not applied: %v", lister.cutoff)
	}
}

func TestRequeuer_SkipsPublishFailures(t *testing.T) {
	j1 := domain.Job{ID: uuid.New(), Status: domain.StatusQueued}
	j2 := domain.Job{ID: uuid.New(), Status: domain.StatusQueued}
	lister := &fakeLister{jobs: []domain.Job{j1, j2}}
	pub := &recordingPublisher{failOn: map[string]bool{j1.ID.String(): true}}

	r := NewRequeuer(lister, pub, discardLogger())
	n, err := r.Sweep(context.Background(), time.Minute, 100)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// One publish failed and was skipped; the other still succeeds.
	if n != 1 || len(pub.ids) != 1 || pub.ids[0] != j2.ID.String() {
		t.Errorf("expected only j2 requeued, got n=%d ids=%v", n, pub.ids)
	}
}

func TestRequeuer_ListerError(t *testing.T) {
	lister := &fakeLister{err: errors.New("db down")}
	r := NewRequeuer(lister, &recordingPublisher{}, discardLogger())
	if _, err := r.Sweep(context.Background(), time.Minute, 100); err == nil {
		t.Error("expected error when the lister fails")
	}
}
