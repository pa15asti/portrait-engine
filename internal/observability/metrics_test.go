package observability

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetrics_NilSafe(t *testing.T) {
	// A nil *Metrics must be safe to call so instrumentation needs no guards.
	var m *Metrics
	m.IncJobsCreated()
	m.IncJobsCompleted()
	m.IncJobsFailed()
	m.IncJobsRetried()
	m.ObserveHTTP("GET", "/x", "200", time.Second)
	m.ObserveQueueLatency(time.Second)
	m.ObserveProcessing(time.Second)
	m.WorkerStarted()
	m.WorkerFinished()
	m.IncProcessorError("transient")
	m.IncStorageError()
}

func TestMetrics_HandlerExposesCounters(t *testing.T) {
	m := NewMetrics()
	m.IncJobsCreated()
	m.IncJobsCompleted()
	m.ObserveHTTP("POST", "/v1/jobs", "201", 5*time.Millisecond)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)

	body, _ := io.ReadAll(rec.Result().Body)
	text := string(body)
	for _, want := range []string{
		"portrait_jobs_created_total",
		"portrait_jobs_completed_total",
		"portrait_http_requests_total",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}
