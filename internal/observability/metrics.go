package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the Prometheus collectors. The observer methods are nil-safe,
// so a nil *Metrics (e.g. in unit tests) just records nothing.
type Metrics struct {
	reg *prometheus.Registry

	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec

	jobsCreated   prometheus.Counter
	jobsCompleted prometheus.Counter
	jobsFailed    prometheus.Counter
	jobsRetried   prometheus.Counter

	queueLatency       prometheus.Histogram
	processingDuration prometheus.Histogram
	activeWorkers      prometheus.Gauge

	processorErrors *prometheus.CounterVec
	storageErrors   prometheus.Counter
}

// NewMetrics constructs and registers all collectors on a private registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		reg: reg,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "portrait_http_requests_total",
			Help: "Total HTTP requests by method, route, and status.",
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "portrait_http_request_duration_seconds",
			Help:    "HTTP request latency by method and route.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
		jobsCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "portrait_jobs_created_total", Help: "Jobs created.",
		}),
		jobsCompleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "portrait_jobs_completed_total", Help: "Jobs completed successfully.",
		}),
		jobsFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "portrait_jobs_failed_total", Help: "Jobs that reached a terminal FAILED state.",
		}),
		jobsRetried: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "portrait_jobs_retried_total", Help: "Job processing attempts that were scheduled for retry.",
		}),
		queueLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "portrait_queue_latency_seconds",
			Help:    "Time from job creation to the start of processing.",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
		}),
		processingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "portrait_processing_duration_seconds",
			Help:    "Pipeline processing duration per attempt.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
		}),
		activeWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "portrait_active_workers", Help: "Workers currently processing a job.",
		}),
		processorErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "portrait_processor_errors_total", Help: "Processing errors by classification.",
		}, []string{"kind"}),
		storageErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "portrait_storage_errors_total", Help: "Object storage operation errors.",
		}),
	}

	reg.MustRegister(
		m.httpRequests, m.httpDuration,
		m.jobsCreated, m.jobsCompleted, m.jobsFailed, m.jobsRetried,
		m.queueLatency, m.processingDuration, m.activeWorkers,
		m.processorErrors, m.storageErrors,
	)
	return m
}

// Handler returns the /metrics HTTP handler for this registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

func (m *Metrics) ObserveHTTP(method, route, status string, d time.Duration) {
	if m == nil {
		return
	}
	m.httpRequests.WithLabelValues(method, route, status).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(d.Seconds())
}

func (m *Metrics) IncJobsCreated() {
	if m != nil {
		m.jobsCreated.Inc()
	}
}

func (m *Metrics) IncJobsCompleted() {
	if m != nil {
		m.jobsCompleted.Inc()
	}
}

func (m *Metrics) IncJobsFailed() {
	if m != nil {
		m.jobsFailed.Inc()
	}
}

func (m *Metrics) IncJobsRetried() {
	if m != nil {
		m.jobsRetried.Inc()
	}
}

func (m *Metrics) ObserveQueueLatency(d time.Duration) {
	if m != nil {
		m.queueLatency.Observe(d.Seconds())
	}
}

func (m *Metrics) ObserveProcessing(d time.Duration) {
	if m != nil {
		m.processingDuration.Observe(d.Seconds())
	}
}

func (m *Metrics) WorkerStarted() {
	if m != nil {
		m.activeWorkers.Inc()
	}
}

func (m *Metrics) WorkerFinished() {
	if m != nil {
		m.activeWorkers.Dec()
	}
}

func (m *Metrics) IncProcessorError(kind string) {
	if m != nil {
		m.processorErrors.WithLabelValues(kind).Inc()
	}
}

func (m *Metrics) IncStorageError() {
	if m != nil {
		m.storageErrors.Inc()
	}
}
