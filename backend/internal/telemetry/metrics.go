package telemetry

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type APIMetrics struct {
	ingestedEvents    atomic.Uint64
	rejectedEvents    atomic.Uint64
	authFailures      atomic.Uint64
	httpRequests      atomic.Uint64
	httpDurationNanos atomic.Uint64
}

func (m *APIMetrics) AddIngested(count int) { addPositive(&m.ingestedEvents, count) }
func (m *APIMetrics) AddRejected(count int) { addPositive(&m.rejectedEvents, count) }
func (m *APIMetrics) IncAuthFailure()       { m.authFailures.Add(1) }
func (m *APIMetrics) ObserveHTTPRequest(duration time.Duration) {
	m.httpRequests.Add(1)
	m.httpDurationNanos.Add(uint64(max(duration, 0)))
}

func (m *APIMetrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	serveCounters(w, []prometheusCounter{
		{"logstream_api_ingested_events_total", "Events accepted after Kafka acknowledged publication.", m.ingestedEvents.Load()},
		{"logstream_api_rejected_events_total", "Events rejected by contract validation.", m.rejectedEvents.Load()},
		{"logstream_api_auth_failures_total", "Ingest requests rejected by API-key authentication.", m.authFailures.Load()},
		{"logstream_api_http_requests_total", "HTTP requests completed by this API process.", m.httpRequests.Load()},
		{"logstream_api_http_request_duration_nanoseconds_total", "Cumulative HTTP request processing time in nanoseconds.", m.httpDurationNanos.Load()},
	})
}

type WorkerMetrics struct {
	persistenceSuccess  atomic.Uint64
	persistenceFailures atomic.Uint64
	dlqSuccess          atomic.Uint64
	dlqFailures         atomic.Uint64
	workerRecords       atomic.Uint64
	workerDurationNanos atomic.Uint64
}

func (m *WorkerMetrics) IncPersistenceSuccess() { m.persistenceSuccess.Add(1) }
func (m *WorkerMetrics) IncPersistenceFailure() { m.persistenceFailures.Add(1) }
func (m *WorkerMetrics) IncDLQSuccess()         { m.dlqSuccess.Add(1) }
func (m *WorkerMetrics) IncDLQFailure()         { m.dlqFailures.Add(1) }
func (m *WorkerMetrics) ObserveWorkerRecord(duration time.Duration) {
	m.workerRecords.Add(1)
	m.workerDurationNanos.Add(uint64(max(duration, 0)))
}

func (m *WorkerMetrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	serveCounters(w, []prometheusCounter{
		{"logstream_worker_persistence_successes_total", "Records successfully or idempotently persisted.", m.persistenceSuccess.Load()},
		{"logstream_worker_persistence_failures_total", "Failed PostgreSQL persistence attempts.", m.persistenceFailures.Load()},
		{"logstream_worker_dlq_successes_total", "Poison records successfully published to the DLQ.", m.dlqSuccess.Load()},
		{"logstream_worker_dlq_failures_total", "Failed DLQ publication attempts.", m.dlqFailures.Load()},
		{"logstream_worker_records_processed_total", "Kafka records that reached a terminal processing result.", m.workerRecords.Load()},
		{"logstream_worker_processing_duration_nanoseconds_total", "Cumulative Kafka record processing time in nanoseconds.", m.workerDurationNanos.Load()},
	})
}

type prometheusCounter struct {
	name  string
	help  string
	value uint64
}

func serveCounters(w http.ResponseWriter, counters []prometheusCounter) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	var output strings.Builder
	for _, counter := range counters {
		fmt.Fprintf(
			&output,
			"# HELP %s %s\n# TYPE %s counter\n%s %d\n",
			counter.name,
			counter.help,
			counter.name,
			counter.name,
			counter.value,
		)
	}
	_, _ = fmt.Fprint(w, output.String())
}

func addPositive(counter *atomic.Uint64, count int) {
	if count > 0 {
		counter.Add(uint64(count))
	}
}
