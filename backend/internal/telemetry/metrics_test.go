package telemetry

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAPIMetricsExposeOnlyAPICounters(t *testing.T) {
	metrics := &APIMetrics{}
	metrics.AddIngested(2)
	metrics.AddRejected(1)
	metrics.IncAuthFailure()
	metrics.ObserveHTTPRequest(5 * time.Millisecond)

	recorder := httptest.NewRecorder()
	metrics.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	if contentType := recorder.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/plain") {
		t.Fatalf("unexpected content type %q", contentType)
	}
	for _, sample := range []string{
		"logstream_api_ingested_events_total 2",
		"logstream_api_rejected_events_total 1",
		"logstream_api_auth_failures_total 1",
	} {
		if !strings.Contains(recorder.Body.String(), sample) {
			t.Fatalf("missing metric %q in:\n%s", sample, recorder.Body.String())
		}
	}
	if strings.Contains(recorder.Body.String(), "{") {
		t.Fatal("metrics unexpectedly contain labels")
	}
	if strings.Contains(recorder.Body.String(), "logstream_worker_") {
		t.Fatal("API endpoint unexpectedly exposes worker metrics")
	}
}

func TestWorkerMetricsExposeOnlyWorkerCounters(t *testing.T) {
	metrics := &WorkerMetrics{}
	metrics.IncPersistenceSuccess()
	metrics.IncPersistenceFailure()
	metrics.IncDLQSuccess()
	metrics.IncDLQFailure()
	metrics.ObserveWorkerRecord(7 * time.Millisecond)

	recorder := httptest.NewRecorder()
	metrics.ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	for _, sample := range []string{
		"logstream_worker_persistence_successes_total 1",
		"logstream_worker_persistence_failures_total 1",
		"logstream_worker_dlq_successes_total 1",
		"logstream_worker_dlq_failures_total 1",
	} {
		if !strings.Contains(recorder.Body.String(), sample) {
			t.Fatalf("missing metric %q in:\n%s", sample, recorder.Body.String())
		}
	}
	if strings.Contains(recorder.Body.String(), "logstream_api_") {
		t.Fatal("worker endpoint unexpectedly exposes API metrics")
	}
}
