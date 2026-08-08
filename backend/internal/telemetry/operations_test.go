package telemetry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type healthCheck struct{ err error }

func (h healthCheck) Check(context.Context) error { return h.err }

func TestOperationsHealthAndReadinessHaveDifferentSemantics(t *testing.T) {
	handler := NewOperationsHandler(&WorkerMetrics{}, []HealthChecker{healthCheck{err: errors.New("dependency down")}})

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("liveness should ignore dependencies: %d", health.Code)
	}

	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness should reflect dependencies: %d", ready.Code)
	}
}
