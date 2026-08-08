package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
)

type HealthChecker interface {
	Check(context.Context) error
}

func NewOperationsHandler(metricsHandler http.Handler, readiness []HealthChecker) http.Handler {
	if metricsHandler == nil {
		metricsHandler = http.NotFoundHandler()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeStatus(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		for _, checker := range readiness {
			if err := checker.Check(r.Context()); err != nil {
				writeStatus(w, http.StatusServiceUnavailable, "not_ready")
				return
			}
		}
		writeStatus(w, http.StatusOK, "ready")
	})
	mux.Handle("GET /metrics", metricsHandler)
	return mux
}

func writeStatus(w http.ResponseWriter, status int, value string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": value})
}
