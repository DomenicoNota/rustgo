package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/DomenicoNota/rustgo/backend/internal/telemetry"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type RouterConfig struct {
	Authenticator  APIKeyAuthenticator
	Logger         *slog.Logger
	Readiness      []HealthChecker
	Ingest         Ingestor
	Logs           LogReader
	Metrics        MetricsReader
	ProcessMetrics *telemetry.APIMetrics
	MaxBodyBytes   int64
	DefaultPage    int
	MaxPageSize    int
	MaxFilterBytes int
	RequestTimeout time.Duration
}

func NewRouter(cfg RouterConfig) http.Handler {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.DefaultPage <= 0 {
		cfg.DefaultPage = 100
	}
	if cfg.MaxPageSize <= 0 {
		cfg.MaxPageSize = 500
	}
	if cfg.MaxFilterBytes <= 0 {
		cfg.MaxFilterBytes = 256
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 12 * time.Second
	}
	if cfg.ProcessMetrics == nil {
		cfg.ProcessMetrics = &telemetry.APIMetrics{}
	}
	handler := Handler{
		readiness:      cfg.Readiness,
		ingest:         cfg.Ingest,
		logs:           cfg.Logs,
		metrics:        cfg.Metrics,
		processMetrics: cfg.ProcessMetrics,
		maxBodyBytes:   cfg.MaxBodyBytes,
		defaultPage:    cfg.DefaultPage,
		maxPageSize:    cfg.MaxPageSize,
		maxFilterBytes: cfg.MaxFilterBytes,
	}

	r := chi.NewRouter()
	r.Use(requestID)
	r.Use(recoverer(logger))
	r.Use(cors)
	r.Use(accessLog(logger, cfg.ProcessMetrics))
	r.Use(middleware.Timeout(cfg.RequestTimeout))
	r.Get("/healthz", handler.Health)
	r.Get("/readyz", handler.Ready)
	r.Handle("/metrics", cfg.ProcessMetrics)
	r.Route("/v1", func(r chi.Router) {
		r.With(auth(cfg.Authenticator, cfg.ProcessMetrics)).Post("/ingest", handler.IngestLogs)
		r.Get("/logs", handler.SearchLogs)
		r.Get("/services", handler.Services)
		r.Get("/metrics", handler.Metrics)
	})

	return r
}
