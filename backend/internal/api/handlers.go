package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/DomenicoNota/rustgo/backend/internal/ingest"
	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
	"github.com/DomenicoNota/rustgo/backend/internal/telemetry"
)

type HealthChecker interface {
	Check(ctx context.Context) error
}

type Ingestor interface {
	Ingest(ctx context.Context, req ingest.Request) (ingest.Result, error)
}

type LogReader interface {
	Search(ctx context.Context, params logstore.SearchParams) (logstore.SearchPage, error)
	Services(ctx context.Context) ([]string, error)
}

type MetricsReader interface {
	Metrics(ctx context.Context) (logstore.Metrics, error)
}

type Handler struct {
	readiness      []HealthChecker
	ingest         Ingestor
	logs           LogReader
	metrics        MetricsReader
	processMetrics *telemetry.APIMetrics
	maxBodyBytes   int64
	defaultPage    int
	maxPageSize    int
	maxFilterBytes int
}

func (h Handler) Health(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h Handler) Ready(w http.ResponseWriter, r *http.Request) {
	for _, checker := range h.readiness {
		if err := checker.Check(r.Context()); err != nil {
			respondError(w, http.StatusServiceUnavailable, "not_ready", "a required dependency is unavailable")
			return
		}
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h Handler) IngestLogs(w http.ResponseWriter, r *http.Request) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		respondError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}
	if h.maxBodyBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	}
	var req ingest.Request
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			respondError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the configured limit")
			return
		}
		respondError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			respondError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the configured limit")
			return
		}
		respondError(w, http.StatusBadRequest, "invalid_json", "request body must contain one JSON value")
		return
	}
	result, err := h.ingest.Ingest(r.Context(), req)
	h.processMetrics.AddRejected(result.Rejected)
	if err != nil {
		var invalid ingest.ErrInvalidRequest
		if errors.As(err, &invalid) {
			respondError(w, http.StatusBadRequest, "invalid_request", invalid.Message)
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			respondError(w, http.StatusGatewayTimeout, "request_timeout", "ingestion timed out")
			return
		}
		respondError(w, http.StatusServiceUnavailable, "ingest_unavailable", "ingestion is temporarily unavailable")
		return
	}
	h.processMetrics.AddIngested(result.Accepted)
	respondJSON(w, http.StatusAccepted, result)
}

func (h Handler) SearchLogs(w http.ResponseWriter, r *http.Request) {
	params, err := h.parseSearchParams(r)
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	page, err := h.logs.Search(r.Context(), params)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			respondError(w, http.StatusGatewayTimeout, "request_timeout", "query timed out")
			return
		}
		respondError(w, http.StatusServiceUnavailable, "query_unavailable", "log queries are temporarily unavailable")
		return
	}
	nextCursor, err := encodeCursor(page.Next)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "internal_error", "failed to encode query response")
		return
	}
	items := page.Items
	if items == nil {
		items = []logstore.LogRecord{}
	}
	respondJSON(w, http.StatusOK, searchResponse{Items: items, NextCursor: nextCursor})
}

func (h Handler) Services(w http.ResponseWriter, r *http.Request) {
	services, err := h.logs.Services(r.Context())
	if err != nil {
		respondReadDependencyError(w, err, "services")
		return
	}
	respondJSON(w, http.StatusOK, map[string][]string{"services": services})
}

func (h Handler) Metrics(w http.ResponseWriter, r *http.Request) {
	metrics, err := h.metrics.Metrics(r.Context())
	if err != nil {
		respondReadDependencyError(w, err, "metrics")
		return
	}
	respondJSON(w, http.StatusOK, metrics)
}

func respondReadDependencyError(w http.ResponseWriter, err error, resource string) {
	if errors.Is(err, context.DeadlineExceeded) {
		respondError(w, http.StatusGatewayTimeout, "request_timeout", resource+" query timed out")
		return
	}
	respondError(w, http.StatusServiceUnavailable, "query_unavailable", resource+" are temporarily unavailable")
}

type searchResponse struct {
	Items      []logstore.LogRecord `json:"items"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

func (h Handler) parseSearchParams(r *http.Request) (logstore.SearchParams, error) {
	query := r.URL.Query()
	allowed := map[string]struct{}{
		"service": {}, "level": {}, "q": {}, "start": {}, "end": {}, "limit": {}, "cursor": {},
	}
	for name, values := range query {
		if _, ok := allowed[name]; !ok {
			return logstore.SearchParams{}, fmt.Errorf("unsupported query parameter %q", name)
		}
		if len(values) != 1 {
			return logstore.SearchParams{}, fmt.Errorf("query parameter %q must appear once", name)
		}
	}
	limit := h.defaultPage
	if raw := query.Get("limit"); strings.TrimSpace(raw) != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 || value > h.maxPageSize {
			return logstore.SearchParams{}, fmt.Errorf("limit must be between 1 and %d", h.maxPageSize)
		}
		limit = value
	}
	service, err := boundedFilter(query.Get("service"), "service", h.maxFilterBytes)
	if err != nil {
		return logstore.SearchParams{}, err
	}
	text, err := boundedFilter(query.Get("q"), "q", h.maxFilterBytes)
	if err != nil {
		return logstore.SearchParams{}, err
	}
	start, err := parseOptionalTime(query.Get("start"))
	if err != nil {
		return logstore.SearchParams{}, errors.New("start must be RFC3339")
	}
	end, err := parseOptionalTime(query.Get("end"))
	if err != nil {
		return logstore.SearchParams{}, errors.New("end must be RFC3339")
	}
	if start != nil && end != nil && start.After(*end) {
		return logstore.SearchParams{}, errors.New("start must not be after end")
	}
	cursor, err := decodeCursor(query.Get("cursor"))
	if err != nil {
		return logstore.SearchParams{}, err
	}
	level := strings.TrimSpace(query.Get("level"))
	if level != "" && !logstore.ValidateLevel(level) {
		return logstore.SearchParams{}, errors.New("level is unsupported")
	}
	return logstore.SearchParams{
		Service: service,
		Level:   logstore.NormalizeLevel(level),
		Query:   text,
		Start:   start,
		End:     end,
		Limit:   limit,
		Cursor:  cursor,
	}, nil
}

func boundedFilter(raw, name string, maximum int) (string, error) {
	value := strings.TrimSpace(raw)
	if len(value) > maximum {
		return "", fmt.Errorf("%s exceeds %d bytes", name, maximum)
	}
	return value, nil
}

func parseOptionalTime(value string) (*time.Time, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, err
	}
	parsed = parsed.UTC()
	return &parsed, nil
}
