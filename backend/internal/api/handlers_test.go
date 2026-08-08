package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/DomenicoNota/rustgo/backend/internal/ingest"
	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
	"github.com/DomenicoNota/rustgo/backend/internal/telemetry"
)

func TestIngestAuthenticationIsExplicit(t *testing.T) {
	tests := []struct {
		name   string
		header string
		status int
	}{
		{name: "missing", status: http.StatusUnauthorized},
		{name: "wrong key", header: "Bearer wrong", status: http.StatusUnauthorized},
		{name: "malformed", header: "Basic test-key", status: http.StatusUnauthorized},
		{name: "accepted", header: "Bearer test-key", status: http.StatusAccepted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			router := testRouter(&fakeIngest{result: ingest.Result{Accepted: 1}}, &fakeLogs{})
			req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(`{"events":[]}`))
			req.Header.Set("Authorization", test.header)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != test.status {
				t.Fatalf("got status %d: %s", rec.Code, rec.Body.String())
			}
			if test.status == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Fatal("missing WWW-Authenticate header")
			}
		})
	}
}

func TestIngestRejectsOversizedBodyWithJSONError(t *testing.T) {
	router := testRouterWithLimits(&fakeIngest{}, &fakeLogs{}, 64, 100, 500, 256)
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(`{"events":[{"message":"`+strings.Repeat("x", 100)+`"}]}`))
	req.Header.Set("Authorization", "Bearer test-key")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	assertError(t, rec, http.StatusRequestEntityTooLarge, "request_too_large")
}

func TestIngestRejectsTrailingJSONValue(t *testing.T) {
	router := testRouter(&fakeIngest{}, &fakeLogs{})
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(`{"events":[]} {}`))
	req.Header.Set("Authorization", "Bearer test-key")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	assertError(t, rec, http.StatusBadRequest, "invalid_json")
}

func TestIngestMapsDependencyAndTimeoutErrors(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{name: "dependency", err: errors.New("Kafka unavailable"), status: http.StatusServiceUnavailable, code: "ingest_unavailable"},
		{name: "timeout", err: context.DeadlineExceeded, status: http.StatusGatewayTimeout, code: "request_timeout"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			router := testRouter(&fakeIngest{err: test.err}, &fakeLogs{})
			req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(`{"events":[]}`))
			req.Header.Set("Authorization", "Bearer test-key")
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			assertError(t, rec, test.status, test.code)
		})
	}
}

func TestIngestRejectsUnsupportedMediaType(t *testing.T) {
	router := testRouter(&fakeIngest{}, &fakeLogs{})
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(`{"events":[]}`))
	req.Header.Set("Authorization", "Bearer test-key")
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	assertError(t, rec, http.StatusUnsupportedMediaType, "unsupported_media_type")
}

func TestSearchParsesBoundedFiltersAndOpaqueCursor(t *testing.T) {
	next := &logstore.PageCursor{Timestamp: time.Date(2026, 8, 7, 15, 0, 0, 0, time.UTC), ID: "event-2"}
	cursor, err := encodeCursor(next)
	if err != nil {
		t.Fatal(err)
	}
	logs := &fakeLogs{}
	router := testRouter(&fakeIngest{}, logs)
	query := url.Values{
		"service": {" auth-service "},
		"level":   {"WARNING"},
		"q":       {" failed login "},
		"start":   {"2026-08-07T10:00:00-04:00"},
		"end":     {"2026-08-07T16:00:00Z"},
		"limit":   {"25"},
		"cursor":  {cursor},
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/logs?"+query.Encode(), nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d: %s", rec.Code, rec.Body.String())
	}
	params := logs.params
	if params.Service != "auth-service" || params.Level != "warn" || params.Query != "failed login" || params.Limit != 25 {
		t.Fatalf("unexpected params: %+v", params)
	}
	if params.Start == nil || params.Start.Location() != time.UTC || params.Cursor.ID != "event-2" {
		t.Fatalf("times/cursor were not normalized: %+v", params)
	}
}

func TestSearchRejectsInvalidBounds(t *testing.T) {
	tests := []string{
		"limit=501",
		"service=" + strings.Repeat("x", 257),
		"start=2026-08-08T00%3A00%3A00Z&end=2026-08-07T00%3A00%3A00Z",
		"cursor=not-a-cursor",
		"unexpected=value",
		"service=one&service=two",
	}
	for _, query := range tests {
		router := testRouter(&fakeIngest{}, &fakeLogs{})
		req := httptest.NewRequest(http.MethodGet, "/v1/logs?"+query, nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		assertError(t, rec, http.StatusBadRequest, "invalid_request")
	}
}

func TestSearchMapsStoreFailures(t *testing.T) {
	tests := []struct {
		err    error
		status int
		code   string
	}{
		{err: errors.New("database unavailable"), status: http.StatusServiceUnavailable, code: "query_unavailable"},
		{err: context.DeadlineExceeded, status: http.StatusGatewayTimeout, code: "request_timeout"},
	}
	for _, test := range tests {
		router := testRouter(&fakeIngest{}, &fakeLogs{err: test.err})
		req := httptest.NewRequest(http.MethodGet, "/v1/logs", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		assertError(t, rec, test.status, test.code)
	}
}

func TestRequestIDRejectsUnsafeClientValue(t *testing.T) {
	router := testRouter(&fakeIngest{}, &fakeLogs{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", strings.Repeat("x", maxRequestIDLength+1))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-ID"); got == "" || len(got) > maxRequestIDLength {
		t.Fatalf("unsafe request ID was not replaced: %q", got)
	}
}

func TestOperationalMetricsTrackAuthenticationAndIngestion(t *testing.T) {
	processMetrics := &telemetry.APIMetrics{}
	router := testRouterWithMetrics(&fakeIngest{result: ingest.Result{Accepted: 2, Rejected: 1}}, &fakeLogs{}, processMetrics)

	unauthorized := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(`{"events":[]}`))
	unauthorized.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(httptest.NewRecorder(), unauthorized)

	authorized := httptest.NewRequest(http.MethodPost, "/v1/ingest", strings.NewReader(`{"events":[]}`))
	authorized.Header.Set("Authorization", "Bearer test-key")
	authorized.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(httptest.NewRecorder(), authorized)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, sample := range []string{
		"logstream_api_ingested_events_total 2",
		"logstream_api_rejected_events_total 1",
		"logstream_api_auth_failures_total 1",
	} {
		if !strings.Contains(recorder.Body.String(), sample) {
			t.Fatalf("missing sample %q in:\n%s", sample, recorder.Body.String())
		}
	}
}

func testRouter(ingestor Ingestor, logs LogReader) http.Handler {
	return testRouterWithLimits(ingestor, logs, 1024, 100, 500, 256)
}

func testRouterWithLimits(ingestor Ingestor, logs LogReader, body int64, defaultPage, maxPage, maxFilter int) http.Handler {
	return newTestRouter(ingestor, logs, body, defaultPage, maxPage, maxFilter, &telemetry.APIMetrics{})
}

func testRouterWithMetrics(ingestor Ingestor, logs LogReader, metrics *telemetry.APIMetrics) http.Handler {
	return newTestRouter(ingestor, logs, 1024, 100, 500, 256, metrics)
}

func newTestRouter(ingestor Ingestor, logs LogReader, body int64, defaultPage, maxPage, maxFilter int, processMetrics *telemetry.APIMetrics) http.Handler {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewRouter(RouterConfig{
		Authenticator:  apiKeyAuthenticatorForTest(),
		Logger:         logger,
		Readiness:      []HealthChecker{okHealth{}},
		Ingest:         ingestor,
		Logs:           logs,
		Metrics:        fakeMetrics{},
		ProcessMetrics: processMetrics,
		MaxBodyBytes:   body,
		DefaultPage:    defaultPage,
		MaxPageSize:    maxPage,
		MaxFilterBytes: maxFilter,
		RequestTimeout: time.Second,
	})
}

func apiKeyAuthenticatorForTest() APIKeyAuthenticator {
	return NewAPIKeyAuthenticator([]string{"test-key"})
}

func assertError(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("got status %d: %s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"code":"`+code+`"`) {
		t.Fatalf("unexpected error body: %s", recorder.Body.String())
	}
}

type okHealth struct{}

func (okHealth) Check(context.Context) error { return nil }

type fakeIngest struct {
	result ingest.Result
	err    error
}

func (f *fakeIngest) Ingest(context.Context, ingest.Request) (ingest.Result, error) {
	return f.result, f.err
}

type fakeLogs struct {
	params logstore.SearchParams
	page   logstore.SearchPage
	err    error
}

func (f *fakeLogs) Search(_ context.Context, params logstore.SearchParams) (logstore.SearchPage, error) {
	f.params = params
	return f.page, f.err
}

func (*fakeLogs) Services(context.Context) ([]string, error) {
	return []string{"auth-service"}, nil
}

type fakeMetrics struct{}

func (fakeMetrics) Metrics(context.Context) (logstore.Metrics, error) {
	return logstore.Metrics{}, nil
}
