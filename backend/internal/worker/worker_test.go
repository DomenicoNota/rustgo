package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DomenicoNota/rustgo/backend/internal/delivery"
	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
	"github.com/DomenicoNota/rustgo/backend/internal/telemetry"
	"github.com/segmentio/kafka-go"
)

func TestProcessPersistsBeforeCommitting(t *testing.T) {
	trace := make([]string, 0, 2)
	reader := &fakeReader{trace: &trace}
	store := newFakeStore()
	store.trace = &trace
	processor := testWorker(t, reader, &fakeDLQ{}, store, nil)

	if err := processor.process(context.Background(), validMessage(t)); err != nil {
		t.Fatal(err)
	}
	if len(store.rows) != 1 || reader.successfulCommits != 1 {
		t.Fatalf("terminal state missing: rows=%d successful_commits=%d", len(store.rows), reader.successfulCommits)
	}
	if len(trace) != 2 || trace[0] != "store" || trace[1] != "commit" {
		t.Fatalf("persistence/commit order violated: %#v", trace)
	}
}

func TestTemporaryPersistenceFailureRecoversWithBackoff(t *testing.T) {
	reader := &fakeReader{}
	store := newFakeStore()
	store.remainingFailures = 2
	delays := make([]time.Duration, 0, 2)
	processor := testWorker(t, reader, &fakeDLQ{}, store, &delays)

	if err := processor.process(context.Background(), validMessage(t)); err != nil {
		t.Fatal(err)
	}
	if len(store.rows) != 1 || reader.successfulCommits != 1 {
		t.Fatalf("event did not reach its terminal state: rows=%d commits=%d", len(store.rows), reader.successfulCommits)
	}
	wantDelays := []time.Duration{10 * time.Millisecond, 15 * time.Millisecond}
	if !equalDurations(delays, wantDelays) {
		t.Fatalf("unexpected bounded backoff: got %v want %v", delays, wantDelays)
	}
}

func TestPersistenceMetricsCountAttemptsAndRecovery(t *testing.T) {
	reader := &fakeReader{}
	store := newFakeStore()
	store.remainingFailures = 1
	metrics := &telemetry.WorkerMetrics{}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	processor, err := New(reader, &fakeDLQ{}, store, logger, metrics, testRetryPolicy())
	if err != nil {
		t.Fatal(err)
	}
	processor.wait = func(context.Context, time.Duration) error { return nil }
	if err := processor.process(context.Background(), validMessage(t)); err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	metrics.ServeHTTP(recorder, nil)
	for _, sample := range []string{
		"logstream_worker_persistence_successes_total 1",
		"logstream_worker_persistence_failures_total 1",
		"logstream_worker_records_processed_total 1",
	} {
		if !strings.Contains(recorder.Body.String(), sample) {
			t.Fatalf("missing sample %q in:\n%s", sample, recorder.Body.String())
		}
	}
}

func TestPersistenceRetryExhaustionDoesNotCommitAndRedeliveryRecovers(t *testing.T) {
	reader := &fakeReader{}
	store := newFakeStore()
	store.remainingFailures = 3
	processor := testWorker(t, reader, &fakeDLQ{}, store, nil)
	message := validMessage(t)

	err := processor.process(context.Background(), message)
	var exhausted *RetryExhaustedError
	if !errors.As(err, &exhausted) || exhausted.Operation != "persist event" {
		t.Fatalf("expected persistence retry exhaustion, got %v", err)
	}
	if reader.successfulCommits != 0 || len(store.rows) != 0 {
		t.Fatalf("failed write advanced progress: commits=%d rows=%d", reader.successfulCommits, len(store.rows))
	}

	if err := processor.process(context.Background(), message); err != nil {
		t.Fatalf("redelivery did not recover: %v", err)
	}
	if reader.successfulCommits != 1 || len(store.rows) != 1 {
		t.Fatalf("redelivery terminal state missing: commits=%d rows=%d", reader.successfulCommits, len(store.rows))
	}
}

func TestCommitFailureLeavesEventForIdempotentRedelivery(t *testing.T) {
	reader := &fakeReader{remainingCommitFailures: 3}
	store := newFakeStore()
	processor := testWorker(t, reader, &fakeDLQ{}, store, nil)
	message := validMessage(t)

	err := processor.process(context.Background(), message)
	var exhausted *RetryExhaustedError
	if !errors.As(err, &exhausted) || exhausted.Operation != "commit Kafka offset" {
		t.Fatalf("expected commit retry exhaustion, got %v", err)
	}
	if len(store.rows) != 1 || reader.successfulCommits != 0 {
		t.Fatalf("unexpected state after failed commit: rows=%d commits=%d", len(store.rows), reader.successfulCommits)
	}

	if err := processor.process(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if len(store.rows) != 1 || reader.successfulCommits != 1 {
		t.Fatalf("idempotent redelivery failed: rows=%d commits=%d", len(store.rows), reader.successfulCommits)
	}
}

func TestMalformedRecordIsDeadLetteredBeforeCommitWithoutPayloadLeak(t *testing.T) {
	reader := &fakeReader{}
	dlq := &fakeDLQ{}
	store := newFakeStore()
	processor := testWorker(t, reader, dlq, store, nil)
	message := kafka.Message{
		Topic:     "logs.raw",
		Partition: 0,
		Offset:    9,
		Key:       []byte("sensitive-key"),
		Value:     []byte(`{"api_key":"do-not-copy"}`),
	}

	if err := processor.process(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	if len(dlq.published) != 1 || reader.successfulCommits != 1 || len(store.rows) != 0 {
		t.Fatalf("unexpected terminal state: dlq=%d commits=%d rows=%d", len(dlq.published), reader.successfulCommits, len(store.rows))
	}
	record := dlq.published[0]
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "do-not-copy") || strings.Contains(string(encoded), "sensitive-key") {
		t.Fatalf("dead letter leaked poison content: %s", encoded)
	}
	if record.Failure.Code != "malformed_event" || record.OriginalOffset != message.Offset {
		t.Fatalf("dead letter lacks diagnostic metadata: %+v", record)
	}
}

func TestDLQFailureDoesNotCommitAndRetryUsesStableRecord(t *testing.T) {
	reader := &fakeReader{}
	dlq := &fakeDLQ{remainingFailures: 3}
	processor := testWorker(t, reader, dlq, newFakeStore(), nil)
	message := kafka.Message{Topic: "logs.raw", Partition: 1, Offset: 12, Value: []byte(`not-json`)}

	err := processor.process(context.Background(), message)
	var exhausted *RetryExhaustedError
	if !errors.As(err, &exhausted) || exhausted.Operation != "publish dead letter" {
		t.Fatalf("expected DLQ retry exhaustion, got %v", err)
	}
	if reader.successfulCommits != 0 || len(dlq.published) != 0 || len(dlq.attempted) != 3 {
		t.Fatalf("DLQ failure advanced progress: commits=%d published=%d attempted=%d", reader.successfulCommits, len(dlq.published), len(dlq.attempted))
	}
	for _, record := range dlq.attempted[1:] {
		if record.ID != dlq.attempted[0].ID || !record.FailedAt.Equal(dlq.attempted[0].FailedAt) {
			t.Fatalf("DLQ record changed across retries: %#v", dlq.attempted)
		}
	}

	if err := processor.process(context.Background(), message); err != nil {
		t.Fatalf("poison redelivery did not recover: %v", err)
	}
	if reader.successfulCommits != 1 || len(dlq.published) != 1 {
		t.Fatalf("poison record did not reach terminal state: commits=%d published=%d", reader.successfulCommits, len(dlq.published))
	}
}

func TestCancellationStopsPersistenceWithoutCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := &fakeReader{}
	store := newFakeStore()
	processor := testWorker(t, reader, &fakeDLQ{}, store, nil)

	err := processor.process(ctx, validMessage(t))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if reader.successfulCommits != 0 || len(store.rows) != 0 {
		t.Fatalf("canceled event reached terminal state: commits=%d rows=%d", reader.successfulCommits, len(store.rows))
	}
}

func TestCancellationInterruptsInFlightPersistenceWithoutCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := &fakeReader{}
	store := newFakeStore()
	started := make(chan struct{})
	store.insert = func(operationCtx context.Context, _ []logstore.LogEvent) error {
		close(started)
		<-operationCtx.Done()
		return operationCtx.Err()
	}
	processor := testWorker(t, reader, &fakeDLQ{}, store, nil)
	message := validMessage(t)
	result := make(chan error, 1)
	go func() { result <- processor.process(ctx, message) }()

	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected in-flight cancellation, got %v", err)
	}
	if reader.successfulCommits != 0 || len(store.rows) != 0 {
		t.Fatalf("in-flight cancellation advanced progress: commits=%d rows=%d", reader.successfulCommits, len(store.rows))
	}
}

func TestRunExitsAfterBoundedKafkaFetchFailures(t *testing.T) {
	reader := &fakeReader{fetchErr: errors.New("Kafka unavailable")}
	delays := make([]time.Duration, 0, 2)
	processor := testWorker(t, reader, &fakeDLQ{}, newFakeStore(), &delays)

	err := processor.Run(context.Background())
	var exhausted *RetryExhaustedError
	if !errors.As(err, &exhausted) || exhausted.Operation != "fetch Kafka message" {
		t.Fatalf("expected bounded fetch failure, got %v", err)
	}
	if reader.fetchAttempts != testRetryPolicy().MaxAttempts {
		t.Fatalf("unexpected fetch attempts: %d", reader.fetchAttempts)
	}
	if !equalDurations(delays, []time.Duration{10 * time.Millisecond, 15 * time.Millisecond}) {
		t.Fatalf("unexpected fetch backoff: %v", delays)
	}
}

func TestRunReturnsCleanlyWhenFetchIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := &fakeReader{fetchErr: context.Canceled}
	processor := testWorker(t, reader, &fakeDLQ{}, newFakeStore(), nil)
	if err := processor.Run(ctx); err != nil {
		t.Fatalf("expected clean cancellation, got %v", err)
	}
}

func TestRetryPolicyValidationAndCap(t *testing.T) {
	invalid := RetryPolicy{MaxAttempts: 0, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, AttemptTimeout: time.Second}
	if err := invalid.Validate(); err == nil {
		t.Fatal("expected invalid retry budget")
	}
	policy := testRetryPolicy()
	if got := policy.delayAfterFailure(20); got != policy.MaxDelay {
		t.Fatalf("backoff was not capped: %s", got)
	}
}

func testWorker(t *testing.T, reader *fakeReader, dlq *fakeDLQ, store *fakeStore, delays *[]time.Duration) *Worker {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	processor, err := New(reader, dlq, store, logger, &telemetry.WorkerMetrics{}, testRetryPolicy())
	if err != nil {
		t.Fatal(err)
	}
	processor.now = func() time.Time { return time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC) }
	processor.wait = func(ctx context.Context, delay time.Duration) error {
		if delays != nil {
			*delays = append(*delays, delay)
		}
		return ctx.Err()
	}
	return processor
}

func testRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    3,
		BaseDelay:      10 * time.Millisecond,
		MaxDelay:       15 * time.Millisecond,
		AttemptTimeout: time.Second,
	}
}

func validMessage(t *testing.T) kafka.Message {
	t.Helper()
	event := logstore.LogEvent{
		SchemaVersion: logstore.LogEventSchemaVersion,
		ID:            "worker-event",
		Service:       "worker-test",
		Level:         "info",
		Message:       "hello",
		Timestamp:     time.Date(2026, 8, 7, 11, 0, 0, 0, time.UTC),
		Attributes:    map[string]any{},
		Source:        map[string]string{},
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Topic: "logs.raw", Partition: 0, Offset: 4, Value: payload}
}

func equalDurations(got, want []time.Duration) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

type fakeReader struct {
	remainingCommitFailures int
	commitAttempts          int
	successfulCommits       int
	fetchAttempts           int
	fetchErr                error
	trace                   *[]string
}

func (r *fakeReader) FetchMessage(context.Context) (kafka.Message, error) {
	r.fetchAttempts++
	return kafka.Message{}, r.fetchErr
}

func (r *fakeReader) CommitMessages(context.Context, ...kafka.Message) error {
	r.commitAttempts++
	if r.trace != nil {
		*r.trace = append(*r.trace, "commit")
	}
	if r.remainingCommitFailures > 0 {
		r.remainingCommitFailures--
		return errors.New("commit unavailable")
	}
	r.successfulCommits++
	return nil
}

type fakeStore struct {
	remainingFailures int
	attempts          int
	rows              map[string]logstore.LogEvent
	trace             *[]string
	insert            func(context.Context, []logstore.LogEvent) error
}

func newFakeStore() *fakeStore {
	return &fakeStore{rows: make(map[string]logstore.LogEvent)}
}

func (s *fakeStore) Insert(ctx context.Context, events []logstore.LogEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.attempts++
	if s.trace != nil {
		*s.trace = append(*s.trace, "store")
	}
	if s.remainingFailures > 0 {
		s.remainingFailures--
		return errors.New("database unavailable")
	}
	if s.insert != nil {
		return s.insert(ctx, events)
	}
	for _, event := range events {
		s.rows[event.ID] = event
	}
	return nil
}

type fakeDLQ struct {
	remainingFailures int
	attempted         []delivery.DeadLetter
	published         []delivery.DeadLetter
}

func (d *fakeDLQ) PublishDeadLetter(ctx context.Context, record delivery.DeadLetter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.attempted = append(d.attempted, record)
	if d.remainingFailures > 0 {
		d.remainingFailures--
		return errors.New("DLQ unavailable")
	}
	d.published = append(d.published, record)
	return nil
}
