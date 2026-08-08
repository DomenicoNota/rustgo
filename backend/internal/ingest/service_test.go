package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
)

func TestIngestRejectsEmptyBatch(t *testing.T) {
	service := NewService(&fakePublisher{}, 10)
	_, err := service.Ingest(context.Background(), Request{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestIngestRejectsBatchOverConfiguredMaximum(t *testing.T) {
	service := NewService(&fakePublisher{}, 1)
	_, err := service.Ingest(context.Background(), Request{Events: []logstore.LogEvent{validEvent(), validEvent()}})
	var invalid ErrInvalidRequest
	if !errors.As(err, &invalid) {
		t.Fatalf("expected invalid request, got %v", err)
	}
}

func TestIngestPropagatesPublisherFailureAndCancellation(t *testing.T) {
	publishErr := errors.New("Kafka unavailable")
	publisher := &fakePublisher{err: publishErr}
	service := NewService(publisher, 10)
	if _, err := service.Ingest(context.Background(), Request{Events: []logstore.LogEvent{validEvent()}}); !errors.Is(err, publishErr) {
		t.Fatalf("expected publisher error, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	publisher.err = nil
	if _, err := service.Ingest(ctx, Request{Events: []logstore.LogEvent{validEvent()}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestIngestAcceptsValidBatch(t *testing.T) {
	publisher := &fakePublisher{}
	service := NewService(publisher, 10)
	result, err := service.Ingest(context.Background(), Request{
		Events: []logstore.LogEvent{validEvent()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Accepted != 1 || result.Rejected != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if publisher.published != 1 {
		t.Fatalf("expected published event, got %d", publisher.published)
	}
}

func TestIngestCountsInvalidLogsAsRejected(t *testing.T) {
	publisher := &fakePublisher{}
	service := NewService(publisher, 10)
	result, err := service.Ingest(context.Background(), Request{
		Events: []logstore.LogEvent{
			validEvent(),
			{SchemaVersion: logstore.LogEventSchemaVersion, ID: "bad", Service: "auth", Level: "sideways", Message: "bad", Timestamp: time.Now(), Attributes: map[string]any{}, Source: map[string]string{}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Accepted != 1 || result.Rejected != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func validEvent() logstore.LogEvent {
	return logstore.LogEvent{
		SchemaVersion: logstore.LogEventSchemaVersion,
		ID:            "event-1",
		Service:       "auth-service",
		Level:         "info",
		Message:       "ok",
		Timestamp:     time.Now().UTC(),
		Attributes:    map[string]any{},
		Source:        map[string]string{},
	}
}

type fakePublisher struct {
	published int
	err       error
}

func (f *fakePublisher) Publish(ctx context.Context, events []logstore.LogEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.published += len(events)
	return f.err
}
