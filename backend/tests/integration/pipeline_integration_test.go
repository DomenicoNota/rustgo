package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DomenicoNota/rustgo/backend/internal/broker"
	"github.com/DomenicoNota/rustgo/backend/internal/db"
	"github.com/DomenicoNota/rustgo/backend/internal/delivery"
	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
	"github.com/DomenicoNota/rustgo/backend/internal/store"
	"github.com/DomenicoNota/rustgo/backend/internal/telemetry"
	"github.com/DomenicoNota/rustgo/backend/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func TestKafkaWorkerPostgresDeliverySemantics(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	kafkaBrokers := splitNonEmpty(os.Getenv("TEST_KAFKA_BROKERS"))
	if databaseURL == "" || len(kafkaBrokers) == 0 {
		t.Skip("TEST_DATABASE_URL and TEST_KAFKA_BROKERS are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	rawTopic := "logs.raw.integration." + suffix
	dlqTopic := "logs.dlq.integration." + suffix
	groupID := "logstream-integration-" + suffix
	createKafkaTopics(t, ctx, kafkaBrokers, rawTopic, dlqTopic)

	pool, err := db.Open(ctx, databaseURL, db.Options{
		MaxConns:          4,
		MinConns:          0,
		ConnectTimeout:    3 * time.Second,
		MaxConnLifetime:   time.Minute,
		MaxConnIdleTime:   time.Minute,
		HealthCheckPeriod: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	service := "pipeline-integration-" + suffix
	event := integrationEvent("pipeline-event-"+suffix, service, time.Now().UTC())
	barrier := integrationEvent("pipeline-barrier-"+suffix, service, event.Timestamp.Add(time.Millisecond))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM logs WHERE service = $1`, service); err != nil {
			t.Errorf("clean pipeline integration rows: %v", err)
		}
	})

	reader := broker.NewReader(kafkaBrokers, rawTopic, groupID, 3*time.Second)
	observedReader := &commitObserver{
		Reader:        reader,
		targetEventID: barrier.ID,
		committed:     make(chan struct{}),
	}
	dlqPublisher := broker.NewPublisher(kafkaBrokers, dlqTopic, 3*time.Second)
	processor, err := worker.New(
		observedReader,
		dlqPublisher,
		store.NewPostgres(pool),
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		&telemetry.WorkerMetrics{},
		worker.RetryPolicy{
			MaxAttempts:    5,
			BaseDelay:      50 * time.Millisecond,
			MaxDelay:       250 * time.Millisecond,
			AttemptTimeout: 3 * time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerResult := make(chan error, 1)
	go func() { workerResult <- processor.Run(workerCtx) }()
	var stopOnce sync.Once
	stopAndCloseWorker := func() {
		stopOnce.Do(func() {
			stopWorker()
			select {
			case err := <-workerResult:
				if err != nil {
					t.Errorf("worker did not stop cleanly: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("worker did not stop after cancellation")
			}
			if err := reader.Close(); err != nil {
				t.Errorf("close source reader: %v", err)
			}
			if err := dlqPublisher.Close(); err != nil {
				t.Errorf("close DLQ publisher: %v", err)
			}
		})
	}
	t.Cleanup(stopAndCloseWorker)

	rawPublisher := broker.NewPublisher(kafkaBrokers, rawTopic, 3*time.Second)
	t.Cleanup(func() {
		if err := rawPublisher.Close(); err != nil {
			t.Errorf("close raw publisher: %v", err)
		}
	})
	if err := rawPublisher.Publish(ctx, []logstore.LogEvent{event, event}); err != nil {
		t.Fatal(err)
	}
	poisonPayload := []byte(`{"api_key":"integration-secret-must-not-reach-dlq"}`)
	poisonWriter := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBrokers...),
		Topic:        rawTopic,
		RequiredAcks: kafka.RequireAll,
	}
	t.Cleanup(func() {
		if err := poisonWriter.Close(); err != nil {
			t.Errorf("close poison writer: %v", err)
		}
	})
	if err := poisonWriter.WriteMessages(ctx, kafka.Message{Key: []byte("sensitive-key"), Value: poisonPayload}); err != nil {
		t.Fatal(err)
	}
	if err := rawPublisher.Publish(ctx, []logstore.LogEvent{barrier}); err != nil {
		t.Fatal(err)
	}

	waitForStoredIDs(t, ctx, pool, event.ID, barrier.ID)
	dlqMessage := readMatchingDeadLetter(t, ctx, kafkaBrokers, dlqTopic, rawTopic, poisonPayload)
	if bytes.Contains(dlqMessage.Value, []byte("integration-secret-must-not-reach-dlq")) || bytes.Contains(dlqMessage.Value, []byte("sensitive-key")) {
		t.Fatalf("DLQ record leaked source payload or key: %s", dlqMessage.Value)
	}
	select {
	case <-observedReader.committed:
	case <-ctx.Done():
		t.Fatalf("barrier offset was not committed: %v", ctx.Err())
	}

	stopAndCloseWorker()
	assertGroupHasNoUncommittedRecords(t, kafkaBrokers, rawTopic, groupID)
}

func createKafkaTopics(t *testing.T, ctx context.Context, brokers []string, topics ...string) {
	t.Helper()
	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	}
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}
	response, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Addr:   kafka.TCP(brokers...),
		Topics: configs,
	})
	if err != nil {
		t.Fatalf("create Kafka topics: %v", err)
	}
	for topic, topicErr := range response.Errors {
		if topicErr != nil {
			t.Fatalf("create Kafka topic %s: %v", topic, topicErr)
		}
	}
}

func waitForStoredIDs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID, barrierID string) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var eventRows int
		var barrierRows int
		err := pool.QueryRow(ctx, `
			SELECT
				COUNT(*) FILTER (WHERE id = $1),
				COUNT(*) FILTER (WHERE id = $2)
			FROM logs
			WHERE id IN ($1, $2)
		`, eventID, barrierID).Scan(&eventRows, &barrierRows)
		if err == nil && eventRows == 1 && barrierRows == 1 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for idempotent rows: event_rows=%d barrier_rows=%d query_error=%v context_error=%v", eventRows, barrierRows, err, ctx.Err())
		case <-ticker.C:
		}
	}
}

type commitObserver struct {
	Reader        *kafka.Reader
	targetEventID string
	committed     chan struct{}
	once          sync.Once
}

func (r *commitObserver) FetchMessage(ctx context.Context) (kafka.Message, error) {
	return r.Reader.FetchMessage(ctx)
}

func (r *commitObserver) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	if err := r.Reader.CommitMessages(ctx, messages...); err != nil {
		return err
	}
	for _, message := range messages {
		var event logstore.LogEvent
		if json.Unmarshal(message.Value, &event) == nil && event.ID == r.targetEventID {
			r.once.Do(func() { close(r.committed) })
		}
	}
	return nil
}

func readMatchingDeadLetter(t *testing.T, ctx context.Context, brokers []string, topic, originalTopic string, payload []byte) kafka.Message {
	t.Helper()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   0,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    512 << 10,
		MaxWait:     time.Second,
	})
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close DLQ reader: %v", err)
		}
	})
	wantDigest := sha256.Sum256(payload)
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatalf("read dead letter: %v", err)
		}
		var record delivery.DeadLetter
		if err := json.Unmarshal(message.Value, &record); err != nil {
			continue
		}
		if record.OriginalTopic == originalTopic && record.PayloadSHA256 == hex.EncodeToString(wantDigest[:]) {
			if record.SchemaVersion != delivery.DeadLetterSchemaVersion || record.Failure.Code != "malformed_event" || record.PayloadBytes != len(payload) {
				t.Fatalf("dead letter metadata is incomplete: %+v", record)
			}
			return message
		}
	}
}

func assertGroupHasNoUncommittedRecords(t *testing.T, brokers []string, topic, groupID string) {
	t.Helper()
	reader := broker.NewReader(brokers, topic, groupID, 3*time.Second)
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("close progress reader: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	message, err := reader.FetchMessage(ctx)
	if err == nil {
		t.Fatalf("consumer group redelivered committed record at offset %d", message.Offset)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("check committed progress: %v", err)
	}
}

func splitNonEmpty(raw string) []string {
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	return result
}
