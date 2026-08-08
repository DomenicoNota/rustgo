package delivery

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestDeadLetterFingerprintsWithoutCopyingSensitiveContent(t *testing.T) {
	failedAt := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	message := kafka.Message{
		Topic:     "logs.raw",
		Partition: 2,
		Offset:    42,
		Key:       []byte("secret-key"),
		Value:     []byte(`{"api_key":"do-not-copy"}`),
		Time:      failedAt.Add(-time.Minute),
	}

	record := NewDeadLetter(message, Failure{Code: "malformed_event", Message: "record is not valid LogEvent JSON"}, failedAt)
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	serialized := string(encoded)
	if strings.Contains(serialized, "do-not-copy") || strings.Contains(serialized, "secret-key") {
		t.Fatalf("dead letter leaked source content: %s", serialized)
	}
	if record.PayloadBytes != len(message.Value) || len(record.PayloadSHA256) != 64 || len(record.KeySHA256) != 64 {
		t.Fatalf("unexpected fingerprints: %+v", record)
	}
	if record.OriginalTopic != message.Topic || record.OriginalPartition != message.Partition || record.OriginalOffset != message.Offset {
		t.Fatalf("source coordinates were not preserved: %+v", record)
	}
}

func TestDeadLetterIDIsStableForOneKafkaRecord(t *testing.T) {
	message := kafka.Message{Topic: "logs.raw", Partition: 1, Offset: 9, Value: []byte("first")}
	first := NewDeadLetter(message, Failure{Code: "one", Message: "one"}, time.Unix(1, 0))
	message.Value = []byte("second")
	second := NewDeadLetter(message, Failure{Code: "two", Message: "two"}, time.Unix(2, 0))
	if first.ID != second.ID {
		t.Fatalf("source record ID changed across DLQ retries: %q != %q", first.ID, second.ID)
	}
}
