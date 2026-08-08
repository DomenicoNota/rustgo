package delivery

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

const DeadLetterSchemaVersion = 1

// Failure is deliberately safe to serialize. Callers must not put payloads,
// credentials, connection strings, or arbitrary dependency errors in Message.
type Failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// DeadLetter contains enough information to locate and fingerprint a poison
// record without copying potentially sensitive log content to another topic.
type DeadLetter struct {
	SchemaVersion     int       `json:"schema_version"`
	ID                string    `json:"id"`
	OriginalTopic     string    `json:"original_topic"`
	OriginalPartition int       `json:"original_partition"`
	OriginalOffset    int64     `json:"original_offset"`
	OriginalTimestamp time.Time `json:"original_timestamp,omitempty"`
	FailedAt          time.Time `json:"failed_at"`
	Failure           Failure   `json:"failure"`
	PayloadSHA256     string    `json:"payload_sha256"`
	PayloadBytes      int       `json:"payload_bytes"`
	KeySHA256         string    `json:"key_sha256,omitempty"`
}

func NewDeadLetter(message kafka.Message, failure Failure, failedAt time.Time) DeadLetter {
	payloadDigest := sha256.Sum256(message.Value)
	keyDigest := ""
	if len(message.Key) > 0 {
		digest := sha256.Sum256(message.Key)
		keyDigest = hex.EncodeToString(digest[:])
	}

	identity := sha256.Sum256([]byte(fmt.Sprintf(
		"%s\x00%d\x00%d",
		message.Topic,
		message.Partition,
		message.Offset,
	)))

	return DeadLetter{
		SchemaVersion:     DeadLetterSchemaVersion,
		ID:                "dlq-" + hex.EncodeToString(identity[:]),
		OriginalTopic:     message.Topic,
		OriginalPartition: message.Partition,
		OriginalOffset:    message.Offset,
		OriginalTimestamp: message.Time.UTC(),
		FailedAt:          failedAt.UTC(),
		Failure:           failure,
		PayloadSHA256:     hex.EncodeToString(payloadDigest[:]),
		PayloadBytes:      len(message.Value),
		KeySHA256:         keyDigest,
	}
}
