package logs

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	LogEventSchemaVersion = 1
	MaxEventBytes         = 256 * 1024
	MaxMessageBytes       = 32 * 1024
	MaxIDBytes            = 128
	MaxServiceBytes       = 128
)

type LogEvent struct {
	SchemaVersion int               `json:"schema_version"`
	ID            string            `json:"id"`
	Service       string            `json:"service"`
	Level         string            `json:"level"`
	Message       string            `json:"message"`
	Timestamp     time.Time         `json:"timestamp"`
	Attributes    map[string]any    `json:"attributes"`
	Source        map[string]string `json:"source"`
}

type LogRecord struct {
	SchemaVersion int               `json:"schema_version"`
	ID            string            `json:"id"`
	Service       string            `json:"service"`
	Level         string            `json:"level"`
	Message       string            `json:"message"`
	Timestamp     time.Time         `json:"timestamp"`
	Attributes    map[string]any    `json:"attributes"`
	Source        map[string]string `json:"source"`
	ReceivedAt    time.Time         `json:"received_at,omitempty"`
}

type SearchParams struct {
	Service string
	Level   string
	Query   string
	Start   *time.Time
	End     *time.Time
	Limit   int
	Cursor  PageCursor
}

type SearchPage struct {
	Items []LogRecord
	Next  *PageCursor
}

type Metrics struct {
	LogsIngestedTotal int64 `json:"logs_ingested_total"`
	LogsLastMinute    int64 `json:"logs_last_minute"`
	ErrorsLastMinute  int64 `json:"errors_last_minute"`
	ActiveServices    int64 `json:"active_services"`
}

type PageCursor struct {
	Timestamp time.Time
	ID        string
}

func (c PageCursor) IsZero() bool {
	return strings.TrimSpace(c.ID) == "" || c.Timestamp.IsZero()
}

func ValidateLevel(level string) bool {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "trace", "debug", "info", "warn", "warning", "error", "fatal":
		return true
	default:
		return false
	}
}

func NormalizeLevel(level string) string {
	level = strings.ToLower(strings.TrimSpace(level))
	if level == "warning" {
		return "warn"
	}
	return level
}

func ValidateEvent(event LogEvent) error {
	if event.SchemaVersion != LogEventSchemaVersion {
		return fmt.Errorf("schema_version must be %d", LogEventSchemaVersion)
	}
	if strings.TrimSpace(event.ID) == "" {
		return fmt.Errorf("id is required")
	}
	if len(event.ID) > MaxIDBytes {
		return fmt.Errorf("id exceeds %d bytes", MaxIDBytes)
	}
	if strings.TrimSpace(event.Service) == "" {
		return fmt.Errorf("service is required")
	}
	if len(event.Service) > MaxServiceBytes {
		return fmt.Errorf("service exceeds %d bytes", MaxServiceBytes)
	}
	if strings.TrimSpace(event.Message) == "" {
		return fmt.Errorf("message is required")
	}
	if !ValidateLevel(event.Level) {
		return fmt.Errorf("level is unsupported")
	}
	if event.Timestamp.IsZero() {
		return fmt.Errorf("timestamp is required")
	}
	if len(event.Message) > MaxMessageBytes {
		return fmt.Errorf("message exceeds %d bytes", MaxMessageBytes)
	}
	if event.Attributes == nil {
		return fmt.Errorf("attributes must be an object")
	}
	if event.Source == nil {
		return fmt.Errorf("source must be an object")
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("event is not valid JSON: %w", err)
	}
	if len(encoded) > MaxEventBytes {
		return fmt.Errorf("event exceeds %d bytes", MaxEventBytes)
	}
	return nil
}
