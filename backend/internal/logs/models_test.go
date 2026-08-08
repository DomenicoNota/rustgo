package logs

import (
	"strings"
	"testing"
	"time"
)

func TestValidateEventRejectsMissingService(t *testing.T) {
	err := ValidateEvent(LogEvent{
		SchemaVersion: LogEventSchemaVersion,
		ID:            "event-1",
		Level:         "info",
		Message:       "hello",
		Timestamp:     time.Now(),
		Attributes:    map[string]any{},
		Source:        map[string]string{},
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestValidateEventRejectsUnknownSchemaVersion(t *testing.T) {
	err := ValidateEvent(LogEvent{
		SchemaVersion: 2,
		ID:            "event-1",
		Service:       "auth-service",
		Level:         "info",
		Message:       "hello",
		Timestamp:     time.Now().UTC(),
		Attributes:    map[string]any{},
		Source:        map[string]string{},
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestValidateEventRejectsOversizedEvent(t *testing.T) {
	event := LogEvent{
		SchemaVersion: LogEventSchemaVersion,
		ID:            "event-1",
		Service:       "auth-service",
		Level:         "info",
		Message:       "hello",
		Timestamp:     time.Now().UTC(),
		Attributes:    map[string]any{},
		Source:        map[string]string{},
	}
	event.Attributes["payload"] = strings.Repeat("x", MaxEventBytes)
	if err := ValidateEvent(event); err == nil {
		t.Fatal("expected oversized event to be rejected")
	}
}

func TestNormalizeLevel(t *testing.T) {
	if got := NormalizeLevel("WARNING"); got != "warn" {
		t.Fatalf("got %q", got)
	}
}
