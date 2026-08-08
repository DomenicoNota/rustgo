package api

import (
	"encoding/base64"
	"testing"
	"time"

	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
)

func TestCursorRoundTrip(t *testing.T) {
	original := &logstore.PageCursor{
		Timestamp: time.Date(2026, 7, 7, 20, 15, 0, 123, time.FixedZone("test", -4*60*60)),
		ID:        "event-42",
	}
	encoded, err := encodeCursor(original)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeCursor(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.ID != original.ID || !decoded.Timestamp.Equal(original.Timestamp) || decoded.Timestamp.Location() != time.UTC {
		t.Fatalf("decoded cursor mismatch: got %+v want %+v", decoded, original)
	}
}

func TestCursorRejectsMalformedAndUnknownFields(t *testing.T) {
	tests := []string{
		"not-base64!",
		base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"t":"2026-08-07T12:00:00Z"}`)),
		base64.RawURLEncoding.EncodeToString([]byte(`{"v":2,"t":"2026-08-07T12:00:00Z","i":"event"}`)),
		base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"t":"2026-08-07T12:00:00Z","i":"event","extra":true}`)),
		base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"t":"2026-08-07T12:00:00Z","i":"event"}{}`)),
	}
	for _, raw := range tests {
		if _, err := decodeCursor(raw); err == nil {
			t.Fatalf("expected cursor %q to fail", raw)
		}
	}
}
