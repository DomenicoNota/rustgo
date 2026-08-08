package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
)

const (
	cursorVersion   = 1
	maxCursorLength = 512
)

type cursorPayload struct {
	Version   int       `json:"v"`
	Timestamp time.Time `json:"t"`
	ID        string    `json:"i"`
}

func encodeCursor(cursor *logstore.PageCursor) (string, error) {
	if cursor == nil || cursor.IsZero() {
		return "", nil
	}
	payload, err := json.Marshal(cursorPayload{
		Version:   cursorVersion,
		Timestamp: cursor.Timestamp.UTC(),
		ID:        cursor.ID,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeCursor(raw string) (logstore.PageCursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return logstore.PageCursor{}, nil
	}
	if len(raw) > maxCursorLength {
		return logstore.PageCursor{}, errors.New("invalid cursor")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return logstore.PageCursor{}, errors.New("invalid cursor")
	}
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()
	var payload cursorPayload
	if err := decoder.Decode(&payload); err != nil {
		return logstore.PageCursor{}, errors.New("invalid cursor")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return logstore.PageCursor{}, errors.New("invalid cursor")
	}
	cursor := logstore.PageCursor{Timestamp: payload.Timestamp, ID: payload.ID}
	if payload.Version != cursorVersion || cursor.IsZero() || len(cursor.ID) > logstore.MaxIDBytes {
		return logstore.PageCursor{}, errors.New("invalid cursor")
	}
	return cursor, nil
}
