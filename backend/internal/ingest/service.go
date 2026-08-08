package ingest

import (
	"context"

	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
)

type Request struct {
	Events []logstore.LogEvent `json:"events"`
}

type Result struct {
	Accepted int `json:"accepted"`
	Rejected int `json:"rejected"`
}

type Service struct {
	publisher    Publisher
	maxBatchSize int
}

type Publisher interface {
	Publish(ctx context.Context, events []logstore.LogEvent) error
}

func NewService(publisher Publisher, maxBatchSize int) Service {
	return Service{publisher: publisher, maxBatchSize: maxBatchSize}
}

func (s Service) Ingest(ctx context.Context, req Request) (Result, error) {
	if len(req.Events) == 0 {
		return Result{}, ErrInvalidRequest{Message: "events must not be empty"}
	}
	if s.maxBatchSize > 0 && len(req.Events) > s.maxBatchSize {
		return Result{}, ErrInvalidRequest{Message: "batch exceeds max size"}
	}

	valid := make([]logstore.LogEvent, 0, len(req.Events))
	result := Result{}
	for _, event := range req.Events {
		if err := logstore.ValidateEvent(event); err != nil {
			result.Rejected++
			continue
		}
		event.Level = logstore.NormalizeLevel(event.Level)
		event.Timestamp = event.Timestamp.UTC()
		valid = append(valid, event)
	}
	result.Accepted = len(valid)
	if len(valid) == 0 {
		return result, nil
	}
	return result, s.publisher.Publish(ctx, valid)
}

type ErrInvalidRequest struct {
	Message string
}

func (e ErrInvalidRequest) Error() string {
	return e.Message
}
