package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/DomenicoNota/rustgo/backend/internal/delivery"
	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
	"github.com/DomenicoNota/rustgo/backend/internal/telemetry"
	"github.com/segmentio/kafka-go"
)

type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, messages ...kafka.Message) error
}

type DeadLetterPublisher interface {
	PublishDeadLetter(ctx context.Context, record delivery.DeadLetter) error
}

type EventStore interface {
	Insert(ctx context.Context, events []logstore.LogEvent) error
}

type Worker struct {
	reader  Reader
	dlq     DeadLetterPublisher
	store   EventStore
	logger  *slog.Logger
	metrics *telemetry.WorkerMetrics
	policy  RetryPolicy
	wait    func(context.Context, time.Duration) error
	now     func() time.Time
}

type RetryPolicy struct {
	MaxAttempts    int
	BaseDelay      time.Duration
	MaxDelay       time.Duration
	AttemptTimeout time.Duration
}

func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 1 {
		return errors.New("worker retry max attempts must be positive")
	}
	if p.BaseDelay <= 0 {
		return errors.New("worker retry base delay must be positive")
	}
	if p.MaxDelay < p.BaseDelay {
		return errors.New("worker retry max delay must be at least the base delay")
	}
	if p.AttemptTimeout <= 0 {
		return errors.New("worker attempt timeout must be positive")
	}
	return nil
}

func (p RetryPolicy) delayAfterFailure(attempt int) time.Duration {
	delay := p.BaseDelay
	for current := 1; current < attempt && delay < p.MaxDelay; current++ {
		if delay > p.MaxDelay/2 {
			return p.MaxDelay
		}
		delay *= 2
	}
	if delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}

type RetryExhaustedError struct {
	Operation string
	Attempts  int
	Err       error
}

func (e *RetryExhaustedError) Error() string {
	return fmt.Sprintf("%s failed after %d attempts: %v", e.Operation, e.Attempts, e.Err)
}

func (e *RetryExhaustedError) Unwrap() error {
	return e.Err
}

func New(reader Reader, dlq DeadLetterPublisher, store EventStore, logger *slog.Logger, metrics *telemetry.WorkerMetrics, policy RetryPolicy) (*Worker, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if metrics == nil {
		metrics = &telemetry.WorkerMetrics{}
	}
	return &Worker{
		reader:  reader,
		dlq:     dlq,
		store:   store,
		logger:  logger,
		metrics: metrics,
		policy:  policy,
		wait:    wait,
		now:     time.Now,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		var message kafka.Message
		err := w.retry(ctx, "fetch Kafka message", nil, 0, func(attemptCtx context.Context) error {
			fetched, err := w.reader.FetchMessage(attemptCtx)
			if err == nil {
				message = fetched
			}
			return err
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := w.process(ctx, message); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func (w *Worker) process(ctx context.Context, message kafka.Message) error {
	started := time.Now()
	defer func() { w.metrics.ObserveWorkerRecord(time.Since(started)) }()
	event, err := decodeEvent(message.Value)
	if err != nil {
		return w.deadLetterAndCommit(ctx, message, delivery.Failure{
			Code:    "malformed_event",
			Message: "record is not one valid LogEvent JSON object",
		})
	}
	if err := logstore.ValidateEvent(event); err != nil {
		return w.deadLetterAndCommit(ctx, message, delivery.Failure{
			Code:    "invalid_event",
			Message: err.Error(),
		})
	}

	if err := w.retry(ctx, "persist event", []any{"event_id", event.ID}, w.policy.AttemptTimeout, func(attemptCtx context.Context) error {
		err := w.store.Insert(attemptCtx, []logstore.LogEvent{event})
		if err != nil {
			w.metrics.IncPersistenceFailure()
		} else {
			w.metrics.IncPersistenceSuccess()
		}
		return err
	}); err != nil {
		return err
	}
	return w.commit(ctx, message, []any{"event_id", event.ID})
}

func (w *Worker) deadLetterAndCommit(ctx context.Context, message kafka.Message, failure delivery.Failure) error {
	record := delivery.NewDeadLetter(message, failure, w.now().UTC())
	attributes := []any{
		"partition", message.Partition,
		"offset", message.Offset,
		"failure_code", failure.Code,
		"dead_letter_id", record.ID,
	}
	if err := w.retry(ctx, "publish dead letter", attributes, w.policy.AttemptTimeout, func(attemptCtx context.Context) error {
		err := w.dlq.PublishDeadLetter(attemptCtx, record)
		if err != nil {
			w.metrics.IncDLQFailure()
		} else {
			w.metrics.IncDLQSuccess()
		}
		return err
	}); err != nil {
		return err
	}
	if err := w.commit(ctx, message, attributes); err != nil {
		return err
	}
	w.logger.Warn("dead-lettered poison message", attributes...)
	return nil
}

func (w *Worker) commit(ctx context.Context, message kafka.Message, attributes []any) error {
	return w.retry(ctx, "commit Kafka offset", attributes, w.policy.AttemptTimeout, func(attemptCtx context.Context) error {
		return w.reader.CommitMessages(attemptCtx, message)
	})
}

func (w *Worker) retry(
	ctx context.Context,
	operation string,
	attributes []any,
	attemptTimeout time.Duration,
	action func(context.Context) error,
) error {
	var lastErr error
	for attempt := 1; attempt <= w.policy.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		attemptCtx := ctx
		cancel := func() {}
		if attemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, attemptTimeout)
		}
		err := action(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == w.policy.MaxAttempts {
			break
		}

		delay := w.policy.delayAfterFailure(attempt)
		logAttributes := append([]any{}, attributes...)
		logAttributes = append(logAttributes, "attempt", attempt, "max_attempts", w.policy.MaxAttempts, "delay_ms", delay.Milliseconds(), "error", err)
		w.logger.Warn(operation+" failed; retrying", logAttributes...)
		if err := w.wait(ctx, delay); err != nil {
			return err
		}
	}
	return &RetryExhaustedError{Operation: operation, Attempts: w.policy.MaxAttempts, Err: lastErr}
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func decodeEvent(payload []byte) (logstore.LogEvent, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var event logstore.LogEvent
	if err := decoder.Decode(&event); err != nil {
		return logstore.LogEvent{}, fmt.Errorf("decode log event: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return logstore.LogEvent{}, errors.New("decode log event: payload must contain one JSON value")
	}
	return event, nil
}
