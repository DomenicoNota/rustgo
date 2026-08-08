package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DomenicoNota/rustgo/backend/internal/delivery"
	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(brokers []string, topic string, timeout time.Duration) *Publisher {
	return &Publisher{writer: &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
		ReadTimeout:            timeout,
		WriteTimeout:           timeout,
	}}
}

func (p *Publisher) Publish(ctx context.Context, events []logstore.LogEvent) error {
	messages := make([]kafka.Message, 0, len(events))
	for _, event := range events {
		value, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encode event %q: %w", event.ID, err)
		}
		messages = append(messages, kafka.Message{
			Key:   []byte(event.ID),
			Value: value,
			Time:  event.Timestamp,
		})
	}
	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		return fmt.Errorf("publish events: %w", err)
	}
	return nil
}

func (p *Publisher) PublishDeadLetter(ctx context.Context, record delivery.DeadLetter) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode dead letter: %w", err)
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(record.ID),
		Value: payload,
		Time:  record.FailedAt,
	}); err != nil {
		return fmt.Errorf("publish dead letter: %w", err)
	}
	return nil
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

type Checker struct {
	brokers []string
	timeout time.Duration
}

func NewChecker(brokers []string, timeout time.Duration) Checker {
	return Checker{brokers: brokers, timeout: timeout}
}

func (c Checker) Check(ctx context.Context) error {
	if len(c.brokers) == 0 {
		return fmt.Errorf("no Kafka brokers configured")
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", c.brokers[0])
	if err != nil {
		return err
	}
	return conn.Close()
}

func NewReader(brokers []string, topic, groupID string, timeout time.Duration) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		CommitInterval: 0,
		MinBytes:       1,
		MaxBytes:       512 << 10,
		MaxWait:        time.Second,
		Dialer:         &kafka.Dialer{Timeout: timeout},
	})
}
