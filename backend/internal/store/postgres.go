package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (s *Postgres) Insert(ctx context.Context, events []logstore.LogEvent) error {
	if len(events) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, event := range events {
		attributes, err := json.Marshal(event.Attributes)
		if err != nil {
			return fmt.Errorf("encode attributes for event %q: %w", event.ID, err)
		}
		source, err := json.Marshal(event.Source)
		if err != nil {
			return fmt.Errorf("encode source for event %q: %w", event.ID, err)
		}
		batch.Queue(`
			INSERT INTO logs (schema_version, id, service, level, message, attributes, source, timestamp)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (id) DO NOTHING
		`, event.SchemaVersion, event.ID, event.Service, logstore.NormalizeLevel(event.Level), event.Message, attributes, source, event.Timestamp)
	}

	results := s.pool.SendBatch(ctx, batch)
	for range events {
		if _, err := results.Exec(); err != nil {
			return errors.Join(fmt.Errorf("insert log event: %w", err), results.Close())
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("close insert batch: %w", err)
	}
	return nil
}

func (s *Postgres) Search(ctx context.Context, params logstore.SearchParams) (logstore.SearchPage, error) {
	query, args, limit := buildSearchQuery(params)
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return logstore.SearchPage{}, fmt.Errorf("query logs: %w", err)
	}
	defer rows.Close()

	records := make([]logstore.LogRecord, 0, limit+1)
	for rows.Next() {
		var record logstore.LogRecord
		var attributes []byte
		var source []byte
		if err := rows.Scan(&record.SchemaVersion, &record.ID, &record.Service, &record.Level, &record.Message, &attributes, &source, &record.Timestamp, &record.ReceivedAt); err != nil {
			return logstore.SearchPage{}, fmt.Errorf("scan log event: %w", err)
		}
		if err := json.Unmarshal(attributes, &record.Attributes); err != nil {
			return logstore.SearchPage{}, fmt.Errorf("decode attributes for event %q: %w", record.ID, err)
		}
		if err := json.Unmarshal(source, &record.Source); err != nil {
			return logstore.SearchPage{}, fmt.Errorf("decode source for event %q: %w", record.ID, err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return logstore.SearchPage{}, fmt.Errorf("iterate log events: %w", err)
	}

	page := logstore.SearchPage{Items: records}
	if len(records) > limit {
		last := records[limit-1]
		page.Next = &logstore.PageCursor{Timestamp: last.Timestamp, ID: last.ID}
		page.Items = records[:limit]
	}
	return page, nil
}

func buildSearchQuery(params logstore.SearchParams) (string, []any, int) {
	limit := params.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}

	where := make([]string, 0, 6)
	args := make([]any, 0, 8)
	add := func(clause string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(clause, len(args)))
	}

	if params.Service != "" {
		add("service = $%d", params.Service)
	}
	if params.Level != "" {
		add("level = $%d", logstore.NormalizeLevel(params.Level))
	}
	if params.Query != "" {
		add("to_tsvector('english', message) @@ plainto_tsquery('english', $%d)", params.Query)
	}
	if params.Start != nil {
		add("timestamp >= $%d", *params.Start)
	}
	if params.End != nil {
		add("timestamp <= $%d", *params.End)
	}
	if !params.Cursor.IsZero() {
		args = append(args, params.Cursor.Timestamp, params.Cursor.ID)
		where = append(where, fmt.Sprintf("(timestamp, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	if len(where) == 0 {
		where = append(where, "TRUE")
	}
	args = append(args, limit+1)

	query := fmt.Sprintf(`
		SELECT schema_version, id, service, level, message, attributes, source, timestamp, received_at
		FROM logs
		WHERE %s
		ORDER BY timestamp DESC, id DESC
		LIMIT $%d
	`, strings.Join(where, " AND "), len(args))
	return query, args, limit
}

func (s *Postgres) Services(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT service FROM logs ORDER BY service`)
	if err != nil {
		return nil, fmt.Errorf("query services: %w", err)
	}
	defer rows.Close()

	services := make([]string, 0)
	for rows.Next() {
		var service string
		if err := rows.Scan(&service); err != nil {
			return nil, fmt.Errorf("scan service: %w", err)
		}
		services = append(services, service)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate services: %w", err)
	}
	return services, nil
}

func (s *Postgres) Metrics(ctx context.Context) (logstore.Metrics, error) {
	var metrics logstore.Metrics
	err := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE received_at >= NOW() - INTERVAL '1 minute'),
			COUNT(*) FILTER (WHERE received_at >= NOW() - INTERVAL '1 minute' AND level = 'error'),
			COUNT(DISTINCT service)
		FROM logs
	`).Scan(&metrics.LogsIngestedTotal, &metrics.LogsLastMinute, &metrics.ErrorsLastMinute, &metrics.ActiveServices)
	if err != nil {
		return logstore.Metrics{}, fmt.Errorf("query metrics: %w", err)
	}
	return metrics, nil
}
