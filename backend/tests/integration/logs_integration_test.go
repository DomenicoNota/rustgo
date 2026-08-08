package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/DomenicoNota/rustgo/backend/internal/db"
	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
	"github.com/DomenicoNota/rustgo/backend/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresIdempotencyFiltersAndCursorPagination(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := db.Open(ctx, databaseURL, db.Options{
		MaxConns:          4,
		MinConns:          0,
		ConnectTimeout:    3 * time.Second,
		MaxConnLifetime:   time.Minute,
		MaxConnIdleTime:   time.Minute,
		HealthCheckPeriod: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	repository := store.NewPostgres(pool)
	now := time.Now().UTC().Truncate(time.Microsecond)
	service := fmt.Sprintf("integration-%d", now.UnixNano())
	events := []logstore.LogEvent{
		integrationEvent(service+"-a", service, now.Add(2*time.Second)),
		integrationEvent(service+"-b", service, now.Add(time.Second)),
		integrationEvent(service+"-c", service, now),
	}
	if err := repository.Insert(ctx, events); err != nil {
		t.Fatal(err)
	}
	if err := repository.Insert(ctx, []logstore.LogEvent{events[0]}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM logs WHERE service = $1`, service); err != nil {
			t.Errorf("clean integration rows: %v", err)
		}
	})

	first, err := repository.Search(ctx, logstore.SearchParams{
		Service: service,
		Level:   "error",
		Query:   "idempotentmarker",
		Limit:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 2 || first.Next == nil {
		t.Fatalf("expected two rows and a cursor, got %#v", first)
	}

	second, err := repository.Search(ctx, logstore.SearchParams{
		Service: service,
		Level:   "error",
		Query:   "idempotentmarker",
		Limit:   2,
		Cursor:  *first.Next,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 1 || second.Next != nil {
		t.Fatalf("expected one final row, got %#v", second)
	}
}

func TestPostgresEnforcesSchemaAndIDConstraints(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := db.Open(ctx, databaseURL, db.Options{
		MaxConns:          2,
		MinConns:          0,
		ConnectTimeout:    3 * time.Second,
		MaxConnLifetime:   time.Minute,
		MaxConnIdleTime:   time.Minute,
		HealthCheckPeriod: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	id := fmt.Sprintf("constraint-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if _, err := pool.Exec(cleanupCtx, `DELETE FROM logs WHERE id = $1`, id); err != nil {
			t.Errorf("clean constraint integration row: %v", err)
		}
	})

	_, err = pool.Exec(ctx, `
		INSERT INTO logs (schema_version, id, service, level, message, attributes, source, timestamp)
		VALUES (2, $1, 'constraint-test', 'info', 'invalid schema', '{}'::jsonb, '{}'::jsonb, NOW())
	`, id)
	assertPostgresCode(t, err, "23514")

	_, err = pool.Exec(ctx, `
		INSERT INTO logs (schema_version, id, service, level, message, attributes, source, timestamp)
		VALUES (1, $1, 'constraint-test', 'info', 'first', '{}'::jsonb, '{}'::jsonb, NOW())
	`, id)
	if err != nil {
		t.Fatalf("insert valid constraint row: %v", err)
	}
	_, err = pool.Exec(ctx, `
		INSERT INTO logs (schema_version, id, service, level, message, attributes, source, timestamp)
		VALUES (1, $1, 'constraint-test', 'info', 'duplicate', '{}'::jsonb, '{}'::jsonb, NOW())
	`, id)
	assertPostgresCode(t, err, "23505")
}

func assertPostgresCode(t *testing.T, err error, want string) {
	t.Helper()
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != want {
		t.Fatalf("expected PostgreSQL error %s, got %v", want, err)
	}
}

func integrationEvent(id, service string, timestamp time.Time) logstore.LogEvent {
	return logstore.LogEvent{
		SchemaVersion: logstore.LogEventSchemaVersion,
		ID:            id,
		Service:       service,
		Level:         "error",
		Message:       "integration idempotentmarker",
		Timestamp:     timestamp,
		Attributes:    map[string]any{"test": true},
		Source:        map[string]string{"test": "integration"},
	}
}
