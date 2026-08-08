package integration

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DomenicoNota/rustgo/backend/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMigrationsAreTrackedAndRepeatable(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	directory := filepath.Join("..", "..", "..", "db", "migrations")
	files, err := filepath.Glob(filepath.Join(directory, "*.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("discover migrations: files=%d err=%v", len(files), err)
	}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	if _, err := db.ApplyMigrations(ctx, pool, directory, logger); err != nil {
		t.Fatal(err)
	}
	result, err := db.ApplyMigrations(ctx, pool, directory, logger)
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied != 0 || result.AlreadyApplied != len(files) {
		t.Fatalf("unexpected repeat result: %+v, files=%d", result, len(files))
	}

	var recorded int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&recorded); err != nil {
		t.Fatal(err)
	}
	if recorded != len(files) {
		t.Fatalf("migration ledger has %d rows, want %d", recorded, len(files))
	}
}
