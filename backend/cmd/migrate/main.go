package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/DomenicoNota/rustgo/backend/internal/config"
	"github.com/DomenicoNota/rustgo/backend/internal/db"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("migration failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := db.Open(ctx, cfg.DatabaseURL, databaseOptions(cfg))
	if err != nil {
		return err
	}
	defer pool.Close()

	migrationsDir := os.Getenv("MIGRATIONS_DIR")
	if migrationsDir == "" {
		migrationsDir = filepath.Join("..", "db", "migrations")
	}
	result, err := db.ApplyMigrations(ctx, pool, migrationsDir, logger)
	if err != nil {
		return err
	}
	logger.Info("migrations complete", "applied", result.Applied, "already_applied", result.AlreadyApplied)
	return nil
}

func databaseOptions(cfg config.Config) db.Options {
	return db.Options{
		MaxConns:          cfg.DBMaxConns,
		MinConns:          cfg.DBMinConns,
		ConnectTimeout:    cfg.DBConnectTimeout,
		MaxConnLifetime:   cfg.DBMaxConnLifetime,
		MaxConnIdleTime:   cfg.DBMaxConnIdleTime,
		HealthCheckPeriod: cfg.DBHealthCheckPeriod,
	}
}
