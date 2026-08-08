package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/DomenicoNota/rustgo/backend/internal/api"
	"github.com/DomenicoNota/rustgo/backend/internal/broker"
	"github.com/DomenicoNota/rustgo/backend/internal/config"
	"github.com/DomenicoNota/rustgo/backend/internal/db"
	"github.com/DomenicoNota/rustgo/backend/internal/store"
	"github.com/DomenicoNota/rustgo/backend/internal/telemetry"
	"github.com/DomenicoNota/rustgo/backend/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, cfg.DatabaseURL, databaseOptions(cfg))
	if err != nil {
		return err
	}
	defer pool.Close()

	reader := broker.NewReader(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID, cfg.KafkaReadTimeout)
	dlq := broker.NewPublisher(cfg.KafkaBrokers, cfg.KafkaDLQTopic, cfg.KafkaWriteTimeout)
	metrics := &telemetry.WorkerMetrics{}
	processor, err := worker.New(reader, dlq, store.NewPostgres(pool), logger, metrics, worker.RetryPolicy{
		MaxAttempts:    cfg.WorkerRetryAttempts,
		BaseDelay:      cfg.WorkerRetryBaseDelay,
		MaxDelay:       cfg.WorkerRetryMaxDelay,
		AttemptTimeout: cfg.WorkerAttemptTimeout,
	})
	if err != nil {
		return errors.Join(fmt.Errorf("configure worker: %w", err), reader.Close(), dlq.Close())
	}
	operations := api.NewServer(telemetry.NewOperationsHandler(metrics, []telemetry.HealthChecker{
		db.NewHealthChecker(pool, cfg.DBConnectTimeout),
		broker.NewChecker(cfg.KafkaBrokers, cfg.KafkaReadTimeout),
	}), api.ServerConfig{
		Address:           ":" + strconv.Itoa(cfg.WorkerObservabilityPort),
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       cfg.HTTPIdleTimeout,
		ShutdownTimeout:   cfg.HTTPShutdownTimeout,
		MaxHeaderBytes:    cfg.HTTPMaxHeaderBytes,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	go func() { results <- processor.Run(runCtx) }()
	go func() { results <- operations.Run(runCtx) }()
	logger.Info("persistence worker started", "topic", cfg.KafkaTopic, "group_id", cfg.KafkaGroupID, "observability_port", cfg.WorkerObservabilityPort)
	firstErr := <-results
	cancel()
	secondErr := <-results
	return errors.Join(firstErr, secondErr, reader.Close(), dlq.Close())
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
