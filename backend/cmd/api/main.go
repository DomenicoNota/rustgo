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
	"github.com/DomenicoNota/rustgo/backend/internal/ingest"
	"github.com/DomenicoNota/rustgo/backend/internal/store"
	"github.com/DomenicoNota/rustgo/backend/internal/telemetry"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(logger); err != nil {
		logger.Error("API stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	if err := cfg.ValidateAPI(); err != nil {
		return fmt.Errorf("validate API configuration: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Open(ctx, cfg.DatabaseURL, databaseOptions(cfg))
	if err != nil {
		return err
	}
	defer pool.Close()

	publisher := broker.NewPublisher(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaWriteTimeout)
	repository := store.NewPostgres(pool)
	processMetrics := &telemetry.APIMetrics{}
	router := api.NewRouter(api.RouterConfig{
		Authenticator: api.NewAPIKeyAuthenticator(cfg.APIKeys),
		Logger:        logger,
		Readiness: []api.HealthChecker{
			db.NewHealthChecker(pool, cfg.DBConnectTimeout),
			broker.NewChecker(cfg.KafkaBrokers, cfg.KafkaReadTimeout),
		},
		Ingest:         ingest.NewService(publisher, cfg.MaxBatchSize),
		Logs:           repository,
		Metrics:        repository,
		ProcessMetrics: processMetrics,
		MaxBodyBytes:   cfg.MaxBodyBytes,
		DefaultPage:    cfg.DefaultPage,
		MaxPageSize:    cfg.MaxPageSize,
		MaxFilterBytes: cfg.MaxFilterBytes,
		RequestTimeout: cfg.HTTPHandlerTimeout,
	})
	server := api.NewServer(router, api.ServerConfig{
		Address:           ":" + strconv.Itoa(cfg.Port),
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       cfg.HTTPIdleTimeout,
		ShutdownTimeout:   cfg.HTTPShutdownTimeout,
		MaxHeaderBytes:    cfg.HTTPMaxHeaderBytes,
	})

	logger.Info("API starting", "port", cfg.Port)
	return errors.Join(server.Run(ctx), publisher.Close())
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
