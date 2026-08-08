package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Options struct {
	MaxConns          int32
	MinConns          int32
	ConnectTimeout    time.Duration
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

func Open(ctx context.Context, databaseURL string, options Options) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database configuration: %w", err)
	}
	cfg.MaxConns = options.MaxConns
	cfg.MinConns = options.MinConns
	cfg.ConnConfig.ConnectTimeout = options.ConnectTimeout
	cfg.MaxConnLifetime = options.MaxConnLifetime
	cfg.MaxConnIdleTime = options.MaxConnIdleTime
	cfg.HealthCheckPeriod = options.HealthCheckPeriod

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	connectCtx, cancel := context.WithTimeout(ctx, options.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(connectCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

type HealthChecker struct {
	pool    *pgxpool.Pool
	timeout time.Duration
}

func NewHealthChecker(pool *pgxpool.Pool, timeout time.Duration) HealthChecker {
	return HealthChecker{pool: pool, timeout: timeout}
}

func (h HealthChecker) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()
	return h.pool.Ping(ctx)
}
