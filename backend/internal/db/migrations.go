package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationLockID int64 = 7_181_764_319

type MigrationResult struct {
	Applied        int
	AlreadyApplied int
}

// ApplyMigrations serializes migration runners, records immutable checksums,
// and applies each file transactionally in lexical filename order.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool, directory string, logger *slog.Logger) (result MigrationResult, returnErr error) {
	if logger == nil {
		logger = slog.Default()
	}
	files, err := filepath.Glob(filepath.Join(directory, "*.sql"))
	if err != nil {
		return result, fmt.Errorf("list migrations: %w", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return result, fmt.Errorf("no migrations found in %s", directory)
	}

	connection, err := pool.Acquire(ctx)
	if err != nil {
		return result, fmt.Errorf("acquire migration connection: %w", err)
	}
	defer connection.Release()
	if _, err := connection.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return result, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := connection.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, migrationLockID)
		if err != nil {
			returnErr = errors.Join(returnErr, fmt.Errorf("release migration lock: %w", err))
			_ = connection.Conn().Close(unlockCtx)
		}
	}()

	if _, err := connection.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version TEXT PRIMARY KEY,
			checksum TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`); err != nil {
		return result, fmt.Errorf("create migration ledger: %w", err)
	}

	for _, file := range files {
		version := filepath.Base(file)
		contents, err := os.ReadFile(file)
		if err != nil {
			return result, fmt.Errorf("read migration %s: %w", version, err)
		}
		digest := sha256.Sum256(contents)
		checksum := hex.EncodeToString(digest[:])

		var recordedChecksum string
		err = connection.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, version).Scan(&recordedChecksum)
		switch {
		case err == nil:
			if recordedChecksum != checksum {
				return result, fmt.Errorf("migration %s changed after it was applied", version)
			}
			result.AlreadyApplied++
			logger.Debug("migration already applied", "version", version)
			continue
		case !errors.Is(err, pgx.ErrNoRows):
			return result, fmt.Errorf("read migration state for %s: %w", version, err)
		}

		if err := applyMigration(ctx, connection, version, checksum, string(contents)); err != nil {
			return result, err
		}
		result.Applied++
		logger.Info("migration applied", "version", version)
	}
	return result, nil
}

func applyMigration(ctx context.Context, connection *pgxpool.Conn, version, checksum, statement string) error {
	tx, err := connection.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", version, err)
	}
	defer func() {
		rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	if _, err := tx.Exec(ctx, statement); err != nil {
		return fmt.Errorf("apply migration %s: %w", version, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)`, version, checksum); err != nil {
		return fmt.Errorf("record migration %s: %w", version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", version, err)
	}
	return nil
}
