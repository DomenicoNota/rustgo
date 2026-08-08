package config

import (
	"strings"
	"testing"
)

func TestLoadRejectsMalformedLimits(t *testing.T) {
	values := map[string]string{"MAX_BATCH_SIZE": "many"}
	_, err := load(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err == nil || !strings.Contains(err.Error(), "MAX_BATCH_SIZE") {
		t.Fatalf("expected MAX_BATCH_SIZE error, got %v", err)
	}
}

func TestLoadRejectsEmptyAPIKeys(t *testing.T) {
	values := map[string]string{"API_KEYS": " , "}
	cfg, err := load(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.ValidateAPI(); err == nil || !strings.Contains(err.Error(), "API_KEYS") {
		t.Fatalf("expected API_KEYS error, got %v", err)
	}
}

func TestLoadRejectsOversizedAPIKey(t *testing.T) {
	values := map[string]string{"API_KEYS": strings.Repeat("x", 257)}
	_, err := load(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err == nil || !strings.Contains(err.Error(), "256 bytes") {
		t.Fatalf("expected API_KEYS length error, got %v", err)
	}
}

func TestLoadRejectsPoolMinimumAboveMaximum(t *testing.T) {
	values := map[string]string{"DB_MAX_CONNS": "2", "DB_MIN_CONNS": "3"}
	_, err := load(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err == nil || !strings.Contains(err.Error(), "DB_MIN_CONNS") {
		t.Fatalf("expected DB_MIN_CONNS error, got %v", err)
	}
}

func TestLoadRejectsWorkerRetryMaximumBelowBase(t *testing.T) {
	values := map[string]string{
		"WORKER_RETRY_BASE_DELAY_MS": "1000",
		"WORKER_RETRY_MAX_DELAY_MS":  "999",
	}
	_, err := load(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err == nil || !strings.Contains(err.Error(), "WORKER_RETRY_MAX_DELAY_MS") {
		t.Fatalf("expected worker retry delay error, got %v", err)
	}
}

func TestLoadRejectsMalformedWorkerRetryBudget(t *testing.T) {
	values := map[string]string{"WORKER_RETRY_MAX_ATTEMPTS": "forever"}
	_, err := load(func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	})
	if err == nil || !strings.Contains(err.Error(), "WORKER_RETRY_MAX_ATTEMPTS") {
		t.Fatalf("expected worker retry attempts error, got %v", err)
	}
}

func TestLoadAppliesValidatedDefaults(t *testing.T) {
	cfg, err := load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 8080 || cfg.DBMinConns > cfg.DBMaxConns || len(cfg.APIKeys) != 0 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}
