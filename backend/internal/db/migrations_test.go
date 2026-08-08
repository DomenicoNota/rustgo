package db

import (
	"context"
	"strings"
	"testing"
)

func TestApplyMigrationsRejectsEmptyDirectoryBeforeConnecting(t *testing.T) {
	_, err := ApplyMigrations(context.Background(), nil, t.TempDir(), nil)
	if err == nil || !strings.Contains(err.Error(), "no migrations found") {
		t.Fatalf("expected an empty-directory error, got %v", err)
	}
}
