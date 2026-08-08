package store

import (
	"strings"
	"testing"
	"time"

	logstore "github.com/DomenicoNota/rustgo/backend/internal/logs"
)

func TestBuildSearchQueryParameterizesEveryFilter(t *testing.T) {
	start := time.Date(2026, 8, 7, 10, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	query, args, limit := buildSearchQuery(logstore.SearchParams{
		Service: "api' OR TRUE --",
		Level:   "WARNING",
		Query:   "failed login",
		Start:   &start,
		End:     &end,
		Limit:   25,
		Cursor:  logstore.PageCursor{Timestamp: end, ID: "event-9"},
	})

	if strings.Contains(query, "api' OR TRUE") || strings.Contains(query, "failed login") {
		t.Fatalf("user input was interpolated into SQL: %s", query)
	}
	if !strings.Contains(query, "(timestamp, id) < ($6, $7)") {
		t.Fatalf("missing tuple cursor predicate: %s", query)
	}
	if len(args) != 8 || args[0] != "api' OR TRUE --" || args[1] != "warn" || limit != 25 {
		t.Fatalf("unexpected query arguments: %#v, limit %d", args, limit)
	}
}

func TestBuildSearchQueryUsesSafeDefaultLimit(t *testing.T) {
	query, args, limit := buildSearchQuery(logstore.SearchParams{Limit: 5_000})
	if !strings.Contains(query, "WHERE TRUE") || limit != 100 || args[len(args)-1] != 101 {
		t.Fatalf("unexpected default query: %s %#v limit=%d", query, args, limit)
	}
}
