# LogStream Observability Platform

LogStream is a local log pipeline that tails files with Rust, buffers ingestion through Kafka, persists searchable events with Go and PostgreSQL, and exposes them in a small React explorer.

[![CI](https://github.com/DomenicoNota/rustgo/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/DomenicoNota/rustgo/actions/workflows/ci.yml)

## What it demonstrates

The project focuses on systems and backend behavior that can be inspected in code and tests: bounded collection, stable event identities, authenticated and size-limited ingestion, synchronous Kafka publication, offset-safe persistence, database idempotency, poison-message handling, cancellation, graceful shutdown, and cursor-based queries. React has one narrow role: querying the real API and presenting stored events and dependency status.

## Architecture

```mermaid
flowchart LR
    Files[Application log files] --> Agent[Rust agent]
    Agent -->|bounded HTTP batches| API[Go API]
    API -->|RequireAll acknowledgements| Raw[(Kafka: logs.raw)]
    Raw --> Worker[Go worker]
    Worker -->|idempotent insert| DB[(PostgreSQL)]
    Worker -->|poison record| DLQ[(Kafka: logs.dlq)]
    UI[React explorer] -->|filters + cursor| API
    API -->|parameterized queries| DB
```

Kafka separates an acknowledged ingest request from PostgreSQL availability; it is not present as a pass-through decoration. The API owns HTTP authentication, validation, publication, and reads. The worker alone owns Kafka progress, DLQ routing, and database writes. See [architecture](docs/architecture.md) and [delivery semantics](docs/delivery-semantics.md) for the detailed boundaries.

## Engineering decisions

- **At-least-once after Kafka acceptance.** The API returns `202` only after synchronous Kafka publication succeeds. Delivery before that boundary is best effort because the agent has no disk spool.
- **Database-owned idempotency.** The agent creates an event ID before its first HTTP attempt and reuses it for retries. PostgreSQL makes `logs.id` the primary key, and inserts use `ON CONFLICT (id) DO NOTHING`.
- **Terminal-state offset commits.** The worker commits a source offset only after persistence succeeds/idempotently conflicts, or after a poison record is acknowledged by `logs.dlq`.
- **Stable cursor pagination.** `GET /v1/logs` orders by `(timestamp, id)` and returns an opaque `next_cursor`; it does not use offset pagination.
- **Bounded failure behavior.** Agent queues, line buffers, batches, request sizes, retry counts, backoff, per-attempt work, and shutdown waits all have explicit limits.

## Quickstart

Prerequisites:

- Docker Desktop or Docker Engine with Docker Compose v2
- Windows PowerShell 5.1, or PowerShell 7 (`pwsh`) on macOS/Linux, for the demo and verification scripts

From a fresh clone, create the local environment file and start the complete stack, including the React explorer:

```powershell
Copy-Item .env.example .env
docker compose --profile ui up --build -d --wait
```

```bash
cp .env.example .env
docker compose --profile ui up --build -d --wait
```

The checked-in values are local-only examples. Change both PostgreSQL credential locations together, and keep `API_KEYS` aligned with `LOGSTREAM_API_KEY`, before using the stack outside an isolated development machine.

Open the explorer at <http://localhost:5173>. API liveness and dependency readiness are available at <http://localhost:8080/healthz> and <http://localhost:8080/readyz>.

Stop the stack without deleting PostgreSQL data:

```bash
docker compose --profile ui down --remove-orphans
```

## Deterministic demo

With the stack running:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/demo.ps1
```

On macOS/Linux, use `pwsh -NoProfile -File ./scripts/demo.ps1`.

The script appends a unique JSON line to `demo-data/demo.log`, waits for that exact event through the query API, redelivers its event ID twice, sends a barrier event, and asserts that PostgreSQL still contains one row for the original ID. Success has this shape:

```text
PASS: agent -> API -> Kafka -> worker -> PostgreSQL -> query API
Event ID: <generated UUID>
Message: logstreamdemo<unique marker>
Duplicate deliveries stored as rows: 1
```

The script exits non-zero on timeout, malformed data, a missing boundary, or a failed idempotency assertion.

## API overview

Only `POST /v1/ingest` requires the bearer API key. Query endpoints are unauthenticated and intended for the loopback-only local stack.

| Endpoint | Purpose |
| --- | --- |
| `POST /v1/ingest` | Authenticate, bound, validate, and publish an event batch to Kafka |
| `GET /v1/logs` | Filter by `service`, `level`, `q`, `start`, and `end`; paginate with `limit` and `cursor` |
| `GET /v1/services` | List service names stored in PostgreSQL |
| `GET /v1/metrics` | Return PostgreSQL-derived dashboard counts |
| `GET /healthz` | Report API process liveness only |
| `GET /readyz` | Check API access to PostgreSQL and Kafka |
| `GET /metrics` | Return fixed-cardinality API process counters in Prometheus text format |

Ingest one event with the local example key:

```bash
curl -i http://localhost:8080/v1/ingest \
  -H "Authorization: Bearer local-only-example-key" \
  -H "Content-Type: application/json" \
  --data '{"events":[{"schema_version":1,"id":"readme-example-001","timestamp":"2026-08-07T20:15:00Z","service":"auth-service","level":"error","message":"failed login attempt","attributes":{"user_id":"123"},"source":{"agent":"manual","file":"auth.log"}}]}'
```

A successful publication returns `202 Accepted` with `{"accepted":1,"rejected":0}`. It means Kafka acknowledged the event; PostgreSQL persistence may still be in progress.

Query stored errors and follow the returned opaque cursor if present:

```bash
curl "http://localhost:8080/v1/logs?service=auth-service&level=error&limit=20"
```

The full request contract, limits, response envelope, and operational endpoints are documented in [docs/api.md](docs/api.md).

## Testing and CI

Fast verification runs formatting, static analysis, unit/component tests, Go race tests, the frontend audit/build, and Compose configuration validation:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-fast.ps1
```

Full verification adds real PostgreSQL/Kafka integration tests and the Rust-agent-to-query demo:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/verify-full.ps1
```

Use `pwsh` instead of `powershell` on macOS/Linux. `make verify-fast` and `make verify-full` are optional aliases.

The [GitHub Actions workflow](.github/workflows/ci.yml) runs Go, Rust, React, Docker build, infrastructure integration, and full E2E jobs on pull requests and pushes to `main`. The badge above reports GitHub's actual status; a local passing run is not presented as a green GitHub run. The test-to-command mapping is in [docs/testing.md](docs/testing.md).

For local source checks outside Docker, the pinned toolchains are Go 1.26.5, Rust 1.97.1 with rustfmt/Clippy, Node.js 20.19 or newer with npm, and PowerShell.

## Repository structure

```text
agent/                  Rust tailer, parser, batcher, retrying HTTP client
backend/cmd/api/        Go HTTP API composition root
backend/cmd/worker/     Go Kafka persistence worker composition root
backend/cmd/migrate/    Checksum-tracked PostgreSQL migration runner
backend/internal/       HTTP, broker, delivery, store, and worker boundaries
dashboard/              React/Vite log explorer backed by the real query API
db/migrations/          PostgreSQL schema, constraints, and query indexes
scripts/                Readiness, demo, and verification entry points
docs/                   Detailed contracts, architecture, and operations notes
```

## Failure semantics and limitations

- The agent uses an in-memory bounded queue and finite retry budget. Queue overflow, exhausted delivery retries, or forced shutdown can lose events before Kafka accepts them.
- File offsets are not checkpointed. Restarting the agent rereads files from byte zero and generates new IDs, so restart-time duplicates are not deduplicated.
- Rename-and-create rotation and truncation are detected, but late writes to the renamed file are not drained.
- The worker processes one Kafka record at a time. Retry exhaustion exits the worker without committing the unresolved record; Compose restarts it for redelivery.
- DLQ records contain safe metadata and payload hashes, not raw payloads. There is no DLQ replay tool.
- PostgreSQL and Kafka are single-node local services. The repository does not bundle TLS, Kafka authentication, a monitoring stack, tracing, alerting, or high-availability deployment.
- Query endpoints are intentionally unauthenticated for local loopback use. That contract must change before exposing the API on an untrusted network.

See [delivery semantics](docs/delivery-semantics.md), [agent behavior](docs/agent.md), and [operations/security](docs/operations.md) for the complete failure and shutdown rules.
