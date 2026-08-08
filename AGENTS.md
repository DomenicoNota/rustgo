# Repository Guide

## Layout

- `agent/`: Rust log collection agent.
- `backend/`: single Go module containing the API, Kafka worker, migrations command, and internal packages.
- `dashboard/`: React/Vite log explorer.
- `db/migrations/`: PostgreSQL schema migrations.
- `docs/`: architecture and API notes.
- `scripts/`: deterministic developer smoke/demo helpers.

## Required checks

A task is not complete until every relevant command below has actually passed. If a tool or dependency is unavailable, report that explicitly instead of claiming success.

```bash
cd backend && gofmt -w . && go vet ./... && go test -race ./...
cd agent && cargo fmt --check && cargo clippy --locked --all-targets --all-features -- -D warnings && cargo test --locked
cd dashboard && npm ci && npm audit --audit-level=high && npm run format:check && npm run lint && npm run typecheck && npm run test && npm run build
docker compose config
docker compose up --build
cd backend && TEST_DATABASE_URL=postgres://logstream:local-only-example-password@localhost:5432/logstream?sslmode=disable TEST_KAFKA_BROKERS=localhost:29092 go test -race ./tests/integration
powershell -ExecutionPolicy Bypass -File scripts/demo.ps1
```

Run `make help` for the supported root shortcuts. The principal gates are `make verify-fast` and `make e2e`; `make up`, `make up-ui`, `make down`, `make demo`, `make fmt`, `make lint`, `make test`, and `make build` are focused developer commands.

The supported one-command gates are:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify-fast.ps1
powershell -ExecutionPolicy Bypass -File scripts/verify-full.ps1
```

Fast verification intentionally excludes live PostgreSQL/Kafka/E2E execution. Full verification must not report success unless the real infrastructure tests and `scripts/demo.ps1` pass.
Full verification tears down its containers in a `finally` path unless `-KeepStack` is explicitly supplied; it preserves the PostgreSQL volume.

## Engineering conventions

- Preserve the at-least-once delivery contract. An event ID is stable across retries, and PostgreSQL uniqueness makes persistence idempotent.
- The API owns HTTP validation/authentication and Kafka publication. The worker owns Kafka consumption and PostgreSQL writes.
- Commit Kafka offsets only after an event is persisted or its poison payload is successfully published to the DLQ.
- Keep queues, batches, request bodies, messages, attributes, retries, and shutdown waits bounded.
- Add interfaces only at real boundaries such as broker, store, clock, or transport.
- Configuration comes from environment variables or checked-in examples. Never commit credentials or local `.env` files.
- Do not add fake metrics, benchmarks, screenshots, tests, or success responses.
- Keep application logs structured and never log API keys or authorization headers.
