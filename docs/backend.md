# Go API and worker behavior

## Process boundaries

`cmd/api` and `cmd/worker` only load validated configuration, create concrete dependencies, install signal cancellation, and run the component. HTTP parsing and encoding live in `internal/api`; event validation and Kafka publication live in `internal/ingest`; parameterized PostgreSQL statements live in `internal/store`; Kafka consumption policy lives in `internal/worker`.

The interfaces for ingestion, reads, Kafka consumption, DLQ publication, and persistence are declared by their consumers. There are no forwarding service or manager layers between handlers and the PostgreSQL store.

## API behavior

Startup fails on malformed numeric limits, empty required Kafka/database settings, or an empty API-key list. API keys are retained as SHA-256 digests by the authenticator and compared in constant time. Authorization headers are never logged.

Ingest requires `application/json`, a valid bearer key, one strict JSON value, and the configured body and batch limits. Kafka publication is synchronous and request-scoped. Query filters are length-bounded, time ranges must be ordered, page sizes are bounded, and cursor payloads are versioned, strictly decoded, and treated as opaque HTTP tokens.

The HTTP server has explicit header/read/write/idle/handler timeouts and a maximum header size. Root signal cancellation reaches request contexts, Kafka operations, and PostgreSQL calls. Shutdown stops accepting new HTTP work and waits up to the configured deadline for handlers.

## Worker behavior

The worker fetches one Kafka record, strictly decodes and validates it, then performs one of two ordered paths:

1. Valid event: retry PostgreSQL persistence within the configured budget, then commit the Kafka offset only after success.
2. Poison event: retry safe DLQ publication within the configured budget, then commit the source offset only after success.

Retries use capped exponential backoff, per-attempt timeouts, and cancellation-aware waits. Exhausting the retry budget returns an error without committing the record; the process supervisor must restart the worker so Kafka can redeliver it. The local Compose stack uses `restart: on-failure`. Full guarantees and failure cases are specified in [delivery-semantics.md](delivery-semantics.md).

## PostgreSQL

The pool has explicit minimum/maximum connections, connect timeout, connection lifetime, idle lifetime, and health-check interval. Startup pings PostgreSQL before reporting the process started. Dynamic query construction only selects fixed SQL fragments; every user filter and cursor value remains a positional parameter.
