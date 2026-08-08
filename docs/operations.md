# Operations and security

## Local configuration

Copy `.env.example` to `.env` before starting Compose. Every credential in the example is deliberately named `local-only-example-*`; replace those values outside an isolated local machine. `.env` is ignored by Git. The API has no built-in key and refuses to start when `API_KEYS` is absent or empty. Keys are read once at startup, compared through SHA-256 digests in constant time, and are never included in structured logs or metrics.

Compose binds every published port to `127.0.0.1`. PostgreSQL and Kafka are published only because the real integration checks run from the host. The API, worker, agent, and optional dashboard containers run as non-root users; the agent's log mount is read-only.

The API bounds headers, request bodies, event batches, messages, event fields, filters, and page sizes. It accepts exactly one `application/json` value and rejects unknown fields. SQL values are parameterized. HTTP, database, Kafka, worker-attempt, agent-delivery, and shutdown operations all have deadlines.

## Health semantics

| Component | Endpoint | Meaning |
| --- | --- | --- |
| API | `:8080/healthz` | The process can serve HTTP; it does not probe dependencies. |
| API | `:8080/readyz` | PostgreSQL and Kafka are reachable within configured deadlines. |
| Worker | `:9091/healthz` | The worker process's operations server is alive. |
| Worker | `:9091/readyz` | PostgreSQL and Kafka are reachable within configured deadlines. |
| Agent | `:9090/healthz` | The agent process's operations server is alive. |

The agent intentionally has no `readyz`: whether a tailed file currently has new data is not a useful readiness signal, and backend delivery failures are already visible through retry/failure/drop counters and structured logs.

## Metrics

`GET :8080/metrics`, `GET :9091/metrics`, and `GET :9090/metrics` use Prometheus text format. Values are live in-process counters and reset on restart. The API endpoint reports ingestion, rejection, authentication, and HTTP counters; the worker endpoint reports persistence, DLQ, and processing counters; the agent endpoint reports reads, delivery batches, retries, and drops. A process does not expose zero-filled counters owned by another component.

Metrics use fixed names with no labels. In particular, service names, event IDs, request IDs, paths, errors, and source attributes are never metric dimensions, avoiding secret leakage and unbounded cardinality. `/v1/metrics` remains a separate database-derived dashboard summary.

This repository deliberately does not include Prometheus, Grafana, distributed tracing, alert rules, or invented SLOs. Operators can scrape the endpoints with their existing monitoring system.
