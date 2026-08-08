# HTTP API

## Operational endpoints

- `GET /healthz` reports process liveness and does not depend on external services.
- `GET /readyz` returns `200` only when PostgreSQL and Kafka are reachable.

## Ingest

`POST /v1/ingest` requires `Authorization: Bearer <API key>` and `Content-Type: application/json`:

```json
{
  "events": [
    {
      "schema_version": 1,
      "id": "3d3814e8-1a11-4cc1-bf93-7397a726793e",
      "timestamp": "2026-08-07T20:15:00Z",
      "service": "auth-service",
      "level": "error",
      "message": "failed login attempt",
      "attributes": { "user_id": "123" },
      "source": { "agent": "local-dev-agent-1", "file": "auth.log" }
    }
  ]
}
```

A `202 Accepted` response reports accepted/rejected validation counts and means Kafka acknowledged the accepted events; it does not mean PostgreSQL persistence has completed.

Requests are limited to `MAX_BODY_BYTES` (2 MiB by default) and `MAX_BATCH_SIZE` events (500 by default). Each encoded event is limited to 256 KiB and each message to 32 KiB. Unknown JSON fields and unsupported schema versions are rejected.

## Query

`GET /v1/logs` accepts `service`, `level`, `q`, `start`, `end`, `limit`, and `cursor`. Service/text filters are limited to 256 bytes, `limit` is between 1 and 500, and `start` must not be later than `end`. Results use stable descending `(timestamp, id)` ordering. Cursors are opaque, versioned tokens and malformed values return a JSON `400` error:

```json
{
  "items": [],
  "next_cursor": "..."
}
```

`GET /v1/services` lists observed services. `GET /v1/metrics` returns database-derived summary counts for the dashboard. The unauthenticated local operational endpoint `GET /metrics` returns fixed-cardinality Prometheus text counters; it does not expose events, API keys, request IDs, or user-derived labels.
