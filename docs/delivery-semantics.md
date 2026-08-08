# Delivery semantics

## The short version

After Kafka acknowledges an event, LogStream provides **at-least-once processing**, not exactly-once delivery. An event may be delivered through HTTP or Kafka more than once. The Rust agent assigns the event ID before its first HTTP attempt and reuses the same in-memory event for every retry. PostgreSQL owns the idempotency boundary: `logs.id` is a primary key, and the worker uses `ON CONFLICT (id) DO NOTHING`.

This means repeated delivery of one stable event ID creates one database row. If two different payloads reuse an ID, the first stored payload wins; LogStream does not merge them or detect that semantic conflict.

The durable at-least-once guarantee begins when the API successfully publishes an event to Kafka. The agent has a bounded in-memory queue and retry budget but no disk spool. Queue overflow, exhausted HTTP retries, forced shutdown, or an agent restart can lose or reread source lines. Agent restarts currently reread files from byte zero and generate new IDs, so restart-time duplicates are not deduplicated.

Kafka publication errors can be ambiguous: part of an HTTP batch may have reached the broker even though the API returns a dependency error. The agent retries the original batch with the original IDs, and PostgreSQL idempotency makes that whole-batch retry safe.

## Ordered worker state machine

The worker processes one fetched record at a time. `CommitInterval: 0` makes Kafka commits synchronous. For each source record, progress advances only through one of these paths:

```text
valid record  -> PostgreSQL insert succeeds/idempotently conflicts -> source offset commit
poison record -> logs.dlq publication succeeds                    -> source offset commit
failure       -> bounded retries exhaust                           -> no commit; worker exits
cancellation  -> in-flight operation is cancelled                  -> no premature commit
```

Sequential processing matters because Kafka commits the highest supplied offset for a partition. The worker never fetches and processes later records concurrently within this implementation, so it cannot commit past an unresolved earlier record.

Persistence, DLQ publication, offset commits, and Kafka fetch errors use a configurable finite retry budget. Defaults are five attempts, exponential delays beginning at 250 ms and capped at 5 seconds, and a 10-second timeout for each persistence/publication/commit attempt. Waiting is cancellation-aware. After exhaustion the worker returns an error; Docker Compose restarts it with `restart: on-failure`, and Kafka redelivers any uncommitted record.

## Poison records and `logs.dlq`

A record is poison when it is not exactly one strict version-1 `LogEvent` JSON object or fails event validation. The worker publishes a versioned DLQ envelope containing:

- a stable DLQ ID derived from source topic, partition, and offset;
- source topic, partition, offset, and timestamp;
- a controlled failure code and safe validation message;
- failure time, payload byte count, and SHA-256 fingerprints of the payload and optional key.

The envelope deliberately excludes the raw payload, raw key, Kafka headers, arbitrary dependency errors, API keys, and connection strings. Operators can correlate a known source record or payload by hash without copying potentially sensitive log content into another topic.

DLQ delivery is also at least once. If DLQ publication succeeds but the source offset commit fails, redelivery can produce another DLQ record with the same stable DLQ ID. Kafka does not deduplicate those records.

## Failure behavior

| Situation | Observable behavior | Kafka progress |
| --- | --- | --- |
| Kafka unavailable during API ingestion | The API does not acknowledge acceptance; it returns a dependency error and the agent applies its bounded HTTP retry policy. | No source record exists. |
| Kafka unavailable while the worker fetches or commits | The worker retries with backoff, then exits non-zero after the budget. | The uncommitted record is eligible for redelivery. |
| PostgreSQL unavailable | The worker retries each bounded database attempt, then exits non-zero. | The source offset is not committed. |
| Malformed or invalid record | A safe diagnostic envelope is synchronously published to `logs.dlq`. | Committed only after DLQ acknowledgement. |
| Duplicate event ID | PostgreSQL's primary key turns the insert into a successful no-op. | The duplicate delivery is committed normally. |
| DLQ publication unavailable | Publication is retried; exhaustion stops the worker. | The poison source offset is not committed. |
| Worker terminated during persistence or publication | Root cancellation reaches the active operation and retry wait. | No commit occurs unless the terminal action had already succeeded and its synchronous commit completed. |
| Worker terminated after persistence but before commit | Kafka may redeliver the record. The database conflict is a successful idempotent outcome. | A later worker attempt commits it. |

## Guarantees and non-guarantees

LogStream guarantees, within Kafka retention and PostgreSQL availability:

- synchronous API publication with Kafka `RequireAll` acknowledgements;
- stable event IDs across one agent process's HTTP retries;
- database-enforced idempotency for a stable event ID;
- no source-offset commit before persistence or acknowledged DLQ routing;
- bounded, cancellation-aware worker retries with no hot loop.

LogStream does not guarantee:

- end-to-end exactly-once processing;
- source durability before Kafka acceptance;
- stable IDs across agent restarts;
- atomicity between PostgreSQL and Kafka offset commits;
- deduplicated DLQ records;
- global ordering across Kafka partitions;
- high availability from the single-node local Kafka/PostgreSQL stack.

The PostgreSQL/Kafka integration test publishes a duplicate event, a poison record, and a barrier through a real broker; it verifies one stored row for the duplicate ID, a redacted DLQ envelope, and committed consumer-group progress through the barrier.
