# Test and verification matrix

The test pyramid is organized around observable contracts, not a target coverage percentage. Fast verification runs deterministic component checks without infrastructure. Full verification adds disposable test records and topics on the real local stack.

| Layer | Critical behavior proved | Main tests | Fast | Full | CI |
| --- | --- | --- | --- | --- | --- |
| Rust unit/component | JSON/plain parsing, lossless fallback, field normalization, bounded queue/line behavior, truncation/rotation state, batch size/time flush, retry classification/backoff, stable event ID across retries, final flush | `agent/src/*` unit tests | Yes | Yes | `agent-test` |
| Go unit/component | API-key authentication, body/batch/filter limits, validation/error mapping, opaque cursor round trips, parameterized query construction, bounded worker retries, cancellation, offset ordering, safe DLQ records | `backend/internal/**/*_test.go` | Yes | Yes | `backend-test` |
| PostgreSQL integration | Schema-version check constraint, unique event ID constraint, conflict-safe duplicate delivery, filters, tuple-cursor pagination | `TestPostgresEnforcesSchemaAndIDConstraints`, `TestPostgresIdempotencyFiltersAndCursorPagination` | No: explicitly skipped without `TEST_DATABASE_URL` | Yes | `backend-test`, `e2e` |
| Migration integration | Ordered checksum ledger and repeatable migration application | `TestMigrationsAreTrackedAndRepeatable` | No: explicitly skipped without `TEST_DATABASE_URL` | Yes | `backend-test`, `e2e` |
| Kafka + worker integration | Real topic publication, duplicate delivery to one DB row, poison routing with redacted metadata, successful source progress through a committed barrier | `TestKafkaWorkerPostgresDeliverySemantics` | No: explicitly skipped without both infrastructure variables | Yes | `backend-test`, `e2e` |
| Full end to end | Unique log line through the real Rust agent, Go API, Kafka, worker, PostgreSQL, and query API; returned schema/attributes/source/ID; repeated delivery remains one row | `scripts/demo.ps1` | No | Yes | `e2e` |
| React component/client | Filtering, cursor pagination, runtime response validation, and failure states | `dashboard/src/App.test.tsx`, `dashboard/src/api.test.ts` | Yes | Yes | `dashboard-test` |

## One-command verification

From the repository root on Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify-fast.ps1
powershell -ExecutionPolicy Bypass -File scripts/verify-full.ps1
```

On macOS/Linux with PowerShell installed, replace `powershell` with `pwsh`. The equivalent Make targets are `make verify-fast` and `make verify-full` (`POWERSHELL` defaults to `pwsh`).

`verify-fast.ps1` performs non-mutating Go formatting validation, Go vet/tests/race tests, Rust formatting/Clippy/tests, React install/lint/tests/build, and Compose configuration validation. It deliberately removes `TEST_DATABASE_URL` and `TEST_KAFKA_BROKERS` for the Go run, making the infrastructure exclusion predictable rather than dependent on a developer's shell.

`verify-full.ps1` runs the fast checks, verifies that the Docker daemon responds within 15 seconds, starts the real Compose stack with `--wait`, polls API/worker/agent health with a deadline, runs PostgreSQL/Kafka integration tests using the resolved Compose database credentials, and runs the real agent-to-query demo. Any native command, readiness timeout, assertion, or integration failure produces a non-zero exit. After a stack attempt, failures print bounded Compose status/log diagnostics.

Full verification removes its containers in a `finally` path while retaining the database volume. Pass `-KeepStack` to retain containers for inspection. Tests use unique event IDs, services, consumer groups, and Kafka topics, so rerunning the command does not depend on prior test data.

## CI policy

All matrix layers run in GitHub Actions. The full E2E is a separate `e2e` job because it builds and starts the whole Compose stack and is materially slower than component jobs. No critical behavioral test is intentionally excluded from CI.
