# Developer workflow

## Prerequisites

Running the application requires Docker Desktop or Docker Engine with the Compose v2 plugin. Local fast checks additionally require Go 1.26.5, Rust 1.97.1 with rustfmt and Clippy, Node.js 20.19 or newer with npm, and PowerShell. Windows PowerShell 5.1 is supported on Windows; use PowerShell 7 (`pwsh`) on Linux or macOS. GNU Make is optional shorthand—the documented PowerShell and Docker commands remain the canonical implementation.

Copy `.env.example` to `.env`. Compose has matching local-only defaults, but the copied file makes configuration explicit and is the place to replace credentials. Keep `POSTGRES_PASSWORD` and the password embedded in `DATABASE_URL` aligned. Keep `API_KEYS` and `LOGSTREAM_API_KEY` aligned so the API and agent authenticate with the same key. `.env` is ignored by Git.

## Root commands

| Command | Purpose |
| --- | --- |
| `make help` | List the supported commands without doing work. |
| `make up` | Build and run the core stack in the foreground. Ctrl+C stops it. |
| `make up-ui` | Run the core stack plus the React explorer. |
| `make down` | Stop containers and remove orphans while retaining PostgreSQL data. |
| `make status` / `make logs` | Inspect health state or follow bounded application logs. |
| `make demo` | Start the detached core stack, poll readiness, and prove the end-to-end path. |
| `make fmt` / `make lint` / `make test` / `make build` | Run the corresponding component command for Go, Rust, and React. |
| `make verify-fast` | Run every deterministic check that does not need live infrastructure. |
| `make e2e` | Run fast checks, real database/Kafka integration tests, and the agent demo. |

On a machine without Make, run `scripts/verify-fast.ps1` or `scripts/verify-full.ps1` directly. Both scripts resolve paths from the repository root and return a non-zero exit status on failure.

## Startup and shutdown

PostgreSQL and Kafka must pass health checks before topic initialization and migrations run. The API and worker start only after both one-shot setup jobs complete successfully. The agent and optional dashboard wait for API readiness. API, worker, agent, and dashboard containers have explicit health checks; `docker compose up --wait` therefore has a concrete readiness boundary.

The API, worker, and agent receive the normal Compose termination signal and have a 15-second grace period, exceeding their 10-second application shutdown deadlines. `verify-full.ps1` removes its containers in a `finally` path unless `-KeepStack` is supplied. Named PostgreSQL data remains intact. To intentionally erase local data, run `docker compose --profile ui down --volumes --remove-orphans`; this is destructive and is never run by verification scripts.

## CI correspondence

`.github/workflows/ci.yml` runs on pull requests and pushes to `main`. Its parallel component jobs use the same formatting, vet/lint, unit, race, audit, and build commands as `verify-fast.ps1`. PostgreSQL and Kafka service containers exercise migrations and integration tests. After those jobs pass, the E2E job runs `verify-full.ps1 -SkipFastChecks` against a fresh Compose stack and always performs a final cleanup. Action references are pinned to commit SHAs; version comments make update intent visible.
