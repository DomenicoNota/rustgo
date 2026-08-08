# React log explorer

The React application has one job: inspect events already stored by LogStream. It calls the Go API directly and does not contain sample datasets, hard-coded chart values, a fake login, or an alternate query model.

## API usage

- `/v1/logs` supplies the event table. Search submits the real `service`, `level`, `q`, `start`, and `end` parameters with a page size of 50.
- “Load more” sends the server's opaque `next_cursor`; the browser never decodes it or uses an offset.
- `/v1/services` supplies suggestions while leaving the service field usable if that auxiliary request fails.
- `/v1/metrics` supplies the four PostgreSQL-derived summary values. Missing or malformed summary data is shown as unavailable, never as fabricated zeroes.
- `/healthz` and `/readyz` drive the pipeline status. A live process with unavailable dependencies is shown as degraded.

Every successful response is checked at runtime before it reaches React state. Query failures distinguish authentication rejection, malformed responses, rejected requests, and an unreachable/unavailable API. In-flight log requests are canceled when filters or refresh state change, preventing older results from overwriting a newer query.

## Local boundary

The dashboard is enabled with the Compose `ui` profile and served by Vite on `127.0.0.1:5173`. It is not embedded into the Go binary. The current query endpoints are unauthenticated and intended for this loopback-only development stack; only ingestion accepts an API key. The browser therefore never receives or persists the ingestion credential.
