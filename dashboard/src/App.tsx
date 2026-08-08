import { type FormEvent, useEffect, useRef, useState } from "react";
import {
  APIError,
  fetchLogs,
  fetchMetrics,
  fetchPipelineStatus,
  fetchServices,
  type Filters,
  type LogRecord,
  type Metrics,
  type PipelineStatus
} from "./api";

const initialFilters: Filters = {
  q: "",
  service: "",
  level: "",
  start: "",
  end: ""
};

type LoadState = "loading" | "ready";
type StatusState = PipelineStatus | "checking" | "offline";
type Query = { filters: Filters; revision: number };

export function App() {
  const [draftFilters, setDraftFilters] = useState<Filters>(initialFilters);
  const [query, setQuery] = useState<Query>({ filters: initialFilters, revision: 0 });
  const [logs, setLogs] = useState<LogRecord[]>([]);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [services, setServices] = useState<string[]>([]);
  const [nextCursor, setNextCursor] = useState<string>();
  const [loadState, setLoadState] = useState<LoadState>("loading");
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<APIError | null>(null);
  const [formError, setFormError] = useState<string | null>(null);
  const [status, setStatus] = useState<StatusState>("checking");
  const [summaryUnavailable, setSummaryUnavailable] = useState(false);
  const [overviewSequence, setOverviewSequence] = useState(0);
  const activeLogRequest = useRef<AbortController | null>(null);

  // biome-ignore lint/correctness/useExhaustiveDependencies: the sequence is an explicit user-triggered refresh token
  useEffect(() => {
    const controller = new AbortController();
    setStatus("checking");
    setSummaryUnavailable(false);

    void Promise.allSettled([
      fetchMetrics(controller.signal),
      fetchServices(controller.signal),
      fetchPipelineStatus(controller.signal)
    ]).then(([metricsResult, servicesResult, statusResult]) => {
      if (controller.signal.aborted) return;
      if (metricsResult.status === "fulfilled") {
        setMetrics(metricsResult.value);
      } else {
        setMetrics(null);
        setSummaryUnavailable(true);
      }
      if (servicesResult.status === "fulfilled") setServices(servicesResult.value);
      if (statusResult.status === "fulfilled") setStatus(statusResult.value);
      else setStatus("offline");
    });

    return () => controller.abort();
  }, [overviewSequence]);

  useEffect(() => {
    const controller = new AbortController();
    activeLogRequest.current?.abort();
    activeLogRequest.current = controller;
    setLoadState("loading");
    setLoadingMore(false);
    setError(null);

    void fetchLogs(query.filters, undefined, controller.signal)
      .then((response) => {
        if (controller.signal.aborted) return;
        setLogs(response.items);
        setNextCursor(response.next_cursor);
      })
      .catch((reason: unknown) => {
        if (isAbort(reason)) return;
        setLogs([]);
        setNextCursor(undefined);
        setError(toAPIError(reason));
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoadState("ready");
      });

    return () => controller.abort();
  }, [query]);

  function applyFilters(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (draftFilters.start && draftFilters.end && new Date(draftFilters.start) > new Date(draftFilters.end)) {
      setFormError("Start time must not be after end time.");
      return;
    }
    setFormError(null);
    setQuery((current) => ({ filters: { ...draftFilters }, revision: current.revision + 1 }));
  }

  function resetFilters() {
    setFormError(null);
    setDraftFilters(initialFilters);
    setQuery((current) => ({ filters: initialFilters, revision: current.revision + 1 }));
  }

  function refresh() {
    setQuery((current) => ({ ...current, revision: current.revision + 1 }));
    setOverviewSequence((value) => value + 1);
  }

  async function loadMore() {
    if (!nextCursor || loadingMore) return;
    const controller = new AbortController();
    activeLogRequest.current?.abort();
    activeLogRequest.current = controller;
    setLoadingMore(true);
    setError(null);
    try {
      const response = await fetchLogs(query.filters, nextCursor, controller.signal);
      if (controller.signal.aborted) return;
      setLogs((current) => appendUnique(current, response.items));
      setNextCursor(response.next_cursor);
    } catch (reason) {
      if (!isAbort(reason)) setError(toAPIError(reason));
    } finally {
      if (!controller.signal.aborted) setLoadingMore(false);
    }
  }

  const initialLoading = loadState === "loading" && logs.length === 0;

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">LogStream observability</p>
          <h1>Log explorer</h1>
          <p className="subtitle">Search events collected by the Rust agent and persisted through Kafka.</p>
        </div>
        <div className="header-actions">
          <PipelineStatusView status={status} />
          <button className="secondary-button" type="button" onClick={refresh} disabled={loadState === "loading"}>
            Refresh
          </button>
        </div>
      </header>

      <section className="summary-section" aria-labelledby="summary-heading">
        <div className="section-heading">
          <div>
            <p className="eyebrow">PostgreSQL summary</p>
            <h2 id="summary-heading">Pipeline activity</h2>
          </div>
          {summaryUnavailable ? <span className="quiet-error">Summary unavailable</span> : null}
        </div>
        {metrics ? (
          <div className="metrics-grid">
            <Metric label="Stored events" value={metrics.logs_ingested_total} />
            <Metric label="Last minute" value={metrics.logs_last_minute} />
            <Metric label="Errors / minute" value={metrics.errors_last_minute} />
            <Metric label="Active services" value={metrics.active_services} />
          </div>
        ) : (
          <div className="summary-placeholder">
            {summaryUnavailable ? "Database summary unavailable." : "Waiting for database summary…"}
          </div>
        )}
      </section>

      <section className="explorer" aria-labelledby="logs-heading">
        <div className="section-heading explorer-heading">
          <div>
            <p className="eyebrow">PostgreSQL query API</p>
            <h2 id="logs-heading">Events</h2>
          </div>
          {loadState === "ready" && !error ? <span className="result-count">Showing {logs.length} events</span> : null}
        </div>

        <form className="filters" aria-label="Log filters" onSubmit={applyFilters}>
          <label className="field field-search">
            <span>Message contains</span>
            <input
              maxLength={256}
              placeholder="failed login"
              value={draftFilters.q}
              onChange={(event) => setDraftFilters({ ...draftFilters, q: event.target.value })}
            />
          </label>
          <label className="field">
            <span>Service</span>
            <input
              list="service-options"
              maxLength={128}
              placeholder="All services"
              value={draftFilters.service}
              onChange={(event) => setDraftFilters({ ...draftFilters, service: event.target.value })}
            />
            <datalist id="service-options">
              {services.map((service) => (
                <option key={service} value={service} />
              ))}
            </datalist>
          </label>
          <label className="field">
            <span>Level</span>
            <select
              value={draftFilters.level}
              onChange={(event) => setDraftFilters({ ...draftFilters, level: event.target.value })}
            >
              <option value="">All levels</option>
              <option value="trace">Trace</option>
              <option value="debug">Debug</option>
              <option value="info">Info</option>
              <option value="warn">Warn</option>
              <option value="error">Error</option>
              <option value="fatal">Fatal</option>
            </select>
          </label>
          <label className="field">
            <span>From</span>
            <input
              type="datetime-local"
              value={draftFilters.start}
              onChange={(event) => setDraftFilters({ ...draftFilters, start: event.target.value })}
            />
          </label>
          <label className="field">
            <span>To</span>
            <input
              type="datetime-local"
              value={draftFilters.end}
              onChange={(event) => setDraftFilters({ ...draftFilters, end: event.target.value })}
            />
          </label>
          <div className="filter-actions">
            <button type="submit" disabled={loadState === "loading"}>
              Search
            </button>
            <button className="text-button" type="button" onClick={resetFilters} disabled={loadState === "loading"}>
              Clear
            </button>
          </div>
          {formError ? (
            <p className="form-error" role="alert">
              {formError}
            </p>
          ) : null}
        </form>

        {error ? <ErrorState error={error} retry={refresh} hasResults={logs.length > 0} /> : null}
        {initialLoading ? <LoadingState /> : null}
        {loadState === "ready" && logs.length === 0 && !error ? (
          <EmptyState filtered={hasFilters(query.filters)} />
        ) : null}
        {logs.length > 0 ? <LogTable logs={logs} /> : null}

        {logs.length > 0 && nextCursor ? (
          <div className="pagination">
            <button type="button" onClick={() => void loadMore()} disabled={loadingMore}>
              {loadingMore ? "Loading…" : "Load more"}
            </button>
            <span>Uses the API’s opaque cursor; no offset pagination.</span>
          </div>
        ) : null}
      </section>
    </main>
  );
}

function PipelineStatusView({ status }: { status: StatusState }) {
  const labels: Record<StatusState, string> = {
    checking: "Checking pipeline",
    ready: "Pipeline ready",
    degraded: "Dependencies unavailable",
    offline: "API unavailable"
  };
  return (
    <div className={`pipeline-status status-${status}`} role="status">
      <span className="status-dot" aria-hidden="true" />
      {labels[status]}
    </div>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <article className="metric">
      <span>{label}</span>
      <strong>{value.toLocaleString()}</strong>
    </article>
  );
}

function LogTable({ logs }: { logs: LogRecord[] }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Service</th>
            <th>Level</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody>
          {logs.map((log) => (
            <tr key={log.id}>
              <td data-label="Timestamp">
                <time dateTime={log.timestamp} title={log.timestamp}>
                  {formatTimestamp(log.timestamp)}
                </time>
              </td>
              <td data-label="Service">
                <span className="service-name">{log.service}</span>
              </td>
              <td data-label="Level">
                <span className={`level level-${log.level}`}>{log.level}</span>
              </td>
              <td data-label="Message" className="message-cell">
                {log.message}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function LoadingState() {
  return (
    <div className="state-panel" role="status">
      Loading events from the query API…
    </div>
  );
}

function EmptyState({ filtered }: { filtered: boolean }) {
  return (
    <div className="state-panel empty-state">
      <strong>{filtered ? "No matching events" : "No events stored yet"}</strong>
      <span>
        {filtered
          ? "Adjust the filters and search again."
          : "Run the deterministic demo to send a real event through the pipeline."}
      </span>
    </div>
  );
}

function ErrorState({ error, retry, hasResults }: { error: APIError; retry: () => void; hasResults: boolean }) {
  const titles: Record<APIError["kind"], string> = {
    authentication: "Authentication rejected",
    "invalid-response": "Malformed API response",
    request: "Query rejected",
    unavailable: "API unavailable"
  };
  return (
    <div className={`error-panel${hasResults ? " compact" : ""}`} role="alert">
      <div>
        <strong>{titles[error.kind]}</strong>
        <span>{error.message}</span>
      </div>
      <button className="secondary-button" type="button" onClick={retry}>
        Try again
      </button>
    </div>
  );
}

function appendUnique(current: LogRecord[], incoming: LogRecord[]): LogRecord[] {
  const known = new Set(current.map((log) => log.id));
  const unique = incoming.filter((log) => {
    if (known.has(log.id)) return false;
    known.add(log.id);
    return true;
  });
  return [...current, ...unique];
}

function hasFilters(filters: Filters): boolean {
  return Object.values(filters).some(Boolean);
}

function formatTimestamp(value: string): string {
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium"
  }).format(new Date(value));
}

function isAbort(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

function toAPIError(error: unknown): APIError {
  return error instanceof APIError ? error : new APIError("unavailable", "The LogStream API could not be reached.");
}
