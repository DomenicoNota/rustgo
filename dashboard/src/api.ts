export type LogRecord = {
  schema_version: number;
  id: string;
  service: string;
  level: string;
  message: string;
  timestamp: string;
  received_at?: string;
  attributes: Record<string, unknown>;
  source: Record<string, string>;
};

export type Metrics = {
  logs_ingested_total: number;
  logs_last_minute: number;
  errors_last_minute: number;
  active_services: number;
};

export type LogSearchResponse = {
  items: LogRecord[];
  next_cursor?: string;
};

export type Filters = {
  q: string;
  service: string;
  level: string;
  start: string;
  end: string;
};

export type PipelineStatus = "ready" | "degraded";
export type APIErrorKind = "authentication" | "invalid-response" | "request" | "unavailable";

export class APIError extends Error {
  constructor(
    public readonly kind: APIErrorKind,
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "APIError";
  }
}

const apiBase = (import.meta.env.VITE_API_BASE_URL ?? "").replace(/\/$/, "");

export async function fetchMetrics(signal?: AbortSignal): Promise<Metrics> {
  return fetchJSON("/v1/metrics", isMetrics, signal);
}

export async function fetchServices(signal?: AbortSignal): Promise<string[]> {
  const response = await fetchJSON("/v1/services", isServicesResponse, signal);
  return response.services;
}

export async function fetchLogs(filters: Filters, cursor?: string, signal?: AbortSignal): Promise<LogSearchResponse> {
  const params = new URLSearchParams();
  const values: Record<string, string> = { ...filters };
  if (cursor) values.cursor = cursor;
  for (const [key, value] of Object.entries(values)) {
    if (!value) continue;
    if (key === "start" || key === "end") {
      params.set(key, new Date(value).toISOString());
    } else {
      params.set(key, value);
    }
  }
  params.set("limit", "50");
  return fetchJSON(`/v1/logs?${params.toString()}`, isLogSearchResponse, signal);
}

export async function fetchPipelineStatus(signal?: AbortSignal): Promise<PipelineStatus> {
  await fetchJSON("/healthz", isHealthResponse, signal);
  const response = await request("/readyz", signal);
  if (response.status === 503) return "degraded";
  await parseResponse(response, isReadyResponse);
  return "ready";
}

async function fetchJSON<T>(path: string, validate: (value: unknown) => value is T, signal?: AbortSignal): Promise<T> {
  return parseResponse(await request(path, signal), validate);
}

async function request(path: string, signal?: AbortSignal): Promise<Response> {
  try {
    return await fetch(`${apiBase}${path}`, {
      headers: { Accept: "application/json" },
      signal
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") throw error;
    throw new APIError("unavailable", "The LogStream API could not be reached.");
  }
}

async function parseResponse<T>(response: Response, validate: (value: unknown) => value is T): Promise<T> {
  if (!response.ok) {
    if (response.status === 401 || response.status === 403) {
      throw new APIError("authentication", "The API rejected this browser session.", response.status);
    }
    if (response.status >= 500) {
      throw new APIError("unavailable", "The LogStream API is temporarily unavailable.", response.status);
    }
    throw new APIError("request", `The API rejected the request (${response.status}).`, response.status);
  }

  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw new APIError("invalid-response", "The API returned malformed JSON.", response.status);
  }
  if (!validate(body)) {
    throw new APIError("invalid-response", "The API returned an unexpected response shape.", response.status);
  }
  return body;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isMetrics(value: unknown): value is Metrics {
  return (
    isObject(value) &&
    isFiniteNumber(value.logs_ingested_total) &&
    isFiniteNumber(value.logs_last_minute) &&
    isFiniteNumber(value.errors_last_minute) &&
    isFiniteNumber(value.active_services)
  );
}

function isServicesResponse(value: unknown): value is { services: string[] } {
  return (
    isObject(value) && Array.isArray(value.services) && value.services.every((service) => typeof service === "string")
  );
}

function isLogSearchResponse(value: unknown): value is LogSearchResponse {
  return (
    isObject(value) &&
    Array.isArray(value.items) &&
    value.items.every(isLogRecord) &&
    (value.next_cursor === undefined || typeof value.next_cursor === "string")
  );
}

function isLogRecord(value: unknown): value is LogRecord {
  return (
    isObject(value) &&
    value.schema_version === 1 &&
    typeof value.id === "string" &&
    value.id.length > 0 &&
    typeof value.service === "string" &&
    value.service.length > 0 &&
    typeof value.level === "string" &&
    ["trace", "debug", "info", "warn", "error", "fatal"].includes(value.level) &&
    typeof value.message === "string" &&
    typeof value.timestamp === "string" &&
    !Number.isNaN(Date.parse(value.timestamp)) &&
    isObject(value.attributes) &&
    isStringRecord(value.source) &&
    (value.received_at === undefined ||
      (typeof value.received_at === "string" && !Number.isNaN(Date.parse(value.received_at))))
  );
}

function isStringRecord(value: unknown): value is Record<string, string> {
  return isObject(value) && Object.values(value).every((item) => typeof item === "string");
}

function isHealthResponse(value: unknown): value is { status: "ok" } {
  return isObject(value) && value.status === "ok";
}

function isReadyResponse(value: unknown): value is { status: "ready" } {
  return isObject(value) && value.status === "ready";
}
