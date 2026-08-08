import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { App } from "./App";

const fetchMock = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  cleanup();
  fetchMock.mockReset();
  vi.unstubAllGlobals();
});

describe("App", () => {
  test("renders real status, summary, logs, and cursor pagination", async () => {
    installWorkingAPI();
    render(<App />);

    expect(screen.getByText("Loading events from the query API…")).toBeInTheDocument();
    expect(await screen.findByText("failed login")).toBeInTheDocument();
    expect(screen.getByText("Pipeline ready")).toBeInTheDocument();
    expect(screen.getByText("12")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Load more" }));
    expect(await screen.findByText("token refreshed")).toBeInTheDocument();
    expect(fetchMock.mock.calls.some(([input]) => String(input).includes("cursor=cursor-page-2"))).toBe(true);
    expect(screen.queryByRole("button", { name: "Load more" })).not.toBeInTheDocument();
  });

  test("applies supported filters only after search", async () => {
    installWorkingAPI();
    render(<App />);
    await screen.findByText("failed login");
    const initialLogCalls = logRequestURLs().length;

    fireEvent.change(screen.getByLabelText("Message contains"), { target: { value: "timeout" } });
    fireEvent.change(screen.getByLabelText("Service"), { target: { value: "billing-service" } });
    fireEvent.change(screen.getByLabelText("Level"), { target: { value: "error" } });
    expect(logRequestURLs()).toHaveLength(initialLogCalls);

    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    await waitFor(() => expect(logRequestURLs().length).toBeGreaterThan(initialLogCalls));
    const query = logRequestURLs().at(-1) ?? "";
    expect(query).toContain("q=timeout");
    expect(query).toContain("service=billing-service");
    expect(query).toContain("level=error");
  });

  test("shows empty and malformed-response states", async () => {
    installWorkingAPI({ logs: { items: [] } });
    const { unmount } = render(<App />);
    expect(await screen.findByText("No events stored yet")).toBeInTheDocument();
    unmount();

    fetchMock.mockReset();
    installWorkingAPI({ logs: { items: [{ id: "missing-required-fields" }] } });
    render(<App />);
    const alert = await screen.findByRole("alert");
    expect(within(alert).getByText("Malformed API response")).toBeInTheDocument();
  });

  test("distinguishes authentication and unavailable failures", async () => {
    installWorkingAPI({ logStatus: 401 });
    const { unmount } = render(<App />);
    let alert = await screen.findByRole("alert");
    expect(within(alert).getByText("Authentication rejected")).toBeInTheDocument();
    unmount();

    fetchMock.mockReset();
    fetchMock.mockRejectedValue(new TypeError("connection refused"));
    render(<App />);
    alert = await screen.findByRole("alert");
    expect(within(alert).getByText("API unavailable")).toBeInTheDocument();
  });
});

type WorkingAPIOptions = {
  logs?: unknown;
  logStatus?: number;
};

function installWorkingAPI(options: WorkingAPIOptions = {}) {
  fetchMock.mockImplementation((input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/v1/logs")) {
      if (options.logStatus) return Promise.resolve(jsonResponse({}, options.logStatus));
      if (url.includes("cursor=cursor-page-2")) {
        return Promise.resolve(jsonResponse({ items: [logRecord("event-2", "token refreshed", "info")] }));
      }
      return Promise.resolve(
        jsonResponse(
          options.logs ?? { items: [logRecord("event-1", "failed login", "error")], next_cursor: "cursor-page-2" }
        )
      );
    }
    if (url.endsWith("/v1/metrics")) {
      return Promise.resolve(
        jsonResponse({ logs_ingested_total: 12, logs_last_minute: 2, errors_last_minute: 1, active_services: 2 })
      );
    }
    if (url.endsWith("/v1/services"))
      return Promise.resolve(jsonResponse({ services: ["auth-service", "billing-service"] }));
    if (url.endsWith("/healthz")) return Promise.resolve(jsonResponse({ status: "ok" }));
    if (url.endsWith("/readyz")) return Promise.resolve(jsonResponse({ status: "ready" }));
    throw new Error(`Unexpected request: ${url}`);
  });
}

function logRecord(id: string, message: string, level: string) {
  return {
    schema_version: 1,
    id,
    service: "auth-service",
    level,
    message,
    timestamp: "2026-08-07T20:15:00Z",
    received_at: "2026-08-07T20:15:01Z",
    attributes: { user_id: "123" },
    source: { file: "auth.log" }
  };
}

function logRequestURLs(): string[] {
  return fetchMock.mock.calls.map(([input]) => String(input)).filter((url) => url.includes("/v1/logs"));
}

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body
  } as Response;
}
