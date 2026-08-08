import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { fetchLogs, fetchPipelineStatus } from "./api";

const fetchMock = vi.fn();

beforeEach(() => vi.stubGlobal("fetch", fetchMock));
afterEach(() => {
  fetchMock.mockReset();
  vi.unstubAllGlobals();
});

describe("query API client", () => {
  test("encodes the actual filters and opaque cursor", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ items: [] }));
    await fetchLogs(
      {
        q: "failed login",
        service: "auth-service",
        level: "error",
        start: "2026-08-07T10:00",
        end: "2026-08-07T11:00"
      },
      "opaque+/cursor="
    );

    const url = new URL(String(fetchMock.mock.calls[0][0]), "http://dashboard.local");
    expect(url.pathname).toBe("/v1/logs");
    expect(url.searchParams.get("q")).toBe("failed login");
    expect(url.searchParams.get("service")).toBe("auth-service");
    expect(url.searchParams.get("level")).toBe("error");
    expect(url.searchParams.get("cursor")).toBe("opaque+/cursor=");
    expect(url.searchParams.get("limit")).toBe("50");
    expect(url.searchParams.get("start")).toMatch(/2026-08-07T/);
  });

  test("rejects a successful response with the wrong shape", async () => {
    fetchMock.mockResolvedValue(jsonResponse({ items: "not-an-array" }));
    await expect(fetchLogs({ q: "", service: "", level: "", start: "", end: "" })).rejects.toMatchObject({
      kind: "invalid-response"
    });
  });

  test("reports dependency readiness without inventing a status", async () => {
    fetchMock
      .mockResolvedValueOnce(jsonResponse({ status: "ok" }))
      .mockResolvedValueOnce(jsonResponse({ status: "not_ready" }, 503));
    await expect(fetchPipelineStatus()).resolves.toBe("degraded");
  });
});

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body
  } as Response;
}
