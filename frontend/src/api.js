import { useCallback, useEffect, useRef, useState } from "react";

export class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

/** GET a JSON endpoint of the dashboard API. */
export async function getJson(path, params = {}, { signal } = {}) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "" && value !== false) {
      query.set(key, String(value));
    }
  }
  const url = query.size ? `${path}?${query}` : path;
  const response = await fetch(url, { signal, headers: { Accept: "application/json" } });
  let body = null;
  try {
    body = await response.json();
  } catch {
    // A proxy error page or an empty body; the status code says enough.
  }
  if (!response.ok && !(response.status === 503 && path === "/api/health")) {
    const detail = body && body.detail ? body.detail : response.statusText;
    throw new ApiError(response.status, typeof detail === "string" ? detail : "request failed");
  }
  return body;
}

/**
 * Fetch an endpoint now and then every `intervalMs`, again whenever the
 * params change.
 *
 * The previous data stays on screen while a refresh is in flight, flagged
 * by `refreshing`, so charts dim instead of flashing empty. A failed refresh
 * keeps the last good data and reports the error beside it. Requests left
 * over from earlier params are cancelled, so a slow answer can never
 * overwrite a newer one.
 */
export function usePolling(path, params = {}, intervalMs = 10000) {
  const [state, setState] = useState({ data: null, error: null, loading: true, refreshing: false });
  const key = JSON.stringify(params);
  const controller = useRef(null);

  const load = useCallback(async () => {
    controller.current?.abort();
    const current = new AbortController();
    controller.current = current;
    setState((s) => ({ ...s, refreshing: s.data !== null }));
    try {
      const data = await getJson(path, JSON.parse(key), { signal: current.signal });
      if (!current.signal.aborted) {
        setState({ data, error: null, loading: false, refreshing: false });
      }
    } catch (error) {
      if (current.signal.aborted) return;
      setState((s) => ({ ...s, error, loading: false, refreshing: false }));
    }
  }, [path, key]);

  useEffect(() => {
    setState((s) => ({ ...s, loading: s.data === null }));
    load();
    if (!intervalMs) return () => controller.current?.abort();
    const timer = setInterval(() => {
      // A hidden tab does not need fresh numbers; catch up when it is shown.
      if (document.visibilityState !== "hidden") load();
    }, intervalMs);
    const onVisible = () => document.visibilityState === "visible" && load();
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisible);
      controller.current?.abort();
    };
  }, [load, intervalMs]);

  return { ...state, reload: load };
}
