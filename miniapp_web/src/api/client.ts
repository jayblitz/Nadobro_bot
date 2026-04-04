/**
 * HTTP client for the Mini App API.
 *
 * Automatically attaches the Telegram `initData` as an auth header
 * (`Authorization: tma <initData>`).
 */

import { getInitData } from "@/lib/telegram";

const BASE_URL = import.meta.env.VITE_API_URL ?? "";

class ApiError extends Error {
  status: number;
  body: unknown;
  constructor(status: number, body: unknown) {
    super(`API ${status}`);
    this.status = status;
    this.body = body;
  }
}

async function request<T>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };

  const initData = getInitData();
  if (initData) {
    headers["Authorization"] = `tma ${initData}`;
  }

  const res = await fetch(`${BASE_URL}${path}`, {
    method,
    headers,
    body: body != null ? JSON.stringify(body) : undefined,
  });

  const json = await res.json().catch(() => null);

  if (!res.ok) {
    throw new ApiError(res.status, json);
  }

  return json as T;
}

export const api = {
  get: <T>(path: string) => request<T>("GET", path),
  post: <T>(path: string, body?: unknown) => request<T>("POST", path, body),
  patch: <T>(path: string, body?: unknown) => request<T>("PATCH", path, body),
  delete: <T>(path: string) => request<T>("DELETE", path),
};

export { ApiError };
