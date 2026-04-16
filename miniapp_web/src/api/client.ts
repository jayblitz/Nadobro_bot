/** Mini App API client; sends `Authorization: tma <initData>` when `initData` exists. */

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

/** Human-readable message from fetch/JSON/API errors (for UI). */
export function getApiErrorMessage(err: unknown): string {
  if (err instanceof ApiError) {
    const b = err.body;
    if (b && typeof b === "object" && "detail" in b) {
      const d = (b as { detail: unknown }).detail;
      if (typeof d === "string") return d;
    }
    return err.message;
  }
  if (err instanceof Error) return err.message;
  return "Something went wrong";
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

  const text = await res.text();
  let json: unknown = null;
  if (text.trim()) {
    try {
      json = JSON.parse(text) as unknown;
    } catch {
      throw new ApiError(res.status, {
        detail: "Server returned non-JSON response",
        raw: text.slice(0, 500),
      });
    }
  }

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
