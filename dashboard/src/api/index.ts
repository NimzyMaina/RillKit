import { createHttpApi } from "./client";
import type { Api } from "./types";
import { clearToken, getToken } from "@/lib/auth";

export const IS_MOCK = import.meta.env.VITE_MOCK === "1";

/**
 * Builds the API implementation. With VITE_MOCK=1 the in-memory fake is loaded through a dynamic
 * import; Vite replaces the condition at build time, so a normal build drops the mock entirely.
 */
export async function loadApi(): Promise<Api> {
  if (import.meta.env.VITE_MOCK === "1") {
    const { createMockApi } = await import("./mock");
    return createMockApi();
  }
  return createHttpApi({
    getToken,
    onUnauthorized: () => clearToken("expired"),
  });
}

export { ApiError } from "./client";
export type * from "./types";
