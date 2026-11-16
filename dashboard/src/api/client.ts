import type {
  Api,
  ApiKey,
  Connection,
  CreatedStream,
  Directive,
  DirectiveInput,
  ErrorBody,
  EventHandlers,
  Health,
  LogRecord,
  MessagePage,
  Metrics,
  NewApiKey,
  NewTenantInput,
  Overview,
  Page,
  Policy,
  PolicyInput,
  Sample,
  StreamMessage,
  Tenant,
  Trace,
} from "./types";

/** A query string from the defined, non-empty values ("" when there are none). */
function qs(params: Record<string, string | number | null | undefined>): string {
  const sp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === null || v === "") continue;
    sp.set(k, String(v));
  }
  const s = sp.toString();
  return s ? `?${s}` : "";
}

/** An error response from the API: `{error: {code, message}}`, or a network failure (`NETWORK`). */
export class ApiError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(code: string, message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.code = code;
    this.status = status;
  }
}

const enc = encodeURIComponent;

function isErrorBody(value: unknown): value is ErrorBody {
  if (typeof value !== "object" || value === null || !("error" in value)) return false;
  const err = (value as { error: unknown }).error;
  return typeof err === "object" && err !== null && "code" in err && "message" in err;
}

/**
 * The HTTP client. `getToken` is read on every request so a sign-in or sign-out takes effect
 * immediately; `onUnauthorized` is called on any 401.
 */
export function createHttpApi(opts: {
  baseUrl?: string;
  getToken: () => string | null;
  onUnauthorized?: () => void;
}): Api {
  const base = opts.baseUrl ?? "/api";

  async function request<T>(
    method: string,
    path: string,
    body?: unknown,
    tokenOverride?: string,
  ): Promise<T> {
    const headers: Record<string, string> = { Accept: "application/json" };
    const token = tokenOverride ?? opts.getToken();
    if (token) headers.Authorization = `Bearer ${token}`;
    if (body !== undefined) headers["Content-Type"] = "application/json";

    let res: Response;
    try {
      res = await fetch(base + path, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    } catch {
      throw new ApiError("NETWORK", "Could not reach the RillKit server", 0);
    }

    if (res.status === 204) return undefined as T;
    const text = await res.text();
    let data: unknown = undefined;
    if (text) {
      try {
        data = JSON.parse(text);
      } catch {
        data = undefined;
      }
    }
    if (!res.ok) {
      if (res.status === 401 && tokenOverride === undefined) opts.onUnauthorized?.();
      if (isErrorBody(data)) throw new ApiError(data.error.code, data.error.message, res.status);
      throw new ApiError(`HTTP_${res.status}`, res.statusText || "Request failed", res.status);
    }
    return data as T;
  }

  const tenant = (t: string) => `/tenants/${enc(t)}`;

  return {
    checkToken: async (token) => {
      await request<Tenant[]>("GET", "/tenants", undefined, token);
    },
    health: () => request<Health>("GET", "/health"),
    listTenants: () => request<Tenant[]>("GET", "/tenants"),
    createTenant: (input: NewTenantInput) => request<Tenant>("POST", "/tenants", input),
    getTenant: (t) => request<Tenant>("GET", tenant(t)),
    listApiKeys: (t) => request<ApiKey[]>("GET", `${tenant(t)}/api-keys`),
    createApiKey: (t, label) =>
      request<NewApiKey>("POST", `${tenant(t)}/api-keys`, label ? { label } : {}),
    revokeApiKey: (t, id) => request<ApiKey>("DELETE", `${tenant(t)}/api-keys/${enc(id)}`),
    getConnection: (t) => request<Connection>("GET", `${tenant(t)}/connection`),
    getOverview: (t) => request<Overview>("GET", `${tenant(t)}/overview`),
    getSamples: (t, stream, group, windowSec = 900) =>
      request<Sample[]>(
        "GET",
        `${tenant(t)}/streams/${enc(stream)}/${enc(group)}/samples?window_sec=${windowSec}`,
      ),
    createDirective: (t, input: DirectiveInput) =>
      request<Directive>("POST", `${tenant(t)}/directives`, input),
    listDirectives: (t, q = {}) =>
      request<Page<Directive>>(
        "GET",
        `${tenant(t)}/directives${qs({ limit: q.limit ?? 25, offset: q.offset ?? 0, source: q.source })}`,
      ),
    createStream: (t, input) => request<CreatedStream>("POST", `${tenant(t)}/streams`, input),
    listMessages: (t, stream, group, q = {}) =>
      request<MessagePage>(
        "GET",
        `${tenant(t)}/streams/${enc(stream)}/${enc(group)}/messages${qs({
          source: q.source ?? "stream",
          before: q.before,
          after: q.after,
          count: q.count ?? 50,
        })}`,
      ),
    getMessage: (t, stream, group, id, source = "stream") =>
      request<StreamMessage>(
        "GET",
        `${tenant(t)}/streams/${enc(stream)}/${enc(group)}/messages/${enc(id)}${qs({ source })}`,
      ),
    getMetrics: (t, windowSec = 3600) =>
      request<Metrics>("GET", `${tenant(t)}/metrics${qs({ window_sec: windowSec })}`),
    listPolicies: (t) => request<Policy[]>("GET", `${tenant(t)}/policies`),
    putPolicy: (t, stream, group, input: PolicyInput) =>
      request<Policy>("PUT", `${tenant(t)}/policies/${enc(stream)}/${enc(group)}`, input),
    deletePolicy: (t, stream, group) =>
      request<void>("DELETE", `${tenant(t)}/policies/${enc(stream)}/${enc(group)}`),
    getLogs: (t, stream, msgId) =>
      request<{ records: LogRecord[] }>("GET", `${tenant(t)}/logs/${enc(stream)}/${enc(msgId)}`),
    getTrace: (t, traceId) => request<Trace>("GET", `${tenant(t)}/traces/${enc(traceId)}`),

    subscribe(t: string, handlers: EventHandlers) {
      // EventSource cannot send an Authorization header, and the admin token must never go in
      // a url (access logs keep urls). So each connection uses a single-use ticket, fetched with
      // the token in the header. Tickets cannot be reused, so this code reconnects itself
      // instead of letting EventSource retry with a spent ticket.
      let closed = false;
      let source: EventSource | null = null;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let retryMs = 1000;
      let hadError = false;

      const parse = <T,>(e: MessageEvent<string>, fn: (v: T) => void) => {
        try {
          fn(JSON.parse(e.data) as T);
        } catch {
          // A malformed frame is dropped; the next overview replaces the state anyway.
        }
      };

      const retry = () => {
        if (closed) return;
        timer = setTimeout(() => void connect(), retryMs);
        retryMs = Math.min(retryMs * 2, 15_000);
      };

      const connect = async () => {
        if (closed) return;
        let ticket: string;
        try {
          ticket = (await request<{ ticket: string }>("POST", `${tenant(t)}/events/ticket`)).ticket;
        } catch {
          hadError = true;
          handlers.onError?.();
          retry();
          return;
        }
        if (closed) return;
        const es = new EventSource(`${base}${tenant(t)}/events?ticket=${enc(ticket)}`);
        source = es;
        es.addEventListener("open", () => {
          retryMs = 1000;
          handlers.onOpen?.(hadError);
          hadError = false;
        });
        es.addEventListener("error", () => {
          es.close();
          if (closed) return;
          hadError = true;
          handlers.onError?.();
          retry();
        });
        es.addEventListener("overview", (e) => parse<Overview>(e, handlers.onOverview));
        es.addEventListener("directive", (e) => parse<Directive>(e, handlers.onDirective));
        // `ping` needs no handling: it only keeps proxies from closing the stream.
      };

      void connect();
      return () => {
        closed = true;
        clearTimeout(timer);
        source?.close();
      };
    },
  };
}
