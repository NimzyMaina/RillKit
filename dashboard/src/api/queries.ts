import {
  keepPreviousData,
  useMutation,
  useQuery,
  useQueryClient,
  type QueryClient,
} from "@tanstack/react-query";
import { toast } from "sonner";
import { ApiError } from "./client";
import type {
  Api,
  Directive,
  DirectiveInput,
  DirectiveStatus,
  DirectivesQuery,
  MessageSource,
  MessagesQuery,
  NewStreamInput,
  NewTenantInput,
  Overview,
  Page,
  Policy,
  PolicyInput,
  Sample,
  StreamMessage,
} from "./types";
import { resultSummary } from "@/lib/directives";

// ---------------------------------------------------------------------------------------------
// The API instance (the HTTP client, or the mock in mock mode), set once at start-up.

let instance: Api | null = null;

export function setApi(api: Api) {
  instance = api;
}

export function api(): Api {
  if (!instance) throw new Error("API not initialised");
  return instance;
}

// ---------------------------------------------------------------------------------------------
// Query keys

export const qk = {
  tenants: ["tenants"] as const,
  tenant: (t: string) => ["tenant", t] as const,
  overview: (t: string) => ["overview", t] as const,
  samplesFor: (t: string, stream: string, group: string) => ["samples", t, stream, group] as const,
  samples: (t: string, stream: string, group: string, windowSec: number) =>
    ["samples", t, stream, group, windowSec] as const,
  /** Every cached page of the directive log. */
  directives: (t: string) => ["directives", t] as const,
  directivesPage: (t: string, q: Required<Pick<DirectivesQuery, "limit" | "offset">> & { source: string }) =>
    ["directives", t, q] as const,
  /** One directive this tab follows (sent from here), kept live by `directive` events. */
  directive: (t: string, id: string) => ["directive", t, id] as const,
  /** Every messages query of a stream (both sources), to refresh after a DLQ change. */
  messagesFor: (t: string, stream: string, group: string) => ["messages", t, stream, group] as const,
  messages: (t: string, stream: string, group: string, q: MessagesQuery) =>
    ["messages", t, stream, group, q.source ?? "stream", q.before ?? null, q.count ?? 50] as const,
  message: (t: string, stream: string, group: string, source: MessageSource, id: string) =>
    ["message", t, stream, group, source, id] as const,
  logs: (t: string, stream: string, msgId: string) => ["logs", t, stream, msgId] as const,
  trace: (t: string, traceId: string) => ["trace", t, traceId] as const,
  metrics: (t: string, windowSec: number) => ["metrics", t, windowSec] as const,
  policies: (t: string) => ["policies", t] as const,
  apiKeys: (t: string) => ["api-keys", t] as const,
  connection: (t: string) => ["connection", t] as const,
};

// ---------------------------------------------------------------------------------------------
// Errors

const FRIENDLY: Record<string, string> = {
  NO_NODE: "No worker is connected",
  TIMEOUT: "The worker did not answer in time",
  UNAUTHORIZED: "The admin token was rejected",
  NOT_FOUND: "Not found",
  CONFLICT: "That already exists",
  NETWORK: "Could not reach the RillKit server",
};

export function errorTitle(err: unknown): string {
  if (err instanceof ApiError) return FRIENDLY[err.code] ?? "Request failed";
  return "Something went wrong";
}

export function errorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}

export function toastError(err: unknown, context?: string) {
  const title = errorTitle(err);
  const msg = errorMessage(err);
  const code = err instanceof ApiError ? err.code : undefined;
  toast.error(context ? `${context}: ${title}` : title, {
    description: msg && msg !== title ? `${msg}${code ? ` (${code})` : ""}` : code,
  });
}

// ---------------------------------------------------------------------------------------------
// Queries

export function useTenants(enabled = true) {
  return useQuery({ queryKey: qk.tenants, queryFn: () => api().listTenants(), enabled });
}

export function useOverview(t: string) {
  return useQuery({
    queryKey: qk.overview(t),
    queryFn: () => api().getOverview(t),
    // Kept fresh by the SSE stream; refetched on reconnect.
    staleTime: Infinity,
  });
}

export function useSamples(t: string, stream: string, group: string, windowSec: number) {
  return useQuery({
    queryKey: qk.samples(t, stream, group, windowSec),
    queryFn: () => api().getSamples(t, stream, group, windowSec),
    refetchInterval: 5000,
    placeholderData: (prev) => prev,
  });
}

/** One page of the directive log, newest first. Kept live by `directive` events. */
export function useDirectives(t: string, q: DirectivesQuery = {}) {
  const limit = q.limit ?? 25;
  const offset = q.offset ?? 0;
  return useQuery({
    queryKey: qk.directivesPage(t, { limit, offset, source: q.source ?? "all" }),
    queryFn: () => api().listDirectives(t, { limit, offset, source: q.source }),
    staleTime: Infinity,
    placeholderData: keepPreviousData,
  });
}

/** A directive sent from this tab, as last seen (kept live by `directive` events). */
export function useDirective(t: string, id: string | null) {
  const { data } = useQuery<Directive | null>({
    queryKey: qk.directive(t, id ?? ""),
    queryFn: () => null,
    enabled: false,
    staleTime: Infinity,
  });
  return id ? (data ?? null) : null;
}

/** A page of a stream's messages (or its DLQ), newest first. `live` refetches every 2 s. */
export function useMessages(
  t: string,
  stream: string,
  group: string,
  q: MessagesQuery,
  opts: { live?: boolean; enabled?: boolean } = {},
) {
  return useQuery({
    queryKey: qk.messages(t, stream, group, q),
    queryFn: () => api().listMessages(t, stream, group, q),
    refetchInterval: opts.live ? 2000 : false,
    staleTime: opts.live ? 0 : Infinity,
    enabled: opts.enabled ?? true,
    placeholderData: keepPreviousData,
    retry: 1,
  });
}

/** One entry by id; `initial` (the row already loaded) shows at once while it refreshes. */
export function useMessage(
  t: string,
  stream: string,
  group: string,
  source: MessageSource,
  id: string | null,
  initial?: StreamMessage,
) {
  return useQuery({
    queryKey: qk.message(t, stream, group, source, id ?? ""),
    queryFn: () => api().getMessage(t, stream, group, id!, source),
    enabled: Boolean(id),
    initialData: initial,
    initialDataUpdatedAt: 0,
    retry: false,
  });
}

/** A message's logs, fetched from a connected worker (only when `enabled`). */
export function useMessageLogs(t: string, stream: string, msgId: string | null, enabled: boolean) {
  return useQuery({
    queryKey: qk.logs(t, stream, msgId ?? ""),
    queryFn: () => api().getLogs(t, stream, msgId!),
    enabled: enabled && Boolean(msgId),
    retry: false,
    staleTime: 10_000,
  });
}

/** Every handler attempt of one trace, across streams, from a connected worker. */
export function useTrace(t: string, traceId: string | null) {
  return useQuery({
    queryKey: qk.trace(t, traceId ?? ""),
    queryFn: () => api().getTrace(t, traceId!),
    enabled: Boolean(traceId),
    retry: false,
    staleTime: 10_000,
  });
}

/** Throughput and success over a window, against the window before it. */
export function useMetrics(t: string, windowSec: number) {
  return useQuery({
    queryKey: qk.metrics(t, windowSec),
    queryFn: () => api().getMetrics(t, windowSec),
    refetchInterval: 10_000,
    placeholderData: keepPreviousData,
  });
}

export function usePolicies(t: string) {
  return useQuery({ queryKey: qk.policies(t), queryFn: () => api().listPolicies(t) });
}

export function useApiKeys(t: string) {
  return useQuery({ queryKey: qk.apiKeys(t), queryFn: () => api().listApiKeys(t) });
}

export function useConnection(t: string) {
  return useQuery({ queryKey: qk.connection(t), queryFn: () => api().getConnection(t) });
}

// ---------------------------------------------------------------------------------------------
// Live updates

const newer = (a: Directive, b: Directive) => Date.parse(a.updated_at) > Date.parse(b.updated_at);

/**
 * Writes a directive (from an event or a POST response) into the cache: it replaces the row on
 * any cached page that shows it, and a new directive is prepended to the first page of every
 * matching filter (bumping the total). Other pages keep their rows until they are refetched.
 */
export function upsertDirective(qc: QueryClient, t: string, d: Directive) {
  // Never move a directive backwards (events can overtake the POST response).
  const one = qk.directive(t, d.id);
  const known = qc.getQueryData<Directive | null>(one);
  if (!known || !newer(known, d)) qc.setQueryData(one, d);

  for (const [queryKey, page] of qc.getQueriesData<Page<Directive>>({ queryKey: qk.directives(t) })) {
    if (!page) continue;
    const q = queryKey[2] as { offset: number; limit: number; source: string } | undefined;
    const i = page.items.findIndex((x) => x.id === d.id);
    if (i !== -1) {
      if (newer(page.items[i], d)) continue;
      const items = page.items.slice();
      items[i] = d;
      qc.setQueryData<Page<Directive>>(queryKey, { ...page, items });
      continue;
    }
    if (!q || q.offset !== 0 || (q.source !== "all" && q.source !== d.source)) continue;
    const newest = page.items[0];
    if (newest && Date.parse(newest.created_at) > Date.parse(d.created_at)) continue;
    qc.setQueryData<Page<Directive>>(queryKey, {
      ...page,
      items: [d, ...page.items].slice(0, q.limit),
      total: page.total + 1,
    });
  }
}

/** How far apart appended chart points are kept (the server samples every ~5 s). */
const SAMPLE_SPACING_MS = 4500;

/** Appends the overview's per-stream numbers to every cached samples window for that stream. */
export function appendSamples(qc: QueryClient, t: string, ov: Overview) {
  const now = Date.now();
  for (const s of ov.streams) {
    for (const [queryKey, data] of qc.getQueriesData<Sample[]>({
      queryKey: qk.samplesFor(t, s.stream, s.group),
    })) {
      if (!data) continue;
      const last = data[data.length - 1];
      if (last && now - Date.parse(last.ts) < SAMPLE_SPACING_MS) continue;
      const windowSec = Number(queryKey[4]) || 900;
      // The overview has no timings: carry the last sample's until the next poll brings fresh ones.
      const point: Sample = {
        ts: new Date(now).toISOString(),
        lag: s.lag,
        pending: s.pending,
        concurrency: s.concurrency,
        ack_rate_sec: s.ack_rate_sec,
        attempts: last?.attempts ?? 0,
        wait_ms: last?.wait_ms ?? null,
        work_ms: last?.work_ms ?? null,
      };
      const from = now - windowSec * 1000;
      qc.setQueryData<Sample[]>(queryKey, [...data.filter((x) => Date.parse(x.ts) >= from), point]);
    }
  }
}

// Directives this tab sent, whose status we report in a toast as it changes.
const watched = new Map<string, string>();

const TERMINAL: DirectiveStatus[] = ["APPLIED", "DUPLICATE", "REJECTED", "FAILED", "UNDELIVERABLE"];

export function isTerminal(s: DirectiveStatus) {
  return TERMINAL.includes(s);
}

export function reportDirective(d: Directive) {
  const label = watched.get(d.id);
  if (!label) return;
  const where = d.sent_to ? ` on ${d.sent_to}` : "";
  switch (d.status) {
    case "PENDING":
      toast.loading(label, { id: d.id, description: "Pending: waiting for a worker" });
      break;
    case "SENT":
      toast.loading(label, { id: d.id, description: `Sent${where}, waiting for the ack` });
      break;
    case "APPLIED": {
      const result = resultSummary(d);
      toast.success(label, {
        id: d.id,
        description: `Applied${where}${result ? ` · ${result}` : ""}`,
        duration: 5000,
      });
      break;
    }
    case "DUPLICATE":
      toast.info(label, { id: d.id, description: "Already applied (duplicate)", duration: 4000 });
      break;
    default:
      toast.error(label, {
        id: d.id,
        description: `${d.status.toLowerCase()}${d.ack?.error ? `: ${d.ack.error}` : ""}`,
        duration: 8000,
      });
  }
  if (isTerminal(d.status)) watched.delete(d.id);
}

// ---------------------------------------------------------------------------------------------
// Mutations

/** Sends a directive, toasts the result and follows its live status. */
export function useSendDirective(t: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ input }: { input: DirectiveInput; label: string }) =>
      api().createDirective(t, input),
    onSuccess: (d, { label }) => {
      followDirective(qc, t, d, label);
    },
    onError: (err, { label }) => toastError(err, label),
  });
}

/** Reports a directive this tab created in a toast that follows its status. */
function followDirective(qc: QueryClient, t: string, d: Directive, label: string) {
  watched.set(d.id, label);
  upsertDirective(qc, t, d);
  reportDirective(qc.getQueryData<Directive>(qk.directive(t, d.id)) ?? d);
}

/** Records a stream (and its group) and, with a worker connected, sends CREATE_STREAM. */
export function useCreateStream(t: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (input: NewStreamInput) => api().createStream(t, input),
    onSuccess: ({ stream, directive }) => {
      const label = `Create ${stream.stream} / ${stream.group}`;
      if (directive) followDirective(qc, t, directive, label);
      else
        toast.success(label, {
          description: "Recorded. It is created in Redis when a worker connects.",
          duration: 6000,
        });
      return qc.invalidateQueries({ queryKey: qk.overview(t) });
    },
  });
}

export function useCreateTenant() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (input: NewTenantInput) => api().createTenant(input),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.tenants }),
  });
}

export function useCreateApiKey(t: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (label?: string) => api().createApiKey(t, label),
    onSuccess: () => qc.invalidateQueries({ queryKey: qk.apiKeys(t) }),
    onError: (err) => toastError(err, "Create API key"),
  });
}

export function useRevokeApiKey(t: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api().revokeApiKey(t, id),
    onSuccess: (k) => {
      toast.success("API key revoked", { description: `${k.prefix}… can no longer connect` });
      return qc.invalidateQueries({ queryKey: qk.apiKeys(t) });
    },
    onError: (err) => toastError(err, "Revoke API key"),
  });
}

export function useSavePolicy(t: string, stream: string, group: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (input: PolicyInput) => api().putPolicy(t, stream, group, input),
    onSuccess: (p) => {
      qc.setQueryData<Policy[]>(qk.policies(t), (prev) => [
        ...(prev ?? []).filter((x) => !(x.stream === stream && x.group === group)),
        p,
      ]);
      toast.success("Autoscaling policy saved", {
        description: p.enabled ? `${p.min_workers} to ${p.max_workers} workers per node` : "Saved, but disabled",
      });
    },
    onError: (err) => toastError(err, "Save policy"),
  });
}

export function useDeletePolicy(t: string, stream: string, group: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api().deletePolicy(t, stream, group),
    onSuccess: () => {
      qc.setQueryData<Policy[]>(qk.policies(t), (prev) =>
        (prev ?? []).filter((x) => !(x.stream === stream && x.group === group)),
      );
      toast.success("Autoscaling policy removed");
    },
    onError: (err) => toastError(err, "Remove policy"),
  });
}
