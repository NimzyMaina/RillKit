// Mirrors server/API.md (rillkit-server HTTP API v1). Keep in sync with that file.

export type WorkerState = "RUNNING" | "PAUSED" | "MAINTENANCE" | "DRAINING";

export interface Tenant {
  id: string;
  name: string;
  /** Cloud mode: workers must use HashTagKeys(prefix "rk", tenant id). */
  enforce_keys: boolean;
  created_at: string;
  nodes_online: number;
}

export interface ApiKey {
  id: string;
  label: string | null;
  /** First characters, for display: "rk_live_3fA9". */
  prefix: string;
  created_at: string;
  revoked_at: string | null;
}

/** Returned once, on creation: `key` is the secret. */
export interface NewApiKey extends ApiKey {
  key: string;
}

/** One subscription on one node. */
export interface NodeStream {
  stream: string;
  group: string;
  stream_key: string;
  dlq_key: string;
  lag: number | null;
  pending: number | null;
  ack_rate_sec: number;
  /** Lanes running on this node. */
  concurrency: number | null;
  /** This subscription's state on this node. */
  state: WorkerState | null;
  // Counters since the worker started (null from workers that do not report them).
  /** Handler runs that finished and were acked. */
  succeeded_total: number | null;
  /** Handler runs that raised, nacked or rejected (each attempt). */
  failed_total: number | null;
  dead_lettered_total: number | null;
  /** Size of the entries handled (each attempt), in bytes. */
  bytes_total: number | null;
  /** Entries in the DLQ now. */
  dlq_length: number | null;
  // Attempt timings since the worker started (null from workers that do not report them).
  attempts_total: number | null;
  /** Summed milliseconds from publish to handler start. */
  wait_ms_total: number | null;
  /** Summed milliseconds spent in the handler. */
  work_ms_total: number | null;
}

/** What the worker allows the dashboard to see. */
export interface Capabilities {
  /** RILLKIT_INSPECT_MESSAGES: payloads and DLQ error texts. */
  inspect_messages: boolean;
  /** RILLKIT_CAPTURE_LOGS: per-message logs. */
  capture_logs: boolean;
  /** RILLKIT_TRACE_RECORDS: per-trace records. */
  trace_records: boolean;
}

export type KeyLayout = "plain" | "hashtag" | "custom";

export interface Node {
  node_id: string;
  /** Has a live Relay connection. */
  connected: boolean;
  state: WorkerState;
  active: number;
  in_flight: number;
  poll_ms: number | null;
  batch_size: number | null;
  cpu_pct: number | null;
  mem_mb: number | null;
  sdk: { language: string; version: string } | null;
  /** All false for workers that do not report them. */
  capabilities: Capabilities;
  keys: { layout: KeyLayout; prefix: string | null; tenant: string | null };
  connected_at: string | null;
  last_seen: string;
  streams: NodeStream[];
}

/** One stream and group across all nodes. */
export interface StreamSummary {
  stream: string;
  group: string;
  stream_key: string;
  dlq_key: string;
  /** The group's lag (the same on every node: not summed). */
  lag: number | null;
  pending: number | null;
  /** Summed over nodes. */
  ack_rate_sec: number;
  /** Lanes summed over nodes. */
  concurrency: number;
  /** Nodes consuming it (0 for a created stream nobody consumes yet). */
  nodes: number;
  /** The most restrictive state reported by its nodes. */
  state: WorkerState;
  policy: Policy | null;
  dlq_length: number | null;
  /** True when any live node consuming it allows it. */
  capabilities: Capabilities;
  /** When it was created from the dashboard, if it was. */
  created_at: string | null;
}

export interface OverviewTotals {
  nodes_online: number;
  active_workers: number;
  in_flight: number;
  lag: number;
  pending: number;
  ack_rate_sec: number;
}

export interface Overview {
  tenant: Tenant;
  totals: OverviewTotals;
  /** The most restrictive node state. */
  state: WorkerState;
  /** Connected first, then recently seen (last 10 minutes). */
  nodes: Node[];
  streams: StreamSummary[];
}

export interface Sample {
  ts: string;
  lag: number | null;
  pending: number | null;
  concurrency: number;
  ack_rate_sec: number;
  /** Handler attempts in this sample. */
  attempts: number;
  /** Average wait from publish to handler start, in ms (null when nothing ran). */
  wait_ms: number | null;
  /** Average handler duration, in ms (null when nothing ran). */
  work_ms: number | null;
}

export type DirectiveCmd =
  | "SET_STATE"
  | "SET_CONCURRENCY"
  | "SET_POLL_CONFIG"
  | "MAINTENANCE_ON"
  | "MAINTENANCE_OFF"
  | "EXECUTE_REPLAY"
  | "DELETE_DLQ"
  | "CREATE_STREAM";

export type DirectiveStatus =
  | "PENDING"
  | "SENT"
  | "APPLIED"
  | "DUPLICATE"
  | "REJECTED"
  | "FAILED"
  | "UNDELIVERABLE";

export type DirectiveScope = "global" | "stream" | "node";

export interface DirectiveTarget {
  node_id?: string;
  stream?: string;
  group?: string;
}

export interface DirectiveAck {
  status: string;
  error?: string;
  result?: Record<string, unknown>;
  current_state?: { concurrency: number; state: WorkerState };
}

export interface Directive {
  /** directive_id, "dir_..." */
  id: string;
  cmd: DirectiveCmd;
  scope: DirectiveScope;
  target: DirectiveTarget;
  params: Record<string, unknown>;
  reason: string | null;
  source: "user" | "autoscaler";
  status: DirectiveStatus;
  /** Node that received it. */
  sent_to: string | null;
  ack: DirectiveAck | null;
  created_at: string;
  updated_at: string;
}

export interface Policy {
  stream: string;
  group: string;
  enabled: boolean;
  /** >= 0 */
  min_workers: number;
  /** >= min_workers, <= 100000, in total across nodes */
  max_workers: number;
  /** > 0: wanted = clamp(ceil(lag / target), min, max) in total, spread evenly over the nodes */
  target_lag_per_worker: number;
  upscale_cooldown_sec: number;
  downscale_cooldown_sec: number;
  last_scaled_at: string | null;
  /** e.g. "lag 4820 -> 5 workers (was 2)" */
  last_decision: string | null;
}

/** Body of PUT /policies/{stream}/{group}: Policy fields except stream, group, last_*. */
export type PolicyInput = Omit<Policy, "stream" | "group" | "last_scaled_at" | "last_decision">;

export interface Page<T> {
  items: T[];
  total: number;
  limit: number;
  offset: number;
}

export type MessageSource = "stream" | "dlq";
export type MessageStatus = "waiting" | "pending" | "acked";

export interface DeadLetter {
  original_id: string;
  stream: string;
  group: string;
  deliveries: number;
  /** Null when payloads are hidden. */
  error: string | null;
}

export interface StreamMessage {
  /** Entry id: in the stream, or in the DLQ for source "dlq". */
  id: string;
  /** Bytes of all the entry's fields. */
  size: number;
  /** The entry ("data" and headers, no x- fields); null when the worker does not allow inspect_messages. */
  fields: Record<string, string> | null;
  /** Always shown: it is not payload. */
  traceparent: string | null;
  // source "stream" only
  /** waiting: not delivered yet; pending: delivered, not acked; acked: done (handled, or moved to the DLQ). */
  status?: MessageStatus;
  /** Pending only. */
  deliveries?: number | null;
  consumer?: string | null;
  idle_ms?: number | null;
  // source "dlq" only
  dead_letter?: DeadLetter;
}

export interface MessagePage {
  /** Newest first. */
  messages: StreamMessage[];
  /** Pass as ?before= for the next (older) page; null at the end. */
  next_before: string | null;
  /** With ?after=: more new entries than `count` arrived; ask again. */
  more_after: boolean;
  /** Entries in the stream (or DLQ) now. */
  total: number;
  /** Whether fields and errors were included (inspect_messages). */
  payloads: boolean;
  /** The worker that answered. */
  node_id: string;
  /** When the worker read the page (ISO 8601). */
  as_of: string;
}

export interface MessagesQuery {
  source?: MessageSource;
  before?: string | null;
  /** Only entries newer than this id (the oldest `count` of them, newest first). */
  after?: string | null;
  /** 1 to 200 (default 50). */
  count?: number;
}

/** Over a window, all streams of the tenant. */
export interface MetricsTotals {
  succeeded: number;
  failed: number;
  dead_lettered: number;
  bytes: number;
  /** succeeded / (succeeded + failed); null when nothing ran. */
  success_rate: number | null;
  /** Succeeded per second. */
  per_sec: number;
  bytes_per_sec: number;
  /** Average total lag over the window. */
  lag: number | null;
  /** DLQ entries at the end of the window. */
  dlq_length: number | null;
}

export interface MetricsPoint {
  ts: string;
  succeeded: number;
  failed: number;
  dead_lettered: number;
  bytes: number;
  lag: number;
  pending: number;
  concurrency: number;
  dlq_length: number;
  /** Handler attempts in this bucket. */
  attempts: number;
  /** Average wait from publish to handler start, in ms (null when nothing ran). */
  wait_ms: number | null;
  /** Average handler duration, in ms (null when nothing ran). */
  work_ms: number | null;
  /** Samples in the bucket; 0 means there is no data for it (not zeros). */
  ticks: number;
}

export interface Metrics {
  window_sec: number;
  /** Oldest first, ~60 buckets over the window; counts are per bucket. */
  series: MetricsPoint[];
  /** The last window_sec. */
  current: MetricsTotals;
  /** The window_sec before that, for showing the change. */
  previous: MetricsTotals;
}

export interface NewStreamInput {
  stream: string;
  group: string;
  /** Where the new group begins: "$" (default, only new messages) or "0" (everything already in the stream). */
  start?: "$" | "0";
}

export interface CreatedStream {
  stream: StreamSummary;
  directive: Directive | null;
}

export interface DirectivesQuery {
  /** 1 to 200 (default 25). */
  limit?: number;
  offset?: number;
  source?: "user" | "autoscaler";
}

export interface LogRecord {
  /** Epoch milliseconds as a string of digits, e.g. "1794765340386". */
  ts: string;
  level: string;
  logger: string;
  message: string;
  consumer?: string;
  delivery?: string;
  traceparent?: string;
  /** The exception class, whenever there is one. */
  exc_type?: string;
  /** Message and traceback, only with LogCapture(exceptions=True). */
  exc?: string;
}

/** One handler attempt of a trace (data-plane 9.2); metadata only. */
export interface TraceRecord {
  trace_id: string;
  /** The handler attempt's span. */
  span_id: string;
  /** The span in the entry's traceparent: the publisher's. */
  parent_span_id: string;
  stream: string;
  group: string;
  message_id: string;
  consumer: string;
  node_id: string;
  /** 1 for the first attempt. */
  delivery: number;
  /** Epoch milliseconds, from the entry id. */
  published_at: number;
  started_at: number;
  finished_at: number;
  outcome: "ok" | "error" | "dead_letter";
}

export interface Trace {
  trace_id: string;
  /** Oldest first; empty when no worker keeps records for it (or they expired). */
  records: TraceRecord[];
  /** The worker that answered. */
  node_id: string;
}

export interface Health {
  status: "ok";
  version: string;
  replica_id: string;
  leader: boolean;
}

export interface NewTenantInput {
  id: string;
  name: string;
  enforce_keys?: boolean;
}

export interface Connection {
  control_url: string;
  /** The variables a worker needs (without the secret key). */
  env: Record<string, string>;
}

export interface DirectiveInput {
  cmd: DirectiveCmd;
  scope: DirectiveScope;
  target?: DirectiveTarget;
  params?: Record<string, unknown>;
  reason?: string;
}

export type ErrorCode =
  | "UNAUTHORIZED"
  | "NOT_FOUND"
  | "INVALID"
  | "CONFLICT"
  | "NO_NODE"
  | "TIMEOUT";

export interface ErrorBody {
  error: { code: ErrorCode | string; message: string };
}

/** Handlers for the SSE stream `GET /api/tenants/{t}/events`. */
export interface EventHandlers {
  onOverview: (overview: Overview) => void;
  onDirective: (directive: Directive) => void;
  /** Called when the stream (re)opens; `reconnect` is true after an error. */
  onOpen?: (reconnect: boolean) => void;
  onError?: () => void;
}

/** Everything the dashboard needs from the server. Implemented by the HTTP client and the mock. */
export interface Api {
  /** Resolves if `token` is accepted (GET /api/tenants with it); never stores it. */
  checkToken(token: string): Promise<void>;
  health(): Promise<Health>;
  listTenants(): Promise<Tenant[]>;
  createTenant(input: NewTenantInput): Promise<Tenant>;
  getTenant(t: string): Promise<Tenant>;
  listApiKeys(t: string): Promise<ApiKey[]>;
  createApiKey(t: string, label?: string): Promise<NewApiKey>;
  revokeApiKey(t: string, id: string): Promise<ApiKey>;
  getConnection(t: string): Promise<Connection>;
  getOverview(t: string): Promise<Overview>;
  getSamples(t: string, stream: string, group: string, windowSec?: number): Promise<Sample[]>;
  createDirective(t: string, input: DirectiveInput): Promise<Directive>;
  listDirectives(t: string, query?: DirectivesQuery): Promise<Page<Directive>>;
  createStream(t: string, input: NewStreamInput): Promise<CreatedStream>;
  listMessages(t: string, stream: string, group: string, query?: MessagesQuery): Promise<MessagePage>;
  getMessage(t: string, stream: string, group: string, id: string, source?: MessageSource): Promise<StreamMessage>;
  getMetrics(t: string, windowSec?: number): Promise<Metrics>;
  listPolicies(t: string): Promise<Policy[]>;
  putPolicy(t: string, stream: string, group: string, input: PolicyInput): Promise<Policy>;
  deletePolicy(t: string, stream: string, group: string): Promise<void>;
  getLogs(t: string, stream: string, msgId: string): Promise<{ records: LogRecord[] }>;
  getTrace(t: string, traceId: string): Promise<Trace>;
  /** Opens the live event stream; returns a function that closes it. */
  subscribe(t: string, handlers: EventHandlers): () => void;
}
