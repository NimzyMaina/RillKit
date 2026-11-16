# rillkit-server HTTP API (v1)

The dashboard and any automation talk to `rillkit-server` through this API. Workers do not: they
use the Relay WebSocket (`/relay/v1`, see `spec/protocol/v1`).

## Conventions

- Base path `/api`. JSON in and out (`Content-Type: application/json`).
- **Auth:** every endpoint except `GET /api/health` and the events stream needs
  `Authorization: Bearer <admin token>` (the server's `RILLKIT_SERVER_ADMIN_TOKEN`). The token is
  never accepted in a url, where access logs and browser history would keep it. `EventSource`
  cannot send headers, so the events stream takes a single-use ticket instead (see Events).
- **Errors:** status 4xx/5xx with `{"error": {"code": "<SNAKE_CASE>", "message": "<text>"}}`.
  Codes: `UNAUTHORIZED` (401), `NOT_FOUND` (404), `INVALID` (422), `CONFLICT` (409),
  `NO_NODE` (409, nothing connected to deliver to), `TIMEOUT` (504).
- Timestamps are ISO 8601 strings in UTC. Durations are milliseconds unless the name says `_sec`.
- `{t}` is a tenant id, e.g. `t-42` (lowercase letters, digits and `-`, 1 to 40 characters).

## Types

```ts
type WorkerState = "RUNNING" | "PAUSED" | "MAINTENANCE" | "DRAINING";
// "most restrictive" ranks them in that order: RUNNING < PAUSED < MAINTENANCE < DRAINING

interface Tenant {
  id: string; name: string;
  enforce_keys: boolean;          // Cloud mode: workers must use HashTagKeys(prefix "rk", tenant id)
  created_at: string;
  nodes_online: number;
}

interface ApiKey {
  id: string; label: string | null;
  prefix: string;                 // first characters, for display: "rk_live_3fA9"
  created_at: string; revoked_at: string | null;
}
interface NewApiKey extends ApiKey { key: string }   // the secret, returned once

interface NodeStream {            // one subscription on one node
  stream: string; group: string; stream_key: string; dlq_key: string;
  lag: number | null; pending: number | null; ack_rate_sec: number;
  concurrency: number | null;     // lanes running on this node
  state: WorkerState | null;      // this subscription's state on this node
  // counters since the worker started (null from workers that do not report them)
  succeeded_total: number | null; // handler runs that finished and were acked
  failed_total: number | null;    // handler runs that raised, nacked or rejected (each attempt)
  dead_lettered_total: number | null;
  bytes_total: number | null;     // size of the entries handled (each attempt), in bytes
  dlq_length: number | null;      // entries in the DLQ now
  // attempt timings since the worker started (null from workers that do not report them)
  attempts_total: number | null;  // handler attempts
  wait_ms_total: number | null;   // summed wait from publish to handler start
  work_ms_total: number | null;   // summed handler duration
}

interface Capabilities {          // what the worker allows the dashboard to see
  inspect_messages: boolean;      // RILLKIT_INSPECT_MESSAGES: payloads and DLQ error texts
  capture_logs: boolean;          // RILLKIT_CAPTURE_LOGS: per-message logs
  trace_records: boolean;         // RILLKIT_TRACE_RECORDS: per-trace records
}

interface Node {
  node_id: string;
  connected: boolean;             // has a live Relay connection
  state: WorkerState;
  active: number; in_flight: number;
  poll_ms: number | null; batch_size: number | null;
  cpu_pct: number | null; mem_mb: number | null;
  sdk: { language: string; version: string } | null;
  capabilities: Capabilities;     // all false for workers that do not report them
  keys: { layout: "plain" | "hashtag" | "custom"; prefix: string | null; tenant: string | null };
  connected_at: string | null; last_seen: string;
  streams: NodeStream[];
}

interface StreamSummary {         // one stream and group across all nodes
  stream: string; group: string; stream_key: string; dlq_key: string;
  lag: number | null;             // the group's lag (the same on every node: not summed)
  pending: number | null;
  ack_rate_sec: number;           // summed over nodes
  concurrency: number;            // lanes summed over nodes
  nodes: number;                  // nodes consuming it (0 for a created stream nobody consumes yet)
  state: WorkerState;             // the most restrictive state reported by its nodes
  policy: Policy | null;
  dlq_length: number | null;
  capabilities: Capabilities;     // true when any live node consuming it allows it
  created_at: string | null;      // when it was created from the dashboard, if it was
}

interface Overview {
  tenant: Tenant;
  totals: { nodes_online: number; active_workers: number; in_flight: number;
            lag: number; pending: number; ack_rate_sec: number };
  state: WorkerState;             // the most restrictive node state
  nodes: Node[];                  // connected first, then recently seen (last 10 minutes)
  streams: StreamSummary[];
}

interface Sample { ts: string; lag: number | null; pending: number | null;
                   concurrency: number; ack_rate_sec: number;
                   attempts: number;              // handler attempts in this sample
                   wait_ms: number | null;        // average wait per attempt, from publish
                   work_ms: number | null }       // average handler duration per attempt

type DirectiveCmd = "SET_STATE" | "SET_CONCURRENCY" | "SET_POLL_CONFIG"
                  | "MAINTENANCE_ON" | "MAINTENANCE_OFF" | "EXECUTE_REPLAY"
                  | "DELETE_DLQ" | "CREATE_STREAM";
type DirectiveStatus = "PENDING" | "SENT" | "APPLIED" | "DUPLICATE" | "REJECTED" | "FAILED"
                     | "UNDELIVERABLE";

interface Directive {
  id: string;                     // directive_id, "dir_..."
  cmd: DirectiveCmd;
  scope: "global" | "stream" | "node";
  target: { node_id?: string; stream?: string; group?: string };
  params: Record<string, unknown>;
  reason: string | null;
  source: "user" | "autoscaler";
  status: DirectiveStatus;
  sent_to: string | null;         // node that received it
  ack: { status: string; error?: string; result?: Record<string, unknown>;
         current_state?: { concurrency: number; state: WorkerState } } | null;
  created_at: string; updated_at: string;
}

interface Policy {
  stream: string; group: string;
  enabled: boolean;
  min_workers: number;            // >= 0, workers in total across nodes
  max_workers: number;            // >= min_workers, <= 100000, in total across nodes
  target_lag_per_worker: number;  // > 0: wanted = clamp(max(ceil(lag / target),
                                  // ceil(arrival / (peak_per_worker * 0.75))), min, max),
                                  // spread evenly over the nodes (1 to 256 per node); the arrival
                                  // rate is finished/s plus backlog growth, with 25 % headroom
  upscale_cooldown_sec: number; downscale_cooldown_sec: number;
  last_scaled_at: string | null;
  last_decision: string | null;   // e.g. "lag 4820, 1980/s -> 5 workers (was 2)"
}

interface Page<T> { items: T[]; total: number; limit: number; offset: number }

interface StreamMessage {
  id: string;                     // entry id: in the stream, or in the DLQ for source "dlq"
  size: number;                   // bytes of all the entry's fields
  fields: Record<string, string> | null;  // the entry ("data" and headers, no x- fields);
                                  // null when the worker does not allow inspect_messages
  traceparent: string | null;     // always shown: it is not payload
  // source "stream" only
  status?: "waiting" | "pending" | "acked";  // waiting: not delivered to the group yet;
                                  // pending: delivered, not acknowledged (being handled or
                                  // retried); acked: done (handled, or moved to the DLQ)
  deliveries?: number | null; consumer?: string | null; idle_ms?: number | null;  // pending only
  // source "dlq" only
  dead_letter?: { original_id: string; stream: string; group: string; deliveries: number;
                  error: string | null };     // error null when payloads are hidden
}

interface MessagePage {
  messages: StreamMessage[];      // newest first
  next_before: string | null;     // pass as ?before= for the next (older) page; null at the end
  more_after: boolean;            // with ?after=: more new entries than count arrived; ask again
  total: number;                  // entries in the stream (or DLQ) now
  payloads: boolean;              // whether fields and errors were included (inspect_messages)
  node_id: string;                // the worker that answered
  as_of: string;                  // when the worker read the page (ISO 8601)
}

interface MetricsTotals {         // over a window, all streams of the tenant
  succeeded: number; failed: number; dead_lettered: number; bytes: number;
  success_rate: number | null;    // succeeded / (succeeded + failed); null when nothing ran
  per_sec: number;                // succeeded per second
  bytes_per_sec: number;
  lag: number | null;             // average total lag over the window
  dlq_length: number | null;      // DLQ entries at the end of the window
}
interface MetricsPoint { ts: string; succeeded: number; failed: number; dead_lettered: number;
                         bytes: number; lag: number; pending: number; concurrency: number;
                         dlq_length: number;
                         attempts: number;        // attempts in this bucket
                         wait_ms: number | null;  // average wait per attempt in this bucket
                         work_ms: number | null;  // average handler time per attempt
                         ticks: number }   // samples in the bucket; 0 means no data (not zeros)
interface Metrics {
  window_sec: number;
  series: MetricsPoint[];         // oldest first, ~60 buckets over the window; counts are per bucket
  current: MetricsTotals;         // the last window_sec
  previous: MetricsTotals;        // the window_sec before that, for showing the change
}

interface LogRecord { ts: string;      // epoch milliseconds as a string of digits, e.g. "1794765340386"
                      level: string; logger: string; message: string;
                      consumer?: string; delivery?: string; traceparent?: string;
                      exc_type?: string;   // the exception class, whenever there is one
                      exc?: string }       // message and traceback, only with LogCapture(exceptions=True)

interface TraceRecord {           // one handler attempt (data-plane 9.2); metadata only
  trace_id: string; span_id: string; parent_span_id: string;  // handler span; publisher's span
  stream: string; group: string; message_id: string;
  consumer: string; node_id: string;
  delivery: number;               // 1 for the first attempt
  published_at: number;           // epoch milliseconds, from the entry id
  started_at: number; finished_at: number;
  outcome: "ok" | "error" | "dead_letter";
}
interface Trace { trace_id: string; records: TraceRecord[];  // oldest first; empty when unknown
                  node_id: string }                          // the worker that answered
```

## Endpoints

| Method and path | Body | Returns |
| --- | --- | --- |
| `GET /api/health` (no auth) | | `{status: "ok", version, replica_id, leader: boolean}` |
| `GET /api/metrics` | | Prometheus text (`text/plain; version=0.0.4`): fleet gauges per tenant (see Metrics) |
| `GET /api/tenants` | | `Tenant[]` |
| `POST /api/tenants` | `{id, name, enforce_keys?}` | `Tenant` (201); `CONFLICT` if the id exists |
| `GET /api/tenants/{t}` | | `Tenant` |
| `GET /api/tenants/{t}/api-keys` | | `ApiKey[]` |
| `POST /api/tenants/{t}/api-keys` | `{label?}` | `NewApiKey` (201) |
| `DELETE /api/tenants/{t}/api-keys/{id}` | | `ApiKey` (revoked; its Relay connections are closed) |
| `GET /api/tenants/{t}/connection` | | `{control_url, env: Record<string,string>}`: the variables a worker needs (without the secret key) |
| `GET /api/tenants/{t}/overview` | | `Overview` |
| `GET /api/tenants/{t}/streams/{stream}/{group}/samples?window_sec=900` | | `Sample[]`, oldest first, one per ~5 s |
| `POST /api/tenants/{t}/directives` | `{cmd, scope, target?, params?, reason?}` | `Directive` (201). Validated with the shared rules; `INVALID` with the reason otherwise. `NO_NODE` if no node is connected |
| `GET /api/tenants/{t}/directives?limit=25&offset=0&source=` | | `Page<Directive>`, newest first. `source` is `user` or `autoscaler` (optional); `limit` 1 to 200 |
| `POST /api/tenants/{t}/streams` | `{stream, group, start?: "$" \| "0"}` | `{stream: StreamSummary, directive: Directive \| null}` (201). Records the stream and, if a worker is connected, sends `CREATE_STREAM` so the stream and its group exist in Redis or Valkey. `start` is where the new group begins: `$` (default, only new messages) or `0` (everything already in the stream) |
| `GET /api/tenants/{t}/streams/{stream}/{group}/messages?source=stream&before=&after=&count=50` | | `MessagePage`. `source` is `stream` (default) or `dlq`; `count` 1 to 200; fetched from a connected worker (`NO_NODE`, `TIMEOUT`). Without a cursor: the newest `count` entries. `before=<id>`: the next older page. `after=<id>`: only entries newer than `<id>` (the oldest `count` of them, still returned newest first; `more_after` says whether even newer ones are left), for appending new messages to a list already shown |
| `GET /api/tenants/{t}/streams/{stream}/{group}/messages/{id}?source=stream` | | `StreamMessage`; `NOT_FOUND` if no such entry |
| `GET /api/tenants/{t}/metrics?window_sec=3600` | | `Metrics`; `window_sec` 300 to 86400 |
| `GET /api/tenants/{t}/policies` | | `Policy[]` |
| `PUT /api/tenants/{t}/policies/{stream}/{group}` | `Policy` fields except `stream`, `group`, `last_*` | `Policy` |
| `DELETE /api/tenants/{t}/policies/{stream}/{group}` | | 204 |
| `GET /api/tenants/{t}/logs/{stream}/{msg_id}` | | `{records: LogRecord[]}`, fetched from a connected node; `TIMEOUT` after 5 s |
| `GET /api/tenants/{t}/traces/{trace_id}` | | `Trace`, fetched from a connected node; `INVALID` for an id that is not 32 lowercase hex digits; empty `records` when the worker keeps none |
| `POST /api/tenants/{t}/events/ticket` | | `{ticket, expires_in_sec: 60}` (201): opens the events stream once, within a minute |
| `GET /api/tenants/{t}/events?ticket=...` (SSE, no header) | | See below; `UNAUTHORIZED` for a missing, used, expired or other tenant's ticket |

Path segments are URL-encoded (stream names may contain `:`).

### Traces

`GET .../traces/{trace_id}` asks a connected worker for one trace (data-plane 9.2): one record per
handler attempt, oldest first, across every stream the trace touched. Records are metadata only
(ids, stream and group, consumer, node, delivery, timestamps and outcome), never payloads or
exception text. A worker keeps them for an hour after the trace's last attempt and only with
`RILLKIT_TRACE_RECORDS=true`; otherwise, or for an unknown trace, `records` is empty. The
capability `trace_records` on a stream says whether its workers keep them.

### Metrics

`GET /api/metrics` (admin token) returns Prometheus text for a scrape:

| Gauge | Meaning |
| --- | --- |
| `rillkit_server_up` | `1` while the replica serves |
| `rillkit_server_autoscaler_leader` | `1` on the replica holding the autoscaler lock |
| `rillkit_tenants` | Tenants known to the server |
| `rillkit_tenant_nodes_online{tenant}` | Connected worker nodes |
| `rillkit_tenant_active_workers{tenant}` | Consumer lanes running |
| `rillkit_tenant_in_flight{tenant}` | Messages being handled now |
| `rillkit_tenant_lag{tenant}` | Messages waiting in their groups |
| `rillkit_tenant_pending{tenant}` | Delivered messages not acknowledged |
| `rillkit_tenant_ack_rate_sec{tenant}` | Messages finished per second |
| `rillkit_tenant_streams{tenant}` | Streams and groups consumed |
| `rillkit_tenant_directives_pending{tenant}` | Directives not yet acknowledged |

Per-stream series are deliberately not included (label cardinality); read those from the
dashboard, workers' telemetry, or Redis. A Prometheus scrape sends
`Authorization: Bearer <admin token>` (for example `authorization.credentials` in
`scrape_configs`).

### Managing the DLQ

DLQ changes are directives (so they are audited in Activity, deduplicated, and applied by a
worker), all with `scope: "stream"` and `target: {stream, group}`:

| `cmd` | `params` | Result (`ack.result`) |
| --- | --- | --- |
| `EXECUTE_REPLAY` | `{count}`: the oldest `count` DLQ entries (1 to 100000, default 100) | `{replayed}` |
| `EXECUTE_REPLAY` | `{ids: [dlq entry ids]}` (1 to 1000), optional `data: {<dlq id>: <new data>}` | `{replayed, missing: [ids]}` |
| `EXECUTE_REPLAY` | `{set_id}`: move the group to an entry id instead | `{set_id}` |
| `DELETE_DLQ` | `{ids: [dlq entry ids]}` (1 to 1000), or `{all: true}` | `{deleted}` |
| `CREATE_STREAM` | `{start?: "$" \| "0"}` | `{created: boolean}` (false if the group already existed) |

Replaying an entry adds it back to the stream without its `x-` fields, then deletes it from the
DLQ. `data` replaces the entry's `data` field (the encoded body, e.g. JSON text) for that entry
only: fix a bad payload, then replay it.

### Events (Server-Sent Events)

Get a ticket with `POST /api/tenants/{t}/events/ticket` (admin token in the header), then open
`GET /api/tenants/{t}/events?ticket=<ticket>`. A ticket opens one stream, for its tenant, within
60 seconds. The stream streams:

| `event:` | `data:` | When |
| --- | --- | --- |
| `overview` | `Overview` | on connect, then at most once per second while telemetry arrives |
| `directive` | `Directive` | a directive is created or its status changes |
| `ping` | `{}` | every 15 s, to keep proxies from closing the stream |

Clients reconnect on error with a **new** ticket: the browser's built-in `EventSource` retry
reuses the url, and its spent ticket, so close the source and open a new one instead.
