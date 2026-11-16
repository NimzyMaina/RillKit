// In-memory fake of rillkit-server, used with VITE_MOCK=1 (npm run dev:mock).
// It is loaded through a dynamic import guarded by import.meta.env.VITE_MOCK, so it is never
// part of the normal production bundle. It follows server/API.md: the same shapes, the same
// validation errors, and directives that really change the fake state.

import { ApiError } from "./client";
import type {
  Api,
  ApiKey,
  Capabilities,
  Connection,
  DeadLetter,
  Directive,
  DirectiveInput,
  EventHandlers,
  LogRecord,
  MessagePage,
  MessageStatus,
  Metrics,
  MetricsPoint,
  MetricsTotals,
  NewApiKey,
  Node,
  NodeStream,
  Overview,
  Policy,
  PolicyInput,
  Sample,
  StreamMessage,
  StreamSummary,
  Tenant,
  TraceRecord,
  WorkerState,
} from "./types";

const TICK_MS = 1000;
const SAMPLE_EVERY_MS = 5000;
const HISTORY_SEC = 3600;
const METRICS_HISTORY_SEC = 2 * 86400;
const RING = 800;

const RESTRICTIVENESS: Record<WorkerState, number> = {
  // the order the spec and the server use (spec/control-plane.md, 2.2)
  RUNNING: 0,
  PAUSED: 1,
  MAINTENANCE: 2,
  DRAINING: 3,
};

function mostRestrictive(states: WorkerState[]): WorkerState {
  return states.reduce<WorkerState>(
    (acc, s) => (RESTRICTIVENESS[s] > RESTRICTIVENESS[acc] ? s : acc),
    "RUNNING",
  );
}

const iso = (ms: number = Date.now()) => new Date(ms).toISOString();
const clone = <T>(v: T): T => structuredClone(v);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const rand = (min: number, max: number) => min + Math.random() * (max - min);
const pick = <T>(xs: T[]): T => xs[Math.floor(Math.random() * xs.length)];
const randId = (n: number) => {
  const alphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let s = "";
  for (let i = 0; i < n; i++) s += alphabet[Math.floor(Math.random() * alphabet.length)];
  return s;
};
const round1 = (v: number) => Math.round(v * 10) / 10;
const ENTRY_ID = /^\d+-\d+$/;
const idMs = (id: string) => Number(id.split("-")[0]);
const cmpId = (a: string, b: string) => {
  const [am, as] = a.split("-").map(Number);
  const [bm, bs] = b.split("-").map(Number);
  return am - bm || as - bs;
};
const sizeOf = (fields: Record<string, string>) =>
  Object.entries(fields).reduce((n, [k, v]) => n + new TextEncoder().encode(k + v).length, 0);
const trace = () => `00-${randId(32).toLowerCase().replace(/[^0-9a-f]/g, "a")}-${randId(16).toLowerCase().replace(/[^0-9a-f]/g, "b")}-01`;

/** A stable 16-hex span id for a seed (the mock has no real spans). */
const spanFor = (seed: string) => {
  let h = 0;
  for (let i = 0; i < seed.length; i++) h = (Math.imul(h, 31) + seed.charCodeAt(i)) >>> 0;
  return h.toString(16).padStart(8, "0").repeat(2).slice(0, 16);
};

/** Queue wait, handler time and attempts for a stream at this moment (mock heuristic). */
function timingFor(perLane: number, lag: number, concurrency: number, ackRate: number) {
  return {
    attempts: Math.max(0, Math.round(ackRate * (SAMPLE_EVERY_MS / 1000))),
    wait_ms: round1((lag / Math.max(0.1, perLane * Math.max(1, concurrency))) * 1000),
    work_ms: round1(1000 / Math.max(1, perLane)),
  };
}

// ---------------------------------------------------------------------------------------------
// State

interface MockMsg {
  id: string;
  fields: Record<string, string>;
  size: number;
  /** When handling starts (pending) and ends (acked); null while the stream is not running. */
  startAt: number | null;
  doneAt: number | null;
  consumer: string | null;
  deliveries: number;
  /** Fails and goes to the DLQ when done. */
  fails: string | null;
  dead?: DeadLetter;
}

interface Counters {
  succeeded: number;
  failed: number;
  dead_lettered: number;
  bytes: number;
}

interface StreamDef {
  stream: string;
  group: string;
  /** Messages per second arriving (base, before the wave). */
  inRate: number;
  /** Period of the arrival wave in seconds; lag rises and falls with it. */
  period: number;
  /** Messages per second one lane can process. */
  perLane: number;
  lag: number;
  pending: number;
  phase: number;
  messages: MockMsg[];
  dlq: MockMsg[];
  seq: number;
  counters: Counters;
}

interface NodeSub {
  stream: string;
  group: string;
  concurrency: number;
  state: WorkerState;
}

interface NodeRec {
  node_id: string;
  connected: boolean;
  state: WorkerState;
  poll_ms: number;
  batch_size: number;
  cpu_pct: number;
  mem_mb: number;
  sdk: { language: string; version: string };
  capabilities: Capabilities;
  connected_at: string | null;
  last_seen: number;
  subs: NodeSub[];
  ackRates: Record<string, number>;
  inFlight: number;
}

interface MetricRow extends Omit<MetricsPoint, "ts" | "ticks"> {
  ms: number;
}

interface TenantRec {
  tenant: Omit<Tenant, "nodes_online">;
  streams: StreamDef[];
  nodes: NodeRec[];
  policies: Map<string, Policy>;
  apiKeys: ApiKey[];
  directives: Directive[];
  samples: Map<string, Sample[]>;
  logs: Map<string, LogRecord[]>;
  registered: Map<string, string>;
  metrics: MetricRow[];
  lastSampleAt: number;
  sinceSample: Counters;
}

const key = (stream: string, group: string) => `${stream}\u0000${group}`;
const zero = (): Counters => ({ succeeded: 0, failed: 0, dead_lettered: 0, bytes: 0 });

function keysFor(t: TenantRec, stream: string) {
  if (t.tenant.enforce_keys) {
    const tag = `rk:{${t.tenant.id}:${stream}}`;
    return { stream_key: `${tag}:stream`, dlq_key: `${tag}:dlq` };
  }
  return { stream_key: stream, dlq_key: `${stream}-dlq` };
}

const ERRORS: Record<string, string[]> = {
  orders: [
    "card declined for order {n}",
    "GatewayTimeout: no response from the payment gateway after 500 ms",
    "ValueError: amount must be positive, got -12.00",
    "KeyError: 'customer_id'",
    "currency XAU is not supported",
  ],
  emails: [
    "SMTPRecipientsRefused: mailbox unavailable",
    "template 'welcome_v3' not found",
    "rate limited by provider, retry after 30 s",
  ],
  default: [
    "TimeoutError: downstream service did not answer",
    "ValidationError: field 'id' is required",
    "ConnectionResetError: peer closed the connection",
    "JSONDecodeError: Expecting ',' delimiter: line 1 column 42",
  ],
};

function errorsFor(stream: string): string[] {
  if (stream.startsWith("orders")) return ERRORS.orders;
  if (stream.startsWith("emails")) return ERRORS.emails;
  return ERRORS.default;
}

function payloadFor(stream: string, n: number): Record<string, unknown> {
  if (stream.startsWith("orders"))
    return { order_id: 80000 + n, customer_id: `cus_${randId(6)}`, amount: round1(rand(5, 480)), currency: pick(["EUR", "USD", "KES"]) };
  if (stream.startsWith("emails"))
    return { to: `user${Math.floor(rand(1, 9000))}@example.com`, template: pick(["receipt", "welcome", "reminder"]), locale: pick(["en", "sw", "fr"]) };
  if (stream.startsWith("payments"))
    return { payment_id: `pay_${randId(10)}`, order_id: 80000 + n, settled_at: iso(), amount: round1(rand(5, 480)) };
  return { id: n, kind: pick(["created", "updated", "deleted"]), at: iso() };
}

function makeMsg(s: StreamDef, at: number, body?: string): MockMsg {
  s.seq += 1;
  const id = `${at}-${s.seq % 7}`;
  const fields: Record<string, string> = {
    data: body ?? JSON.stringify(payloadFor(s.stream, s.seq)),
    traceparent: trace(),
  };
  return {
    id,
    fields,
    size: sizeOf(fields),
    startAt: null,
    doneAt: null,
    consumer: null,
    deliveries: 0,
    fails: !body && Math.random() < 0.03 ? pick(errorsFor(s.stream)).replace("{n}", String(80000 + s.seq)) : null,
  };
}

function seedDlq(s: StreamDef, nodes: NodeRec[]): MockMsg[] {
  const out: MockMsg[] = [];
  const count = Math.round(rand(40, 80));
  const now = Date.now();
  for (let i = 0; i < count; i++) {
    const at = now - Math.round(rand(60_000, 3 * 86400_000));
    const m = makeMsg(s, at);
    const deliveries = Math.ceil(rand(1, 4));
    m.dead = {
      original_id: m.id,
      stream: s.stream,
      group: s.group,
      deliveries,
      error: pick(errorsFor(s.stream)).replace("{n}", String(80000 + s.seq)),
    };
    m.id = `${at + Math.round(rand(2000, 60_000))}-0`;
    m.consumer = nodes[0]?.node_id ?? null;
    out.push(m);
  }
  return out.sort((a, b) => cmpId(a.id, b.id));
}

function makeTenant(
  id: string,
  name: string,
  enforce: boolean,
  streams: Pick<StreamDef, "stream" | "group" | "inRate" | "period" | "perLane">[],
  nodes: { id: string; lang: string; version: string; connected: boolean; inspect: boolean; streams: string[] }[],
): TenantRec {
  const now = Date.now();
  const nodeRecs: NodeRec[] = nodes.map((n, i) => ({
    node_id: n.id,
    connected: n.connected,
    state: "RUNNING",
    poll_ms: 2000,
    batch_size: 32,
    cpu_pct: rand(12, 40),
    mem_mb: rand(180, 420),
    sdk: { language: n.lang, version: n.version },
    capabilities: { inspect_messages: n.inspect, capture_logs: true, trace_records: true },
    connected_at: n.connected ? iso(now - 3600_000 * (i + 2)) : null,
    last_seen: n.connected ? now : now - 1000 * (190 + i * 40),
    subs: streams
      .filter((s) => n.streams.includes(s.stream))
      .map((s) => ({ stream: s.stream, group: s.group, concurrency: 4, state: "RUNNING" })),
    ackRates: {},
    inFlight: 0,
  }));
  const t: TenantRec = {
    tenant: { id, name, enforce_keys: enforce, created_at: iso(now - 86400_000 * 21) },
    streams: streams.map((s) => ({
      ...s,
      lag: Math.round(s.inRate * 6),
      pending: Math.round(rand(4, 30)),
      phase: Math.random() * Math.PI * 2,
      messages: [],
      dlq: [],
      seq: 0,
      counters: zero(),
    })),
    nodes: nodeRecs,
    policies: new Map(),
    apiKeys: [],
    directives: [],
    samples: new Map(),
    logs: new Map(),
    registered: new Map(),
    metrics: [],
    lastSampleAt: now,
    sinceSample: zero(),
  };
  for (const s of t.streams) {
    s.dlq = seedDlq(s, nodeRecs);
    // a backlog of recent messages, already handled
    for (let ms = now - 120_000; ms < now; ms += rand(250, 900)) {
      const m = makeMsg(s, Math.round(ms));
      m.fails = null;
      m.startAt = idMs(m.id) + 200;
      m.doneAt = idMs(m.id) + Math.round(rand(400, 2500));
      m.consumer = pick(consumersOf(t, s)) ?? null;
      m.deliveries = 1;
      s.messages.push(m);
    }
  }
  return t;
}

function consumersOf(t: TenantRec, s: StreamDef): string[] {
  return t.nodes
    .filter((n) => n.connected && n.subs.some((x) => x.stream === s.stream && x.group === s.group))
    .map((n) => `${s.group}-${n.node_id}`);
}

function seed(): Map<string, TenantRec> {
  const acme = makeTenant(
    "acme",
    "Acme Payments",
    false,
    [
      { stream: "orders", group: "billing", inRate: 120, period: 240, perLane: 9 },
      { stream: "emails:outbound", group: "mailer", inRate: 40, period: 180, perLane: 6 },
      { stream: "payments.settled", group: "ledger", inRate: 70, period: 300, perLane: 11 },
    ],
    [
      // payments.settled is consumed only by nodes that keep payloads private
      { id: "worker-eu-1", lang: "python", version: "0.4.0", connected: true, inspect: false, streams: ["orders", "emails:outbound", "payments.settled"] },
      { id: "worker-eu-2", lang: "python", version: "0.4.0", connected: true, inspect: false, streams: ["orders", "payments.settled"] },
      { id: "worker-us-1", lang: "node", version: "0.1.0", connected: true, inspect: true, streams: ["orders", "emails:outbound"] },
      { id: "worker-us-2", lang: "python", version: "0.3.0", connected: false, inspect: true, streams: ["payments.settled"] },
    ],
  );
  acme.policies.set(key("orders", "billing"), {
    stream: "orders",
    group: "billing",
    enabled: true,
    min_workers: 2,
    max_workers: 16,
    target_lag_per_worker: 200,
    upscale_cooldown_sec: 30,
    downscale_cooldown_sec: 120,
    last_scaled_at: iso(Date.now() - 140_000),
    last_decision: "lag 812 -> 5 workers (was 4)",
  });
  acme.apiKeys = [
    { id: "key_01", label: "production workers", prefix: "rk_3fA9xQ", created_at: iso(Date.now() - 86400_000 * 14), revoked_at: null },
    { id: "key_02", label: "staging", prefix: "rk_Qm7c2a", created_at: iso(Date.now() - 86400_000 * 6), revoked_at: null },
    { id: "key_03", label: null, prefix: "rk_x02Lp9", created_at: iso(Date.now() - 86400_000 * 30), revoked_at: iso(Date.now() - 86400_000 * 9) },
  ];

  const north = makeTenant(
    "t-42",
    "Northwind Cloud",
    true,
    [
      { stream: "events", group: "indexer", inRate: 300, period: 200, perLane: 25 },
      { stream: "thumbnails", group: "render", inRate: 15, period: 150, perLane: 2 },
      { stream: "webhooks", group: "dispatch", inRate: 55, period: 260, perLane: 8 },
    ],
    [
      { id: "nw-a", lang: "python", version: "0.4.0", connected: true, inspect: true, streams: ["events", "thumbnails", "webhooks"] },
      { id: "nw-b", lang: "java", version: "0.1.4", connected: true, inspect: true, streams: ["events", "webhooks"] },
      { id: "nw-c", lang: "python", version: "0.4.0", connected: true, inspect: true, streams: ["events", "thumbnails"] },
    ],
  );
  north.apiKeys = [
    { id: "key_11", label: "cluster-a", prefix: "rk_Nw4tZ1", created_at: iso(Date.now() - 86400_000 * 3), revoked_at: null },
  ];

  const tenants = new Map([
    [acme.tenant.id, acme],
    [north.tenant.id, north],
  ]);
  for (const t of tenants.values()) {
    backfillSamples(t);
    backfillMetrics(t);
    seedDirectives(t);
    seedLogs(t);
  }
  return tenants;
}

function backfillSamples(t: TenantRec) {
  const now = Date.now();
  for (const s of t.streams) {
    const out: Sample[] = [];
    const lanes = t.nodes.flatMap((n) => n.subs).filter((x) => x.stream === s.stream).length * 4;
    for (let ms = now - HISTORY_SEC * 1000; ms <= now; ms += SAMPLE_EVERY_MS) {
      const wave = Math.sin((ms / 1000 / s.period) * Math.PI * 2 + s.phase);
      // A second, slower wave and some noise keep the history from looking synthetic.
      const drift = Math.sin((ms / 1000 / (s.period * 3.7)) * Math.PI * 2);
      const lag = Math.max(0, Math.round(s.inRate * 6 * (1 + 0.8 * wave + 0.35 * drift) * rand(0.9, 1.1)));
      const concurrency = Math.max(lanes - 2, Math.round(lanes + 3 * wave + drift));
      out.push({
        ts: iso(ms),
        lag,
        pending: Math.round(rand(4, 40)),
        concurrency,
        ack_rate_sec: Math.round(s.perLane * concurrency * rand(0.8, 1)),
        ...timingFor(s.perLane, lag, concurrency, Math.round(s.perLane * concurrency)),
      });
    }
    t.samples.set(key(s.stream, s.group), out);
  }
}

/** Two days of tenant metrics, one row a minute, so every window has a previous one. */
function backfillMetrics(t: TenantRec) {
  const now = Date.now();
  const rate = t.streams.reduce((a, s) => a + s.inRate, 0);
  const dlq = t.streams.reduce((a, s) => a + s.dlq.length, 0);
  for (let ms = now - METRICS_HISTORY_SEC * 1000; ms < now - 60_000; ms += 60_000) {
    const day = Math.sin((ms / 86400_000) * Math.PI * 2);
    const hour = Math.sin((ms / 3600_000) * Math.PI * 2 * 1.3);
    const perSec = Math.max(0, rate * (0.75 + 0.2 * day + 0.08 * hour) * rand(0.92, 1.08));
    const handled = perSec * 60;
    const failRate = 0.02 + 0.015 * Math.max(0, hour) + rand(0, 0.006);
    const failed = Math.round(handled * failRate);
    const age = (now - ms) / (METRICS_HISTORY_SEC * 1000);
    const lag = Math.round(rate * 6 * (1 + 0.6 * hour) * rand(0.85, 1.15));
    t.metrics.push({
      ms,
      succeeded: Math.round(handled - failed),
      failed,
      dead_lettered: Math.round(failed * rand(0.001, 0.004)),
      bytes: Math.round(handled * rand(170, 230)),
      lag,
      attempts: Math.round(handled),
      wait_ms: round1((lag / Math.max(0.1, rate)) * 1000),
      work_ms: round1(18 + 6 * (1 + hour)),
      pending: Math.round(rand(20, 90)),
      concurrency: 24 + Math.round(6 * hour),
      dlq_length: Math.max(0, Math.round(dlq * (1 - age * 0.3) * rand(0.97, 1.03))),
    });
  }
}

function seedDirectives(t: TenantRec) {
  const node = t.nodes[0];
  const s = t.streams[0];
  const mk = (ago: number, d: Partial<Directive>): Directive => ({
    id: `dir_${randId(10)}`,
    cmd: "SET_STATE",
    scope: "global",
    target: {},
    params: {},
    reason: null,
    source: "user",
    status: "APPLIED",
    sent_to: node.node_id,
    ack: { status: "APPLIED", current_state: { concurrency: 4, state: "RUNNING" } },
    created_at: iso(Date.now() - ago),
    updated_at: iso(Date.now() - ago + 900),
    ...d,
  });
  const out: Directive[] = [];
  // a long history, so the Activity page has several pages
  for (let i = 0; i < 64; i++) {
    const ago = 140_000 + i * 600_000 + Math.round(rand(0, 200_000));
    const auto = i % 3 !== 2;
    out.push(
      auto
        ? mk(ago, {
            cmd: "SET_CONCURRENCY",
            scope: "stream",
            target: { stream: s.stream, group: s.group },
            params: { concurrency: 3 + (i % 5) },
            source: "autoscaler",
            reason: `AUTOSCALE: lag ${Math.round(rand(200, 1800))} -> ${(3 + (i % 5)) * 3} workers`,
            ack: { status: "APPLIED", current_state: { concurrency: 3 + (i % 5), state: "RUNNING" } },
          })
        : mk(ago, {
            cmd: pick(["MAINTENANCE_ON", "MAINTENANCE_OFF", "SET_POLL_CONFIG"] as const),
            scope: "global",
            params: {},
          }),
    );
  }
  out[3] = mk(900_000, {
    cmd: "EXECUTE_REPLAY",
    scope: "stream",
    target: { stream: s.stream, group: s.group },
    params: { count: 25 },
    ack: { status: "APPLIED", result: { replayed: 25 } },
  });
  out[5] = mk(3_000_000, {
    cmd: "SET_POLL_CONFIG",
    scope: "stream",
    target: { stream: s.stream, group: s.group },
    params: { block_ms: 900000 },
    status: "REJECTED",
    ack: { status: "REJECTED", error: "block_ms must be between 1 and 600000" },
  });
  t.directives = out.sort((a, b) => Date.parse(b.created_at) - Date.parse(a.created_at));
}

function seedLogs(t: TenantRec) {
  const s = t.streams[0];
  const m = s.messages[s.messages.length - 1];
  if (!m) return;
  const base = idMs(m.id);
  t.logs.set(`${s.stream}\u0000${m.id}`, [
    { ts: String(base + 12), level: "INFO", logger: "app.billing", message: "charging order 81723 (EUR 42.10)", consumer: `${t.nodes[0].node_id}`, delivery: "1", traceparent: m.fields.traceparent },
    { ts: String(base + 340), level: "WARNING", logger: "app.billing.gateway", message: "gateway slow, retrying in 250 ms", consumer: `${t.nodes[0].node_id}`, delivery: "1" },
    { ts: String(base + 910), level: "ERROR", logger: "rillkit", message: "handler raised GatewayTimeout", consumer: `${t.nodes[0].node_id}`, delivery: "1", exc_type: "GatewayTimeout", exc: "Traceback (most recent call last):\n  File \"billing.py\", line 88, in charge\n    resp = gateway.post(payload, timeout=0.5)\nGatewayTimeout: no response after 500 ms" },
    { ts: String(base + 31_020), level: "INFO", logger: "app.billing", message: "charged order 81723 on retry", consumer: `${t.nodes[1].node_id}`, delivery: "2" },
  ]);
}

// ---------------------------------------------------------------------------------------------
// Snapshot building

function laneCount(sub: NodeSub, node: NodeRec) {
  return sub.state === "RUNNING" && node.connected && node.state === "RUNNING" ? sub.concurrency : 0;
}

function running(t: TenantRec, s: StreamDef) {
  return t.nodes.some((n) => n.subs.some((x) => x.stream === s.stream && x.group === s.group && laneCount(x, n) > 0));
}

function streamCaps(t: TenantRec, s: { stream: string; group: string }): Capabilities {
  const live = t.nodes.filter(
    (n) => n.connected && n.subs.some((x) => x.stream === s.stream && x.group === s.group),
  );
  return {
    inspect_messages: live.some((n) => n.capabilities.inspect_messages),
    capture_logs: live.some((n) => n.capabilities.capture_logs),
    trace_records: live.some((n) => n.capabilities.trace_records),
  };
}

function nodeStreams(t: TenantRec, n: NodeRec): NodeStream[] {
  return n.subs.map((sub) => {
    const def = t.streams.find((s) => s.stream === sub.stream && s.group === sub.group)!;
    const consumers = Math.max(1, t.nodes.filter((x) => x.subs.some((y) => y.stream === sub.stream)).length);
    const share = (v: number) => (n.connected ? Math.round(v / consumers) : null);
    return {
      stream: sub.stream,
      group: sub.group,
      ...keysFor(t, sub.stream),
      lag: n.connected ? Math.round(def.lag) : null,
      pending: n.connected ? Math.round(def.pending) : null,
      ack_rate_sec: n.connected ? round1(n.ackRates[key(sub.stream, sub.group)] ?? 0) : 0,
      concurrency: n.connected ? sub.concurrency : null,
      state: n.connected ? sub.state : null,
      succeeded_total: share(def.counters.succeeded),
      failed_total: share(def.counters.failed),
      dead_lettered_total: share(def.counters.dead_lettered),
      bytes_total: share(def.counters.bytes),
      dlq_length: n.connected ? def.dlq.length : null,
      attempts_total: share(def.counters.succeeded + def.counters.failed),
      wait_ms_total: share(Math.round(def.lag * 2)),
      work_ms_total: share(Math.round((def.counters.succeeded + def.counters.failed) * (1000 / Math.max(1, def.perLane)))),
    };
  });
}

function buildNode(t: TenantRec, n: NodeRec): Node {
  const active = n.subs.reduce((acc, s) => acc + laneCount(s, n), 0);
  return {
    node_id: n.node_id,
    connected: n.connected,
    state: n.state,
    active: n.connected ? active : 0,
    in_flight: n.connected ? Math.round(n.inFlight) : 0,
    poll_ms: n.connected ? n.poll_ms : null,
    batch_size: n.connected ? n.batch_size : null,
    cpu_pct: n.connected ? round1(n.cpu_pct) : null,
    mem_mb: n.connected ? Math.round(n.mem_mb) : null,
    sdk: n.sdk,
    capabilities: clone(n.capabilities),
    keys: t.tenant.enforce_keys
      ? { layout: "hashtag", prefix: "rk", tenant: t.tenant.id }
      : { layout: "plain", prefix: null, tenant: null },
    connected_at: n.connected_at,
    last_seen: iso(n.last_seen),
    streams: nodeStreams(t, n),
  };
}

function tenantOut(t: TenantRec): Tenant {
  return { ...t.tenant, nodes_online: t.nodes.filter((n) => n.connected).length };
}

function buildSummary(t: TenantRec, s: StreamDef): StreamSummary {
  const subs = t.nodes.flatMap((n) =>
    n.connected
      ? n.subs.filter((x) => x.stream === s.stream && x.group === s.group).map((x) => ({ n, x }))
      : [],
  );
  const consumed = subs.length > 0;
  return {
    stream: s.stream,
    group: s.group,
    ...keysFor(t, s.stream),
    lag: consumed ? Math.round(s.lag) : null,
    pending: consumed ? Math.round(s.pending) : null,
    ack_rate_sec: round1(subs.reduce((acc, { n }) => acc + (n.ackRates[key(s.stream, s.group)] ?? 0), 0)),
    concurrency: subs.reduce((acc, { x }) => acc + x.concurrency, 0),
    nodes: subs.length,
    state: mostRestrictive(subs.map(({ n, x }) => mostRestrictive([n.state, x.state]))),
    policy: clone(t.policies.get(key(s.stream, s.group)) ?? null),
    dlq_length: consumed ? s.dlq.length : null,
    capabilities: streamCaps(t, s),
    created_at: t.registered.get(key(s.stream, s.group)) ?? null,
  };
}

function buildOverview(t: TenantRec): Overview {
  const nodes = [...t.nodes]
    .filter((n) => n.connected || Date.now() - n.last_seen < 600_000)
    .sort((a, b) => Number(b.connected) - Number(a.connected))
    .map((n) => buildNode(t, n));

  const streams = [...t.streams]
    .sort((a, b) => a.stream.localeCompare(b.stream) || a.group.localeCompare(b.group))
    .map((s) => buildSummary(t, s));

  const connected = nodes.filter((n) => n.connected);
  return {
    tenant: tenantOut(t),
    totals: {
      nodes_online: connected.length,
      active_workers: connected.reduce((a, n) => a + n.active, 0),
      in_flight: connected.reduce((a, n) => a + n.in_flight, 0),
      lag: streams.reduce((a, s) => a + (s.lag ?? 0), 0),
      pending: streams.reduce((a, s) => a + (s.pending ?? 0), 0),
      ack_rate_sec: round1(streams.reduce((a, s) => a + s.ack_rate_sec, 0)),
    },
    state: mostRestrictive(connected.map((n) => n.state)),
    nodes,
    streams,
  };
}

function statusOf(m: MockMsg, now: number): { status: MessageStatus; consumer: string | null; deliveries: number | null; idle_ms: number | null } {
  if (m.startAt === null || now < m.startAt) return { status: "waiting", consumer: null, deliveries: null, idle_ms: null };
  if (m.doneAt === null || now < m.doneAt)
    return { status: "pending", consumer: m.consumer, deliveries: Math.max(1, m.deliveries), idle_ms: now - m.startAt };
  return { status: "acked", consumer: null, deliveries: null, idle_ms: null };
}

function messageOut(m: MockMsg, source: "stream" | "dlq", payloads: boolean, now: number): StreamMessage {
  const out: StreamMessage = {
    id: m.id,
    size: m.size,
    fields: payloads ? clone(m.fields) : null,
    traceparent: m.fields.traceparent ?? null,
  };
  if (source === "stream") Object.assign(out, statusOf(m, now));
  else if (m.dead) out.dead_letter = { ...m.dead, error: payloads ? m.dead.error : null };
  return out;
}

// ---------------------------------------------------------------------------------------------
// Simulation

function tick(t: TenantRec, dtSec: number) {
  const now = Date.now();
  for (const s of t.streams) {
    const consumed = t.nodes.some((n) => n.subs.some((x) => x.stream === s.stream && x.group === s.group));
    const wave = Math.sin((now / 1000 / s.period) * Math.PI * 2 + s.phase);
    const arriving = consumed ? Math.max(0, s.inRate * (1 + 0.9 * wave) + rand(-0.1, 0.1) * s.inRate) : 0;
    let capacity = 0;
    for (const n of t.nodes) {
      for (const sub of n.subs) {
        if (sub.stream !== s.stream || sub.group !== s.group) continue;
        capacity += laneCount(sub, n) * s.perLane * rand(0.85, 1.05);
      }
    }
    const processed = Math.min(capacity, s.lag + arriving * dtSec * 0.999);
    s.lag = Math.max(0, s.lag + arriving * dtSec - processed * dtSec);
    s.pending = Math.max(0, Math.min(s.lag, capacity > 0 ? rand(3, 25) + capacity * 0.05 : s.pending));

    // throughput counters follow the simulated rate
    const handled = processed * dtSec;
    const failed = Math.round(handled * rand(0.015, 0.035));
    const add: Counters = {
      succeeded: Math.round(handled) - failed,
      failed,
      dead_lettered: Math.random() < 0.04 ? 1 : 0,
      bytes: Math.round(handled * rand(170, 230)),
    };
    for (const k of Object.keys(add) as (keyof Counters)[]) {
      s.counters[k] += add[k];
      t.sinceSample[k] += add[k];
    }

    // Split the acks between nodes in proportion to their running lanes.
    const lanes = t.nodes.map((n) =>
      n.subs
        .filter((x) => x.stream === s.stream && x.group === s.group)
        .reduce((a, x) => a + laneCount(x, n), 0),
    );
    const total = lanes.reduce((a, b) => a + b, 0);
    t.nodes.forEach((n, i) => {
      n.ackRates[key(s.stream, s.group)] = total > 0 ? (processed * lanes[i]) / total : 0;
    });

    // the visible messages: a few new ones a second while consumed, statuses moving on
    if (consumed) {
      const n = Math.round(rand(1, 4));
      for (let i = 0; i < n; i++) s.messages.push(makeMsg(s, now - Math.round(rand(0, 900))));
      s.messages.sort((a, b) => cmpId(a.id, b.id));
    }
    const isRunning = running(t, s);
    const consumers = consumersOf(t, s);
    for (const m of s.messages) {
      if (m.doneAt !== null) continue;
      if (m.startAt === null && isRunning) {
        m.startAt = Math.max(now, idMs(m.id)) + Math.round(rand(300, 1200));
        m.doneAt = m.startAt + Math.round(rand(800, 3500));
        m.consumer = pick(consumers) ?? null;
        m.deliveries = 1;
      }
    }
    for (const m of s.messages) {
      if (m.fails && m.doneAt !== null && m.doneAt <= now && !m.dead) {
        m.dead = { original_id: m.id, stream: s.stream, group: s.group, deliveries: 3, error: m.fails };
        const copy: MockMsg = { ...clone(m), id: `${now}-${s.dlq.length % 9}` };
        s.dlq.push(copy);
        s.dlq.sort((a, b) => cmpId(a.id, b.id));
      }
    }
    if (s.messages.length > RING) s.messages.splice(0, s.messages.length - RING);
  }

  for (const n of t.nodes) {
    if (!n.connected) continue;
    n.last_seen = now;
    const active = n.subs.reduce((a, s) => a + laneCount(s, n), 0);
    if (n.state === "DRAINING") n.inFlight = Math.max(0, n.inFlight * 0.4 - 1);
    else n.inFlight = active > 0 ? Math.min(active, Math.max(0, active * rand(0.3, 0.95))) : 0;
    const target = active > 0 ? 14 + active * 3.2 : 3;
    n.cpu_pct = Math.min(97, Math.max(1, n.cpu_pct + (target - n.cpu_pct) * 0.3 + rand(-3, 3)));
    n.mem_mb = Math.max(120, n.mem_mb + rand(-4, 4.4));
  }

  if (now - t.lastSampleAt >= SAMPLE_EVERY_MS) {
    t.lastSampleAt = now;
    const ov = buildOverview(t);
    for (const s of ov.streams) {
      if (s.nodes === 0) continue;
      const def = t.streams.find((x) => x.stream === s.stream && x.group === s.group);
      const list = t.samples.get(key(s.stream, s.group)) ?? [];
      list.push({ ts: iso(now), lag: s.lag, pending: s.pending, concurrency: s.concurrency,
                  ack_rate_sec: s.ack_rate_sec,
                  ...timingFor(def?.perLane ?? 10, s.lag ?? 0, s.concurrency, s.ack_rate_sec) });
      while (list.length && Date.parse(list[0].ts) < now - HISTORY_SEC * 1000) list.shift();
      t.samples.set(key(s.stream, s.group), list);
    }
    const timed = ov.streams.flatMap((s) => {
      const def = t.streams.find((x) => x.stream === s.stream && x.group === s.group);
      return def ? [timingFor(def.perLane, s.lag ?? 0, s.concurrency, s.ack_rate_sec)] : [];
    });
    const attempts = timed.reduce((a, x) => a + x.attempts, 0);
    const weighted = (k: "wait_ms" | "work_ms") =>
      attempts ? round1(timed.reduce((a, x) => a + x[k] * x.attempts, 0) / attempts) : null;
    t.metrics.push({
      ms: now,
      ...t.sinceSample,
      lag: ov.totals.lag,
      pending: ov.totals.pending,
      concurrency: ov.streams.reduce((a, s) => a + s.concurrency, 0),
      dlq_length: t.streams.reduce((a, s) => a + s.dlq.length, 0),
      attempts,
      wait_ms: weighted("wait_ms"),
      work_ms: weighted("work_ms"),
    });
    t.sinceSample = zero();
    while (t.metrics.length && t.metrics[0].ms < now - METRICS_HISTORY_SEC * 1000) t.metrics.shift();
  }
}

function totalsOf(rows: MetricRow[], seconds: number): MetricsTotals {
  const sum = (k: keyof Counters) => rows.reduce((a, r) => a + r[k], 0);
  const succeeded = sum("succeeded");
  const failed = sum("failed");
  const ran = succeeded + failed;
  return {
    succeeded,
    failed,
    dead_lettered: sum("dead_lettered"),
    bytes: sum("bytes"),
    success_rate: ran ? Math.round((succeeded / ran) * 10000) / 10000 : null,
    per_sec: Math.round((succeeded / seconds) * 1000) / 1000,
    bytes_per_sec: Math.round((sum("bytes") / seconds) * 10) / 10,
    lag: rows.length ? round1(rows.reduce((a, r) => a + r.lag, 0) / rows.length) : null,
    dlq_length: rows.length ? rows[rows.length - 1].dlq_length : null,
  };
}

function metricsFor(t: TenantRec, windowSec: number): Metrics {
  const now = Date.now();
  const middle = now - windowSec * 1000;
  const start = now - 2 * windowSec * 1000;
  const current = t.metrics.filter((r) => r.ms > middle);
  const previous = t.metrics.filter((r) => r.ms > start && r.ms <= middle);
  const width = (windowSec * 1000) / 60;
  const series: MetricsPoint[] = [];
  for (let i = 0; i < 60; i++) {
    const lo = middle + i * width;
    const hi = lo + width;
    const inside = current.filter((r) => r.ms > lo && r.ms <= hi);
    const last = inside[inside.length - 1];
    const attempts = inside.reduce((a, r) => a + r.attempts, 0);
    const average = (k: "wait_ms" | "work_ms") =>
      attempts ? round1(inside.reduce((a, r) => a + (r[k] ?? 0) * r.attempts, 0) / attempts) : null;
    series.push({
      ts: iso(hi),
      succeeded: inside.reduce((a, r) => a + r.succeeded, 0),
      failed: inside.reduce((a, r) => a + r.failed, 0),
      dead_lettered: inside.reduce((a, r) => a + r.dead_lettered, 0),
      bytes: inside.reduce((a, r) => a + r.bytes, 0),
      lag: last?.lag ?? 0,
      pending: last?.pending ?? 0,
      concurrency: last?.concurrency ?? 0,
      dlq_length: last?.dlq_length ?? 0,
      attempts,
      wait_ms: average("wait_ms"),
      work_ms: average("work_ms"),
      ticks: inside.length,
    });
  }
  return { window_sec: windowSec, series, current: totalsOf(current, windowSec), previous: totalsOf(previous, windowSec) };
}

// ---------------------------------------------------------------------------------------------
// The fake API

type Listener = { tenant: string; handlers: EventHandlers };

const ACTIONS = ["EXECUTE_REPLAY", "DELETE_DLQ", "CREATE_STREAM"];

class MockServer {
  tenants = seed();
  listeners = new Set<Listener>();
  private lastTick = Date.now();

  constructor() {
    setInterval(() => {
      const now = Date.now();
      const dt = (now - this.lastTick) / 1000;
      this.lastTick = now;
      for (const t of this.tenants.values()) {
        tick(t, dt);
        this.autoscale(t);
        this.emitOverview(t);
      }
    }, TICK_MS);
  }

  get(tid: string): TenantRec {
    const t = this.tenants.get(tid);
    if (!t) throw new ApiError("NOT_FOUND", `no tenant '${tid}'`, 404);
    return t;
  }

  stream(t: TenantRec, stream: string, group: string): StreamDef {
    const s = t.streams.find((x) => x.stream === stream && x.group === group);
    if (!s) throw new ApiError("NOT_FOUND", `no stream ${stream}/${group}`, 404);
    return s;
  }

  emitOverview(t: TenantRec) {
    const ov = buildOverview(t);
    for (const l of this.listeners) if (l.tenant === t.tenant.id) l.handlers.onOverview(clone(ov));
  }

  emitDirective(t: TenantRec, d: Directive) {
    for (const l of this.listeners) if (l.tenant === t.tenant.id) l.handlers.onDirective(clone(d));
  }

  /** Nodes a directive is applied on. */
  targets(t: TenantRec, input: DirectiveInput): NodeRec[] {
    const online = t.nodes.filter((n) => n.connected);
    if (input.scope === "node") return online.filter((n) => n.node_id === input.target?.node_id);
    if (input.scope === "stream" && !ACTIONS.includes(input.cmd))
      return online.filter((n) =>
        n.subs.some((s) => s.stream === input.target?.stream && s.group === input.target?.group),
      );
    return online;
  }

  /** The shared directive rules (spec/conformance/directives.json), as the server applies them. */
  validate(input: DirectiveInput) {
    const p = input.params ?? {};
    const bad = (msg: string) => {
      throw new ApiError("INVALID", msg, 422);
    };
    const int = (v: unknown, name: string, top: number) => {
      if (typeof v !== "number" || !Number.isInteger(v)) bad(`${name} must be an integer`);
      if ((v as number) < 1 || (v as number) > top) bad(`${name} must be between 1 and ${top}`);
    };
    const ids = (v: unknown) => {
      if (!Array.isArray(v) || v.length < 1 || v.length > 1000) bad("ids must be a list of 1 to 1000 entry ids");
      for (const id of v as unknown[]) if (typeof id !== "string" || !ENTRY_ID.test(id)) bad(`'${String(id)}' is not a stream entry id like 1718000000000-0`);
    };
    if (input.scope === "stream" && (!input.target?.stream || !input.target?.group))
      bad("scope 'stream' needs target.stream and target.group");
    if (input.scope === "node" && !input.target?.node_id) bad("scope 'node' needs target.node_id");
    if (ACTIONS.includes(input.cmd) && (!input.target?.stream || !input.target?.group))
      bad(`${input.cmd} needs target.stream and target.group`);
    switch (input.cmd) {
      case "SET_STATE":
        if (!["RUNNING", "PAUSED", "DRAINING", "MAINTENANCE"].includes(String(p.state)))
          bad("state must be one of ('RUNNING', 'PAUSED', 'MAINTENANCE', 'DRAINING')");
        break;
      case "SET_CONCURRENCY":
        int(p.concurrency, "concurrency", 256);
        break;
      case "SET_POLL_CONFIG": {
        const block = p.block_ms ?? p.poll_ms;
        if (block === undefined && p.batch_size === undefined) bad("SET_POLL_CONFIG needs block_ms (or poll_ms) or batch_size");
        if (block !== undefined) int(block, "block_ms", 600000);
        if (p.batch_size !== undefined) int(p.batch_size, "batch_size", 10000);
        break;
      }
      case "EXECUTE_REPLAY": {
        const given = ["count", "ids", "set_id"].filter((k) => p[k] !== undefined && p[k] !== null);
        if (given.length > 1) bad("EXECUTE_REPLAY takes one of count, ids or set_id");
        if (p.data !== undefined && !given.includes("ids")) bad("data can only be given with ids");
        if (given.includes("ids")) {
          ids(p.ids);
          const data = (p.data ?? {}) as Record<string, unknown>;
          for (const [k, v] of Object.entries(data)) {
            if (!(p.ids as string[]).includes(k)) bad(`data names '${k}', which is not in ids`);
            if (typeof v !== "string") bad("each data value must be a string of at most 1 MB");
          }
        } else if (!given.includes("set_id")) int(p.count ?? 100, "count", 100000);
        break;
      }
      case "DELETE_DLQ":
        if (p.all === true && p.ids !== undefined) bad("DELETE_DLQ takes ids or all: true, not both");
        if (p.all !== true) {
          if (p.ids === undefined) bad("DELETE_DLQ needs ids or all: true");
          ids(p.ids);
        }
        break;
      case "CREATE_STREAM":
        if (p.start !== undefined && p.start !== "$" && p.start !== "0") bad('start must be "$" (new messages only) or "0" (everything)');
        break;
    }
  }

  createDirective(tid: string, input: DirectiveInput, source: "user" | "autoscaler" = "user"): Directive {
    const t = this.get(tid);
    this.validate(input);
    const online = t.nodes.filter((n) => n.connected);
    const nodes = this.targets(t, input);
    if (online.length === 0 || (input.scope === "node" && nodes.length === 0))
      throw new ApiError("NO_NODE", "no worker is connected to deliver the directive to", 409);

    const now = iso();
    const d: Directive = {
      id: `dir_${randId(16)}`,
      cmd: input.cmd,
      scope: input.scope,
      target: input.target ?? {},
      params: input.params ?? {},
      reason: input.reason ?? null,
      source,
      status: "PENDING",
      sent_to: null,
      ack: null,
      created_at: now,
      updated_at: now,
    };
    t.directives.unshift(d);
    if (t.directives.length > 500) t.directives.length = 500;

    const update = (patch: Partial<Directive>) => {
      Object.assign(d, patch, { updated_at: iso() });
      this.emitDirective(t, d);
    };
    setTimeout(() => this.emitDirective(t, d), 30);
    setTimeout(() => update({ status: "SENT", sent_to: (nodes[0] ?? online[0]).node_id }), 350);
    setTimeout(() => {
      const ack = this.apply(t, d, nodes.length ? nodes : online);
      update({ status: ack.status === "APPLIED" ? "APPLIED" : "FAILED", ack });
      this.emitOverview(t);
    }, 1100);
    return clone(d);
  }

  /** Applies a directive to the fake state and returns the ack. */
  apply(t: TenantRec, d: Directive, nodes: NodeRec[]): NonNullable<Directive["ack"]> {
    const p = d.params;
    const matches = (s: NodeSub) =>
      d.scope !== "stream" || (s.stream === d.target.stream && s.group === d.target.group);
    const setState = (state: WorkerState) => {
      for (const n of nodes) {
        if (d.scope !== "stream") n.state = state;
        for (const s of n.subs) if (matches(s)) s.state = state;
      }
    };
    const def = () => t.streams.find((s) => s.stream === d.target.stream && s.group === d.target.group);
    switch (d.cmd) {
      case "SET_STATE":
        setState(p.state as WorkerState);
        break;
      case "MAINTENANCE_ON":
        setState("MAINTENANCE");
        break;
      case "MAINTENANCE_OFF":
        setState("RUNNING");
        break;
      case "SET_CONCURRENCY":
        for (const n of nodes) for (const s of n.subs) if (matches(s)) s.concurrency = Number(p.concurrency);
        break;
      case "SET_POLL_CONFIG":
        for (const n of nodes) {
          const block = p.block_ms ?? p.poll_ms;
          if (block !== undefined) n.poll_ms = Number(block);
          if (p.batch_size !== undefined) n.batch_size = Number(p.batch_size);
        }
        break;
      case "EXECUTE_REPLAY": {
        const s = def();
        if (!s) return { status: "APPLIED", result: { replayed: 0, missing: (p.ids as string[]) ?? [] } };
        if (p.set_id !== undefined) return { status: "APPLIED", result: { set_id: String(p.set_id) } };
        const chosen = Array.isArray(p.ids) ? (p.ids as string[]) : s.dlq.slice(0, Number(p.count ?? 100)).map((m) => m.id);
        const data = (p.data ?? {}) as Record<string, string>;
        const missing: string[] = [];
        let replayed = 0;
        for (const id of chosen) {
          const i = s.dlq.findIndex((m) => m.id === id);
          if (i < 0) {
            missing.push(id);
            continue;
          }
          const [entry] = s.dlq.splice(i, 1);
          const fresh = makeMsg(s, Date.now(), data[id] ?? entry.fields.data);
          fresh.fields.traceparent = entry.fields.traceparent ?? fresh.fields.traceparent;
          s.messages.push(fresh);
          replayed += 1;
        }
        s.lag += replayed;
        return { status: "APPLIED", result: Array.isArray(p.ids) ? { replayed, missing } : { replayed } };
      }
      case "DELETE_DLQ": {
        const s = def();
        if (!s) return { status: "APPLIED", result: { deleted: 0 } };
        if (p.all === true) {
          const deleted = s.dlq.length;
          s.dlq = [];
          return { status: "APPLIED", result: { deleted } };
        }
        const ids = new Set(p.ids as string[]);
        const before = s.dlq.length;
        s.dlq = s.dlq.filter((m) => !ids.has(m.id));
        return { status: "APPLIED", result: { deleted: before - s.dlq.length } };
      }
      case "CREATE_STREAM": {
        const k = key(d.target.stream!, d.target.group!);
        if (def()) return { status: "APPLIED", result: { created: false } };
        t.streams.push(newStream(d.target.stream!, d.target.group!));
        if (!t.registered.has(k)) t.registered.set(k, iso());
        return { status: "APPLIED", result: { created: true } };
      }
    }
    const first = nodes[0];
    const sub = first.subs.find(matches) ?? first.subs[0];
    return {
      status: "APPLIED",
      current_state: { concurrency: sub?.concurrency ?? 0, state: sub ? mostRestrictive([first.state, sub.state]) : first.state },
    };
  }

  autoscale(t: TenantRec) {
    const now = Date.now();
    for (const p of t.policies.values()) {
      if (!p.enabled) continue;
      const def = t.streams.find((s) => s.stream === p.stream && s.group === p.group);
      if (!def) continue;
      const subs = t.nodes.flatMap((n) => (n.connected ? n.subs.filter((s) => s.stream === p.stream && s.group === p.group) : []));
      if (subs.length === 0) continue;
      // Leave paused, draining or maintenance streams alone.
      if (subs.some((x) => x.state !== "RUNNING")) continue;
      const nodes = subs.length;
      const current = subs[0].concurrency;
      const wanted = Math.min(p.max_workers, Math.max(p.min_workers, Math.ceil(def.lag / p.target_lag_per_worker)));
      const perNode = Math.max(1, Math.min(256, Math.ceil(wanted / nodes)));
      if (perNode === current) continue;
      const cooldown = (perNode > current ? p.upscale_cooldown_sec : p.downscale_cooldown_sec) * 1000;
      if (p.last_scaled_at && now - Date.parse(p.last_scaled_at) < cooldown) continue;
      const decision = `lag ${Math.round(def.lag)} -> ${perNode * nodes} workers (${perNode} per node, was ${current * nodes})`;
      p.last_scaled_at = iso(now);
      p.last_decision = decision;
      try {
        this.createDirective(
          t.tenant.id,
          { cmd: "SET_CONCURRENCY", scope: "stream", target: { stream: p.stream, group: p.group }, params: { concurrency: perNode }, reason: `AUTOSCALE: ${decision}` },
          "autoscaler",
        );
      } catch {
        // No node to deliver to; try again after the cooldown.
      }
    }
  }
}

function newStream(stream: string, group: string): StreamDef {
  return {
    stream,
    group,
    inRate: 0,
    period: 200,
    perLane: 5,
    lag: 0,
    pending: 0,
    phase: 0,
    messages: [],
    dlq: [],
    seq: 0,
    counters: zero(),
  };
}

function connectionFor(t: TenantRec): Connection {
  const control_url = window.location.origin;
  const env: Record<string, string> = {
    RILLKIT_CONTROL: "relay",
    RILLKIT_CONTROL_URL: control_url,
    RILLKIT_API_KEY: "<your API key>",
  };
  if (t.tenant.enforce_keys) env.RILLKIT_TENANT = t.tenant.id;
  return { control_url, env };
}

function logsFor(t: TenantRec, stream: string, msgId: string): LogRecord[] {
  const stored = t.logs.get(`${stream}\u0000${msgId}`);
  if (stored) return stored;
  // Any well-formed id on a known stream gets a short, plausible trail; others have no logs.
  if (!ENTRY_ID.test(msgId) || !t.streams.some((s) => s.stream === stream)) return [];
  const ms = idMs(msgId) || Date.now();
  const node = t.nodes[0].node_id;
  const s = t.streams.find((x) => x.stream === stream);
  const dead = s?.dlq.find((m) => m.dead?.original_id === msgId)?.dead;
  if (dead) {
    const out: LogRecord[] = [];
    for (let i = 1; i <= dead.deliveries; i++) {
      out.push({ ts: String(ms + i * 5000), level: "INFO", logger: `app.${stream.split(/[:.]/)[0]}`, message: `handling ${msgId}`, consumer: node, delivery: String(i) });
      out.push({ ts: String(ms + i * 5000 + 60), level: "ERROR", logger: "rillkit", message: `handler raised ${(dead.error ?? "Error").split(":")[0].split(" ")[0]}`, consumer: node, delivery: String(i), exc_type: (dead.error ?? "Error").split(":")[0].split(" ")[0], exc: `Traceback (most recent call last):\n  File "handlers.py", line 42, in handle\n    process(body)\n${dead.error ?? "Error"}` });
    }
    return out;
  }
  return [
    { ts: String(ms + 8), level: "INFO", logger: `app.${stream.split(/[:.]/)[0]}`, message: `handling ${msgId}`, consumer: node, delivery: "1" },
    { ts: String(ms + 64), level: "DEBUG", logger: "rillkit.consumer", message: "acked after 56 ms", consumer: node, delivery: "1" },
  ];
}

export function createMockApi(): Api {
  const server = new MockServer();
  const latency = () => sleep(rand(80, 260));
  const online = (t: TenantRec) => {
    if (!t.nodes.some((n) => n.connected)) throw new ApiError("NO_NODE", "no worker is connected", 409);
  };

  return {
    async checkToken(token) {
      await latency();
      if (!token.trim()) throw new ApiError("UNAUTHORIZED", "a valid admin token is required", 401);
    },
    async health() {
      await latency();
      return { status: "ok", version: "0.1.0-mock", replica_id: "mock-1", leader: true };
    },
    async listTenants() {
      await latency();
      return [...server.tenants.values()].map(tenantOut);
    },
    async createTenant(input) {
      await latency();
      if (!/^[a-z0-9][a-z0-9-]{0,39}$/.test(input.id))
        throw new ApiError("INVALID", "tenant ids are 1 to 40 lowercase letters, digits and '-'", 422);
      if (server.tenants.has(input.id)) throw new ApiError("CONFLICT", `tenant '${input.id}' already exists`, 409);
      if (!input.name.trim()) throw new ApiError("INVALID", "name: String should have at least 1 character", 422);
      const t = makeTenant(input.id, input.name.trim(), Boolean(input.enforce_keys), [], []);
      t.tenant.created_at = iso();
      server.tenants.set(input.id, t);
      return tenantOut(t);
    },
    async getTenant(tid) {
      await latency();
      return tenantOut(server.get(tid));
    },
    async listApiKeys(tid) {
      await latency();
      return clone(server.get(tid).apiKeys);
    },
    async createApiKey(tid, label) {
      await latency();
      const t = server.get(tid);
      const secret = `rk_${randId(43)}`;
      const k: NewApiKey = {
        id: `key_${randId(12)}`,
        label: label?.trim() || null,
        prefix: secret.slice(0, 10),
        created_at: iso(),
        revoked_at: null,
        key: secret,
      };
      const { key: _secret, ...stored } = k;
      void _secret;
      t.apiKeys.push(stored);
      return k;
    },
    async revokeApiKey(tid, id) {
      await latency();
      const k = server.get(tid).apiKeys.find((x) => x.id === id);
      if (!k) throw new ApiError("NOT_FOUND", `no API key '${id}'`, 404);
      k.revoked_at ??= iso();
      return clone(k);
    },
    async getConnection(tid) {
      await latency();
      return connectionFor(server.get(tid));
    },
    async getOverview(tid) {
      await latency();
      return buildOverview(server.get(tid));
    },
    async getSamples(tid, stream, group, windowSec = 900) {
      await latency();
      const list = server.get(tid).samples.get(key(stream, group)) ?? [];
      const from = Date.now() - windowSec * 1000;
      return clone(list.filter((s) => Date.parse(s.ts) >= from));
    },
    async createDirective(tid, input) {
      await latency();
      return server.createDirective(tid, input);
    },
    async listDirectives(tid, q = {}) {
      await latency();
      const limit = q.limit ?? 25;
      const offset = q.offset ?? 0;
      const all = server.get(tid).directives.filter((d) => !q.source || d.source === q.source);
      return { items: clone(all.slice(offset, offset + limit)), total: all.length, limit, offset };
    },
    async createStream(tid, input) {
      await latency();
      const t = server.get(tid);
      if (!input.stream.trim() || !input.group.trim()) throw new ApiError("INVALID", "stream and group are required", 422);
      if (input.start !== undefined && input.start !== "$" && input.start !== "0")
        throw new ApiError("INVALID", "start: Input should be '$' or '0'", 422);
      const k = key(input.stream, input.group);
      if (!t.registered.has(k)) t.registered.set(k, iso());
      if (!t.streams.some((s) => s.stream === input.stream && s.group === input.group))
        t.streams.push(newStream(input.stream, input.group));
      let directive: Directive | null = null;
      if (t.nodes.some((n) => n.connected))
        directive = server.createDirective(tid, {
          cmd: "CREATE_STREAM",
          scope: "stream",
          target: { stream: input.stream, group: input.group },
          params: { start: input.start ?? "$" },
        });
      server.emitOverview(t);
      return { stream: buildSummary(t, server.stream(t, input.stream, input.group)), directive };
    },
    async listMessages(tid, stream, group, q = {}): Promise<MessagePage> {
      await latency();
      const t = server.get(tid);
      online(t);
      const s = server.stream(t, stream, group);
      const source = q.source ?? "stream";
      const count = q.count ?? 50;
      if (count < 1 || count > 200) throw new ApiError("INVALID", "count: Input should be less than or equal to 200", 422);
      if (q.before && q.after) throw new ApiError("INVALID", "give before or after, not both", 422);
      const now = Date.now();
      const all = [...(source === "dlq" ? s.dlq : s.messages)].sort((a, b) => cmpId(b.id, a.id)); // newest first
      let rows: MockMsg[];
      let moreAfter = false;
      if (q.after) {
        const newer = all.filter((m) => cmpId(m.id, q.after!) > 0).reverse(); // oldest first
        rows = newer.slice(0, count).reverse();
        moreAfter = newer.length > count;
      } else {
        const older = q.before ? all.filter((m) => cmpId(m.id, q.before!) < 0) : all;
        rows = older.slice(0, count);
      }
      const payloads = streamCaps(t, s).inspect_messages;
      const node = t.nodes.find((n) => n.connected && n.subs.some((x) => x.stream === stream)) ?? t.nodes.find((n) => n.connected)!;
      const more = !q.after && rows.length === count && all.indexOf(rows[rows.length - 1]) < all.length - 1;
      return {
        messages: rows.map((m) => messageOut(m, source, payloads, now)),
        next_before: more ? rows[rows.length - 1].id : null,
        more_after: moreAfter,
        total: all.length,
        payloads,
        node_id: node.node_id,
        as_of: iso(now),
      };
    },
    async getMessage(tid, stream, group, id, source = "stream") {
      await latency();
      const t = server.get(tid);
      online(t);
      if (!ENTRY_ID.test(id)) throw new ApiError("INVALID", `'${id}' is not a message id like 1718000000000-0`, 422);
      const s = server.stream(t, stream, group);
      const m = (source === "dlq" ? s.dlq : s.messages).find((x) => x.id === id);
      if (!m) throw new ApiError("NOT_FOUND", `no message ${id} in ${stream}${source === "dlq" ? " (DLQ)" : ""}`, 404);
      return messageOut(m, source, streamCaps(t, s).inspect_messages, Date.now());
    },
    async getMetrics(tid, windowSec = 3600) {
      await latency();
      if (windowSec < 300 || windowSec > 86400) throw new ApiError("INVALID", "window_sec must be between 300 and 86400", 422);
      return metricsFor(server.get(tid), windowSec);
    },
    async listPolicies(tid) {
      await latency();
      return clone([...server.get(tid).policies.values()]);
    },
    async putPolicy(tid, stream, group, input: PolicyInput) {
      await latency();
      const t = server.get(tid);
      if (input.min_workers < 0) throw new ApiError("INVALID", "min_workers: Input should be greater than or equal to 0", 422);
      if (input.max_workers < input.min_workers || input.max_workers > 100000)
        throw new ApiError("INVALID", "max_workers must be at least min_workers", 422);
      if (!(input.target_lag_per_worker > 0)) throw new ApiError("INVALID", "target_lag_per_worker: Input should be greater than 0", 422);
      const prev = t.policies.get(key(stream, group));
      const p: Policy = {
        stream,
        group,
        ...input,
        last_scaled_at: prev?.last_scaled_at ?? null,
        last_decision: prev?.last_decision ?? null,
      };
      t.policies.set(key(stream, group), p);
      return clone(p);
    },
    async deletePolicy(tid, stream, group) {
      await latency();
      const t = server.get(tid);
      if (!t.policies.delete(key(stream, group))) throw new ApiError("NOT_FOUND", `no policy for ${stream}/${group}`, 404);
    },
    async getLogs(tid, stream, msgId) {
      await sleep(rand(300, 700));
      const t = server.get(tid);
      online(t);
      return { records: clone(logsFor(t, stream, msgId)) };
    },
    async getTrace(tid, traceId) {
      await sleep(rand(250, 600));
      const t = server.get(tid);
      online(t);
      const byMessage = new Map<string, TraceRecord>();
      for (const s of t.streams) {
        for (const m of [...s.messages, ...s.dlq]) {
          const tp = m.fields.traceparent;
          if (!tp || tp.split("-")[1] !== traceId) continue;
          const original = m.dead?.original_id ?? m.id;
          const id = `${s.stream}\u0000${original}`;
          const outcome: TraceRecord["outcome"] = m.dead ? "dead_letter" : m.fails ? "error" : "ok";
          const before = byMessage.get(id);
          // The DLQ copy of a message is the same attempt: keep the one with the final outcome.
          if (before && before.outcome === "dead_letter") continue;
          const started = m.startAt ?? m.doneAt ?? idMs(original);
          const work = Math.round(1000 / Math.max(1, s.perLane));
          byMessage.set(id, {
            trace_id: traceId,
            span_id: spanFor(`${s.stream}:${original}:${m.consumer ?? ""}`),
            parent_span_id: tp.split("-")[2] ?? spanFor(original),
            stream: s.stream,
            group: s.group,
            message_id: original,
            consumer: m.consumer ?? "mock-consumer",
            node_id: t.nodes[0]?.node_id ?? "node-1",
            delivery: Math.max(1, m.deliveries),
            published_at: idMs(original),
            started_at: started,
            finished_at: started + work,
            outcome,
          });
        }
      }
      const records = [...byMessage.values()].sort((a, b) => a.started_at - b.started_at);
      return clone({ trace_id: traceId, records, node_id: t.nodes[0]?.node_id ?? "node-1" });
    },
    subscribe(tid, handlers) {
      const listener: Listener = { tenant: tid, handlers };
      const open = setTimeout(() => {
        server.listeners.add(listener);
        handlers.onOpen?.(false);
        const t = server.tenants.get(tid);
        if (t) handlers.onOverview(buildOverview(t));
      }, 150);
      return () => {
        clearTimeout(open);
        server.listeners.delete(listener);
      };
    },
  };
}
