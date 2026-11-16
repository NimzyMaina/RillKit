import http from "k6/http";
import ws from "k6/ws";
import { check, sleep } from "k6";
import { Trend, Rate } from "k6/metrics";

const API = __ENV.API || "http://localhost:8091/api";
const WS = __ENV.WS || "ws://localhost:8091/relay/v1";
const KEY = __ENV.KEY;
const TOKEN = __ENV.TOKEN;
const RELAYS = Number(__ENV.RELAYS || 100);
const DURATION = __ENV.DURATION || "60s";
const TELEMETRY_MS = Number(__ENV.TELEMETRY_MS || 1000);

const helloMs = new Trend("relay_hello_ms", true);
const directiveRoundtrip = new Trend("directive_roundtrip_ms", true);
const welcomeSeen = new Rate("relay_welcome");

export const options = {
  scenarios: {
    relays: {
      executor: "constant-vus",
      vus: RELAYS,
      duration: DURATION,
      exec: "relay",
    },
    api: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.API_RATE || 30),
      timeUnit: "1s",
      duration: "50s",
      preAllocatedVUs: 30,
      maxVUs: 100,
      startTime: "3s",
      exec: "api",
    },
    directives: {
      executor: "constant-vus",
      vus: 1,
      duration: "50s",
      startTime: "5s",
      exec: "directive",
    },
  },
  thresholds: {
    relay_welcome: ["rate>0.99"],
    relay_hello_ms: ["p(99)<3000"],
    directive_roundtrip_ms: ["p(99)<3000"],
    "http_req_duration{name:overview}": ["p(95)<400"],
    http_req_failed: ["rate<0.02"],
  },
  discardResponseBodies: false,
  summaryTrendStats: ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"],
};

const HEADERS = { Authorization: `Bearer ${TOKEN}` };

export function relay() {
  const node = `bench-${__VU}`;
  const helloAt = Date.now();
  const keys = { layout: "plain", prefix: null, tenant: null };
  const hello = {
    type: "HELLO", protocol_version: 1, node_id: node, keys,
    sdk: { language: "k6", version: "0" },
    capabilities: { inspect_messages: false, capture_logs: false, trace_records: false },
  };
  const telemetry = () => JSON.stringify({
    type: "TELEMETRY_REPORT", node_id: node, timestamp: Math.floor(Date.now() / 1000), keys,
    workers: { active: 1, in_flight: 0, state: "RUNNING", poll_ms: 1000, batch_size: 10,
               cpu_pct: 10, mem_mb: 100 },
    streams: [],
  });

  const res = ws.connect(WS, { headers: { Authorization: `Bearer ${KEY}` } }, (socket) => {
    let timer = null;
    socket.on("open", () => socket.send(JSON.stringify(hello)));
    socket.on("message", (raw) => {
      let frame;
      try { frame = JSON.parse(raw); } catch { return; }
      if (frame.type === "WELCOME") {
        helloMs.add(Date.now() - helloAt);
        welcomeSeen.add(true);
        timer = socket.setInterval(() => socket.send(telemetry()), TELEMETRY_MS);
      } else if (frame.type === "DIRECTIVE") {
        socket.send(JSON.stringify({
          type: "DIRECTIVE_ACK", directive_id: frame.directive_id, node_id: node,
          status: "APPLIED", current_state: { concurrency: 1, state: "RUNNING" },
        }));
      } else if (frame.type === "ERROR") {
        welcomeSeen.add(false);
      }
    });
    socket.on("close", () => { if (timer) socket.clearInterval(timer); });
    socket.setTimeout(() => socket.close(), Number(__ENV.HOLD_MS || 65000));
  });
  check(res, { "ws upgrade": (r) => r && r.status === 101 });
}

export function api() {
  const paths = [
    ["overview", `${API}/tenants/bench/overview`],
    ["directives", `${API}/tenants/bench/directives?limit=25`],
    ["metrics", `${API}/tenants/bench/metrics?window_sec=3600`],
  ];
  const [name, url] = paths[__ITER % paths.length];
  const r = http.get(url, { headers: HEADERS, tags: { name } });
  check(r, { "api 200": (x) => x.status === 200 });
}

export function directive() {
  const before = Date.now();
  const body = JSON.stringify({
    cmd: "SET_CONCURRENCY", scope: "global", params: { concurrency: 1 },
  });
  const created = http.post(`${API}/tenants/bench/directives`, body, {
    headers: { ...HEADERS, "Content-Type": "application/json" },
    tags: { name: "directive-create" },
  });
  if (created.status !== 201) {
    sleep(0.5);  // no nodes yet, or the last relay just left
    return;
  }
  const id = created.json("id");
  for (let i = 0; i < 40; i++) {
    sleep(0.1);
    const page = http.get(`${API}/tenants/bench/directives?limit=1`, {
      headers: HEADERS, tags: { name: "directive-poll" },
    });
    const latest = page.json("items")[0];
    if (latest && latest.id === id &&
        ["APPLIED", "FAILED", "UNDELIVERABLE", "DUPLICATE", "REJECTED"].includes(latest.status)) {
      break;
    }
  }
  directiveRoundtrip.add(Date.now() - before);
  sleep(1);
}
