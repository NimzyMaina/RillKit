// SSE fan-out client: N dashboard streams against one server, no dependencies (Node 18+).
//
//   API=http://localhost:8091/api TOKEN=... CLIENTS=200 DURATION=30 node bench/sse.mjs
//
// Each client gets a single-use ticket, opens the events stream, and counts overview events.
// Run it while k6-control.js keeps a fleet reporting telemetry; compare CPU at 50 vs 200 clients.

import http from "node:http";

const API = process.env.API || "http://localhost:8091/api";
const TOKEN = process.env.TOKEN || "bench-admin-token-0123456789";
const CLIENTS = Number(process.env.CLIENTS || 50);
const DURATION_S = Number(process.env.DURATION || 30);

const stats = { connected: 0, events: 0, errors: 0, first_ms: [], dropped: 0 };
const started = Date.now();

async function ticket() {
  const response = await fetch(`${API}/tenants/bench/events/ticket`, {
    method: "POST",
    headers: { Authorization: `Bearer ${TOKEN}` },
  });
  if (!response.ok) throw new Error(`ticket HTTP ${response.status}`);
  return (await response.json()).ticket;
}

async function subscribe() {
  try {
    const t = await ticket();
    await new Promise((resolve) => {
      let finished = false;
      let stopping = false;
      let buffer = "";
      let seen = false;
      const done = () => {
        if (!finished) {
          finished = true;
          resolve();
        }
      };
      const req = http.get(`${API}/tenants/bench/events?ticket=${encodeURIComponent(t)}`,
        (res) => {
          if (res.statusCode !== 200) {
            stats.errors += 1;
            res.resume();
            done();
            return;
          }
          stats.connected += 1;
          res.setEncoding("utf8");
          res.on("data", (chunk) => {
            buffer += chunk;
            let cut;
            while ((cut = buffer.indexOf("\n\n")) >= 0) {
              const block = buffer.slice(0, cut);
              buffer = buffer.slice(cut + 2);
              if (block.startsWith("event: overview")) {
                stats.events += 1;
                if (!seen) {
                  seen = true;
                  stats.first_ms.push(Date.now() - started);
                }
              }
            }
          });
          res.on("end", done);
          res.on("error", () => {
            if (!stopping) stats.errors += 1;
            done();
          });
        });
      req.on("error", () => {
        if (!stopping) stats.errors += 1;
        done();
      });
      setTimeout(() => { stopping = true; req.destroy(); done(); }, DURATION_S * 1000);
    });
  } catch {
    stats.errors += 1;
  }
}

await Promise.all(Array.from({ length: CLIENTS }, () => subscribe()));
const first = stats.first_ms.sort((a, b) => a - b);
console.log(JSON.stringify({
  clients: CLIENTS,
  duration_s: DURATION_S,
  connected: stats.connected,
  errors: stats.errors,
  events: stats.events,
  events_per_client: stats.connected ? +(stats.events / stats.connected).toFixed(1) : 0,
  first_ms_min: first[0] ?? null,
  first_ms_max: first[first.length - 1] ?? null,
}));
