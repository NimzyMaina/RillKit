# Benchmarks

Reproducible load tests for the Python SDK and `rillkit-server`, kept out of the normal test
suite because they take minutes and need a machine of their own.

The numbers below are from one 11 vCPU / 8 GB Apple Silicon laptop running Docker Redis 7, the
Python SDK on the host, and the dashboard demo alongside. Treat them as shapes and ratios, not
as sizing data for production hardware.

## What is measured

```text
k6 ──HTTP──▶ gateway.py ──Broker.publish──▶ Redis ──Broker consume──▶ worker.py ──RPUSH──▶ sink
                                                  │
k6 ──WebSocket/HTTP──▶ rillkit-server ◀──Relay── simulated workers (k6-control.js)
```

- **Data plane:** end-to-end latency from `sent_ms` (k6) to `done_ms` (the worker's handler),
  calculated from the sink; throughput from the gateway's publish counter; loss and duplicates
  from unique sequence ids.
- **Producer path:** no consumers; the gateway's own publish rate and enqueue latency.
- **Control plane:** real Relay sessions, telemetry, directives over `/api/relay`, dashboard API
  requests, HELLO→WELCOME and directive→ACK latency.
- The worker also refreshes its heartbeat and every completion is an `RPUSH`, so consumer
  numbers include that bookkeeping. A production worker with heartbeats coalesced and no sink
  would do better.

k6 has no Redis client. `gateway.py` is a thin threaded HTTP server that calls
`Broker.publish`, so k6 measures RillKit's publish path plus one HTTP hop. For control-plane
tests k6 talks to the real server directly.

## Prerequisites

- Docker, `k6` (v0.5x or newer), and the SDK development environment:

```bash
cd sdks/python && uv sync --group dev
```

- Start a throwaway Redis on port 6380. Never point this at the demo or a real database:

```bash
docker run -d --rm --name bench-redis -p 6380:6379 redis:7 \
  redis-server --save '' --appendonly no --maxclients 20000
```

- Run the scripts with the SDK environment's Python, from the repository root:

```bash
sdks/python/.venv/bin/python bench/run.py curve
# or: uv run --project sdks/python python bench/run.py curve
```

## Data-plane scenarios

`run.py` starts a gateway and workers, flushes the bench Redis between stages, and prints one
JSON line per stage. Results are also written to `k6-<stage>.json`.

```bash
python bench/run.py curve       # 200, 500, 1000, 2000, 4000 msg/s, 2 workers × 4 lanes
python bench/run.py tune        # lanes/batch/block combinations at 800 msg/s
python bench/run.py features    # baseline, control, traces, logs, idempotent at 1500 msg/s
python bench/run.py failures    # 5 % handler failures, retries and the DLQ
python bench/run.py producer    # publish-only capacity, no consumers
python bench/run.py recovery    # SIGKILL one worker mid-load
```

Payload size: `1 KB` by default. For a sweep:

```bash
python -c "import run
for pad in (200, 1024, 10240):
    run.stage(1000, '20s', 2, pad=pad, name=f'payload-{pad}')"
```

Cluster mode (one stream still lives on one shard):

```bash
../../scripts/test-cluster.sh valkey 7101
BENCH_CLUSTER=127.0.0.1:7101 python -c "import run; run.stage(2000, '40s', 2)"
../../scripts/test-cluster.sh stop valkey
```

## Control-plane scenarios

`k6-control.js` opens `RELAYS` WebSocket sessions, reports telemetry every `TELEMETRY_MS`,
acknowledges directives, and drives the dashboard API and a directive round-trip. It expects a
server with the `bench` tenant and API key (see `scale.py` for the bootstrap environment).

```bash
# k6/ws, not the newer k6/websockets constructor: the latter dropped events with a params arg
KEY=rk_bench_local_only_000000000000000000 \
TOKEN=bench-admin-token-0123456789 RELAYS=1000 DURATION=60s TELEMETRY_MS=1000 \
k6 run bench/k6-control.js
```

`scale.py` starts a throwaway Postgres and `rillkit-server:latest` (build with
`docker build -f server/Dockerfile -t rillkit-server:latest .`), creates an autoscaling policy,
runs load, and prints the concurrency/lag timeline.

`pause.py` exercises pause, resume and drain through the `valkey` control plane. `noise.py`
runs a hot and a light stream on one Redis to show the lack of performance isolation.

`failover.py` starts three server replicas against the same Postgres, pins the worker to the
third, loads the stream, kills whichever replica holds the autoscaler lock, and records the
election and concurrency/lag. It assumes Postgres on 5440 and the bench Redis.

SSE fan-out does not use k6 (it has no streaming client). `sse.mjs` opens N dashboard streams with
single-use tickets:

```bash
# while k6-control.js is running with, say, 100 relays at 1 s telemetry:
API=http://localhost:8091/api TOKEN=bench-admin-token-0123456789 \
  CLIENTS=200 DURATION=30 node bench/sse.mjs
```

## Results (this laptop, November 2026)

Data plane, 1 KB, 2 workers × 4 lanes, batch 50, block 1000 ms:

| Target | Achieved | p50 | p95 | p99 |
|---|---|---|---|---|
| 200/s | 194 | 1 ms | 2 ms | 9 ms |
| 500/s | 485 | 0 ms | 1 ms | 14 ms |
| 1,000/s | 964 | 1 ms | 52 ms | 191 ms |
| 2,000/s | 1,929 | 2 ms | 77 ms | 211 ms |
| 4,000/s | 2,217 | 4,888 ms | 6,475 ms | 6,560 ms |

After coalescing heartbeats (one write per 5 s instead of per message): 2,000/s p95 **3 ms**,
p99 **28 ms**; 4,000/s p50 **137 ms**, p95 768 ms, p99 842 ms, effective **2.94k/s**. No loss
or duplicates at any point.

Valkey at 2,000/s: p95 **36 ms**, p99 **120 ms**.

Publishing a batch (`publish_many`, 1,000 × 1 KB, one pipeline): **52,592 msg/s** versus 2,763
msg/s with 1,000 individual `publish` calls (19×), with `TraceContext` still giving every entry
its own span. Run it with `bench/batch_bench.py`.

Cluster (Valkey, one stream): 1,000/s p95 **2 ms**, p99 11 ms; 2,000/s p95 **162 ms**, p99
276 ms. A single stream cannot use more than its shard; add streams for more shards.

Tuning at 800/s, one worker: 1 lane cannot keep up (p95 4.7 s); **4 lanes / batch 50** is best
(p95 5 ms, p99 32 ms); 8 lanes is worse at the tail. Feature cost at 1,500/s (p99): baseline
27 ms, traces 49 ms, logs 158 ms, idempotency 134 ms.

Failure/recovery: 5 % handler failures at 800/s gave p95 ≈ 2 s (retry wait), 4 DLQ entries of
32k, no loss/duplicates; killing a worker left 0 lost, 0 duplicated. DLQ replay: 4,976 entries
in 1.6 s (≈3,100/s).

Control plane: 100 relays hello p95 153 ms / directive p95 130 ms / overview 20 ms; 500 relays
566 / 293 / 134 ms; 1,000 relays 1.1 s / 0.57 s / **218 ms p95** (63 % of a core, 103 MB), no
HTTP failures. Telemetry was every **1 s**, five times the default.

Multi-tenant noise: a light stream's p99 went **7 ms → 291 ms** next to a 2.5k/s neighbour;
there is no per-tenant performance isolation.

SSE fan-out, 100 relays at 1 s telemetry (100 nodes in the overview), events arrive once a second:

| Tabs | Events in 16 s | Errors | First event | Server CPU peak |
|---|---|---|---|---|
| 50 | 850 (17 per tab) | 0 | 188–281 ms | ~48 % of a core |
| 200 | 3,014 (15 per tab) | 0 | 838–1,634 ms | ~1 core |

The full overview is rendered and encoded **once per tenant per tick** (not once per tab), but
200 tabs still mean 200 writes of the whole 100-node overview every second. The next step for
much larger fleets is a compact/delta overview or a slower live cadence, not more JSON work.

Autoscaler failover: killed the leader while a worker sustained 2k/s; the standby was leader in
**1.6 s**, concurrency stayed at **8** through the failover (no collapse to the minimum), and
downscaled only after the load stopped. The new leader seeds arrival/capacity from the samples
table, so it does not relearn from zero.

Pause at 500/s: **1.15 s** to stop reading (bounded by `block_ms`), 7 in-flight messages
finished, resume 60 ms, drain removed every consumer.

Autoscaler (2k/s, min 1, max 8, target lag 300): rate-aware sizing settles at **4 lanes** with
lag 0, then scales to 1 only after the load stops — no 1↔8 oscillation.

## Caveats

- One host; Docker and the host share CPU. The gateway is a proxy and the sink is one Redis
  `RPUSH` per message.
- A single stream on a cluster uses one shard.
- Not covered: multi-hour leaks, thousands of dashboard tabs, and production network conditions.
