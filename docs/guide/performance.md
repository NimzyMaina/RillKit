# Performance

These numbers come from the load tests in [`bench/`](https://github.com/NimzyMaina/RillKit/tree/main/bench),
run on one 11 vCPU / 8 GB Apple Silicon laptop in November 2026: Redis 7 in Docker, the Python
SDK on the host, and the demo fleet alongside. Treat them as shapes and ratios, not as sizing
data for your hardware. The full method, scripts and raw stages are in
[`bench/README.md`](https://github.com/NimzyMaina/RillKit/blob/main/bench/README.md).

## Data plane

1 KB payloads, two workers with four lanes each, batch 50, `block_ms` 1000:

| Target | Achieved | p50 | p95 | p99 |
|---|---|---|---|---|
| 200/s | 194 | 1 ms | 2 ms | 9 ms |
| 500/s | 485 | 0 ms | 1 ms | 14 ms |
| 1,000/s | 964 | 1 ms | 52 ms | 191 ms |
| 2,000/s | 1,929 | 2 ms | 77 ms | 211 ms |
| 4,000/s | 2,217 | 4,888 ms | 6,475 ms | 6,560 ms |

A single worker does not keep up at 4,000/s; add lanes or workers. Latency is end to end, from
the publisher's send to the end of the handler, and includes the heartbeat write and a sink
`RPUSH` that a real handler may not have.

**Heartbeat coalescing matters.** Refreshing the heartbeat at most once every 5 seconds, instead
of on every message, moved 2,000/s from p95 77 ms / p99 211 ms to p95 **3 ms** / p99 **28 ms**.
At 4,000/s it changed p50 4,888 ms → 137 ms and effective throughput 2.2k/s → **2.94k/s**, with
no loss or duplicates. This is why the default is coalesced.

Valkey at 2,000/s: p95 **36 ms**, p99 **120 ms**.

**Batching publishes.** `publish_many` with 1,000 × 1 KB bodies in one pipeline reached
**52,592 msg/s**, against 2,763 msg/s for 1,000 individual `publish` calls (19×). Each entry
still gets its own trace span; see [Publishing](/guide/publishing#publishing-many-at-once).

## Cluster

On a Valkey cluster, one stream still lives on one shard: at 1,000/s p95 **2 ms**, p99 11 ms;
at 2,000/s p95 **162 ms**, p99 276 ms. Add streams to use more shards; raising lanes on one
stream will not spread it.

## Tuning

At 800/s on one worker, one lane cannot keep up (p95 4.7 s). **Four lanes with batch 50** was
best (p95 5 ms, p99 32 ms); eight lanes was worse at the tail. Lane count is a per-subscription
setting (`concurrency=`) and can be changed live by the control plane.

The cost of the optional features at 1,500/s (p99, against a 27 ms baseline):

| Feature | p99 |
|---|---|
| Baseline | 27 ms |
| Trace records | 49 ms |
| Idempotency middleware | 134 ms |
| Log capture | 158 ms |

Log capture is the expensive one: it stores whatever the handler logged for every attempt. Turn
it on when you need it, not permanently.

## Failures and recovery

- 5 % handler failures at 800/s: p95 ≈ 2 s (the retry wait), 4 dead letters out of 32k, no loss
  or duplicates.
- `SIGKILL` on one worker mid-load: 0 lost, 0 duplicated. Pending messages were claimed again.
- DLQ replay: 4,976 entries in 1.6 s (≈3,100/s).
- Pause at 500/s: 1.15 s to stop reading (bounded by `block_ms`), 7 in-flight messages finished,
  resume 60 ms, drain removed every consumer.

## Control plane

Real Relay sessions with telemetry every **1 s**, five times the default:

| Relays | HELLO p95 | Directive p95 | Overview p95 |
|---|---|---|---|
| 100 | 153 ms | 130 ms | 20 ms |
| 500 | 566 ms | 293 ms | 134 ms |
| 1,000 | 1.1 s | 570 ms | **218 ms** |

At 1,000 relays the server used 63 % of a core and 103 MB, with no HTTP failures.

**Autoscaling** (2,000/s, min 1, max 8, target lag 300): rate-aware sizing settled at **4 lanes**
with lag 0 and scaled back to 1 only after the load stopped, with no 1↔8 oscillation. Killing
the autoscaler leader elected a standby in **1.6 s**; concurrency stayed at 8 through the
failover, and the new leader seeded its estimate from recent samples instead of relearning from
zero.

**Dashboard streams** (SSE), 100 relays at 1 s telemetry, 100 nodes in the overview:

| Tabs | Events in 16 s | Errors | First event | Server CPU peak |
|---|---|---|---|---|
| 50 | 850 (17 per tab) | 0 | 188–281 ms | ~48 % of a core |
| 200 | 3,014 (15 per tab) | 0 | 838–1,634 ms | ~1 core |

The overview is rendered and encoded once per tenant per tick, not once per tab, but every tab
still receives the whole document. 200 tabs is the practical ceiling per replica at 100 nodes on
this laptop; much larger fleets would want a compact or delta overview, addressed separately.

## What these numbers do not cover

- Multi-hour leaks. No soak test was run.
- Per-tenant performance isolation: a light stream's p99 went from 7 ms to 291 ms next to a
  2.5k/s neighbour on the same Redis. RillKit does not isolate tenants beyond the server's
  separate connections.
- Production network conditions, TLS, or managed Redis limits.

Run the same scenarios yourself with the scripts and prerequisites in
[`bench/README.md`](https://github.com/NimzyMaina/RillKit/blob/main/bench/README.md).
