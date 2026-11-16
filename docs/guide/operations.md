# Operations

Everything on this page works with the library alone, from code or `redis-cli`. With the [control plane](/guide/server), the [dashboard](/guide/dashboard) shows the same information live (workers, lag, pending, the DLQ, throughput) and lets you pause, drain, scale and replay from the browser. Measured throughput and latency are on [Performance](/guide/performance).

## Heartbeat and active_workers

Every consumer writes a heartbeat key, `rillkit:workers:<stream>:<group>:<consumer>` (or `rk:{<stream>}:workers:<group>:<consumer>` with [hash-tagged keys](/guide/keys); `broker.keys.heartbeat(...)` gives the exact name), with a time-to-live of `heartbeat_ttl` seconds (default 60). It is refreshed at the top of the loop and after messages, coalesced to at most once every `min(5 s, max(0.5 s, heartbeat_ttl / 3))` (5 s at the default TTL), and deleted on a clean shutdown. A worker that crashes simply stops refreshing it and drops off after the TTL. Each refresh also records the worker in the stream and group's [consumer registry](/guide/keys#where-the-group-and-worker-name-appear) (`rillkit:consumers:<stream>:<group>`), a sorted set scored by when the heartbeat expires.

```python
broker.active_workers("orders", group="billing")   # ['billing-web1-4242', 'billing-web2-977']
broker.active_workers(Stream("orders", group="billing"))
```

`active_workers` returns the sorted consumer names whose heartbeat is alive, read from the registry in one command, with no `SCAN`, so it also works for ACL users that may not list keys. Workers on RillKit 0.2 or older write no registry entry and are not listed; upgrade every worker of a group together. `XINFO CONSUMERS` only lists consumers that have already received a message, so it misses idle workers. The heartbeat does not.

```python [g_ops.py]
import time
import logging
from rillkit import Broker, Stream

logging.basicConfig(level=logging.WARNING)
broker = Broker.from_env()
S = Stream("tasks", group="workers")


@broker.subscribe(S, consumer="w1", block_ms=200)
def a(task: dict):
    pass


broker.start()
broker.publish(S, {"n": 1})
time.sleep(0.5)
print(broker.active_workers(S))
print(broker.active_workers("tasks", group="workers"))
print(sorted(k.decode() for k in broker.client.scan_iter("rillkit:workers:*:*")) )
print(broker.client.ttl("rillkit:workers:tasks:workers:w1"))
broker.stop()
print("after stop:", Broker.from_env().active_workers(S))
```

```text
['w1']
['w1']
['rillkit:workers:tasks:workers:w1']
60
after stop: []
```

::: warning Long handlers and the heartbeat
The heartbeat is refreshed between messages, not during a handler, and at most once every `min(5 s, max(0.5 s, heartbeat_ttl / 3))`. A handler that runs longer than `heartbeat_ttl` makes a busy, healthy worker look gone. Set `heartbeat_ttl` above your longest handler, or pass `heartbeat=False` to turn the feature off.
:::

## Scaling

- One lane is one thread, processing one message at a time. A subscription runs `concurrency` lanes (default 1).
- Scale by raising `concurrency` on the subscription, running more worker processes in the same group (see [Lifecycle](/guide/lifecycle#running-several-worker-processes)), or adding more `@broker.subscribe` handlers on different streams.
- The server spreads new entries over the consumers in the group, so throughput rises with the number of lanes and workers. Ordering across lanes and workers is not guaranteed.
- Keep an eye on `count`: a larger batch means a worker holds more entries pending at a time, and on shutdown the unprocessed part of a batch waits for `claim_idle_ms`.

## Claim tuning

| Setting | Lower it when | Raise it when |
| --- | --- | --- |
| `claim_idle_ms` | you want failed or abandoned messages back sooner | handlers are slow: it must exceed the longest handler |
| `backoff` (`> 1`) | retries should space out when a dependency is down | retries should stay quick |
| `block_ms` | you want fast retries and fast shutdown | you want fewer round trips on a quiet stream |
| `count` | handlers are slow and you want fair sharing | handlers are fast and you want fewer round trips |

Recovery time for the work of a crashed worker is roughly `claim_idle_ms` plus up to `block_ms`.

## Trimming

Streams keep every entry until you remove it. Choose one:

- `Stream("orders", maxlen=100_000)` or `publish(..., maxlen=...)` trims on write (approximately; pass `approximate=False` to `publish` for an exact cap).
- A scheduled job running `XTRIM orders MAXLEN ~ 100000`.

RillKit never trims the dead letter stream. Cap it yourself, for example with `redis-cli XTRIM orders-dlq MAXLEN ~ 10000` (or `valkey-cli`), and watch its length. `Idempotent` markers expire on their own after `ttl`.

## Monitoring with redis-cli or valkey-cli

Everything RillKit does is visible with the server's own commands; `valkey-cli` takes exactly the same ones as `redis-cli`. The key names below are the single-server defaults. On a cluster, or with another [key layout](/guide/keys), use the physical keys instead: `broker.keys.stream(s)` and `broker.keys.dlq(s)` print them, for example `'rk:{tasks}:stream'` (quote the braces in a shell), and add `-c` so the CLI follows the cluster. These are real outputs from a worker (`w1`) that handled four messages, one of which failed and is waiting for its retry. `redis-cli` prints one value per line when piped, as below:

```bash
redis-cli XLEN tasks                       # entries in the stream
redis-cli XINFO GROUPS tasks               # per group: consumers, pending, lag
redis-cli XINFO CONSUMERS tasks workers    # per consumer: pending, idle
redis-cli XPENDING tasks workers           # summary of the pending list
redis-cli XPENDING tasks workers - + 10    # detail: id, consumer, idle ms, deliveries
redis-cli XRANGE tasks-dlq - +             # dead letters
redis-cli --scan --pattern 'rillkit:workers:*'   # live heartbeats (on a cluster: broker.active_workers)
```

```text
$ redis-cli XLEN tasks
4

$ redis-cli XINFO GROUPS tasks
name
workers
consumers
1
pending
1
last-delivered-id
1793095029760-0
entries-read
4
lag
0

$ redis-cli XINFO CONSUMERS tasks workers
name
w1
pending
1
idle
151
inactive
1065

$ redis-cli XPENDING tasks workers
1
1793095029759-0
1793095029759-0
w1
1

$ redis-cli XPENDING tasks workers - + 10
1793095029759-0
w1
1094
1

$ redis-cli --scan --pattern 'rillkit:workers:*'
rillkit:workers:tasks:workers:w1
```

### What to watch

| Signal | Meaning |
| --- | --- |
| `lag` in `XINFO GROUPS` keeps growing | consumers are not keeping up or are down |
| `pending` stays high, `idle` is large | handlers are stuck or messages keep failing |
| `XPENDING` deliveries count rising | a message is being retried |
| `XLEN <stream>` growing without bound | producers are outrunning consumers; cap it with `maxlen` or add workers |
| `XLEN <stream>-dlq` above zero | something gave up, go and look |
| `active_workers` smaller than expected | a worker died or its heartbeat expired |
| `PublishError: Too many connections` | the client pool is too small for the concurrency; raise `max_connections` |

With [`rillkit-server`](/guide/server), `GET /api/metrics` exposes lag, pending, ack rate,
nodes, workers and pending directives per tenant for Prometheus. It deliberately omits
per-stream series; use the dashboard or `XINFO` for those. A hot tenant can affect a quiet one
on a shared Redis or Valkey: there are no per-tenant CPU or memory quotas, so isolate noisy
tenants with separate servers or clusters.

In the output above, `XPENDING` shows one entry delivered once and owned by `w1`. That is the failed message waiting out `claim_idle_ms`.
