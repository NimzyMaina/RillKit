# Control plane

A control plane can pause, resume, drain and scale your workers while they run. It is optional and **off by default**: off, RillKit is the plain library, with no extra keys, connections, threads or log handlers.

## Turning it on

| Mode | What the worker does |
| --- | --- |
| `off` (default) | Consumes its streams on its own. |
| `valkey` | Reads its desired state from control keys on your Redis or Valkey server before polling, and listens for changes. Something with access to the server writes that state: `rillkit-server`, or `broker.control.apply(...)` from your own tooling. |
| `relay` | `valkey` plus an outbound connection to a RillKit server. The Relay client is not in this release yet, so a broker refuses to start in this mode. |

Strongest first: code, then the environment, then the [config file](/guide/configuration#config-file).

::: code-group
```bash [Environment]
export RILLKIT_CONTROL=valkey
export RILLKIT_NODE_ID=orders-worker-1    # optional, default <hostname>-<pid>
```
```python [Code]
from rillkit import Broker, Control

broker = Broker.from_env(control=Control(mode="valkey", node_id="orders-worker-1"))
```
```toml [rillkit.toml]
[control]
mode = "valkey"
node_id = "orders-worker-1"
```
:::

`Broker(...)` and `Broker.from_env()` read `RILLKIT_CONTROL` (and the config file) when you pass no `control=`.

## States

| State | Polling | In-flight messages | Consumers |
| --- | --- | --- | --- |
| `RUNNING` | yes | | registered |
| `PAUSED` | no | finished first | registered, heartbeats continue |
| `MAINTENANCE` | no | finished first | registered, heartbeats continue |
| `DRAINING` | no | finished and acked first | released: heartbeats and registry entries removed, so `active_workers` is empty |

A change takes effect as soon as each consumer finishes the batch it is handling. A consumer already blocked in `XREADGROUP` sees it when that read returns, at most `block_ms` later; a message that read returns is still handled. The process stays up in every state, and returns to consuming when the state is `RUNNING` again.

A booting worker reads the state before its first poll, so a new container in a paused namespace never consumes.

## Scopes

Desired state is stored at three scopes: global (the namespace), a stream and group, and a node. A node directive can also name one stream and group on that node.

- **State:** the most restrictive scope wins. A paused stream stays paused when the global state is `RUNNING`, and a global pause pauses every stream.
- **Concurrency, `block_ms`, `batch_size`:** the most specific scope wins, over the values given to `subscribe()`. `block_ms` is still capped below the client's socket timeout.

## Concurrency

`subscribe(..., concurrency=n)` runs `n` consumer loops (threads) for a stream, named `<consumer>`, `<consumer>-1`, ... The default is 1, as before. A `SET_CONCURRENCY` directive changes it at run time: new loops start right away, and loops that are no longer needed finish their batch, then remove their heartbeat.

## Directives

```python
ack = broker.control.apply({
    "type": "DIRECTIVE",
    "directive_id": "dir_99812",
    "cmd": "SET_CONCURRENCY",
    "scope": "stream",
    "target": {"stream": "orders", "group": "billing"},
    "params": {"concurrency": 4, "batch_size": 25, "poll_ms": 200},
})
# {"type": "DIRECTIVE_ACK", "directive_id": "dir_99812", "node_id": "...",
#  "status": "APPLIED", "current_state": {"concurrency": 4, "state": "RUNNING"}}
```

| `cmd` | `params` |
| --- | --- |
| `SET_STATE` | `state`: `RUNNING`, `PAUSED`, `MAINTENANCE` or `DRAINING` |
| `MAINTENANCE_ON`, `MAINTENANCE_OFF` | none (same as `SET_STATE` `MAINTENANCE` / `RUNNING`) |
| `SET_CONCURRENCY` | `concurrency` (1 to 256), optional `batch_size` and `block_ms` (or `poll_ms`) |
| `SET_POLL_CONFIG` | `block_ms` (or `poll_ms`) and/or `batch_size` |
| `EXECUTE_REPLAY` | `count` (default 100): move that many DLQ entries back, as `replay_dlq`; or `set_id`: `XGROUP SETID` the group. Needs `target.stream` and `target.group` |

`scope` is `global`, `stream` (with `target.stream` and `target.group`) or `node` (with `target.node_id`, and optionally a stream and group).

Directives are checked strictly, the same way in every RillKit SDK: commands, scopes and states are exact-case, and numbers must be JSON integers (`2.5`, `"12"` and `true` are rejected, not converted). The JSON Schemas and conformance vectors that define this are in the repository's [`spec/`](https://github.com/NimzyMaina/RillKit/tree/main/spec) directory.

Any node can apply any directive. It is stored in the matching state key, and a wakeup is published on the broadcast channel (or the target node's channel); every node it concerns re-reads its state immediately. Each worker also re-reads every `refresh_interval` seconds (default 30), in case a wakeup was lost.

**Each directive is applied once.** Its id is recorded for 60 seconds; applying it again returns status `DUPLICATE`. When the dedup key and the state key share a slot (always on a single server, and with `HashTagKeys`), the check and the write are one transaction. A malformed directive returns `REJECTED`, an error while applying it `FAILED`, both with an `error`.

On a cluster, wakeups use sharded Pub/Sub (`SPUBLISH`/`SSUBSCRIBE`): the channels carry the namespace's control tag, so they reach one shard only.

## Node heartbeats and telemetry

Each managed worker writes a heartbeat every `heartbeat_interval` seconds (default 10): a hash at `keys.node(node_id)` with its state, active consumers, in-flight messages and a full telemetry report, and an entry in the node registry. `broker.control.nodes()` lists live nodes; `broker.control.telemetry()` returns the report:

```json
{
  "type": "TELEMETRY_REPORT",
  "node_id": "orders-worker-1",
  "keys": {"layout": "hashtag", "prefix": "rk", "tenant": null},
  "workers": {"active": 4, "in_flight": 1, "state": "RUNNING", "poll_ms": 5000,
              "batch_size": 10, "cpu_pct": 24.5, "mem_mb": 142.0},
  "streams": [{"stream": "orders", "group": "billing", "stream_key": "rk:{orders}:stream",
               "dlq_key": "rk:{orders}:dlq", "lag": 4820, "pending": 12, "ack_rate_sec": 38.5}]
}
```

`mem_mb` is reported on Linux only.

## Tracing and logs

A managed broker adds the [`TraceContext` and `LogCapture`](/reference/middleware#tracecontext) middleware, so W3C `traceparent` headers follow messages across streams, and each handler run's log records are kept for a day in a stream per message (`read_logs(broker, stream, msg_id)`). Both work without the control plane too, if you add them yourself.

## Failure behaviour

- **Processing fails open.** If the server that writes the state is gone, workers keep consuming under the last state stored in Redis or Valkey. If the wakeup listener loses its connection, it reconnects with jittered exponential backoff and re-reads the state.
- **Security fails closed.** With one ACL user per tenant (see [Several tenants on one server](/guide/keys#several-tenants-on-one-server)), a misconfigured worker gets `NOPERM`, never another tenant's data.

## Mixed fleets

A worker with the control plane off ignores pauses and maintenance. Run every worker of a consumer group in the same mode.
