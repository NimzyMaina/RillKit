# Control plane

A control plane can pause, resume, drain and scale your workers while they run. It is optional and **off by default**: off, RillKit is the plain library, with no extra keys, connections, threads or log handlers.

This page is about the workers' side: how they connect, what they do in each state, and how directives reach them. [Running the server](/guide/server) sets up `rillkit-server`; [the dashboard](/guide/dashboard) is a tour of what you can do with it.

## Turning it on

Control is off by default. Turn it on with `Control(mode="relay", ...)` or `RILLKIT_CONTROL=relay`; sources are, strongest first, code, then the environment, then the [config file](/guide/configuration#config-file). The next section has the connection variables.

`Broker(...)` and `Broker.from_env()` read `RILLKIT_CONTROL` (and the config file) when you pass no `control=`.

## Connecting to rillkit-server

[`rillkit-server`](https://github.com/NimzyMaina/RillKit/tree/main/server) is the control plane with a dashboard. Workers connect out to it, so they need no inbound port:

```bash
pip install "rillkit[control]"
export RILLKIT_CONTROL=relay
export RILLKIT_CONTROL_URL=https://rillkit.example.com   # the server's public url
export RILLKIT_API_KEY=rk_...                             # created in the dashboard, shown once
export RILLKIT_TENANT=t-42                                # only if the tenant enforces its keys
```

- The worker sends its telemetry every few seconds, applies the directives the server sends (each exactly once), and answers requests for a message's captured logs.
- If the server is unreachable, or refuses the worker (unknown or revoked key, wrong tenant), the worker logs an error, **keeps consuming** under the last stored state, and retries with backoff (1 s doubling to 30 s).
- The url must use `wss://` or `https://`. Plain `ws://` or `http://` is accepted for `localhost`, or anywhere with `RILLKIT_CONTROL_INSECURE=1` (`Control(insecure=True)`), which sends the API key unencrypted: only on a private network, such as a local Docker Compose setup.
- `broker.control.relay.connected` is a `threading.Event` set while the session is up.

To try it all locally, download the demo and start it; no clone needed:

```bash
curl -fsSLO https://raw.githubusercontent.com/NimzyMaina/RillKit/main/deploy/demo.yaml
docker compose -f demo.yaml up -d
```

That starts the server, its dashboard at `http://localhost:8080`, and a small demo fleet using published images.

## What the dashboard may see

Workers decide what leaves them. By default the dashboard sees ids, sizes, delivery states, attempt timings and trace ids of messages, and the reason a DLQ entry failed only as its exception class: never message contents. Three switches open more:

| Variable | `Control(...)` | Shows |
| --- | --- | --- |
| `RILLKIT_INSPECT_MESSAGES=true` | `inspect_messages=True` | message payloads and DLQ error texts; lets you edit a DLQ entry before replaying it |
| `RILLKIT_CAPTURE_LOGS=true` | `capture_logs=True` | what handlers logged for each message ([log capture](/reference/middleware#logcapture)) |
| `RILLKIT_TRACE_RECORDS=true` | `trace_records=True` | a trace timeline across streams, from one record per handler attempt (metadata only, kept for an hour) |

Each worker announces them when it connects, and the dashboard explains how to turn them on where they are off.

## The DLQ from the dashboard, or from code

The dashboard pages through a stream's DLQ and replays or deletes entries, one, several or all, and can replay an entry with an edited payload (to fix a message that can never succeed as it is). Each change is a directive, so it is applied once and listed in Activity. The same operations are on the broker:

```python
broker.replay_dlq("orders", count=100)                       # the oldest 100
broker.replay_dlq_entries("orders", ["1794765340386-0"])      # chosen entries
broker.replay_dlq_entries("orders", [entry_id], {entry_id: '{"id": 7, "amount": 12.5}'})  # edited
broker.delete_dlq_entries("orders", [entry_id])
broker.purge_dlq("orders")
broker.create_group("invoices", "mailer", start="0")           # stream and group, if missing
```

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

Desired state is stored at three scopes: global (the namespace), a stream and group, and a node. A per-node override can also name one stream and group on that node.

- **State:** the most restrictive scope wins. A paused stream stays paused when the global state is `RUNNING`, and a global pause pauses every stream.
- **Concurrency, `block_ms`, `batch_size`:** the most specific scope wins, over the values given to `subscribe()`. `block_ms` is still capped below the client's socket timeout.

## Concurrency

`subscribe(..., concurrency=n)` runs `n` consumer loops (threads) for a stream, named `<consumer>`, `<consumer>-1`, ... The default is 1, as before. The control plane can change it at run time: new loops start right away, and loops that are no longer needed finish their batch, then remove their heartbeat.

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
               "dlq_key": "rk:{orders}:dlq", "lag": 4820, "pending": 12, "ack_rate_sec": 38.5,
               "attempts_total": 1840, "wait_ms_total": 2145600.0, "work_ms_total": 73600.0}]
}
```

`mem_mb` is reported on Linux only. `attempts_total`, `wait_ms_total` and `work_ms_total` are the attempt timings: handler runs, their summed wait from publish to handler start, and their summed handler time.

## Tracing and logs

A managed broker adds the [`TraceContext`](/reference/middleware#tracecontext) middleware, so W3C `traceparent` headers follow messages across streams.

**Attempt timings are always on with the control plane.** For every handler run the worker records how long the message waited from publish to the start of the handler and how long the handler took, and reports the running totals in its telemetry. The dashboard turns them into the **Time per message** chart on a stream: a growing queue wait with a flat handler time means the stream needs more workers; a growing handler time means the handler (or something it calls) is the bottleneck. These are counts and durations, never message contents.

**Per-trace records are opt-in.** `Control(trace_records=True)`, `RILLKIT_TRACE_RECORDS=true` or `trace_records = true` under `[control]` stores one small entry per handler attempt in a stream keyed by the message's trace id: ids, stream and group, consumer, node, delivery number, timestamps and outcome. The key expires an hour after the trace's last attempt. Opening a message in the dashboard then shows **View trace**: every attempt of that trace in order, across every stream it touched, with queue wait against handler time. No payload, header, log line or exception text is stored, and the server does not keep the records: a connected worker reads them when you ask.

Log capture is opt-in. Handler logs often contain message data, and captured logs are stored in your Redis or Valkey and shown to anyone with dashboard access. Turn it on with `Control(capture_logs=True)`, `RILLKIT_CAPTURE_LOGS=true` or `capture_logs = true` under `[control]`: each handler run's log records are then kept for a day in a stream per message (`read_logs(broker, stream, msg_id)`), without exception messages or tracebacks. To choose loggers, redact records or keep exception text, add [`LogCapture(...)`](/reference/middleware#logcapture) yourself instead. Both middleware work without the control plane too.

## Failure behaviour

- **Processing fails open.** If the server that writes the state is gone, workers keep consuming under the last state stored in Redis or Valkey. If the wakeup listener loses its connection, it reconnects with jittered exponential backoff and re-reads the state.
- **Security fails closed.** With one ACL user per tenant (see [Several tenants on one server](/guide/keys#several-tenants-on-one-server)), a misconfigured worker gets `NOPERM`, never another tenant's data.

## Mixed fleets

A worker with the control plane off ignores pauses and maintenance. Run every worker of a consumer group in the same mode.
