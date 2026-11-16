# Control

```python
from rillkit import Broker, Control
```

`Control` says whether and how a broker is managed by a control plane. With it on, `broker.control` is the running [`ControlPlane`](#controlplane). The [control plane guide](/guide/control-plane) explains the behaviour; this page lists the API.

## `Control`

```python
Control(
    mode: str = "off",
    url: Optional[str] = None,
    api_key: Optional[str] = None,
    node_id: Optional[str] = None,
    heartbeat_interval: float = 10.0,
    refresh_interval: float = 30.0,
    capture_logs: bool = False,
    insecure: bool = False,
    inspect_messages: bool = False,
    trace_records: bool = False,
)
```

| Field | Variable | Default | Description |
| --- | --- | --- | --- |
| `mode` | `RILLKIT_CONTROL` | `off` | `off`: the plain library. `relay`: connect to [`rillkit-server`](/guide/server) for telemetry, directives and the dashboard. |
| `url` | `RILLKIT_CONTROL_URL` | `None` | `relay`: the server, `https://...` or `wss://...` (the Relay path `/relay/v1` is added). |
| `api_key` | `RILLKIT_API_KEY` | `None` | `relay`: the worker's API key. Shown as `***` in `repr`. |
| `node_id` | `RILLKIT_NODE_ID` | `<hostname>-<pid>` | This process's name on the control plane. |
| `heartbeat_interval` | | `10.0` | Seconds between node heartbeats. |
| `refresh_interval` | | `30.0` | Seconds between re-reads of the desired state, in case a wakeup was lost. |
| `capture_logs` | `RILLKIT_CAPTURE_LOGS` | `False` | Add [`LogCapture`](/reference/middleware#logcapture), so the dashboard can show each message's logs. |
| `inspect_messages` | `RILLKIT_INSPECT_MESSAGES` | `False` | Let the control plane read payloads and DLQ error texts, and edit DLQ entries before replaying them. |
| `trace_records` | `RILLKIT_TRACE_RECORDS` | `False` | Keep one small entry per handler attempt in the trace key, so the dashboard can draw a trace across streams. Metadata only (ids, stream and group, consumer, node, timings, outcome); kept for an hour. |
| `insecure` | `RILLKIT_CONTROL_INSECURE` | `False` | Allow a `ws://` or `http://` url on a host other than `localhost`. The API key then travels unencrypted. |

Boolean variables accept `1`, `true`, `yes` or `on`. `Control.from_env(config=None, **overrides)` reads the variables over the `[control]` section of a [config file](/guide/configuration#config-file); keyword arguments win. An invalid combination raises [`ConfigurationError`](/reference/exceptions): an unknown mode, `relay` without a url or key, or an insecure url without `insecure`. `relay` mode needs `pip install "rillkit[control]"`; without it, `Broker(...)` raises `ConfigurationError`.

```python
broker = Broker.from_env(control=Control(mode="relay", url="https://rillkit.example.com",
                                         api_key=os.environ["RILLKIT_API_KEY"],
                                         capture_logs=True))
```

## `ControlPlane`

`broker.control`, when the control plane is on. Its threads start and stop with the broker.

| Member | Description |
| --- | --- |
| `node_id` | This node's id. |
| `settings` | The `Control`. |
| `desired(sub=None) -> Desired` | What a subscriber (`broker.subscribers[i]`), or without one the node as a whole, should do now: `state` (`RUNNING`, `PAUSED`, `MAINTENANCE` or `DRAINING`) and `concurrency`, `block_ms`, `batch_size` (`None` means the value given in code). |
| `hydrate() -> bool` | Re-reads the desired state now. `True` if it changed. The listener does this on every wakeup. |
| `telemetry() -> dict` | This node's `TELEMETRY_REPORT`: workers, key layout, and for each stream its keys, lag, pending, ack rate, lanes, state, counters, attempt timings and DLQ size. |
| `browse(stream, group, source="stream", before=None, count=50, ids=None, after=None) -> dict` | A page of a stream's or DLQ's entries, newest first, with delivery statuses: what the dashboard's Messages and DLQ tabs show. Payloads are included only with `inspect_messages`. |
| `trace(trace_id) -> dict` | One trace's recorded attempts, oldest first, across streams: what the dashboard's trace view shows. Empty without `trace_records` or after an hour. |
| `nodes() -> list[str]` | Ids of the nodes with a live heartbeat in this namespace. |
| `capabilities() -> dict` | `{"inspect_messages": ..., "capture_logs": ..., "trace_records": ...}`, as announced to the server. |
| `relay` | In `relay` mode, the Relay client: `connected` is a `threading.Event` set while the session is up, `tenant_id` the tenant the server bound the key to. `None` otherwise. |

The wire format of everything above is specified in the repository's [`spec/`](https://github.com/NimzyMaina/RillKit/tree/main/spec) directory: prose, JSON Schemas for every message, and conformance vectors every SDK passes.
