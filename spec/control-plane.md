# RillKit Platform & Control Plane Technical Specification

> **Status (November 2026).** Milestone 1 (section 6) is implemented in the Python SDK.
> Milestone 2 is implemented except OpenTelemetry trace ingestion: the Relay client
> (`sdks/python`), and `rillkit-server` (`server/`) with the Relay gateway, directive routing
> across replicas, the leader-elected autoscaler and live events. Milestone 3 is implemented
> except producer cordoning with FIFO replay: the dashboard
> (`dashboard/`) with the fleet console, stream controls, autoscaling policies, DLQ replay,
> wait/work charts, per-trace timelines and the inline message detail. The data
> plane is specified in [data-plane.md](data-plane.md); the messages in section 5 have JSON
> Schemas in [protocol/v1](protocol/v1); and the rules in sections 2.1, 2.2 and 4.2 are checked
> by the [conformance vectors](conformance). Where this document and a vector disagree, the
> vector is wrong or this document is, and both must be fixed together (see the
> [README](README.md)). Decisions made while implementing milestone 1 are marked
> *Implementation note*.

## 1. Architectural Overview & Core Principles

`rillkit` is evolving from a standalone Redis/Valkey Streams consumer library into a distributed event-streaming runtime structured into **two deployable components** following a strict **Smart Control Plane / Compliant Worker** model:

1. **`rillkit` (Unified Worker SDK & Outbound Relay Client):** Runs inside customer infrastructure as identical, stateless peer nodes (no worker-side leader election or autoscaling policy logic). Each node processes streams, reports telemetry, and relays directives between `rillkit-server` and the local Valkey/Redis instance.
2. **`rillkit-server` (Control Plane, Autoscaler & Dashboard):** Runs in the Cloud or as a single self-hosted container. Terminates outbound Relay connections from customer workers, runs a leader-elected Autoscaler decision engine, serves the React Dashboard, and dispatches directives back to workers.

```text
┌─────────────────────────────────────────────────────────────────┐
│  rillkit-server Cluster (Cloud SaaS or Self-Hosted Docker)      │
│  • Replica 1 [AUTOSCALER LEADER via Lock] ──► Evaluates Scaling │
│  • Replica 2 [STANDBY]                    ──► Serves UI & WS    │
│  • Routes global cmds via 1 active Relay; node cmds directly    │
└───────────────▲────────────────────────────────▲────────────────┘
                │ Outbound Relay                 │ Outbound Relay
                │ (WSS / HTTP/2)                 │ (WSS / HTTP/2)
     ┌──────────┴──────────┐          ┌──────────┴──────────┐
     │ Worker Node 1       │          │ Worker Node 2       │
     │ (Identical Peer)    │          │ (Identical Peer)    │
     └──────────┬──────────┘          └──────────┬──────────┘
                └───────────────┬────────────────┘
                                ▼
                    ┌───────────────────────┐
                    │  Local Valkey / Redis │
                    │  • Streams, PEL, DLQ  │
                    │  • control keys (§4)  │
                    │  • state, ctl, nodes  │
                    └───────────────────────┘
```

---

## 2. Component 1: `rillkit` (Compliant Worker SDK & Relay Client)

Every worker node runs identical code with zero policy or autoscaling logic inside it.

### 2.0 The control plane is optional

`rillkit` is first a standalone SDK. **With the control plane off, which is the default, it behaves exactly like the plain library:** no Relay connection, no state hydration, no wakeup listener, no control keys read or written, no extra dependencies. Everything in this spec beyond the shipped data keys applies only when a control mode is on.

| Control mode | What the worker does | Used for |
| --- | --- | --- |
| `off` (default) | Consumes its streams on its own. | SDK-only users |
| `valkey` | Hydrates desired state from the control keys and listens on the control channels, but makes no connection to `rillkit-server`. A server with direct Valkey access writes the keys. | Self-hosted, server in the same network as Valkey |
| `relay` | Everything in `valkey` mode, plus the outbound Relay: telemetry up, directives down, applied to Valkey by the worker. | RillKit Cloud (shared cluster or the customer's own Valkey), or a self-hosted server the workers connect to |

*Implementation note:* until the Relay client ships (milestone 2), a broker configured with `relay` refuses to start with a `ConfigurationError`, rather than running without its Relay.

| Deployment | Data | Control mode | Keys |
| --- | --- | --- | --- |
| SDK only | customer's Redis/Valkey | `off` | `PlainKeys` / `HashTagKeys`, no tenant |
| Self-hosted | customer's Redis/Valkey | `valkey` or `relay` | same as SDK only |
| Hybrid | customer's Redis/Valkey, RillKit Cloud control plane | `relay` (Cloud cannot reach their Valkey) | same as SDK only |
| Cloud | shared RillKit cluster | `relay` | `HashTagKeys(tenant=<tenant_id>)` with the default prefix `rk`, the tenant id from the worker's own configuration (copied from the dashboard), checked at the handshake; plus the tenant ACL user (4.4) |

**Turning it on.** Sources, strongest first: code (`Broker(control=Control(mode="relay", url=..., api_key=...))`), then environment (`RILLKIT_CONTROL=off|valkey|relay`, `RILLKIT_CONTROL_URL`, `RILLKIT_API_KEY`), then a config file, then the default `off`. This is the same precedence as `Settings.from_env()` (keyword arguments over environment).

**Config file.** Optional. It is read only when `RILLKIT_CONFIG=<path>` or `config=<path>` names it; there is no implicit discovery of a file in the working directory, so a stray file can never change a worker's behaviour. One file covers everything, mapped onto the existing settings:

```toml
[connection]            # Settings fields, same names as the RILLKIT_* variables
url = "rediss://cache.internal:6379/0"
backend = "valkey"
mode = "cluster"

[keys]
layout = "hashtag"
prefix = "shop"

[control]
mode = "relay"
url = "wss://cloud.rillkit.dev"
api_key_env = "RILLKIT_API_KEY"   # name of the variable holding the secret, never the secret
```

- TOML and JSON are supported. TOML uses `tomllib` (Python 3.11+) or the `tomli` backport (3.9–3.10, installed by an environment marker). YAML is available through the `rillkit[yaml]` extra.
- Secrets are referenced (`*_env`, or `*_file` for mounted secrets), not stored in the file.
- An unknown section or field raises `ConfigurationError`, consistent with `client_kwargs` validation.

**Failure behaviour.**
- **Processing fails open.** If the server or Relay is unreachable, workers keep consuming under the last desired state stored in Valkey, and still honour `PAUSED`/`MAINTENANCE` on boot. Directives resume when the Relay reconnects.
- **Security fails closed.** On the shared cluster, isolation is the ACL user (4.4), so a broken or misconfigured worker gets `NOPERM`, never another tenant's data.
- **Cloud credentials (decided: configured by the customer).** On the shared cluster, everything a worker needs to process comes from its own configuration, never from `rillkit-server`:
  - the Valkey connection with the tenant's ACL username and password (`RILLKIT_URL`, or `RILLKIT_HOST`/`RILLKIT_USERNAME`/`RILLKIT_PASSWORD`...),
  - the tenant id (`RILLKIT_TENANT`, or `tenant` in the `[keys]` section of the config file). It is the only per-tenant key setting: the prefix is the SDK default `rk` for every tenant, and setting a tenant implies the hash-tagged layout,
  - and `RILLKIT_API_KEY` for the Relay.

  The dashboard shows these values for each tenant, ready to paste into an env file or secret manager. A worker can therefore start and process while `rillkit-server` is down, the same as in every other deployment. Customers keep the credentials in their own secret store; RillKit stores none on disk.
  - **Rotation:** Valkey ACL users accept two passwords at once. Rotate by adding the new password, letting customers update and restart workers, then removing the old one (existing connections are not dropped by a password removal; deleting the user does drop them).
  - **Revocation:** delete or disable the tenant's ACL user (data access) and/or revoke the API key (control plane).
  - **Not chosen:** credentials handed out at the handshake (a worker could not start during a server outage) and a local credential cache (secret handling in the SDK). Handshake delivery may be added later as an optional fallback when no credentials are configured; configured credentials would always win.

**Mixed fleets.** A worker with control `off` in a consumer group alongside managed workers ignores pause and maintenance. The dashboard detects this by comparing the group's consumers (`XINFO CONSUMERS`) with the consumer registry and the node registry, and warns about unmanaged consumers.

**Packaging.** The Relay client's dependencies (websocket/HTTP2) live in an extra, `rillkit[control]`, so SDK-only installs gain nothing. `valkey` mode needs no extra.

### 2.1 Outbound Relay Client (`wss://` / HTTP/2), `relay` mode
* **Zero Inbound Ports:** Initiates an outbound TLS connection from the customer's private network to `rillkit-server` using `api_key` and `node_id`, reconnecting automatically with exponential jittered backoff.
* **Telemetry Reporting:** Periodically pushes node-level metrics (`active_workers`, `in_flight`, `state`, `cpu_pct`, `mem_mb`), the node's key layout, and per-stream physical keys and lag metrics (`XINFO GROUPS`, `XPENDING`) up the Relay (5.1).
* **Directive Relaying (Relay $\rightarrow$ Valkey Fan-Out):**
  * When a node receives a `DIRECTIVE` frame over its Relay connection, it applies it to local Valkey/Redis guarded by the dedup key (`SET <dedup key> 1 NX EX 60`), then:
    1. Persists the desired state into the matching state key (source of truth for running and newly booting workers).
    2. Publishes a wakeup signal on the broadcast channel (or the node's channel).
  * All key and channel names come from the worker's key builder (see section 4). Under `HashTagKeys` the dedup and state keys share one hash slot, so the dedup check and state write run as one `MULTI`/`EXEC` (or one Lua script where Lua is available) even on a cluster. Without a shared slot (`PlainKeys` on a cluster), the worker runs `SET NX` first and then writes the state and publishes. That is safe because writing the same desired state twice is idempotent.
  * Because all peer nodes subscribe to the namespace's control channels, every worker in the cluster converges immediately regardless of which Relay connection delivered the frame.
  * On a cluster, wakeups use **sharded** Pub/Sub (`SPUBLISH`/`SSUBSCRIBE`). The control channels carry the tenant's control tag, so they map to one shard, and a tenant's wakeups reach only that shard instead of fanning out to every node in the shared cluster, as classic `PUBLISH` does.

### 2.2 Worker State Machine & Compliance Engine
* **Startup State Hydration:** Before polling any stream, a booting worker reads the global state key and the state keys of its streams (section 4) so new containers immediately honor active `PAUSED` or `MAINTENANCE` states.
* **Deterministic States:**
  * `RUNNING`: Actively polling `XREADGROUP` and running handlers.
  * `PAUSED`: Process and Relay stay alive with active heartbeats, but `XREADGROUP` polling suspends cleanly after finishing in-flight messages.
  * `DRAINING`: Stops polling new messages, finishes in-flight handlers, `XACK`s them, and exits excess worker tasks/processes cleanly.
  * `MAINTENANCE`: Pauses consumption globally or per-stream; optionally activates atomic producer cordoning for safe downstream maintenance and ordered FIFO replay on exit. The cordon flag and buffer keys are built by the key builder in the stream's own hash slot (`rk:{orders}:cordon`), so a producer can check the flag and write to the stream or the buffer atomically, even on a cluster.
* **Dynamic Compliance Commands:**
  * `SET_CONCURRENCY(n)`: Scales local worker coroutines/subprocesses up or drains excess down to `n`.
  * `SET_STATE(RUNNING | PAUSED | MAINTENANCE | DRAINING)`: Transitions polling state immediately via async wakeup events (zero blind sleep delays).
  * `SET_POLL_CONFIG(block_ms, batch_size)`: Adjusts `XREADGROUP` batch size and block timeout on the next tick.
  * `DELETE_DLQ(ids | all)` and `CREATE_STREAM(start)` (*implementation note*, added with the dashboard's DLQ manager and stream creation): delete chosen DLQ entries or the whole DLQ; create a stream and its group (`XGROUP CREATE ... MKSTREAM`).
  * `EXECUTE_REPLAY(...)`: also takes `ids` (chosen DLQ entries) with optional `data` (a replacement `data` field per entry, to fix a payload before it is handled again). Executes `XGROUP SETID` or an atomic DLQ-to-primary-stream replay (the stream and DLQ share a slot under `HashTagKeys`; ordered add-then-delete otherwise, as `replay_dlq` does today) and emits `DIRECTIVE_ACK`.
* **Crash Recovery & Observability:**
  * Runs jittered periodic `XAUTOCLAIM` sweeps (atomic in Valkey/Redis, safe across concurrent peers).
  * Propagates W3C `traceparent` headers and captures per-message handler logs via `contextvars` into TTL-bounded Valkey streams (the log key for `(stream, msg_id)` from the key builder, see section 4), forwarding them up the Relay on demand.
  * *Implementation note:* log capture is **opt-in** (`Control(capture_logs=True)`, `RILLKIT_CAPTURE_LOGS`). Handler logs often contain message data, and captured logs are stored in Valkey (on Cloud, the shared cluster) and shown on the dashboard. By default only the exception class name is stored, never its message or traceback; capture can be limited to named loggers and passed through a redaction hook.
  * *Implementation note:* per-trace records are **opt-in** (`Control(trace_records=True)`, `RILLKIT_TRACE_RECORDS`). Each handler attempt appends one entry to the trace key of the message's trace id, kept for one hour and capped at roughly 1000 entries: ids, stream and group, consumer and node, delivery number, timestamps and outcome, never the payload or exception text. Aggregate attempt timings (`attempts_total`, `wait_ms_total`, `work_ms_total`) are part of telemetry whenever the control plane is on: they are counts and durations, not contents.
  * *Implementation note:* directives are parsed strictly: commands, scopes and states are exact-case, numeric parameters must be JSON integers, and `EXECUTE_REPLAY` parameters are validated before the directive id is recorded. Concurrency is implemented as consumer loops ("lanes") named `<consumer>`, `<consumer>-1`, ...; `DRAINING` retires every lane but keeps the process up.

---

## 3. Component 2: `rillkit-server` (Control Plane, Autoscaler & Dashboard)

Deployed as a single unified service (Cloud SaaS or `docker run rillkit/server` for self-hosted).

### 3.1 Control Plane Backend & Leader-Elected Autoscaler
* **Relay Connection Registry:** Tracks all active customer worker Relay connections mapped by `tenant_id` and `node_id`.
  * `tenant_id` comes from the `api_key` and is part of every Valkey key name. Tenants share one Valkey/Redis Cluster, so every worker uses `HashTagKeys(tenant=<tenant_id>)` with the default prefix **`rk`, the same for every tenant** (section 4): tenant `t-42`'s stream is `rk:{t-42:orders}:stream`, tenant `t-43`'s is `rk:{t-43:orders}:stream`. No separate Cloud prefix is needed: since rillkit 0.3, tenant keys can never collide with keys built without a tenant, whatever the prefix. `PlainKeys` is never used on the shared cluster.
  * **The handshake checks; it does not configure.** Workers bring their own Valkey credentials and tenant id (2.0, "Cloud credentials"). At the Relay handshake the worker reports its `keys` descriptor (5.1); `rillkit-server` compares it with the tenant bound to the `api_key` and, on a mismatch, rejects the Relay session and raises an alert. Data isolation never depends on this check: it is the ACL user (4.4). A worker configured with another tenant's id but its own credentials gets `NOPERM` on every command and fails closed.
  * Routes **node-targeted directives** (e.g., `SET_CONCURRENCY` on `node-02`, live log tailing) directly down that specific node's Relay.
  * Routes **cluster-wide directives** (e.g., `MAINTENANCE_ON`, `EXECUTE_REPLAY`) down any single healthy Relay in that tenant's cluster (failing over instantaneously to another peer socket if that connection drops).
* **High-Availability Autoscaler Loop (Leader-Elected):**
  * Across multiple `rillkit-server` replicas, a single instance acquires an advisory lock (`pg_try_advisory_lock` / Redis lock) to act as the active **Autoscaler Brain**.
  * Evaluates deduplicated stream lag, PEL depth, processing velocity, and node capacity against user-configured scaling policies (`min_workers`, `max_workers`, `target_lag_per_worker`, `upscale_cooldown_sec`, `downscale_cooldown_sec`). It sizes from both the backlog (`ceil(lag / target_lag_per_worker)`) and the observed arrival rate (finished per second plus backlog growth) with headroom, so a stream that is keeping up at zero lag is not scaled down to its minimum and does not oscillate. A new leader seeds that rate/capacity estimate from the recent samples, so a failover does not restart the estimate from zero.
  * Dispatches `SET_CONCURRENCY` directives down the target node Relays.
* **Direct-Valkey Writes:** `rillkit-server` may write the state keys and publish on the control channels directly, using a privileged ACL user of its own. It builds key names with the same rules as the worker's key builder (section 4.2), from the tenant id and prefix in **its own records**, never from what a worker reports. A mismatch between a node's reported `keys` descriptor (5.1) and the server's records is raised as an alert. Self-hosted single-customer deployments use the same rules with whatever layout their nodes report.

### 3.2 Modern Dashboard (React / TypeScript SPA)
* **Fleet & Autoscaling Console:** Real-time view of connected worker nodes, lag vs. concurrency charts, autoscaling policy controls, dynamic `block_ms` / `batch_size` tuning, and one-click **Pause / Resume / Drain** buttons.
* **Maintenance Mode & Cordon Switch:** Global or per-stream `MAINTENANCE` toggle with live buffered message counters and ordered FIFO replay on exit.
* **Stream & DLQ Replay Inspector:** Browse DLQ payloads (the DLQ key each node reports per stream, e.g. `orders-dlq`, `orders-failed` or `rk:{orders}:dlq`; never a name derived from the stream name), filter by error or timestamp, and trigger selective or point-in-time replays (`EXECUTE_REPLAY`).
* **Distributed Trace Timeline & Wait/Work Charts:** Renders a per-trace waterfall from the timings the workers themselves report — queue wait vs. handler execution, in order, across every stream the trace touches — and per-stream wait/work trends. Captured `contextvars` per-message logs are fetched on demand. Ingesting OpenTelemetry spans from callers is future work (milestone 2); the timeline needs no tracing backend.

---

## 4. Canonical Valkey / Redis Keyspace

Every key and channel name is produced by the worker's **key builder** (`rillkit.keys`), never written out by hand. That includes the data keys that ship today (streams, DLQs, consumer heartbeats, idempotency markers) and the control-plane keys added by this spec. The key builder is what lets one Valkey/Redis host several apps (prefixes), several tenants (`tenant`), or a cluster (hash tags) without any change to this spec.

> Notation: `<name>` is a placeholder. `{...}` is always a literal Redis hash tag.

### 4.1 Keys by layout

Examples use stream `orders`, group `billing`, consumer `worker-7`, node `worker-node-01`, directive `dir_99812` and message `1794669071538-1`.

**Data keys (shipped in 0.1–0.3, unchanged by this spec):**

| Purpose | Type | `PlainKeys` (single server, default) | `HashTagKeys(prefix="shop", tenant="t-42")` |
| --- | --- | --- | --- |
| Stream | STREAM | `orders` | `shop:{t-42:orders}:stream` |
| DLQ | STREAM | `orders-dlq` (or the custom `dlq=` name) | `shop:{t-42:orders}:dlq` (`...:dlq:<custom>`) |
| Consumer heartbeat | STRING, TTL | `rillkit:workers:orders:billing:worker-7` | `shop:{t-42:orders}:workers:billing:worker-7` |
| Idempotency marker | STRING, TTL | `rillkit:done:orders:<key>` | `shop:{t-42:orders}:done:<key>` |
| Consumer registry (since rillkit 0.3; `active_workers` reads it instead of `SCAN`) | ZSET: consumer → expiry ms | `rillkit:consumers:orders:billing` | `shop:{t-42:orders}:consumers:billing` |

**Control-plane keys (this spec; names reserved in rillkit 0.3):**

| Purpose | Type | `PlainKeys` | `HashTagKeys(prefix="shop", tenant="t-42")` |
| --- | --- | --- | --- |
| Global desired state | HASH | `rillkit:state:global` | `shop:{t-42:}:state:global` |
| Stream desired state | HASH | `rillkit:state:stream:orders:billing` | `shop:{t-42:}:state:stream:orders:billing` |
| Node desired state | HASH | `rillkit:state:node:worker-node-01` | `shop:{t-42:}:state:node:worker-node-01` |
| Directive dedup | STRING, TTL 60 s | `rillkit:dedup:dir_99812` | `shop:{t-42:}:dedup:dir_99812` |
| Node heartbeat | HASH | `rillkit:nodes:worker-node-01` | `shop:{t-42:}:nodes:worker-node-01` |
| Node registry | ZSET: node id → expiry ms | `rillkit:nodes` | `shop:{t-42:}:nodes` |
| Broadcast wakeups | Pub/Sub channel | `rillkit:ctl:broadcast` | `shop:{t-42:}:ctl:broadcast` |
| Node wakeups | Pub/Sub channel | `rillkit:ctl:node:worker-node-01` | `shop:{t-42:}:ctl:node:worker-node-01` |
| Per-message logs | STREAM, TTL | `rillkit:logs:orders:1794669071538-1` | `shop:{t-42:orders}:logs:1794669071538-1` |
| Per-trace records | STREAM, TTL 1 h | `rillkit:trace:4bf92f3577b34da6a3ce929d0e0e4736` | `shop:{t-42:}:trace:4bf92f3577b34da6a3ce929d0e0e4736` |
| Producer cordon flag | STRING | `rillkit:cordon:orders` | `shop:{t-42:orders}:cordon` |
| Cordon buffer | STREAM | `rillkit:cordon:orders:buffer` | `shop:{t-42:orders}:cordon:buffer` |

With the cluster default `HashTagKeys()` (prefix `rk`, no tenant), the control tag is `{:}`, as in `rk:{:}:state:global` and `rk:{:}:nodes:worker-node-01`.

**On RillKit Cloud** the prefix is the default `rk` for every tenant and only the tenant varies (`shop` above is just an example prefix). Tenant `t-42`'s stream is `rk:{t-42:orders}:stream`, its DLQ `rk:{t-42:orders}:dlq`, its consumer registry `rk:{t-42:orders}:consumers:billing`, and its control keys live under `rk:{t-42:}` (`rk:{t-42:}:state:global`).

Changes from the first draft of this spec:
- **Node heartbeats are `nodes:`, not `workers:`.** `rillkit:workers:` already holds per-consumer heartbeats (`rillkit:workers:<stream>:<group>:<consumer>`, strings with a TTL), which `Broker.active_workers` scans. A node id such as `orders:billing:worker-7` would have produced exactly such a key, as a hash.
- **The DLQ is not `<stream>:dlq`.** It is whatever the key builder returns (`orders-dlq`, a custom name, or `rk:{orders}:dlq`). Readers get it from telemetry (5.1).
- **Cordon keys belong to the stream** (`rk:{orders}:cordon`), not `<stream>:cordon` in the application's own namespace.
- **Control keys follow the prefix and tenant.** Two apps with prefixes `billing` and `search` on one server no longer share one `rillkit:state:global`, so pausing one does not pause the other.

### 4.2 Naming rules

`HashTagKeys(prefix, tenant)`:
- `enc(x)` percent-encodes `%`, `{`, `}` and `:` (`%25`, `%7B`, `%7D`, `%3A`).
- **Stream tag:** `{enc(stream)}`, or `{enc(tenant):enc(stream)}`. Stream names must be non-empty.
- **Control tag:** `{enc(tenant):}`, or `{:}` without a tenant.
- **Base:** `<prefix>:<tag>`, or just `<tag>` when the prefix is empty.
- **Stream-scoped keys:** `<stream base>:` + `stream` | `dlq` | `dlq:<custom>` | `workers:<group>:<consumer>` | `consumers:<group>` | `done:<key>` | `logs:<msg_id>` | `cordon` | `cordon:buffer`.
- **Control keys:** `<control base>:` + `state:global` | `state:stream:enc(stream):<group>` | `state:node:<node_id>` | `dedup:<directive_id>` | `nodes` | `nodes:<node_id>` | `ctl:broadcast` | `ctl:node:<node_id>` | `trace:<trace_id>`.

`PlainKeys`:
- **Data keys:** unchanged from 0.1 (raw names, as in the table above).
- **New control, log and cordon keys:** under `rillkit:`, with the stream name encoded as `%`→`%25` and `:`→`%3A`, so `(stream, group)` pairs can never collide.
- **Reserved namespace:** `rillkit:` is reserved for RillKit; streams should not be named with it.

### 4.3 Guarantees

Tested in rillkit today: data keys share their stream's slot, never collide across tenants, prefixes or tenant and non-tenant builders, and no stream tag can equal the control tag. The control keys have the same tests since milestone 1, and every SDK checks them through `conformance/keys.json`.

- **Stream-scoped keys share the stream's hash slot.** The DLQ move, replay, and cordon check-and-write can be atomic on a cluster.
- **Control keys share one slot per namespace.** Directive dedup plus state write is one `MULTI`/`EXEC` on a cluster. Channels carry the same tag, so classic `PUBLISH` and sharded `SPUBLISH` both work.
- **No stream can produce the control tag.** Stream names are non-empty, and their colons are encoded, so a stream tag never ends with `:`.
- **Keys never collide across tenants, prefixes, or tenant and non-tenant builders.**
- **Lua is optional.** Every multi-key operation above has a same-slot `MULTI`/`EXEC` form or a defined ordered fallback.
- **Nothing needs `SCAN`, `KEYS` or `RANDOMKEY`.** Every key is either computed from known names or listed in a registry (consumers per stream and group, nodes per tenant). Tenant ACL users can therefore be denied those commands, which otherwise reveal other tenants' key names (4.4).

### 4.4 Tenant isolation on the shared cluster

Every tenant's keys and channels start with `rk:{<tenant>:` (the tenant is encoded, and the pattern ends at its `:`), so one ACL user per tenant fences them. For tenant `t-42`:

```text
ACL SETUSER tenant-t-42 on >SECRET resetkeys resetchannels \
  ~rk:{t-42:* &rk:{t-42:* \
  -@all +@connection +@stream +@string +@hash +@sortedset +@transaction +del +exists +expire \
  +ssubscribe +sunsubscribe +spublish +subscribe +unsubscribe +publish \
  +cluster|slots +cluster|shards +cluster|nodes +readonly +readwrite
```

- **Allowlist, not `+@all -@dangerous`.** Verified on Redis 7 and Valkey 8: key patterns stop a tenant from reading or writing other tenants' keys, but **not** from listing their names with `SCAN` (`SCAN` is not in `@dangerous`). `SCAN` returned `shop:{t-43:payroll}:stream` to tenant `t-42`. The rule above leaves out `SCAN`, `KEYS`, `RANDOMKEY`, `PUBSUB` introspection, `CLIENT`, `MONITOR`, `CONFIG`, `INFO` and every admin command.
- rillkit's test suite (`sdks/python/tests/test_acl.py`) runs a whole worker as a user with exactly this rule, on a single server and on a cluster: publishing, consuming, retries, DLQ, replay, `Idempotent`, heartbeats and `active_workers`. `+del` (clean-stop heartbeat removal) and `+exists` (`Idempotent`) were found to be required that way, and `+expire` (node heartbeat and per-message log TTLs) when the managed-worker test was added in milestone 1. Every control-plane feature that adds a command must extend that test and this rule together.
- `rillkit-server` uses its own privileged user. Tenants never receive it.
- Hash tags place keys; ACL users isolate them.
- **Not solved by keys (platform work):** there are no per-tenant memory or CPU quotas in Valkey/Redis. Bound growth with platform defaults (stream `maxlen`, DLQ trimming, log TTLs) and watch per-tenant usage.

---

## 5. Wire Protocol Envelope (Worker $\leftrightarrow$ Control Plane)

### 5.1 Worker $\rightarrow$ Control Plane (Telemetry Report)
```json
{
  "type": "TELEMETRY_REPORT",
  "node_id": "worker-node-01",
  "timestamp": 1763137140,
  "keys": {
    "layout": "hashtag",
    "prefix": "rk",
    "tenant": null
  },
  "workers": {
    "active": 4,
    "in_flight": 4,
    "state": "RUNNING",
    "poll_ms": 1000,
    "batch_size": 10,
    "cpu_pct": 24.5,
    "mem_mb": 142
  },
  "streams": [
    {
      "stream": "orders",
      "group": "billing",
      "stream_key": "rk:{orders}:stream",
      "dlq_key": "rk:{orders}:dlq",
      "lag": 4820,
      "pending": 12,
      "ack_rate_sec": 38.5,
      "attempts_total": 1840,
      "wait_ms_total": 2145600.0,
      "work_ms_total": 73600.0
    }
  ]
}
```

`keys` describes the node's key builder (`layout` is `plain`, `hashtag` or `custom`). The self-hosted direct-Valkey mode uses it to build control keys with the rules in 4.2. `stream_key` and `dlq_key` are the physical keys of each stream, which the dashboard uses for lag, DLQ browsing and replay instead of deriving names. For a `custom` layout, the server only uses reported keys and relays directives through a node rather than writing to Valkey itself.

`attempts_total`, `wait_ms_total` and `work_ms_total` are cumulative since the worker started:
handler attempts, their summed wait from publish to handler start, and their summed handler
duration (both in milliseconds; data-plane section 9). They are optional: a reader MUST treat an
absent value as unknown. With the deltas between two reports the server shows the average wait
and work per attempt; it never stores per-message timings.

### 5.2 Control Plane $\rightarrow$ Worker (Directive)
```json
{
  "type": "DIRECTIVE",
  "directive_id": "dir_99812",
  "cmd": "SET_CONCURRENCY",
  "scope": "node",
  "target": {
    "node_id": "worker-node-01",
    "stream": "orders",
    "group": "billing"
  },
  "params": {
    "concurrency": 12,
    "batch_size": 25,
    "poll_ms": 200
  },
  "reason": "AUTOSCALE_LAG_THRESHOLD_EXCEEDED"
}
```

### 5.3 Worker $\rightarrow$ Control Plane (Compliance ACK)
```json
{
  "type": "DIRECTIVE_ACK",
  "directive_id": "dir_99812",
  "node_id": "worker-node-01",
  "status": "APPLIED",
  "current_state": {
    "concurrency": 12,
    "state": "RUNNING"
  }
}
```

### 5.4 The Relay session

Schemas for every frame are in [protocol/v1](protocol/v1). One WebSocket connection carries one
session; every frame is one JSON text message.

1. The worker connects to `<control url>/relay/v1` (`wss://`; `ws://` only when the worker opts
   in to an insecure connection) with `Authorization: Bearer <api key>`.
2. The worker sends `HELLO`: its node id, its `keys` descriptor (5.1), its SDK and the highest
   protocol version it speaks.
3. The server checks the key and the descriptor. A tenant in Cloud mode (`enforce_keys`)
   requires `{"layout": "hashtag", "prefix": "rk", "tenant": <the key's tenant>}`. On success it
   answers `WELCOME` with the agreed protocol version and the telemetry interval. Otherwise it
   sends `ERROR` and closes:

   | `code` | Close code | Worker reaction |
   | --- | --- | --- |
   | `UNAUTHORIZED` | 4001 | Log an error; retry with backoff (the key may be restored) |
   | `TENANT_MISMATCH` | 4003 | Log an error; retry with backoff |
   | `UNSUPPORTED_VERSION` | 4006 | Log an error; retry with backoff |
   | `BAD_FRAME` | 4400 | Log; reconnect |
   | `REPLACED` | 4009 | Another connection with the same node id took over; reconnect with backoff |

   In every case the worker keeps processing under the last stored state: the Relay failing
   never stops consumption (2.0, "Processing fails open").
4. During the session:
   - worker → server: `TELEMETRY_REPORT` every `telemetry_interval_sec`, `DIRECTIVE_ACK` for
     every `DIRECTIVE`, `LOGS_RESPONSE` for every `LOGS_REQUEST`, `TRACE_RESPONSE` for every
     `TRACE_REQUEST`;
   - server → worker: `DIRECTIVE` (applied with the dedup rules in 2.1, then acknowledged),
     `LOGS_REQUEST` (answered from the log key of the message; empty when nothing was captured),
     `TRACE_REQUEST` (answered from the trace key of the trace id; empty when the worker keeps no
     trace records or the key expired) and `MESSAGES_REQUEST` (a page of the stream's or DLQ's
     entries, newest first, answered with `MESSAGES_RESPONSE`). Payloads and DLQ error texts are
     included only if the worker allows `inspect_messages` (`RILLKIT_INSPECT_MESSAGES`), which it
     announces in `HELLO` `capabilities` along with `capture_logs` and `trace_records`; otherwise
     only ids, sizes, delivery status and trace ids leave the worker.
   - Unknown frame types are ignored by both sides, so either can add optional frames within v1.
5. The worker reconnects after any disconnect with exponential backoff (1 s doubling to 30 s,
   ±50 % jitter), resetting after a successful `WELCOME`.

*Implementation note:* the server stores each node's latest telemetry, so any replica can serve
the dashboard, and routes a directive to the replica holding a suitable connection through
Postgres `LISTEN`/`NOTIFY`. Exactly one replica claims each directive before sending it, and the
worker-side dedup makes a repeated delivery harmless anyway.

---

## 6. Phased Implementation Plan for Coding Agent

### Milestone 1: `rillkit` SDK — Valkey Control Bus, State Machine & Log Capture (done in the Python SDK)
* Implement the control-mode switch and config loader from 2.0. `off` must stay byte-for-byte today's behaviour: a test asserts that a worker with control off touches no control key, opens no extra connection and imports no `rillkit[control]` dependency.
* Add `RILLKIT_TENANT` (with a matching `Settings.tenant` field and `[keys] tenant` in the config file) so a Cloud worker's key builder can come from configuration; today `tenant` can only be set in code through `HashTagKeys(...)`. A tenant implies the hash-tagged layout (`HashTagKeys(tenant=...)`, default prefix `rk`); a tenant combined with `key_layout="plain"` raises `ConfigurationError`. A Cloud worker therefore needs exactly one key setting.
* Optionally add `RILLKIT_KEY_PREFIX` for self-hosted users who share one server between several apps. Cloud does not use it.
* Add the control-plane key methods to `KeyBuilder`, `PlainKeys` and `HashTagKeys` exactly as in section 4 (state, dedup, nodes and node registry, channels, logs, cordon) before any feature uses them. Extend the uniqueness and same-slot tests to cover them.
* Add the tenant-user integration test from 4.4 to CI (cluster, allowlist ACL, full worker lifecycle).
* Implement startup state hydration (the state keys from section 4) before stream polling begins.
* Implement the async control-channel Pub/Sub listener and atomic directive deduplication (same-slot `MULTI`/`EXEC`, Lua optional; see 2.1 and 4.3).
* Implement elastic worker pool scaling (`SET_CONCURRENCY`) and deterministic state transitions (`RUNNING`, `PAUSED`, `MAINTENANCE`, `DRAINING`) without dropping in-flight messages.
* Implement dynamic `block_ms` and `batch_size` updates on the `XREADGROUP` loop.
* Implement `contextvars`-bound per-message execution log capture into the per-message log keys (section 4) and W3C `traceparent` span propagation.
* Record the wait and work of every handler attempt, report the totals in telemetry, and (opt-in) keep per-trace records in the trace key (section 4, data-plane 9).

### Milestone 2: `rillkit` Outbound Relay + `rillkit-server` Gateway & Autoscaler (done, except OpenTelemetry trace ingestion)
* Implement the persistent outbound WebSocket/HTTP2 Relay client in `rillkit` with exponential jittered backoff, telemetry reporting, and Relay-to-Valkey directive fan-out.
* Build `rillkit-server` Relay connection registry, targeted/cluster-wide directive router, and leader-elected Autoscaler policy evaluation loop.
* Add SSE log streaming and OpenTelemetry trace ingestion endpoints to `rillkit-server`. (On-demand log and trace fetching are implemented; live log streaming and OpenTelemetry ingestion are not.)

### Milestone 3: `rillkit-server` Modern Dashboard (done, except producer cordoning)
* Build the React/TypeScript SPA embedded in `rillkit-server`.
* Implement the Fleet & Autoscaler Console (live worker nodes, lag vs. concurrency charts, policy sliders, and Pause/Resume/Drain controls).
* Implement the Maintenance Mode & Stream Cordon switch with ordered FIFO replay on exit.
* Implement the wait/work bottleneck charts and the per-trace timeline from the timings the workers report (no OpenTelemetry spans).
* Implement the Stream & DLQ Replay Inspector (`EXECUTE_REPLAY`) and the Trace Waterfall + inline message detail.