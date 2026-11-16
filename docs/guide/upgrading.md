# Upgrading

## Docs for other versions and what changed

The version picker in the top bar switches between the latest docs and the docs of every release, keeping you on the same page when it exists there. [All versions](/versions) lists each release with its tag, its docs, the release notes when there are any, and a GitHub compare link to the release before it, such as `https://github.com/NimzyMaina/RillKit/compare/v0.1.0...v0.1.1`. The compare view is the complete list of changes between two releases.

## From 0.3 to 0.4 (unreleased)

**Without the control plane, nothing changes in behaviour.** It is off unless you turn it on, and off means no extra keys, connections, threads or log handlers.

New:

- An optional [control plane](/guide/control-plane): `Control(mode="relay")` or `RILLKIT_CONTROL=relay`, with [`rillkit-server`](/guide/server) and its [dashboard](/guide/dashboard). It needs `pip install "rillkit[control]"`.
- A [config file](/guide/configuration#config-file) (`RILLKIT_CONFIG`), and `RILLKIT_TENANT` and `RILLKIT_KEY_PREFIX`, which imply hash-tagged keys.
- `subscribe(..., concurrency=n)`: several consumer loops per stream.
- `Broker.replay_dlq_entries`, `delete_dlq_entries`, `purge_dlq` and `create_group`.
- [`TraceContext`](/reference/middleware#tracecontext) (W3C `traceparent` across streams) and [`LogCapture`](/reference/middleware#logcapture) (per-message logs), as middleware you can use without the control plane too.
- Attempt timings in telemetry (`attempts_total`, `wait_ms_total`, `work_ms_total`), shown as the dashboard's **Time per message** chart: queue wait against handler time, for finding the bottleneck of a stream.
- `Control(trace_records=True)` / `RILLKIT_TRACE_RECORDS`, `broker.control.trace(trace_id)` and **View trace** on the dashboard: every handler attempt of one trace, across streams, kept for an hour. Metadata only, never payloads or exception text.
- `Settings(max_connections=...)` / `RILLKIT_MAX_CONNECTIONS`: the client connection-pool cap. redis-py 8 defaults it to **100**, which a producer with many threads (or a consumer with more than 100 lanes) can exhaust; raise it to peak concurrent commands plus headroom.
- Heartbeats are now coalesced: a consumer writes its heartbeat at the top of the loop and after messages, but at most once every `min(5 s, max(0.5 s, heartbeat_ttl/3))` instead of after every message. Liveness and `active_workers` are unchanged; it removes three Redis commands per message. A handler that runs longer than `heartbeat_ttl` still looks gone, as before.
- `Broker.publish_many(stream, bodies)`: several entries in one pipeline, with middleware (and `TraceContext`) applied per entry. Use it for high publish rates; `publish` is unchanged.
- `rillkit-server` serves `GET /api/metrics` for Prometheus (per tenant fleet gauges), and caches each tenant's overview for a second, with a single build at a time, so large fleets do not multiply overview work.
- The autoscaler now sizes from the observed arrival rate as well as lag. A stream that is keeping up at zero lag stays at its current worker count instead of dropping to `min_workers` and oscillating; it scales down when the rate actually falls. `last_decision` now includes the rate. A new autoscaler leader seeds the rate and per-worker capacity from the latest samples, so a failover does not restart the estimate from zero.
- `rillkit-server` is published as `ghcr.io/nimzymaina/rillkit-server`, with a no-clone Compose file and installer under `deploy/`. Running it needs Postgres and nothing else; see [Running the server](/guide/server#deploy-it).

**License.** From 0.4 the SDK is licensed under the Apache License 2.0; releases up to and including 0.3.0 stay MIT. Apache 2.0 is permissive like MIT, and adds an explicit patent grant: nothing changes in how you may use it. `rillkit-server` and the dashboard are a separate project in the same repository, under FSL-1.1-ALv2 (see [the server's license](/guide/server#license)).

For ACL users (several tenants on one server): a managed worker also needs `+expire`, now part of the [documented rule](/guide/keys#several-tenants-on-one-server).

## From 0.2 to 0.3

**Single-server users with the default keys: nothing changes.** Stream, DLQ, heartbeat and `Idempotent` keys are identical.

New:

- `RILLKIT_HOST`, `RILLKIT_PORT`, `RILLKIT_DB`, `RILLKIT_USERNAME`, `RILLKIT_PASSWORD`, `RILLKIT_SSL` and `RILLKIT_SOCKET_TIMEOUT`, each winning over its `REDIS_` name. A URL still replaces the separate fields; setting one of them next to a URL now logs a warning. See [environment variables](/guide/configuration#environment-variables).
- `KeyBuilder.idempotency(stream, key)`: `Idempotent` markers follow the broker's key builder, so brokers with different prefixes or tenants no longer share them. See [`Idempotent` markers](/guide/keys#idempotent-markers).
- A consumer registry per stream and group (`KeyBuilder.consumers`). `active_workers` reads it instead of scanning for heartbeat keys, so it works for ACL users that may not run `SCAN`. See [several tenants on one server](/guide/keys#several-tenants-on-one-server) for an ACL rule that hides other tenants' key names.

For **everyone**: `active_workers` only lists workers running 0.3 or newer, since older workers write no registry entry. Upgrade all workers of a group together, or expect older ones to be missing from `active_workers` until they are upgraded. Heartbeat keys themselves are unchanged.

Changes for **`HashTagKeys`** users (clusters, or `RILLKIT_KEY_LAYOUT=hashtag`):

- `Idempotent` markers move from `rillkit:done:<stream>:<key>` to `rk:{<stream>}:done:<key>` (with your prefix and tenant). Markers written by 0.2 are not checked, so a message redelivered right after the upgrade can be processed once more. Pass `Idempotent(prefix="rillkit:done:")` to keep the 0.2 location.
- Colons in stream names are now percent-encoded inside the tag: stream `orders:eu` moves from `rk:{orders:eu}:stream` to `rk:{orders%3Aeu}:stream`, and the same for its DLQ and heartbeats. Streams without a colon keep their keys. In 0.2, stream `t-42:orders` and stream `orders` with tenant `t-42` shared keys; they no longer can. Drain or move affected streams before upgrading (pending entries and DLQ entries stay under the old keys).
- An empty stream name raises `ConfigurationError` instead of producing keys that do not share a slot.
- Custom `KeyBuilder` subclasses keep `rillkit:done:<stream>:<key>` for markers unless they override `idempotency`.

## From 0.1 to 0.2: Redis and Valkey support

Nothing changes for an existing Redis deployment. Upgrade and keep your configuration:

- Redis (redis-py) is still the default and the only required dependency.
- `REDIS_URL` and the other `REDIS_*` variables work as before, including `Broker.from_env(prefix=...)`.
- Standalone key names are unchanged (`orders`, `orders-dlq`, `rillkit:workers:...`), so pending messages, DLQs and heartbeats carry over.
- `Broker(client=...)`, `Broker(url)` and `Broker(settings=...)` behave the same, and `stop()` still closes the client.

New, all optional:

- `pip install "rillkit[valkey]"` and `Settings(backend="valkey")` (or `RILLKIT_BACKEND=valkey`) to use valkey-py.
- `Settings(mode="cluster", cluster_nodes=[...])` (or `RILLKIT_MODE=cluster`) for Redis Cluster and Valkey Cluster. See [Redis and Valkey Cluster](/guide/cluster).
- Key builders that map logical stream names to physical keys, with prefixes and tenants. See [Keys](/guide/keys).
- `RILLKIT_*` environment variables: `RILLKIT_URL`, `RILLKIT_HOST`, `RILLKIT_PORT`, `RILLKIT_DB`, `RILLKIT_USERNAME`, `RILLKIT_PASSWORD`, `RILLKIT_SSL`, `RILLKIT_SOCKET_TIMEOUT`, plus `RILLKIT_BACKEND`, `RILLKIT_MODE`, `RILLKIT_CLUSTER_NODES` and `RILLKIT_KEY_LAYOUT`. Each `RILLKIT_` connection variable wins over its `REDIS_` name; moving to them is optional. `REDIS_USERNAME` is new for ACL users.
- `Settings(username=..., client_kwargs={...})` to pass any client option.
- `Broker(keys=...)`, `broker.keys`, `msg.key`, and `PlainKeys`, `HashTagKeys`, `KeyBuilder`.
- `ConfigurationError` for bad settings.

Small differences to check:

- `client_kwargs` options are validated: an option the client does not accept, or one that has its own `Settings` field, raises `ConfigurationError` when the client is built.
- `Settings(url=..., password=...)` used to ignore `password`. Now `username` and `password` fill in when the URL has no credentials of its own (credentials in the URL still win). `Settings.from_env()` is unaffected: it does not read `REDIS_PASSWORD` when a URL is set.
- `repr(Settings(...))` lists the new fields and hides passwords.
- A `redis.RedisCluster` passed as `Broker(client=...)` now uses hash-tagged keys (`rk:{orders}:stream`). RillKit 0.1 did not support clusters; if you ran one anyway with the plain names, keep them with `Broker(client=..., keys=PlainKeys())`.
