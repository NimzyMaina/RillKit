# Upgrading

## Docs for other versions and what changed

The version picker in the top bar switches between the latest docs and the docs of every release, keeping you on the same page when it exists there. [All versions](/versions) lists each release with its tag, its docs, the release notes when there are any, and a GitHub compare link to the release before it, such as `https://github.com/NimzyMaina/RillKit/compare/v0.1.0...v0.1.1`. The compare view is the complete list of changes between two releases.

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
