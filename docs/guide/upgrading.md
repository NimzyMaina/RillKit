# Upgrading

## Docs for other versions and what changed

The version picker in the top bar switches between the latest docs and the docs of every release, keeping you on the same page when it exists there. [All versions](/versions) lists each release with its tag, its docs, the release notes when there are any, and a GitHub compare link to the release before it, such as `https://github.com/NimzyMaina/RillKit/compare/v0.1.0...v0.1.1`. The compare view is the complete list of changes between two releases.

## From 0.1 to 0.2: Redis and Valkey support

Nothing changes for an existing Redis deployment. Upgrade and keep your configuration:

- Redis (redis-py) is still the default and the only required dependency.
- `REDIS_URL` and the other `REDIS_*` variables work as before, including `Broker.from_env(prefix=...)`.
- Standalone key names are unchanged (`orders`, `orders-dlq`, `rillkit:workers:...`), so pending messages, DLQs and heartbeats carry over.
- `Broker(client=...)`, `Broker(url)` and `Broker(settings=...)` behave the same, and `stop()` still closes the client.

New, all optional:

- `pip install "rillkit[valkey]"` and `Settings(backend="valkey")` (or `RILLKIT_BACKEND=valkey`) to use valkey-py.
- `Settings(mode="cluster", cluster_nodes=[...])` (or `RILLKIT_MODE=cluster`) for Redis Cluster and Valkey Cluster. See [Redis and Valkey Cluster](/guide/cluster).
- `RILLKIT_URL`, which wins over `REDIS_URL`; `REDIS_USERNAME` for ACL users.
- `Settings(username=..., client_kwargs={...})` to pass any client option.
- `Broker(keys=...)`, `broker.keys`, `msg.key`, and `PlainKeys`, `HashTagKeys`, `KeyBuilder`.
- `ConfigurationError` for bad settings.

Small differences to check:

- `client_kwargs` options are validated: an option the client does not accept, or one that has its own `Settings` field, raises `ConfigurationError` when the client is built.
- `Settings(url=..., password=...)` used to ignore `password`. Now `username` and `password` fill in when the URL has no credentials of its own (credentials in the URL still win). `Settings.from_env()` is unaffected: it does not read `REDIS_PASSWORD` when a URL is set.
- `repr(Settings(...))` lists the new fields and hides passwords.
- A `redis.RedisCluster` passed as `Broker(client=...)` now uses hash-tagged keys (`rk:{orders}:stream`). RillKit 0.1 did not support clusters; if you ran one anyway with the plain names, keep them with `Broker(client=..., keys=PlainKeys())`.
