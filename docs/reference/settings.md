# Settings

```python
from rillkit import Settings
```

```python
@dataclass
class Settings:
    url: Optional[str] = None
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    ssl: bool = False
    socket_timeout: Optional[float] = 15.0
    backend: str = "redis"
    mode: str = "standalone"
    username: Optional[str] = None
    cluster_nodes: Sequence[Union[str, tuple[str, int]]] = ()
    key_layout: Optional[str] = None
    client_kwargs: Mapping[str, Any] = field(default_factory=dict)
```

| Field | Default | Description |
| --- | --- | --- |
| `url` | `None` | A URL (`redis://`, `rediss://`, `unix://`). If set, `host`, `port`, `db` and `ssl` are ignored. On a cluster it is one seed node. |
| `host` | `"localhost"` | Server host (or the seed node on a cluster without `url` and `cluster_nodes`). |
| `port` | `6379` | Server port. |
| `db` | `0` | Database number. Must be 0 on a cluster. |
| `password` | `None` | Password. Used only when `url` has no credentials. |
| `ssl` | `False` | Use TLS. |
| `socket_timeout` | `15.0` | Read timeout in seconds, `None` for none. Keep it above `block_ms`. |
| `backend` | `"redis"` | `"redis"` (redis-py) or `"valkey"` (valkey-py, needs `rillkit[valkey]`). |
| `mode` | `"standalone"` | `"standalone"` or `"cluster"`. |
| `username` | `None` | ACL username. Used only when `url` has no credentials. |
| `cluster_nodes` | `()` | Cluster seed nodes: `"host:port"` or `(host, port)`. A comma-separated string also works. |
| `key_layout` | `None` | `"plain"` or `"hashtag"`; `None` means plain standalone and hash-tagged on a cluster. See [Keys](/guide/keys). |
| `client_kwargs` | `{}` | Extra options for the client constructor, checked against what that client accepts. See [client options](/guide/configuration#client-options). |

Invalid values raise [`ConfigurationError`](/reference/exceptions) when the `Settings` is created (`backend`, `mode`, `key_layout`, `cluster_nodes`) or when the client is built (`client_kwargs`, cluster database).

The `repr` hides passwords, the password in `url`, and `client_kwargs` values whose name contains `password`, `secret`, `token` or `credential`.

## `Settings.from_env`

```python
Settings.from_env(prefix: str = "REDIS_", **overrides) -> Settings
```

Reads these environment variables. Keyword arguments override them. For each connection field a non-empty `RILLKIT_` variable wins over the `<prefix>` one (`REDIS_` by default); an empty one counts as unset.

| Variable | Fallback | Maps to | Default if unset |
| --- | --- | --- | --- |
| `RILLKIT_URL` | `<prefix>URL` | `url` | `None` |
| `RILLKIT_HOST` | `<prefix>HOST` | `host` | `"localhost"` |
| `RILLKIT_PORT` | `<prefix>PORT` | `port` (as `int`) | `6379` |
| `RILLKIT_DB` | `<prefix>DB` | `db` (as `int`) | `0` |
| `RILLKIT_USERNAME` | `<prefix>USERNAME` | `username`, only without a URL | `None` |
| `RILLKIT_PASSWORD` | `<prefix>PASSWORD` | `password`, only without a URL | `None` |
| `RILLKIT_SSL` | `<prefix>SSL` | `ssl`: true for `1`, `true`, `yes` (any case) | `False` |
| `RILLKIT_SOCKET_TIMEOUT` | `<prefix>SOCKET_TIMEOUT` | `socket_timeout`: seconds, or `none`/`off`/`0` | `15.0` |
| `RILLKIT_BACKEND` | | `backend` | `"redis"` |
| `RILLKIT_MODE` | | `mode` | `"standalone"` |
| `RILLKIT_CLUSTER_NODES` | | `cluster_nodes`, comma-separated | `()` |
| `RILLKIT_KEY_LAYOUT` | | `key_layout` | `None` |
| `RILLKIT_KEY_PREFIX` | | `key_prefix` | `None` |
| `RILLKIT_TENANT` | | `tenant` | `None` |

`Settings.from_env(prefix="REDIS_", *, config=None, **overrides)`: `config` (else `RILLKIT_CONFIG`) names a [config file](/guide/configuration#config-file) whose `[connection]` and `[keys]` sections fill in what the environment leaves unset.

When a URL is set, the username and password variables are not read, and host, port, db and ssl are ignored by the client. Setting `RILLKIT_HOST`, `PORT`, `DB`, `USERNAME`, `PASSWORD` or `SSL` next to a URL logs a warning on the `rillkit` logger.

## `Settings.client`

```python
Settings.client() -> redis.Redis | redis.RedisCluster | valkey.Valkey | valkey.ValkeyCluster
```

Builds a new client for `backend` and `mode`:

- standalone with `url`: `Redis.from_url(url, ...)` (or `Valkey.from_url`)
- standalone without: `Redis(host=..., port=..., db=..., ssl=..., ...)`
- cluster: `RedisCluster(startup_nodes=[...], ...)`, or `RedisCluster.from_url(url, startup_nodes=[...], ...)` with a URL

plus `socket_timeout`, `username`/`password` when they apply, and `client_kwargs`.

## `Settings.key_builder`

```python
Settings.key_builder() -> KeyBuilder
```

The [key builder](/reference/keys) for `key_layout`, or the default for `mode`.

See [Configuration](/guide/configuration) and [Redis and Valkey Cluster](/guide/cluster).
