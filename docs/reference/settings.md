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
| `key_layout` | `None` | `"plain"` or `"hashtag"`; `None` means plain standalone and hash-tagged on a cluster. See [key layout](/guide/cluster#key-layout). |
| `client_kwargs` | `{}` | Extra options for the client constructor, checked against what that client accepts. See [client options](/guide/configuration#client-options). |

Invalid values raise [`ConfigurationError`](/reference/exceptions) when the `Settings` is created (`backend`, `mode`, `key_layout`, `cluster_nodes`) or when the client is built (`client_kwargs`, cluster database).

The `repr` hides passwords, the password in `url`, and `client_kwargs` values whose name contains `password`, `secret`, `token` or `credential`.

## `Settings.from_env`

```python
Settings.from_env(prefix: str = "REDIS_", **overrides) -> Settings
```

Reads these environment variables. Keyword arguments override them.

| Variable | Maps to | Default if unset |
| --- | --- | --- |
| `RILLKIT_URL` | `url` (wins over `<prefix>URL`) | `None` |
| `<prefix>URL` | `url` | `None` |
| `<prefix>HOST` | `host` | `"localhost"` |
| `<prefix>PORT` | `port` (as `int`) | `6379` |
| `<prefix>DB` | `db` (as `int`) | `0` |
| `<prefix>USERNAME` | `username`, only without a URL | `None` |
| `<prefix>PASSWORD` | `password`, only without a URL | `None` |
| `<prefix>SSL` | `ssl`: true for `1`, `true`, `yes` (any case) | `False` |
| `<prefix>SOCKET_TIMEOUT` | `socket_timeout`: seconds, or `none`/`off`/`0` | `15.0` |
| `RILLKIT_BACKEND` | `backend` | `"redis"` |
| `RILLKIT_MODE` | `mode` | `"standalone"` |
| `RILLKIT_CLUSTER_NODES` | `cluster_nodes`, comma-separated | `()` |
| `RILLKIT_KEY_LAYOUT` | `key_layout` | `None` |

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
