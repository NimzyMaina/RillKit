# Configuration

A `Broker` needs a connection to a Redis or Valkey server. There are four ways to give it one: a URL, a [`Settings`](/reference/settings) object, environment variables, or a client you built yourself.

## A URL

```python
broker = Broker("redis://localhost:6379/0")
broker = Broker("redis://:secret@redis.internal:6379/2")
```

With no arguments, `Broker()` connects to `localhost:6379`, db 0, with redis-py.

## Redis or Valkey

RillKit speaks to the server through either client library. Redis (redis-py) is the default and always installed. Valkey (valkey-py) is an optional extra:

```bash
pip install "rillkit[valkey]"
```

Pick it with `backend`:

```python
from rillkit import Broker, Settings

broker = Broker(settings=Settings(backend="valkey", url="redis://valkey.internal:6379/0"))
```

The `backend` chooses the **client library**, not the server: both libraries talk to both servers, since Valkey keeps the Redis protocol. URLs use the `redis://` and `rediss://` schemes with either one. Everything else in RillKit (streams, retries, the DLQ, middleware) behaves the same.

| `backend` | `mode="standalone"` | `mode="cluster"` |
| --- | --- | --- |
| `"redis"` (default) | `redis.Redis` | `redis.RedisCluster` |
| `"valkey"` | `valkey.Valkey` | `valkey.ValkeyCluster` |

For clusters, see [Redis and Valkey Cluster](/guide/cluster).

## Settings

[`Settings`](/reference/settings) is a dataclass:

```python
from rillkit import Broker, Settings

broker = Broker(settings=Settings(
    host="redis.internal", port=6380, db=2, username="app", password="secret", ssl=True,
))
```

If `url` is set, `host`, `port`, `db` and `ssl` are ignored (the URL says all of that).

## Credentials in URLs

```text
redis://username:password@hostname:6379/0     # ACL user
redis://:password@hostname:6379/0             # password only (the default user)
rediss://username:password@hostname:6379/0    # the same over TLS
```

Characters that mean something in a URL (`@ : / % # ?` and so on) must be **percent-encoded** in the username and password. `p@ss:w/rd` becomes `p%40ss%3Aw%2Frd`. In Python:

```python
from urllib.parse import quote

url = f"rediss://{quote(user, safe='')}:{quote(password, safe='')}@redis.example.com:6379/0"
```

Which credentials win when there are two sources:

- **Credentials in the URL win.** `username` and `password` set separately on `Settings` are used only when the URL has none.
- `Settings.from_env()` does not read `REDIS_USERNAME` or `REDIS_PASSWORD` at all when a URL is set, the same as RillKit 0.1. Put the credentials in the URL.

Passwords never appear in the `repr` of a `Settings` (or in its URL), so logging a `Settings` is safe.

## Client options

`client_kwargs` passes any other option straight to the client constructor: timeouts, pool size, health checks, decoding, TLS certificates.

```python
Settings(
    backend="redis",
    url="rediss://username:password@redis.example.com:6379/0",
    client_kwargs={
        "socket_connect_timeout": 5,
        "socket_timeout": 15,
        "health_check_interval": 30,
        "max_connections": 50,
        "ssl_ca_certs": "/etc/ssl/certs/ca.pem",
    },
)
```

The options are checked against the client that will be built, so a typo fails at once instead of being dropped:

- An option the client does not accept raises `ConfigurationError` naming it.
- Options with their own `Settings` field (`host`, `port`, `db`, `username`, `password`, `ssl`, and `startup_nodes`, which is `cluster_nodes`) are refused in `client_kwargs`. Set the field instead.
- `socket_timeout` is allowed in both places; `client_kwargs` wins.
- Cluster clients forward only some options to their nodes and silently ignore the rest. RillKit refuses an option the cluster client would ignore, and a cluster-only option (such as `require_full_coverage`) in standalone mode.

Which options exist depends on the client library and its version. The redis-py and valkey-py lists are similar but not identical (for example `ssl_check_hostname` is redis-py only).

## Environment variables

`Broker.from_env()` (or `Settings.from_env()`) reads:

| Variable | Default | Notes |
| --- | --- | --- |
| `RILLKIT_URL` | none | Wins over `REDIS_URL` |
| `REDIS_URL` | none | If set (and `RILLKIT_URL` is not), takes priority over the host/port variables below |
| `REDIS_HOST` | `localhost` | |
| `REDIS_PORT` | `6379` | converted to `int` |
| `REDIS_DB` | `0` | converted to `int` |
| `REDIS_USERNAME` | none | not read when a URL is set |
| `REDIS_PASSWORD` | none | not read when a URL is set |
| `REDIS_SSL` | off | `1`, `true` or `yes` (any case) turn TLS on |
| `REDIS_SOCKET_TIMEOUT` | `15` | seconds, or `none` for no timeout. Must be longer than `block_ms` (see below) |
| `RILLKIT_BACKEND` | `redis` | `redis` or `valkey` |
| `RILLKIT_MODE` | `standalone` | `standalone` or `cluster` |
| `RILLKIT_CLUSTER_NODES` | none | cluster seed nodes, `host:port,host:port` |
| `RILLKIT_KEY_LAYOUT` | depends on mode | `plain` or `hashtag`, see [key layout](/guide/cluster#key-layout) |

The `REDIS_` variables take the prefix you pass (`Settings.from_env("MYAPP_REDIS_")` reads `MYAPP_REDIS_URL`); the `RILLKIT_` ones are always read under that name.

Precedence, from strongest to weakest:

1. Keyword arguments to `Settings.from_env(...)`, such as `Settings.from_env(backend="valkey")`.
2. `RILLKIT_URL`.
3. `<prefix>URL` (`REDIS_URL` by default).
4. Without a URL: `<prefix>HOST`, `PORT`, `DB`, `USERNAME`, `PASSWORD` and `SSL`.

Without `RILLKIT_BACKEND` the backend is Redis, and without `RILLKIT_MODE` the mode is standalone, so a deployment that only sets `REDIS_URL` behaves exactly as it did before.

```python [g_config.py]
import os

from rillkit import Broker, Settings

os.environ.update(REDIS_URL="redis://localhost:6390/3", REDIS_PASSWORD="ignored-with-a-url")
s = Settings.from_env()
print(s.backend, s.mode, s.url, s.password)

os.environ.update(RILLKIT_URL="redis://app:s3cr%40t@localhost:6390/3")
print(repr(Settings.from_env()).split(", host")[0])

print(Settings.from_env(backend="valkey").client())
```

```text
redis standalone redis://localhost:6390/3 None
Settings(url='redis://app:***@localhost:6390/3'
<valkey.client.Valkey(<valkey.connection.ConnectionPool(<valkey.connection.Connection(host=localhost,port=6390,db=3)>)>)>
```

`Broker.from_env(prefix="REDIS_", **kwargs)` forwards extra keyword arguments such as `middlewares=[...]` to `Broker`. To override settings, build them yourself: `Broker(settings=Settings.from_env(backend="valkey"))`.

## TLS and managed Redis or Valkey (Upstash and friends)

Hosted services usually require TLS. Either use a `rediss://` URL (note the double s), or set `ssl`:

::: code-group
```bash [Environment]
export REDIS_URL="rediss://default:YOUR_PASSWORD@eu1-example-12345.upstash.io:6379"
```
```bash [Host and port]
export REDIS_HOST=eu1-example-12345.upstash.io
export REDIS_PORT=6379
export REDIS_PASSWORD=YOUR_PASSWORD
export REDIS_SSL=true
```
```python [In code]
broker = Broker("rediss://default:YOUR_PASSWORD@eu1-example-12345.upstash.io:6379")
```
:::

Certificate options (`ssl_ca_certs`, `ssl_certfile`, `ssl_keyfile`, `ssl_cert_reqs`, ...) go in `client_kwargs`.

::: warning Blocking reads and server timeouts
RillKit blocks in `XREADGROUP` for up to `block_ms` (5 seconds by default). Some managed proxies close idle connections or limit command duration. If you see repeated connection errors in the logs, lower `block_ms` and check the provider's idle timeout. RillKit logs the error and retries after 5 seconds rather than crashing.
:::

## Bring your own client

Pass any client you have configured: `redis.Redis`, `redis.RedisCluster`, `valkey.Valkey` or `valkey.ValkeyCluster` (custom pools, retries, Sentinel, and so on):

```python
import redis
from rillkit import Broker

client = redis.Redis(host="redis.internal", socket_timeout=10, health_check_interval=30)
broker = Broker(client=client)
```

RillKit uses the client as it is and does not build another one. It works with both `decode_responses=False` (the default) and `True`. Two cautions:

- A client `socket_timeout` shorter than `block_ms` makes blocking reads time out. RillKit checks the client's timeout when the consumer starts and, if needed, shortens its own blocking reads to one second under it, logging a warning. This matters on redis-py 8, whose default `socket_timeout` is 5 seconds, the same as the default `block_ms`. Clients that RillKit builds from `Settings` use 15 seconds.
- `broker.stop()` closes the client, so do not share a client you still need afterwards.

## Supported versions

| | Supported | Tested in CI |
| --- | --- | --- |
| Python | 3.9 to 3.13 | 3.9, 3.10, 3.11, 3.12, 3.13 |
| redis-py | 5.0 and newer | the locked version (7.0 on Python 3.9, 8.x on newer) and the newest release |
| valkey-py | 6.1.1 to 6.2.x | 6.1.1 on Python 3.9 and 3.10, 6.2 on 3.11 and newer (6.2 needs Python 3.11) |
| Redis server | 6.2 and newer | Redis 7, standalone and cluster |
| Valkey server | 7.2 and newer | Valkey 8, standalone and cluster |
