# Keys

Your code only ever uses **logical names**: `Stream("orders")`, `broker.publish("orders", ...)`, `broker.replay_dlq("orders")`. The broker's **key builder**, `broker.keys`, turns each one into the **physical keys** that are actually read and written: the stream, its dead-letter queue, the worker heartbeats and the `Idempotent` markers. Every command RillKit sends goes through it, so the naming rules live in one place.

## Why there is a key builder

**Clusters.** Redis Cluster and Valkey Cluster split keys over 16384 hash slots, and a transaction may only touch keys in one slot. When a message gives up, RillKit copies it to the DLQ and acknowledges it in the stream, and that has to happen together, or a crash in between could leave it in both places or neither. The cluster layout puts the stream, its DLQ and its heartbeats in one slot with a **hash tag** (only the part between `{` and `}` decides the slot), so the move stays one transaction.

**Sharing a server.** A prefix keeps applications, teams or environments apart on one server: `billing:{orders}:stream` and `search:{orders}:stream` never meet, even though both apps call their stream `orders`.

**Tenants.** An opaque tenant id inside the hash tag gives each tenant its own keys while the code stays the same: `rk:{t-42:orders}:stream`.

**Compatibility.** On a single server the default keeps the names RillKit has always used (`orders`, `orders-dlq`), so existing deployments, dashboards and producers that write to `orders` directly keep working.

## The example names on this page

Every input below has a different value, so you can tell which part of a key comes from where:

| Input | Example value | Where you set it |
| --- | --- | --- |
| stream name | **`orders`** | `Stream("orders", ...)` |
| consumer group | **`billing`** | `Stream(..., group="billing")` |
| worker (consumer) name | **`worker-7`** | `@broker.subscribe(..., consumer="worker-7")` |
| custom DLQ name | **`orders-failed`** | `Stream(..., dlq="orders-failed")` (optional) |
| idempotency key | **`req-1`** | `Idempotent(key=lambda m: m.body["request_id"])` |
| prefix | **`shop`** (default `rk`) | `HashTagKeys(prefix="shop")` |
| tenant | **`t-42`** (default none) | `HashTagKeys(tenant="t-42")` |

Everything else in a key is a fixed word added by RillKit: `rillkit:workers`, `rillkit:done`, `-dlq`, `stream`, `dlq`, `workers`, `done`.

```python
orders = Stream("orders", group="billing")                          # default DLQ
orders_custom = Stream("orders", group="billing", dlq="orders-failed")

@broker.subscribe(orders, consumer="worker-7")
def handle(order: dict): ...
```

## `PlainKeys`: a single server (default)

| Key | Result |
| --- | --- |
| stream | `orders` |
| DLQ (default) | `orders-dlq` |
| DLQ (custom) | `orders-failed` |
| worker heartbeat | `rillkit:workers:orders:billing:worker-7` |
| `Idempotent` marker | `rillkit:done:orders:req-1` |

```text
orders                                   ← the stream name, as is

orders-dlq                               ← default DLQ
└─┬──┘└┬─┘
  │    └── fixed suffix "-dlq"
  └─────── stream name

orders-failed                            ← custom DLQ: your dlq= value, as is

rillkit:workers:orders:billing:worker-7  ← worker heartbeat
└──────┬──────┘ └─┬──┘ └──┬──┘ └──┬───┘
       │          │       │       └── worker name
       │          │       └────────── consumer group
       │          └────────────────── stream name
       └───────────────────────────── fixed "rillkit:workers"

rillkit:done:orders:req-1                ← Idempotent marker
└────┬─────┘ └─┬──┘ └─┬─┘
     │         │      └── idempotency key
     │         └───────── stream name
     └─────────────────── fixed "rillkit:done"
```

On a cluster these keys usually land on different nodes. That works, but the move to the DLQ is then two ordered steps instead of one transaction (see [DLQ atomicity](/guide/cluster#dlq-atomicity)).

## `HashTagKeys`: a cluster (default there)

| Key | Result |
| --- | --- |
| stream | `rk:{orders}:stream` |
| DLQ (default) | `rk:{orders}:dlq` |
| DLQ (custom) | `rk:{orders}:dlq:orders-failed` |
| worker heartbeat | `rk:{orders}:workers:billing:worker-7` |
| `Idempotent` marker | `rk:{orders}:done:req-1` |

```text
rk:{orders}:stream                       ← stream
└┬┘└───┬──┘ └─┬──┘
 │     │      └── fixed "stream"
 │     └───────── hash tag: { stream name }
 └─────────────── prefix (default "rk")

rk:{orders}:dlq:orders-failed            ← custom DLQ
└┬┘└───┬──┘ └┬┘ └─────┬─────┘
 │     │     │        └── your dlq= value (absent for the default DLQ)
 │     │     └─────────── fixed "dlq"
 │     └───────────────── hash tag: { stream name }
 └─────────────────────── prefix

rk:{orders}:workers:billing:worker-7     ← worker heartbeat
└┬┘└───┬──┘ └──┬──┘ └──┬──┘ └──┬───┘
 │     │       │       │       └── worker name
 │     │       │       └────────── consumer group
 │     │       └────────────────── fixed "workers"
 │     └────────────────────────── hash tag: { stream name }
 └──────────────────────────────── prefix

rk:{orders}:done:req-1                   ← Idempotent marker
└┬┘└───┬──┘ └┬─┘ └─┬─┘
 │     │     │     └── idempotency key
 │     │     └──────── fixed "done"
 │     └────────────── hash tag: { stream name }
 └──────────────────── prefix
```

On a cluster only the text inside `{ }` decides which node holds a key. Every key of the stream shares `{orders}`, so they always live together, and moving a message to the DLQ is one transaction.

A custom DLQ name becomes a suffix inside the stream's tag so it stays in the stream's slot. Two streams that share `dlq="dead"` on a single server get one DLQ each here.

### Prefix and tenant

| Builder | Stream | DLQ (custom) | Worker heartbeat | `Idempotent` marker |
| --- | --- | --- | --- | --- |
| `HashTagKeys(prefix="shop")` | `shop:{orders}:stream` | `shop:{orders}:dlq:orders-failed` | `shop:{orders}:workers:billing:worker-7` | `shop:{orders}:done:req-1` |
| `HashTagKeys(prefix="")` | `{orders}:stream` | `{orders}:dlq:orders-failed` | `{orders}:workers:billing:worker-7` | `{orders}:done:req-1` |
| `HashTagKeys(tenant="t-42")` | `rk:{t-42:orders}:stream` | `rk:{t-42:orders}:dlq:orders-failed` | `rk:{t-42:orders}:workers:billing:worker-7` | `rk:{t-42:orders}:done:req-1` |
| `HashTagKeys(prefix="shop", tenant="t-42")` | `shop:{t-42:orders}:stream` | `shop:{t-42:orders}:dlq:orders-failed` | `shop:{t-42:orders}:workers:billing:worker-7` | `shop:{t-42:orders}:done:req-1` |

With the default DLQ, drop the last part: `shop:{t-42:orders}:dlq`.

```text
shop:{t-42:orders}:workers:billing:worker-7
└─┬┘ └─┬┘ └──┬─┘  └──┬──┘ └──┬──┘ └──┬───┘
  │    │     │       │       │       └── worker name
  │    │     │       │       └────────── consumer group
  │    │     │       └────────────────── fixed "workers"
  │    │     └────────────────────────── stream name  ┐ together, the hash tag
  │    └──────────────────────────────── tenant       ┘ {t-42:orders}
  └───────────────────────────────────── prefix
```

### Special characters

Inside the tag, `%`, `{`, `}` and `:` are percent-encoded in the stream name and the tenant. A tag therefore holds exactly one plain `:` when there is a tenant and none when there is not, so no stream name can end the tag early, land on another stream's keys, or produce another tenant's keys.

| Input | Key |
| --- | --- |
| stream `orders:eu` | `rk:{orders%3Aeu}:stream` |
| stream `orders:eu`, tenant `t-42` | `rk:{t-42:orders%3Aeu}:stream` |
| stream `a{b}c` | `rk:{a%7Bb%7Dc}:stream` |
| stream `50%` | `rk:{50%25}:stream` |
| tenant `acme:eu` | `rk:{acme%3Aeu:orders}:stream` |
| stream `""` (empty) | refused with `ConfigurationError`: `{}` is not a hash tag |

## Where the group and worker name appear

| Layout | Stream key | DLQ key | Heartbeat key |
| --- | --- | --- | --- |
| `PlainKeys` | stream name | stream name or your `dlq=` | stream + **group + worker** |
| `HashTagKeys` | prefix, (tenant), stream | prefix, (tenant), stream, (your `dlq=`) | prefix, (tenant), stream + **group + worker** |

Alongside the heartbeats, each stream and group has a **consumer registry**: a sorted set of the workers that are alive, each scored by when its heartbeat expires. `active_workers` reads it, so it never has to scan for keys.

| Layout | Consumer registry for group `billing` |
| --- | --- |
| `PlainKeys` | `rillkit:consumers:orders:billing` (a `:` or `%` in the stream name is encoded) |
| `HashTagKeys` | `rk:{orders}:consumers:billing`, in the stream's slot |
| `HashTagKeys(prefix="shop", tenant="t-42")` | `shop:{t-42:orders}:consumers:billing` |

- **Only heartbeats contain the group and worker name.** There is one per running worker, removed on a clean stop. The registry holds the worker names as members, under a key with the group name.
- The stream and DLQ are shared by every group and worker. A consumer group and its pending messages are stored *inside* the stream, not as keys of their own.

## `Idempotent` markers

[`Idempotent`](/guide/middleware#idempotent) asks the broker's key builder where to store its markers, so they follow the stream's prefix and tenant: `rillkit:done:orders:req-1` with `PlainKeys`, `shop:{t-42:orders}:done:req-1` with `HashTagKeys(prefix="shop", tenant="t-42")`. Brokers with different tenants or prefixes never share markers, and on a cluster a marker lives in its stream's slot.

Pass `prefix` to choose the location yourself: `Idempotent(key=..., prefix="app:seen:")` stores `app:seen:orders:req-1` whatever the layout.

## Choosing a layout

From strongest to weakest:

| How | Example |
| --- | --- |
| Pass a builder to the broker | `Broker.from_env(keys=HashTagKeys(prefix="shop", tenant="t-42"))` |
| Set a tenant or prefix in the environment | `RILLKIT_TENANT=t-42`, `RILLKIT_KEY_PREFIX=shop` (both imply `hashtag`) |
| Name a layout in the environment | `RILLKIT_KEY_LAYOUT=hashtag` (or `plain`) |
| Name a layout in code | `Settings(key_layout="hashtag")` |
| Nothing: follow the client | `PlainKeys` for a standalone client, `HashTagKeys` for a cluster client, including one you pass as `client=` |

The prefix and tenant can also be set as `Settings(key_prefix=..., tenant=...)`, or in the `[keys]` section of the [config file](/guide/configuration#config-file). Either one implies hash-tagged keys; combining them with `key_layout="plain"` raises `ConfigurationError`.

```bash
export RILLKIT_URL=redis://localhost:6379/0
```

```python [g_keys.py]
import os
import time

from rillkit import Broker, HashTagKeys, Idempotent, Message, Settings, Stream

orders = Stream("orders", group="billing")

# 1. the default on a single server: the names you pass are the keys
plain = Broker.from_env()  # RILLKIT_URL
print(plain.keys)
print(" ", plain.keys.stream(orders), plain.keys.dlq(orders))
print(" ", plain.keys.heartbeat(orders, "billing", "worker-7"))

# 2. pick a layout from the environment
os.environ["RILLKIT_KEY_LAYOUT"] = "hashtag"
print(Settings.from_env().key_builder())

# 3. or pass a builder: your own prefix and a tenant id inside the hash tag
shop = Broker.from_env(
    keys=HashTagKeys(prefix="shop", tenant="t-42"),
    middlewares=[Idempotent(key=lambda m: m.body["request_id"])],
)
print(shop.keys)
print(" ", shop.keys.stream(orders), shop.keys.dlq(orders))


@shop.subscribe(orders, consumer="worker-7", block_ms=200)
def handle(order: dict, msg: Message):
    print("handled", order, "| stream:", msg.stream.name, "| key:", msg.key)


shop.start()
shop.publish(orders, {"id": 1, "request_id": "req-1"})  # still the logical name
time.sleep(0.5)
print("workers:", shop.active_workers(orders))
print("stream exists?", bool(shop.client.exists("shop:{t-42:orders}:stream")))
print("idempotency marker?", bool(shop.client.exists("shop:{t-42:orders}:done:req-1")))
shop.stop()
```

```text
PlainKeys()
  orders orders-dlq
  rillkit:workers:orders:billing:worker-7
HashTagKeys(prefix='rk', tenant=None)
HashTagKeys(prefix='shop', tenant='t-42')
  shop:{t-42:orders}:stream shop:{t-42:orders}:dlq
handled {'id': 1, 'request_id': 'req-1'} | stream: orders | key: shop:{t-42:orders}:stream
workers: ['worker-7']
stream exists? True
idempotency marker? True
```

The handler, `publish` and `active_workers` all use the name `orders`; only the key builder knows about `shop:{t-42:orders}:stream`.

## Several tenants on one server

When one Redis or Valkey server holds data for several tenants:

- **One broker per tenant**, each with `HashTagKeys(prefix=..., tenant=...)`. The tenant id is part of every stream, DLQ, heartbeat and `Idempotent` key, so the same stream name and the same idempotency key never mix between tenants.
- **Opaque tenant ids** such as `t-42`, not customer names: they end up in key names, logs and monitoring.
- **Take the tenant id from trusted server-side context** (who is authenticated), never from a message body or a client-supplied value.
- **Isolate with ACL users**, not with key names. Give each tenant a user that may only touch its own keys and channels, and only run the commands RillKit needs:

  ```text
  ACL SETUSER tenant-t-42 on >SECRET resetkeys resetchannels ~shop:{t-42:* &shop:{t-42:* -@all +@connection +@stream +@string +@hash +@sortedset +@transaction +del +exists +expire +ssubscribe +sunsubscribe +spublish +subscribe +unsubscribe +publish +cluster|slots +cluster|shards +cluster|nodes +readonly +readwrite
  ```

  The key pattern ends right after the tenant's `:`, so it does not match tenant `t-42x`, and it does not match keys built without a tenant. On a cluster, create the user on every node, replicas included: ACL users are per server.

  ::: warning Use an allowlist, not `+@all -@dangerous`
  Key patterns stop a user from reading or writing other keys, but **not from listing their names**: `SCAN` returns every key name in the database, and it is not in `@dangerous`. With `+@all -@dangerous`, tenant `t-42` can list `shop:{t-43:payroll}:stream` and learn another tenant's id and stream names. The allowlist above leaves out `SCAN`, `KEYS`, `RANDOMKEY`, `PUBSUB`, `CLIENT`, `INFO`, `CONFIG` and every admin command. RillKit never needs them: `active_workers` reads the [consumer registry](#where-the-group-and-worker-name-appear) instead of scanning.

  RillKit's test suite runs a whole worker (publishing, consuming, retries, the DLQ, replay, `Idempotent`, heartbeats and `active_workers`) as a user with exactly this rule, on a single server and on a cluster, and a [managed worker](/guide/control-plane) too (desired state, wakeups, node heartbeats, directives and log capture; `+expire` is for their TTLs).
  :::
- **One client per broker.** Each broker has its own connection pool and consumer threads, and `broker.stop()` closes its client, so do not share one client between tenant brokers.
- **Busy tenants:** each (tenant, stream) pair lives in one cluster slot. Spread a heavy tenant over several streams rather than expecting one stream to use several nodes.

## Finding the real keys

- `broker.keys.stream(s)`, `broker.keys.dlq(s)`, `broker.keys.heartbeat(s, group, consumer)`, `broker.keys.consumers(s, group)`, `broker.keys.idempotency(s, key)` and `broker.keys.trace(trace_id)` return them.
- `msg.key` is the physical key of the stream a message came from (`msg.stream.name` and the DLQ's `x-stream` field stay the logical name).
- The DLQ log line names the physical key: `moving message ... to rk:{orders}:dlq: ...`.
- In `redis-cli` or `valkey-cli`, quote keys with braces: `XLEN 'rk:{orders}:stream'`, and add `-c` on a cluster.

Anything that writes to a stream without RillKit (another service, a script) must use the physical key.

## Your own layout

Subclass [`KeyBuilder`](/reference/keys) and implement `stream`, `dlq` and `heartbeat_prefix`; override `idempotency` to move `Idempotent` markers too (by default they stay at `rillkit:done:<stream>:<key>`). On a cluster, keep the stream and DLQ in one hash slot so the DLQ move stays a transaction; if they are not, RillKit falls back to two ordered steps (see [DLQ atomicity](/guide/cluster#dlq-atomicity)).

## Control-plane keys

A [managed worker](/guide/control-plane) also uses these. With the control plane off, none of them is ever read or written.

| Purpose | Type | `PlainKeys` | `HashTagKeys(prefix="shop", tenant="t-42")` |
| --- | --- | --- | --- |
| Global desired state | HASH | `rillkit:state:global` | `shop:{t-42:}:state:global` |
| Stream desired state | HASH | `rillkit:state:stream:orders:billing` | `shop:{t-42:}:state:stream:orders:billing` |
| Node desired state | HASH | `rillkit:state:node:<node>` | `shop:{t-42:}:state:node:<node>` |
| Directive dedup | STRING, TTL 60 s | `rillkit:dedup:<directive>` | `shop:{t-42:}:dedup:<directive>` |
| Node heartbeat | HASH, TTL | `rillkit:nodes:<node>` | `shop:{t-42:}:nodes:<node>` |
| Node registry | ZSET | `rillkit:nodes` | `shop:{t-42:}:nodes` |
| Broadcast wakeups | channel | `rillkit:ctl:broadcast` | `shop:{t-42:}:ctl:broadcast` |
| Node wakeups | channel | `rillkit:ctl:node:<node>` | `shop:{t-42:}:ctl:node:<node>` |
| Per-message logs | STREAM, TTL | `rillkit:logs:orders:<id>` | `shop:{t-42:orders}:logs:<id>` |
| Per-trace records | STREAM, TTL 1 h | `rillkit:trace:<trace id>` | `shop:{t-42:}:trace:<trace id>` |
| Producer cordon (reserved) | STRING | `rillkit:cordon:orders` | `shop:{t-42:orders}:cordon` |
| Cordon buffer (reserved) | STREAM | `rillkit:cordon:orders:buffer` | `shop:{t-42:orders}:cordon:buffer` |

- The control keys of one namespace share a slot (the tag `{<tenant>:}`, or `{:}` without a tenant), so a directive's dedup check and state write are one transaction on a cluster. No stream can produce that tag, since stream names are non-empty and their colons are encoded.
- Logs and cordon keys share their stream's slot.
- Trace keys live with the control keys (under `{<tenant>:}`), because one trace crosses streams. They need no new Redis command: `XADD`, `XRANGE` and `EXPIRE` are already in the [tenant ACL rule](#several-tenants-on-one-server).
- With `PlainKeys`, stream names in these keys are encoded (`%`, `:`), and `rillkit:` is reserved: do not name streams `rillkit:...`.
- Custom `KeyBuilder`s get the `PlainKeys` names unless they override the methods.

::: warning Changing the layout moves your data
The layout decides which keys are read. Switching an existing deployment to another layout, prefix or tenant starts from empty streams; old pending messages, DLQ entries, consumer groups and `Idempotent` markers stay under the old keys.
:::

::: tip Hash tags are not access control
A prefix or tenant id only chooses names. It does not stop code from reading another tenant's keys; ACL users do.
:::
