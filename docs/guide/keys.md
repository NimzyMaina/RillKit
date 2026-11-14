# Keys

Your code only ever uses **logical names**: `Stream("orders")`, `broker.publish("orders", ...)`, `broker.replay_dlq("orders")`. The broker's **key builder**, `broker.keys`, turns each one into the **physical keys** that are actually read and written: the stream, its dead-letter queue and the worker heartbeats. Every command RillKit sends goes through it, so the naming rules live in one place.

## Why there is a key builder

**Clusters.** Redis Cluster and Valkey Cluster split keys over 16384 hash slots, and a transaction may only touch keys in one slot. When a message gives up, RillKit copies it to the DLQ and acknowledges it in the stream, and that has to happen together, or a crash in between could leave it in both places or neither. The cluster layout puts the stream, its DLQ and its heartbeats in one slot with a **hash tag** (only the part between `{` and `}` decides the slot), so the move stays one transaction.

**Sharing a server.** A prefix keeps applications, teams or environments apart on one server: `billing:{orders}:stream` and `search:{orders}:stream` never meet, even though both apps call their stream `orders`.

**Tenants.** An opaque tenant id inside the hash tag gives each tenant its own keys while the code stays the same: `rk:{t-42:orders}:stream`.

**Compatibility.** On a single server the default keeps the names RillKit has always used (`orders`, `orders-dlq`), so existing deployments, dashboards and producers that write to `orders` directly keep working.

## The two layouts

| | `PlainKeys` | `HashTagKeys` |
| --- | --- | --- |
| default for | a standalone server | a cluster |
| stream | `orders` | `rk:{orders}:stream` |
| DLQ | `orders-dlq` | `rk:{orders}:dlq` |
| custom DLQ (`dlq="orders-failed"`) | `orders-failed` | `rk:{orders}:dlq:orders-failed` |
| heartbeat of consumer `w1` in group `workers` | `rillkit:workers:orders:workers:w1` | `rk:{orders}:workers:workers:w1` |
| with `prefix="billing", tenant="t-42"` | | `billing:{t-42:orders}:stream` |

`HashTagKeys` percent-encodes `{`, `}` and `%` in names (and `:` in the tenant), so no stream name can end the tag early or land on another stream's or tenant's keys.

## Configuring it

From the strongest to the weakest:

| How | Example |
| --- | --- |
| Pass a builder to the broker | `Broker.from_env(keys=HashTagKeys(prefix="billing", tenant="t-42"))` |
| Name a layout in the environment | `RILLKIT_KEY_LAYOUT=hashtag` (or `plain`) |
| Name a layout in code | `Settings(key_layout="hashtag")` |
| Nothing: follow the client | `PlainKeys` for a standalone client, `HashTagKeys` for a cluster client, including a cluster client you pass as `client=` |

```bash
export RILLKIT_URL=redis://localhost:6379/0
export RILLKIT_KEY_LAYOUT=hashtag   # optional: the cluster layout on a single server
```

```python [g_keys.py]
import os
import time

from rillkit import Broker, HashTagKeys, Message, Settings, Stream

orders = Stream("orders", group="workers")

# 1. the default on a single server: the names you pass are the keys
plain = Broker.from_env()  # RILLKIT_URL
print(plain.keys)
print(" ", plain.keys.stream(orders), plain.keys.dlq(orders))
print(" ", plain.keys.heartbeat(orders, "workers", "w1"))

# 2. pick a layout from the environment
os.environ["RILLKIT_KEY_LAYOUT"] = "hashtag"
print(Settings.from_env().key_builder())

# 3. or pass a builder: your own prefix and an opaque tenant id inside the hash tag
billing = Broker.from_env(keys=HashTagKeys(prefix="billing", tenant="t-42"))
print(billing.keys)
print(" ", billing.keys.stream(orders), billing.keys.dlq(orders))


@billing.subscribe(orders, consumer="w1", block_ms=200)
def handle(order: dict, msg: Message):
    print("handled", order, "| stream:", msg.stream.name, "| key:", msg.key)


billing.start()
billing.publish(orders, {"id": 1})  # still the logical name
time.sleep(0.5)
print("workers:", billing.active_workers(orders))
print("plain key exists?", bool(billing.client.exists("orders")))
print("tagged key exists?", bool(billing.client.exists(billing.keys.stream(orders))))
billing.stop()
```

```text
PlainKeys()
  orders orders-dlq
  rillkit:workers:orders:workers:w1
HashTagKeys(prefix='rk', tenant=None)
HashTagKeys(prefix='billing', tenant='t-42')
  billing:{t-42:orders}:stream billing:{t-42:orders}:dlq
handled {'id': 1} | stream: orders | key: billing:{t-42:orders}:stream
workers: ['w1']
plain key exists? False
tagged key exists? True
```

The handler, `publish` and `active_workers` all use the name `orders`; only the key builder knows about `billing:{t-42:orders}:stream`.

## Finding the real keys

- `broker.keys.stream(stream)`, `broker.keys.dlq(stream)` and `broker.keys.heartbeat(stream, group, consumer)` return them.
- `msg.key` is the physical key of the stream a message came from (`msg.stream.name` stays the logical name).
- The DLQ log line names the physical key: `moving message ... to rk:{orders}:dlq: ...`.
- In `redis-cli` or `valkey-cli`, quote keys with braces: `XLEN 'rk:{orders}:stream'`.

Anything that writes to a stream without RillKit (another service, a script) must use the physical key.

## Your own layout

Subclass [`KeyBuilder`](/reference/keys) and implement `stream`, `dlq` and `heartbeat_prefix`. On a cluster, keep the stream and DLQ in one hash slot so the DLQ move stays a transaction; if they are not, RillKit falls back to two ordered steps (see [DLQ atomicity](/guide/cluster#dlq-atomicity)).

::: warning Changing the layout moves your data
The layout decides which keys are read. Switching an existing deployment to another layout, prefix or tenant starts from empty streams; old pending messages, DLQ entries and consumer groups stay under the old keys.
:::

::: tip Hash tags are not access control
A prefix or tenant id only chooses names. It does not stop code from reading another tenant's keys. Use ACL users (or separate deployments) for isolation, and take tenant ids from trusted server-side context, never from message content.
:::
