# Redis and Valkey Cluster

RillKit runs on Redis Cluster and Valkey Cluster with the same handlers, retries, DLQ and middleware as on a single server. Set `mode="cluster"` and give it some nodes:

```python [g_cluster.py]
import time

from rillkit import Broker, Settings, Stream

settings = Settings(
    backend="valkey",  # or "redis"
    mode="cluster",
    cluster_nodes=["127.0.0.1:7201", "127.0.0.1:7202", "127.0.0.1:7203"],
)
broker = Broker(settings=settings)
orders = Stream("orders", group="workers")

print(type(broker.client).__name__, broker.keys)
print(broker.keys.stream(orders), broker.keys.dlq(orders))


@broker.subscribe(orders, consumer="w1", max_retries=0, block_ms=200)
def handle(order: dict):
    if order["id"] == 2:
        raise ValueError("unknown sku")
    print("handled", order)


broker.start()
broker.publish(orders, {"id": 1})
broker.publish(orders, {"id": 2})
time.sleep(1)
print("workers:", broker.active_workers(orders))
print("dlq:", broker.client.xlen(broker.keys.dlq(orders)))
broker.stop()
```

```text
ValkeyCluster HashTagKeys(prefix='rk', tenant=None)
rk:{orders}:stream rk:{orders}:dlq
ERROR handler failed for message 1794662644164-0 (delivery 1): unknown sku
WARNING moving message 1794662644164-0 to rk:{orders}:dlq: unknown sku
handled {'id': 1}
workers: ['w1']
dlq: 1
```

## Connecting

The client is discovered from a few **seed nodes**; it then learns the rest of the cluster by itself. Seed nodes can come from:

- `cluster_nodes`: `"host:port"` strings or `(host, port)` pairs. From the environment: `RILLKIT_CLUSTER_NODES=10.0.0.1:7000,10.0.0.2:7000`.
- `url`: one seed node plus credentials and TLS, for example `rediss://app:secret@cluster.example.com:6379`. It adds to `cluster_nodes` if both are set.
- Neither: `host` and `port`.

A URL never describes a whole cluster, only one node to start from. A cluster has only database 0, so a URL with another database (or `db=3`) raises `ConfigurationError`.

Authentication and TLS work as on a single server: `username`, `password` and `ssl` fields, or a `rediss://` URL, and certificate options in `client_kwargs`. Cluster options of the client (`require_full_coverage`, `read_from_replicas`, `retry`, ...) also go in `client_kwargs`; options a cluster client would silently ignore are refused (see [client options](/guide/configuration#client-options)).

```bash
export RILLKIT_BACKEND=redis
export RILLKIT_MODE=cluster
export RILLKIT_CLUSTER_NODES=10.0.0.1:7000,10.0.0.2:7000,10.0.0.3:7000
export REDIS_USERNAME=app
export REDIS_PASSWORD=secret
export REDIS_SSL=true
```

You can also pass a `redis.RedisCluster` or `valkey.ValkeyCluster` you built yourself as `Broker(client=...)`. RillKit notices it is a cluster client and uses hash-tagged keys.

## Key layout

A cluster spreads keys over 16384 hash slots, and a transaction can only touch keys in one slot. Moving a failed message to the DLQ writes the DLQ and acknowledges the stream, so RillKit wants both in the same slot. It does that with a **hash tag**: only the part of the key between `{` and `}` decides the slot.

| | `PlainKeys` (default standalone) | `HashTagKeys` (default on a cluster) |
| --- | --- | --- |
| stream | `orders` | `rk:{orders}:stream` |
| DLQ | `orders-dlq` | `rk:{orders}:dlq` |
| custom DLQ `dead` | `dead` | `rk:{orders}:dlq:dead` |
| heartbeat | `rillkit:workers:orders:workers:w1` | `rk:{orders}:workers:workers:w1` |

You always use the logical name (`Stream("orders")`, `broker.publish("orders", ...)`, `broker.replay_dlq("orders")`); the broker's key builder, `broker.keys`, maps it to the physical key. `msg.key` is the physical key of the stream a message came from.

Things to know about `HashTagKeys`:

- A **custom DLQ name** becomes a suffix of the stream's own tag, so it stays in the stream's slot. Two streams that share `dlq="dead"` on a single server get one DLQ each on a cluster.
- Braces and `%` in stream names are percent-encoded, so no name can end the tag early or land two streams on the same key.
- Producers that write to the stream directly (not through RillKit) must use the physical key, `broker.keys.stream(...)`.
- `Idempotent` middleware keys are single keys and are unchanged.

Choose the layout with `key_layout` (`"plain"` or `"hashtag"`, or `RILLKIT_KEY_LAYOUT`), or pass your own builder: `Broker(..., keys=HashTagKeys(prefix="app"))`. Subclass [`KeyBuilder`](/reference/keys) for a different scheme.

::: warning Switching layouts moves your data
The layout decides which keys are read. Switching an existing deployment to another layout starts from empty streams, and old pending messages and DLQ entries stay under the old keys. Standalone deployments keep `PlainKeys` unless you ask otherwise.
:::

## DLQ atomicity

| Stream and DLQ keys | Move to DLQ and replay |
| --- | --- |
| standalone server | one `MULTI`/`EXEC` transaction |
| cluster, same slot (`HashTagKeys`) | one `MULTI`/`EXEC` transaction on the node that owns the slot |
| cluster, different slots (`PlainKeys` on a cluster) | two steps, in a safe order |

On a cluster the transaction is sent to the owning node directly, because not every cluster client supports transactional pipelines (valkey-py 6.1 does not). If the slot has just moved, the transaction fails before applying anything and is retried once after the client reloads the slot map. If it still fails, the message stays pending and is retried later, like any other failure.

In two steps (different slots), the DLQ entry is written **before** the stream entry is acknowledged, and a replayed entry is added to the stream **before** it is deleted from the DLQ. A failure between the steps can therefore produce a duplicate (a second DLQ entry, or a message replayed twice) but never loses a message. This matches RillKit's at-least-once delivery; nothing in RillKit is exactly-once.

RillKit does not use Lua scripts.

## Worker discovery

`broker.active_workers(stream)` scans every primary of the cluster and counts a key seen twice (during a slot migration) once. If a node cannot be reached it raises the client's error instead of returning a partial list.

## Failover and slot migration

Consumers keep running through node failures and resharding: a client error is logged and the loop retries after 5 seconds, and unacknowledged messages stay pending, so another delivery or a claim picks them up. The client library follows `MOVED` and `ASK` redirections for ordinary commands.

The CI suite runs the full behavioural test suite against a six-node Redis Cluster and Valkey Cluster, plus cluster tests for same-slot transactions, cross-slot ordering, discovery across primaries, a slot migration while consuming, and a node that stops answering and comes back.

## Hash tags are not security

A hash tag only places keys. `HashTagKeys(tenant="t-42")` adds an opaque tenant id to the tag so tenants never share a key, but it does not stop one tenant's code from reading another's keys. Use ACL users (or separate deployments) for isolation, and take tenant ids from trusted server-side context, never from message content.
