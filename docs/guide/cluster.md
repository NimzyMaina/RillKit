# Redis and Valkey Cluster

RillKit runs on Redis Cluster and Valkey Cluster with the same handlers, retries, DLQ and middleware as on a single server. Point it at a few nodes and set the mode:

```bash
export RILLKIT_BACKEND=valkey   # or redis
export RILLKIT_MODE=cluster
export RILLKIT_CLUSTER_NODES=127.0.0.1:7201,127.0.0.1:7202,127.0.0.1:7203
```

```python [g_cluster.py]
import logging
import time

from rillkit import Broker, Stream

logging.basicConfig(format="%(levelname)s %(message)s")

broker = Broker.from_env()  # RILLKIT_BACKEND, RILLKIT_MODE, RILLKIT_CLUSTER_NODES
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
handled {'id': 1}
ERROR handler failed for message 1794669071538-1 (delivery 1): unknown sku
WARNING moving message 1794669071538-1 to rk:{orders}:dlq: unknown sku
workers: ['w1']
dlq: 1
```

Line by line:

- **`ValkeyCluster HashTagKeys(...)`**: the client RillKit built for `RILLKIT_BACKEND=valkey` and `RILLKIT_MODE=cluster`, and the key builder it chose. A cluster client gets [hash-tagged keys](/guide/keys) by default.
- **`rk:{orders}:stream rk:{orders}:dlq`**: the physical keys behind `Stream("orders")`. Both contain the hash tag `{orders}`, so they live in the same slot, on the same node.
- **`handled {'id': 1}`**: the first message succeeded and was acknowledged.
- **`ERROR handler failed ...`**: the second message raised. `1794669071538-1` is its stream entry id (server time in milliseconds, then a sequence number); `delivery 1` is its first attempt.
- **`WARNING moving message ... to rk:{orders}:dlq`**: with `max_retries=0` one failure is enough. The copy to the DLQ and the acknowledgement run as one transaction on the node that owns `{orders}`, and the log names the physical key.
- **`workers: ['w1']`**: `active_workers` found the consumer's heartbeat, scanning every primary.
- **`dlq: 1`**: the DLQ holds the failed message, with its body plus `x-error`, `x-deliveries`, `x-original-id`, `x-stream` and `x-group`.

The same in code, without environment variables: `Broker(settings=Settings(backend="valkey", mode="cluster", cluster_nodes=["127.0.0.1:7201", ...]))`.

## Connecting

The client is discovered from a few **seed nodes**; it then learns the rest of the cluster by itself. Seed nodes can come from:

- `cluster_nodes`: `"host:port"` strings or `(host, port)` pairs. From the environment: `RILLKIT_CLUSTER_NODES=10.0.0.1:7000,10.0.0.2:7000`.
- `url`: one seed node plus credentials and TLS, for example `rediss://app:secret@cluster.example.com:6379`. It adds to `cluster_nodes` if both are set.
- Neither: `host` and `port`.

A URL never describes a whole cluster, only one node to start from. A cluster has only database 0, so a URL with another database (or `db=3`) raises `ConfigurationError`.

Authentication and TLS work as on a single server: `username`, `password` and `ssl` fields, or a `rediss://` URL, and certificate options in `client_kwargs`. Cluster options of the client (`require_full_coverage`, `read_from_replicas`, `retry`, ...) also go in `client_kwargs`; options a cluster client would silently ignore are refused (see [client options](/guide/configuration#client-options)).

A production cluster behind TLS with an ACL user:

```bash
export RILLKIT_BACKEND=redis
export RILLKIT_MODE=cluster
export RILLKIT_URL="rediss://app:secret@10.0.0.1:7000"            # first seed, credentials, TLS
export RILLKIT_CLUSTER_NODES=10.0.0.2:7000,10.0.0.3:7000          # more seeds
```

Credentials can instead come from `RILLKIT_USERNAME`, `RILLKIT_PASSWORD` and `RILLKIT_SSL=true`, which are read when no URL is set (seeds then come from `RILLKIT_CLUSTER_NODES`).

You can also pass a `redis.RedisCluster` or `valkey.ValkeyCluster` you built yourself as `Broker(client=...)`. RillKit notices it is a cluster client and uses hash-tagged keys.

## Key layout

On a cluster RillKit uses `HashTagKeys`: `rk:{orders}:stream`, `rk:{orders}:dlq` and `rk:{orders}:workers:...` share the hash tag `{orders}`, so a stream, its DLQ and its heartbeats sit in one slot. That is what keeps the move to the DLQ a single transaction. [Keys](/guide/keys) explains the layouts, prefixes and tenants, and how to choose one.

Two things are cluster-specific:

- A **custom DLQ name** becomes a suffix of the stream's own tag (`rk:{orders}:dlq:orders-failed`) so it stays in the stream's slot. Two streams that share `dlq="dead"` on a single server get one DLQ each on a cluster.
- `RILLKIT_KEY_LAYOUT=plain` keeps the single-server names on a cluster (for producers you cannot change). It works, but the DLQ move is then two steps, see below.

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
