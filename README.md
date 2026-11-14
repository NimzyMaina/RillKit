# RillKit

Simple publish and consume for Redis and Valkey streams, standalone or cluster. Consumer groups,
retries with backoff, a dead-letter queue, ack policies, middleware and lifecycle hooks, without the
boilerplate.

**Docs:** https://nimzymaina.github.io/rillkit/

```bash
pip install rillkit             # plain dict bodies
pip install "rillkit[pydantic]" # optional typed bodies
pip install "rillkit[valkey]"   # optional valkey-py client
```

```python
from rillkit import Broker, Stream

orders = Stream("orders", group="workers")
broker = Broker.from_env()  # RILLKIT_URL, or RILLKIT_HOST/PORT/...; REDIS_* still work


@broker.subscribe(orders, max_retries=3)
def handle(order: dict):
    print("got", order)


if __name__ == "__main__":
    broker.publish(orders, {"id": 1})
    broker.run()  # blocks; Ctrl-C / SIGTERM finishes the current message and exits
```

## Features

- Consumer groups: group creation, `XREADGROUP`, and `XAUTOCLAIM`-style takeover of stuck messages
- At-least-once delivery with retries (`max_retries`, optional exponential `backoff`)
- Dead-letter queue with the original id, error and delivery count; `broker.replay_dlq()`
- `msg.nack()` to retry, `msg.reject()` (or `RejectMessage`) to skip retries and go straight to the DLQ
- `AckPolicy`: `NACK_ON_ERROR` (default), `ACK_ALWAYS`, `ACK_FIRST`, `MANUAL`
- Plain dicts by default, Pydantic models when you annotate the handler with one
- Middleware hooks (`on_receive`, `consume_scope`, `publish_scope`, `after_processed`) and an `Idempotent` middleware
- `on_startup`, `after_startup`, `on_shutdown`, `after_shutdown` hooks and graceful SIGTERM shutdown
- Worker heartbeats: `broker.active_workers(stream)` includes idle workers
- Stack `@broker.publisher(...)` under `@broker.subscribe(...)` to publish a handler's return value
- The wire format is a single `data` field holding JSON, so plain redis-py or valkey-py producers interoperate
- Redis or Valkey (`RILLKIT_BACKEND=valkey`), standalone or Cluster (`RILLKIT_MODE=cluster`)
- Key builders map stream names to keys: the original names on one server, hash-tagged keys on a
  cluster so a stream and its DLQ share a slot and the move to the DLQ stays a transaction, plus
  prefixes and tenant ids (`HashTagKeys(prefix="billing", tenant="t-42")`)

## Development

```bash
uv sync --group dev
REDIS_URL=redis://localhost:6379/15 uv run pytest   # needs a Redis; only db 15 is flushed
RILLKIT_TEST_BACKEND=valkey REDIS_URL=redis://localhost:6380/15 uv run pytest   # valkey-py

scripts/test-cluster.sh valkey 7101                 # a six-node cluster in Docker
RILLKIT_TEST_BACKEND=valkey RILLKIT_TEST_MODE=cluster \
  RILLKIT_TEST_CLUSTER_NODES=127.0.0.1:7101 uv run pytest

npm install && npm run docs:dev                     # docs
npm run docs:build:versions                         # latest plus a snapshot per release tag
```

MIT licensed.
