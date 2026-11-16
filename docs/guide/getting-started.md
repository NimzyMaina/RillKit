# Getting started

RillKit is a small, synchronous, thread-based library for [streams](https://redis.io/docs/latest/develop/data-types/streams/) on Redis and Valkey, through redis-py or valkey-py, on one server or a cluster. You decorate a function, RillKit reads messages from a stream, calls your function, and takes care of acknowledging, retrying and dead-lettering.

## Install

::: code-group
```bash [pip]
pip install rillkit
```
```bash [pip + pydantic]
pip install "rillkit[pydantic]"
```
```bash [pip + valkey]
pip install "rillkit[valkey]"
```
:::

RillKit needs Python 3.9 or newer and `redis-py` 5 or newer. The `pydantic` extra is optional and only needed if you want handlers that receive validated models (see [Pydantic models](/guide/pydantic)). The `valkey` extra installs `valkey-py`, for when you would rather use the Valkey client library (see [Redis or Valkey](/guide/configuration#redis-or-valkey)).

You also need a Redis or Valkey server. Retries use the idle filter of `XPENDING`, so use **Redis 6.2 or newer**, or **Valkey 7.2 or newer**. Locally, `docker run -p 6379:6379 redis:7` or `docker run -p 6379:6379 valkey/valkey:8` is enough. Both work standalone or as a [cluster](/guide/cluster).

## Publish and consume in 20 lines

Save this as `app.py`:

```python [getting_started.py]
import logging

from rillkit import Broker, Stream

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

broker = Broker.from_env()


@broker.subscribe(Stream("orders", group="billing"), block_ms=1000)
def handle_order(order: dict):
    print(f"billing order {order['id']}: {order['total']}")


if __name__ == "__main__":
    broker.publish("orders", {"id": 1, "total": 49.5})
    broker.publish("orders", {"id": 2, "total": 12.0})
    broker.run()
```

Point it at your server and run it:

```bash
export RILLKIT_URL=redis://localhost:6379/0
python app.py
```

`Broker.from_env()` reads `RILLKIT_URL`, or the separate `RILLKIT_HOST`, `RILLKIT_PORT`, `RILLKIT_PASSWORD` and so on (and `RILLKIT_BACKEND`, `RILLKIT_MODE` when you need valkey-py or a cluster). Every example in these docs connects this way. Deployments that set `REDIS_URL` keep working; see [Configuration](/guide/configuration#environment-variables).

You will see something like this (stop it with `Ctrl+C`):

```text
INFO consumer started | stream: orders | group: billing | consumer: billing-myhost-71496 | policy: nack_on_error | max retries: 3
billing order 1: 49.5
billing order 2: 12.0
INFO shutdown requested, finishing the current message before exiting
INFO consumer stopped | stream: orders | consumer: billing-myhost-71496
```

::: tip Seeing the log lines
RillKit logs through the standard `logging` module under the name `rillkit`. The `logging.basicConfig(...)` line at the top is what makes the `INFO` lines visible. Without it you only see warnings and errors.
:::

## What just happened

1. `Stream("orders", group="billing")` names the stream (`orders`) and the consumer group (`billing`).
2. `@broker.subscribe(...)` registered `handle_order`. Nothing runs yet.
3. `broker.publish("orders", {...})` did an `XADD` onto the stream. The dict was turned into JSON and stored in a field called `data`.
4. `broker.run()` created the `billing` group if it did not exist (reading from the start of the stream), started a consumer thread, and blocked.
5. The consumer thread read the two entries with `XREADGROUP`, decoded the JSON into a `dict`, and called `handle_order` once for each.
6. Each call returned normally, so RillKit acknowledged the entry with `XACK`.
7. On `Ctrl+C` (SIGINT) RillKit finished the message it was working on, stopped the thread and exited cleanly with status 0.

Run `python app.py` a second time and the same two orders are **not** processed again for the group, because they were acknowledged. Only the two new ones that the script publishes on startup are.

::: warning Reusing the stream name
Because the example publishes on every start, running it repeatedly keeps adding orders. That is normal for a stream: it is a log, not a queue that empties itself. See [Operations](/guide/operations#trimming) for trimming.
:::

## Where next

- New to streams? Read [Concepts](/guide/concepts) first. It is short.
- Want to see a handler fail and recover? [Retries and the DLQ](/guide/retries-and-dlq).
- Want something you can copy? Browse the [examples](/examples/).
- Running on Valkey or a cluster? [Configuration](/guide/configuration) and [Redis and Valkey Cluster](/guide/cluster).
- Want to watch and steer your workers from a dashboard? [Run the server](/guide/server), then [connect your workers](/guide/control-plane). The demo in `deploy/demo.yaml` starts it with one download.
- Upgrading? [Upgrading](/guide/upgrading) has the notes, and the version picker in the top bar opens the docs of earlier releases. [All versions](/versions) lists every release with a link to what changed since the one before.
