# Broker

```python
from rillkit import Broker, Publisher
```

`Broker` owns the client (Redis or Valkey, standalone or cluster), the subscribers and the lifecycle hooks.

## Constructor

```python
Broker(
    url: Optional[str] = None,
    *,
    client: Optional[Client] = None,
    settings: Optional[Settings] = None,
    middlewares: Sequence[BaseMiddleware] = (),
    keys: Optional[KeyBuilder] = None,
    control: Optional[Control] = None,
)
```

| Parameter | Default | Description |
| --- | --- | --- |
| `url` | `None` | Server URL, used if neither `client` nor `settings` is given. With none of the three, it connects to `localhost:6379`, db 0. |
| `client` | `None` | An existing `redis.Redis`, `redis.RedisCluster`, `valkey.Valkey` or `valkey.ValkeyCluster`, used as is. Takes priority over `settings` and `url`. `stop()` closes it. |
| `settings` | `None` | A [`Settings`](/reference/settings) object. Takes priority over `url`. |
| `middlewares` | `()` | Broker-wide [middleware](/reference/middleware), added in order with `use()`. |
| `keys` | `None` | The [key builder](/reference/keys). Default: from `settings` if it sets `key_layout`, `tenant` or `key_prefix`, else `PlainKeys` for a standalone client and `HashTagKeys` for a cluster client. |
| `control` | `None` | The [control plane](/reference/control) settings. Default: `Control.from_env()`, so `RILLKIT_CONTROL` (and the config file) decide; off unless one of them turns it on. With `relay`, needs `pip install "rillkit[control]"`. |

Attributes: `client` (the client), `keys` (the key builder), `middlewares` (list), `subscribers` (list of registered subscribers), `control` (the running [`ControlPlane`](/reference/control#controlplane), or `None` when the control plane is off), `control_settings` (the `Control`).

With the control plane on, the broker adds the [`TraceContext`](/reference/middleware#tracecontext) middleware, and [`LogCapture`](/reference/middleware#logcapture) when `capture_logs` is set, ahead of your own (unless you pass them yourself).

### `Broker.from_env`

```python
Broker.from_env(prefix: str = "REDIS_", *, config: Optional[str] = None, **kwargs) -> Broker
```

Builds `Settings.from_env(prefix, config=config)` and `Control.from_env(config=config)` and passes them as `settings=` and `control=` (unless you pass `control=` yourself). `config` (else `RILLKIT_CONFIG`) names a [config file](/guide/configuration#config-file) whose values fill in what the environment leaves unset. Settings come from the `RILLKIT_*` variables (`RILLKIT_URL`, `RILLKIT_HOST`, `RILLKIT_BACKEND`, ...), falling back to the older `<prefix>URL`, `<prefix>HOST` and friends (see [Settings.from_env](/reference/settings#settings-from-env)). Extra keyword arguments (for example `middlewares=[...]` or `keys=...`) go to the constructor.

## Registering

### `subscribe`

```python
Broker.subscribe(
    stream: Union[str, Stream],
    *,
    group: Optional[str] = None,
    ack_policy: AckPolicy = AckPolicy.NACK_ON_ERROR,
    max_retries: Optional[int] = 3,
    claim_idle_ms: int = 60_000,
    backoff: float = 1.0,
    max_backoff_ms: int = 900_000,
    block_ms: int = 5000,
    count: int = 10,
    middlewares: Sequence[BaseMiddleware] = (),
    consumer: Optional[str] = None,
    heartbeat: bool = True,
    heartbeat_ttl: int = 60,
    group_start_id: str = "0",
    concurrency: int = 1,
    idle_timeout: Optional[float] = None,
) -> Callable[[Callable], Callable]
```

A decorator factory. The decorated function is returned unchanged. Raises `ValueError` if there is no group (neither on the `Stream` nor in `group`) and `TypeError` if the handler has more than one body parameter.

| Parameter | Default | Description |
| --- | --- | --- |
| `stream` | required | Stream name or `Stream`. |
| `group` | `None` | Consumer group. If given, it replaces `Stream.group`; the stream's name, `dlq`, `maxlen` and `codec` are kept. |
| `ack_policy` | `NACK_ON_ERROR` | See [AckPolicy](/reference/ack-policy). Plain strings such as `"manual"` are accepted too. |
| `max_retries` | `3` | Retries after the first attempt (total attempts `max_retries + 1`). `None` means no limit. |
| `claim_idle_ms` | `60000` | Idle time before a pending message is retried. Must exceed your slowest handler. |
| `backoff` | `1.0` | Multiplier per retry: required idle = `min(claim_idle_ms * backoff ** (deliveries - 1), max_backoff_ms)`. |
| `max_backoff_ms` | `900000` | Cap on the required idle time (15 minutes). |
| `block_ms` | `5000` | Milliseconds one `XREADGROUP` blocks. Must be at least 1. |
| `count` | `10` | Entries per read and pending entries inspected per claim pass. |
| `middlewares` | `()` | Middleware for this subscriber only, inside the broker-wide ones. |
| `consumer` | `<group>-<hostname>-<pid>` | Consumer name. |
| `heartbeat` | `True` | Write a heartbeat key. |
| `heartbeat_ttl` | `60` | Heartbeat key TTL in seconds. |
| `group_start_id` | `"0"` | Start position for a newly created group. `"$"` means only new entries. |
| `concurrency` | `1` | Consumer loops (threads) for this stream, named `<consumer>`, `<consumer>-1`, ... A control plane can change it while running. |
| `idle_timeout` | `None` | Seconds of inactivity (no messages and zero in-flight work across lanes) before the subscriber stops itself cleanly. Raises `ValueError` if not positive. A nacked message waiting for its retry does not keep it alive. |

See [Consuming](/guide/consuming) for handler signature rules.

### `publisher`

```python
Broker.publisher(stream: Union[str, Stream], *, maxlen: Optional[int] = None) -> Publisher
```

Returns a `Publisher`. Used as a decorator it publishes the handler's non-`None` return value to `stream`. Decorators stack.

### `use`

```python
Broker.use(middleware: BaseMiddleware) -> BaseMiddleware
```

Adds broker-wide middleware, sets `middleware.broker`, and returns the middleware.

## Publishing

```python
Broker.publish(stream: Union[str, Stream], body: Any, *, maxlen: Optional[int] = None, approximate: bool = True) -> str
```

Adds one entry (`XADD`, field `data` = JSON) and returns its id. `maxlen` defaults to `Stream.maxlen`. Trimming is approximate by default (`XADD MAXLEN ~`), which Redis applies in whole macro-nodes, so streams shorter than about 100 entries are effectively never trimmed. Pass `approximate=False` for an exact cap. The call goes through every broker-wide middleware's `publish_scope`. Raises [`PublishError`](/reference/exceptions) if Redis fails or the body cannot be encoded.

```python
Broker.publish_many(stream, bodies: Iterable[Any], *, maxlen: Optional[int] = None,
                    approximate: bool = True) -> list[str]
```

Adds several entries in one non-transactional pipeline: one network round trip instead of one per
message. Every middleware's `publish_scope` runs for every body, so `TraceContext` gives each
entry its own `traceparent` (same trace, its own span) and other headers are set per entry.
Encoding happens before the pipeline, so an encoding error writes nothing. Returns the ids in the
order given. Middleware sees `""` as the entry id inside `publish_scope` during a batch, because
ids only exist after the pipeline; the built-in middleware does not use that value. A failed
pipeline raises `PublishError` and some commands may have reached Redis.

## Lifecycle hooks

Each takes a function with no arguments, registers it and returns it, so it works as a decorator.

| Method | Runs |
| --- | --- |
| `on_startup(func)` | first in `start()`, before the connection check |
| `after_startup(func)` | after groups exist and consumer threads have started |
| `on_shutdown(func)` | in `stop()`, after consumers finish, before the client closes |
| `after_shutdown(func)` | last in `stop()`, after the client closes |

## Running

| Method | Description |
| --- | --- |
| `start() -> None` | Runs `on_startup` hooks, pings the server, creates the groups (`XGROUP CREATE ... MKSTREAM`), reads the desired state if the control plane is on (so a paused stream is never polled), starts the consumer threads (`concurrency` per subscriber), starts the control plane's threads, runs `after_startup` hooks, and returns. |
| `stop(timeout: Optional[float] = None) -> None` | Asks consumers to stop, joins each thread for up to `timeout` seconds (`None` waits forever), stops the control plane (closing its Relay connection and removing the node heartbeat), runs `on_shutdown`, closes the client, runs `after_shutdown`. Safe to call more than once and from another thread: the shutdown work happens once per start. |
| `run(handle_signals: bool = True, shutdown_timeout: Optional[float] = None) -> None` | `start()`, then blocks until SIGINT or SIGTERM (first signal: graceful, second: `sys.exit(1)`), until `stop()` is called, or until all consumer threads have ended (for example when every subscriber sets `idle_timeout` and goes idle); then `stop(shutdown_timeout)`. Signal handlers are installed only from the main thread and the previous handlers are restored afterwards. |

## Operations

| Method | Description |
| --- | --- |
| `active_workers(stream, group: Optional[str] = None) -> list[str]` | Sorted consumer names that have a live heartbeat for the stream and group. `group` defaults to the `Stream`'s group. Reads the consumer registry (`broker.keys.consumers(stream, group)`), so it needs no `SCAN`. Expiry is judged by the clocks of the workers and the caller, which should agree to within the heartbeat TTL. |
| `replay_dlq(stream, count: int = 100) -> int` | Moves up to `count` entries from the stream's DLQ back onto the stream, oldest first, without the `x-` fields, deleting them from the DLQ. Returns how many were moved. Pass the **original** stream. Each move is a transaction when the stream and DLQ share a slot; otherwise it adds before it deletes (see [DLQ atomicity](/guide/cluster#dlq-atomicity)). |
| `replay_dlq_entries(stream, ids, data=None) -> tuple[int, list[str]]` | Moves the DLQ entries `ids` back onto the stream the same way. `data` maps an entry id to a replacement `data` field (the encoded body), to fix a payload before it is handled again. Returns how many were moved and the ids that were not in the DLQ. |
| `delete_dlq_entries(stream, ids) -> int` | Deletes the DLQ entries `ids`; returns how many existed. |
| `purge_dlq(stream) -> int` | Deletes the whole DLQ; returns how many entries it held. |
| `create_group(stream, group, start="$") -> bool` | Creates the stream (if needed) and the consumer group, starting at `"$"` (new entries only) or `"0"` (everything already there). Returns `False` if the group existed. |

## Publisher

```python
Publisher(broker: Broker, stream: Stream, maxlen: Optional[int] = None)
```

Returned by `broker.publisher(...)`.

| Member | Description |
| --- | --- |
| `broker`, `stream`, `maxlen` | the values it was created with |
| `__call__(func)` | decorator: records the publisher on `func` and returns `func` |
| `publish(body) -> str` | `broker.publish(self.stream, body, maxlen=self.maxlen)` |
