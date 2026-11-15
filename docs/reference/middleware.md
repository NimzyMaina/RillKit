# Middleware

```python
from rillkit import BaseMiddleware, Idempotent, PublishCommand
```

## BaseMiddleware

Subclass it and override any hook. The default implementations do nothing and pass through. One instance is shared by all messages, so keep per-message state on the `Message`. `self.broker` is set when the middleware is added to a broker or a subscriber.

| Hook | Signature | When |
| --- | --- | --- |
| `on_receive` | `(self, msg: Message) -> None` | First, for every delivered message, before decoding. |
| `consume_scope` | `(self, call_next: Callable[[Message], Any], msg: Message) -> Any` | Wraps decoding plus the handler. Return `call_next(msg)`. Raise `AckMessage` or `NackMessage` to short-circuit. |
| `publish_scope` | `(self, call_next: Callable[[PublishCommand], str], cmd: PublishCommand) -> str` | Wraps every publish, including handler return values. Only broker-wide middleware takes part. |
| `after_processed` | `(self, msg: Message, exc: Optional[BaseException]) -> None` | Always called last. `exc` is the handler error, if any. Errors raised here are logged and ignored. |

The first middleware in a list is the outermost. Broker-wide middleware wraps a subscriber's own. See [Middleware](/guide/middleware) for the full ordering.

## PublishCommand

```python
@dataclass
class PublishCommand:
    stream: Stream
    body: Any
    maxlen: Optional[int] = None
    approximate: bool = True
    headers: dict[str, str] = {}   # extra entry fields next to "data", e.g. traceparent
```

What `publish_scope` receives. Mutate its fields to change what gets published.

## Idempotent

```python
Idempotent(
    key: Callable[[Message], str],
    ttl: int = 7 * 24 * 3600,
    prefix: Optional[str] = None,
)
```

Skips messages whose key was already processed successfully.

| Parameter | Default | Description |
| --- | --- | --- |
| `key` | required | Returns the idempotency key for a message, for example `lambda m: m.body["request_id"]`. |
| `ttl` | `604800` | Seconds the done-marker is kept. |
| `prefix` | `None` | `None`: the key is `broker.keys.idempotency(stream, key)`, i.e. `rillkit:done:<stream>:<key>` with `PlainKeys` and `<prefix>:{<tenant>:<stream>}:done:<key>` with `HashTagKeys`. A string: the key is `<prefix><stream name>:<key>`, whatever the layout. |

Behaviour: if the key exists, it raises `AckMessage` (the duplicate is acknowledged and the handler is skipped). Otherwise it calls the handler and, if it returned without raising, stores the key with `SET ... EX ttl` and the message id as value.

## TraceContext

```python
from rillkit import TraceContext, current_traceparent, use_traceparent

broker = Broker(middlewares=[TraceContext()])
```

Propagates the [W3C `traceparent`](https://www.w3.org/TR/trace-context/). For each delivered entry it reads the `traceparent` field, opens a child span (`msg.traceparent`), and adds a `traceparent` header to everything published while the handler runs. `TraceContext(originate=False)` adds nothing to publishes made outside a trace (the default starts a new trace).

- `use_traceparent(header)`: a context manager that makes `header` (for example from an HTTP request) the parent of what is published inside it.
- `current_traceparent()`: the active header, or `None`.

It propagates ids only; exporting spans is up to you. A broker with the [control plane](/guide/control-plane) on adds it automatically.

## LogCapture

```python
from rillkit import LogCapture, read_logs

broker = Broker(middlewares=[LogCapture(ttl=24 * 3600, max_records=200)])
records = read_logs(broker, "orders", msg_id)
```

Stores the log records emitted while a handler runs (in its thread) in a stream per message, the key builder's `logs(stream, id)` key, which expires after `ttl` seconds. Each record has `ts`, `level`, `logger`, `message`, and when present `exc`, `traceparent`, `consumer` and `delivery`. An exception raised by the handler is recorded as an `ERROR`. Logging levels still apply: records below a logger's effective level are never emitted, so never captured. Failing to store the logs never fails the message. A broker with the control plane on adds it automatically.

