# Key builders

```python
from rillkit import KeyBuilder, PlainKeys, HashTagKeys
```

A key builder maps a logical `Stream` to the physical keys RillKit reads and writes. The broker's builder is `broker.keys`. Pick one with `Broker(keys=...)` or `Settings(key_layout=...)`; by default it is `PlainKeys` for a standalone client and `HashTagKeys` for a cluster client. See [key layout](/guide/cluster#key-layout).

## `PlainKeys()`

RillKit's original names, unchanged since 0.1.

| Method | Returns |
| --- | --- |
| `stream(stream)` | `stream.name` |
| `dlq(stream)` | `stream.dlq` or `<name>-dlq` |
| `heartbeat_prefix(stream, group)` | `rillkit:workers:<name>:<group>:` |
| `heartbeat(stream, group, consumer)` | `rillkit:workers:<name>:<group>:<consumer>` |

## `HashTagKeys(prefix="rk", tenant=None)`

Everything for one stream in one cluster slot.

| Method | Returns |
| --- | --- |
| `stream(stream)` | `rk:{<name>}:stream` |
| `dlq(stream)` | `rk:{<name>}:dlq`, or `rk:{<name>}:dlq:<custom name>` |
| `heartbeat(stream, group, consumer)` | `rk:{<name>}:workers:<group>:<consumer>` |
| `tag(stream)` | `{<name>}`, or `{<tenant>:<name>}` |

- `prefix` may be empty, and cannot contain braces.
- `tenant` is an opaque id placed inside the tag. It must not be empty. It is for placement, not access control.
- `{`, `}` and `%` in names are percent-encoded, and `:` too in the tenant, so a name cannot escape the tag or collide with another tenant's.

## `KeyBuilder`

Subclass it to use your own layout. Implement `stream`, `dlq` and `heartbeat_prefix`; `heartbeat` defaults to the prefix followed by the consumer name. For the DLQ move to be a transaction on a cluster, `stream` and `dlq` must hash to the same slot; otherwise it runs in two steps (see [DLQ atomicity](/guide/cluster#dlq-atomicity)).

```python
from rillkit import Broker, KeyBuilder, Stream


class TeamKeys(KeyBuilder):
    def stream(self, stream: Stream) -> str:
        return f"team-a:{{{stream.name}}}"

    def dlq(self, stream: Stream) -> str:
        return f"team-a:{{{stream.name}}}:dead"

    def heartbeat_prefix(self, stream: Stream, group: str) -> str:
        return f"team-a:{{{stream.name}}}:hb:{group}:"


broker = Broker(keys=TeamKeys())
```
