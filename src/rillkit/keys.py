"""Where a logical stream and its DLQ and heartbeats live: the physical key names.

``Stream("orders")`` is a logical name. A key builder turns it into the keys that are actually read
and written. Every command rillkit sends goes through the broker's key builder.

``PlainKeys`` (the default outside a cluster) keeps the names rillkit has always used, so an
existing deployment sees the same keys. ``HashTagKeys`` (the default on a cluster) puts a stream,
its DLQ and its heartbeats in one hash slot so the move to the DLQ can stay a transaction.

Reserved for planned control-plane keys (worker state, directive dedup, node heartbeats, logs and
producer cordons), so that adding them never moves or collides with existing keys:

- ``PlainKeys``: names under ``rillkit:`` other than ``rillkit:workers:``, ``rillkit:done:`` and
  ``rillkit:consumers:``,
  namely ``rillkit:state:``, ``rillkit:dedup:``, ``rillkit:nodes:``, ``rillkit:ctl:``,
  ``rillkit:logs:`` and ``rillkit:cordon:``.
- ``HashTagKeys``: the control tag ``{<tenant>:}`` (``{:}`` without a tenant) inside each
  prefix, and the suffixes ``logs`` and ``cordon`` after a stream's tag. No stream can produce
  the control tag: stream names are non-empty and their colons are percent-encoded.
"""

from typing import Optional

from .exceptions import ConfigurationError
from .stream import Stream

HEARTBEAT_PREFIX = "rillkit:workers:"
IDEMPOTENCY_PREFIX = "rillkit:done:"
CONSUMERS_PREFIX = "rillkit:consumers:"
_GLOB_SPECIAL = str.maketrans({c: f"\\{c}" for c in "*?[]\\"})


def glob_escape(text: str) -> str:
    """``text`` as a literal inside a SCAN MATCH pattern."""
    return text.translate(_GLOB_SPECIAL)


class KeyBuilder:
    """Maps a ``Stream`` to the keys rillkit uses for it. Subclass to choose your own layout.

    The stream key and the DLQ key must hash to the same cluster slot for the move to the DLQ to
    be atomic on a cluster. Heartbeat keys for one stream and group must share the prefix
    returned by ``heartbeat_prefix`` so ``Broker.active_workers`` can find them.
    """

    def stream(self, stream: Stream) -> str:
        raise NotImplementedError

    def dlq(self, stream: Stream) -> str:
        raise NotImplementedError

    def heartbeat_prefix(self, stream: Stream, group: str) -> str:
        raise NotImplementedError

    def heartbeat(self, stream: Stream, group: str, consumer: str) -> str:
        return f"{self.heartbeat_prefix(stream, group)}{consumer}"

    def idempotency(self, stream: Stream, key: str) -> str:
        """Where ``Idempotent`` records that ``key`` was processed for ``stream``.

        The default is the key rillkit has always used, so custom builders that do not override
        it keep their existing markers."""
        return f"{IDEMPOTENCY_PREFIX}{stream.name}:{key}"

    def consumers(self, stream: Stream, group: str) -> str:
        """The registry of live consumers of ``stream`` in ``group``: a sorted set of consumer
        names scored by when their heartbeat expires (milliseconds). ``active_workers`` reads it,
        so finding workers never needs ``SCAN``."""
        return f"{CONSUMERS_PREFIX}{_escape_plain(stream.name)}:{group}"


class PlainKeys(KeyBuilder):
    """The stream name is the key; the DLQ is ``dlq`` or ``<name>-dlq``. rillkit's original layout.

    On a cluster the stream and DLQ usually land in different slots. That works, but the move to
    the DLQ is then two steps instead of one transaction (see the cluster guide).
    """

    def stream(self, stream: Stream) -> str:
        return stream.name

    def dlq(self, stream: Stream) -> str:
        return stream.dlq_name

    def heartbeat_prefix(self, stream: Stream, group: str) -> str:
        return f"{HEARTBEAT_PREFIX}{stream.name}:{group}:"

    def __repr__(self) -> str:
        return "PlainKeys()"


def _escape_plain(text: str) -> str:
    """Encode ``%`` and ``:`` so a stream name never runs into the group name after it."""
    return text.replace("%", "%25").replace(":", "%3A")


def _escape(text: str, colon: bool = False) -> str:
    """Percent-encode the characters that would end or split a hash tag."""
    out = text.replace("%", "%25").replace("{", "%7B").replace("}", "%7D")
    return out.replace(":", "%3A") if colon else out


class HashTagKeys(KeyBuilder):
    """Keys that share one hash tag per stream, so they live in one cluster slot::

        rk:{orders}:stream
        rk:{orders}:dlq                 (or rk:{orders}:dlq:<custom name>)
        rk:{orders}:workers:<group>:<consumer>
        rk:{orders}:done:<idempotency key>
        rk:{orders}:consumers:<group>   (registry of live consumers)

    ``tenant`` adds an opaque id inside the tag (``rk:{t-42:orders}:stream``) so tenants' keys
    never collide. It is for placement and naming only, not access control: take it from trusted
    server-side context, never from a message.

    Inside the tag, ``%``, ``{``, ``}`` and ``:`` are percent-encoded in both the stream name and
    the tenant. A tag therefore holds exactly one unencoded ``:`` when there is a tenant and none
    when there is not, so no name can escape the tag, and no stream name can produce another
    tenant's keys or a tenant-scoped key.

    A custom ``Stream(dlq=...)`` becomes a suffix of the stream's own tag, so two streams that
    share a DLQ name on a single server get one DLQ each here.
    """

    def __init__(self, prefix: str = "rk", tenant: Optional[str] = None) -> None:
        if "{" in prefix or "}" in prefix:
            raise ConfigurationError("the key prefix cannot contain '{' or '}'")
        if tenant is not None and not tenant:
            raise ConfigurationError("tenant must be a non-empty string or None")
        self.prefix = prefix
        self.tenant = tenant

    def tag(self, stream: Stream) -> str:
        if not stream.name:
            # "{}" is not a hash tag, so the stream's keys would land in different slots
            raise ConfigurationError("hash-tagged keys need a non-empty stream name")
        name = _escape(stream.name, colon=True)
        if self.tenant is not None:
            name = f"{_escape(self.tenant, colon=True)}:{name}"
        return f"{{{name}}}"

    def _base(self, stream: Stream) -> str:
        return f"{self.prefix}:{self.tag(stream)}" if self.prefix else self.tag(stream)

    def stream(self, stream: Stream) -> str:
        return f"{self._base(stream)}:stream"

    def dlq(self, stream: Stream) -> str:
        base = f"{self._base(stream)}:dlq"
        return base if stream.dlq is None else f"{base}:{stream.dlq}"

    def heartbeat_prefix(self, stream: Stream, group: str) -> str:
        return f"{self._base(stream)}:workers:{group}:"

    def idempotency(self, stream: Stream, key: str) -> str:
        return f"{self._base(stream)}:done:{key}"

    def consumers(self, stream: Stream, group: str) -> str:
        return f"{self._base(stream)}:consumers:{group}"

    def __repr__(self) -> str:
        return f"HashTagKeys(prefix={self.prefix!r}, tenant={self.tenant!r})"


KEY_LAYOUTS = ("plain", "hashtag")


def key_builder(layout: str) -> KeyBuilder:
    if layout == "plain":
        return PlainKeys()
    if layout == "hashtag":
        return HashTagKeys()
    raise ConfigurationError(f"unknown key_layout {layout!r}, expected one of {KEY_LAYOUTS}")
