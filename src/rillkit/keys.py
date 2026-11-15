"""Where a logical stream and its DLQ and heartbeats live: the physical key names.

``Stream("orders")`` is a logical name. A key builder turns it into the keys that are actually read
and written. Every command rillkit sends goes through the broker's key builder.

``PlainKeys`` (the default outside a cluster) keeps the names rillkit has always used, so an
existing deployment sees the same keys. ``HashTagKeys`` (the default on a cluster) puts a stream,
its DLQ and its heartbeats in one hash slot so the move to the DLQ can stay a transaction.

Control-plane keys (worker state, directive dedup, node heartbeats, wakeup channels, per-message
logs and producer cordons) are built here too, so they never move or collide with data keys:

- ``PlainKeys``: ``rillkit:state:``, ``rillkit:dedup:``, ``rillkit:nodes``, ``rillkit:ctl:``,
  ``rillkit:logs:`` and ``rillkit:cordon:``, with stream names percent-encoded (``%`` and ``:``).
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
CONTROL_PREFIX = "rillkit:"
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

    # Control-plane keys. The defaults are the plain layout under ``rillkit:``, so a custom
    # builder gets working control keys without overriding anything. ``HashTagKeys`` puts all of
    # them except logs and cordons in one slot per namespace (the control tag).

    def _control(self, suffix: str) -> str:
        return f"{CONTROL_PREFIX}{suffix}"

    def state_global(self) -> str:
        """HASH: the desired state of every worker in the namespace."""
        return self._control("state:global")

    def state_stream(self, stream: Stream, group: str) -> str:
        """HASH: the desired state of the workers of ``stream`` in ``group``."""
        return self._control(f"state:stream:{_escape_plain(stream.name)}:{group}")

    def state_node(self, node_id: str) -> str:
        """HASH: the desired state of one node (worker process)."""
        return self._control(f"state:node:{node_id}")

    def dedup(self, directive_id: str) -> str:
        """STRING with a TTL: set once a directive has been applied, so it is applied once."""
        return self._control(f"dedup:{directive_id}")

    def node(self, node_id: str) -> str:
        """HASH with a TTL: a node's heartbeat and reported state."""
        return self._control(f"nodes:{node_id}")

    def nodes(self) -> str:
        """ZSET: node id -> heartbeat expiry (ms). Lists nodes without ``SCAN``."""
        return self._control("nodes")

    def broadcast_channel(self) -> str:
        """Pub/Sub channel that wakes every node of the namespace."""
        return self._control("ctl:broadcast")

    def node_channel(self, node_id: str) -> str:
        """Pub/Sub channel that wakes one node."""
        return self._control(f"ctl:node:{node_id}")

    def logs(self, stream: Stream, msg_id: str) -> str:
        """STREAM with a TTL: log records captured while handling one message."""
        return self._control(f"logs:{_escape_plain(stream.name)}:{msg_id}")

    def cordon(self, stream: Stream) -> str:
        """STRING: set while producers to ``stream`` are cordoned (maintenance)."""
        return self._control(f"cordon:{_escape_plain(stream.name)}")

    def cordon_buffer(self, stream: Stream) -> str:
        """STREAM: messages published to ``stream`` while it was cordoned, in order."""
        return f"{self.cordon(stream)}:buffer"

    def describe(self) -> dict[str, Optional[str]]:
        """The ``keys`` descriptor reported to the control plane: layout, prefix and tenant."""
        return {"layout": "custom", "prefix": None, "tenant": None}


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

    def describe(self) -> dict[str, Optional[str]]:
        return {"layout": "plain", "prefix": None, "tenant": None}

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
        rk:{orders}:logs:<message id>   (captured handler logs)
        rk:{orders}:cordon              (and rk:{orders}:cordon:buffer)
        rk:{:}:state:global             (control keys share the control tag, see control_tag)

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

    def control_tag(self) -> str:
        """``{<tenant>:}``, or ``{:}`` without a tenant. No stream tag can equal it."""
        tenant = "" if self.tenant is None else _escape(self.tenant, colon=True)
        return f"{{{tenant}:}}"

    def _control(self, suffix: str) -> str:
        tag = self.control_tag()
        return f"{self.prefix}:{tag}:{suffix}" if self.prefix else f"{tag}:{suffix}"

    def state_stream(self, stream: Stream, group: str) -> str:
        if not stream.name:
            raise ConfigurationError("hash-tagged keys need a non-empty stream name")
        return self._control(f"state:stream:{_escape(stream.name, colon=True)}:{group}")

    def logs(self, stream: Stream, msg_id: str) -> str:
        return f"{self._base(stream)}:logs:{msg_id}"

    def cordon(self, stream: Stream) -> str:
        return f"{self._base(stream)}:cordon"

    def describe(self) -> dict[str, Optional[str]]:
        return {"layout": "hashtag", "prefix": self.prefix, "tenant": self.tenant}

    def __repr__(self) -> str:
        return f"HashTagKeys(prefix={self.prefix!r}, tenant={self.tenant!r})"


KEY_LAYOUTS = ("plain", "hashtag")


def key_builder(layout: str) -> KeyBuilder:
    if layout == "plain":
        return PlainKeys()
    if layout == "hashtag":
        return HashTagKeys()
    raise ConfigurationError(f"unknown key_layout {layout!r}, expected one of {KEY_LAYOUTS}")
