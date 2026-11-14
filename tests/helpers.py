import contextlib
import os
import threading
import time
from collections.abc import Iterator
from typing import Any, Callable, Optional

from rillkit import HashTagKeys, KeyBuilder, PlainKeys, Stream
from rillkit.clients import CLIENT_ERRORS
from rillkit.stream import StreamLike, as_stream

BACKEND = os.environ.get("RILLKIT_TEST_BACKEND", "redis")
MODE = os.environ.get("RILLKIT_TEST_MODE", "standalone")

# The layout a broker picks by default for the server under test.
KEYS: KeyBuilder = HashTagKeys() if MODE == "cluster" else PlainKeys()


def sk(stream: StreamLike) -> str:
    """Physical key of a stream."""
    return KEYS.stream(as_stream(stream))


def dk(stream: StreamLike, dlq: Optional[str] = None) -> str:
    """Physical key of a stream's DLQ (``dlq`` overrides the stream's own DLQ name)."""
    s = as_stream(stream)
    if dlq is not None:
        s = Stream(s.name, s.group, dlq)
    return KEYS.dlq(s)


def hk(stream: StreamLike, group: str, consumer: str) -> str:
    """Physical key of a consumer's heartbeat."""
    return KEYS.heartbeat(as_stream(stream), group, consumer)


def ik(stream: StreamLike, key: str) -> str:
    """Physical key of an Idempotent marker."""
    return KEYS.idempotency(as_stream(stream), key)


def wait_until(cond: Callable[[], Any], timeout: float = 10.0, interval: float = 0.02) -> bool:
    """Poll ``cond`` until it is truthy. Returns the final truthiness instead of raising."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return bool(cond())


def fast(**overrides: Any) -> dict[str, Any]:
    """Subscribe kwargs that make consumers react in milliseconds."""
    opts: dict[str, Any] = {"block_ms": 50, "claim_idle_ms": 150}
    opts.update(overrides)
    return opts


@contextlib.contextmanager
def keep_server_awake(client: Any, interval: float = 0.05) -> Iterator[None]:
    """Send a cheap PING every ``interval`` seconds from ``client`` in another thread.

    Harmless against a real server. Some sandboxed local servers only fire blocking-command
    timeouts when other traffic wakes their event loop, which makes idle-consumer tests hang.
    """
    stop = threading.Event()

    def ping() -> None:
        while not stop.is_set():
            try:
                client.ping()
            except CLIENT_ERRORS:
                pass
            stop.wait(interval)

    thread = threading.Thread(target=ping, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(2)


def pending(r: Any, stream: str, group: str = "g") -> int:
    """Pending count of a logical stream."""
    return int(r.xpending(sk(stream), group)["pending"])


def read_count(r: Any, stream: str, group: str = "g") -> int:
    """How many entries the group has read so far from a logical stream."""
    return int(r.xinfo_groups(sk(stream))[0]["entries-read"] or 0)


def entries(r: Any, key: str) -> list:
    """Entries at a physical key as (id, {str: str}) pairs."""
    out = []
    for entry_id, raw in r.xrange(key):
        out.append((entry_id.decode(), {k.decode(): v.decode() for k, v in raw.items()}))
    return out


def settle(seconds: float) -> None:
    """Sleep to prove something does NOT happen (e.g. no redelivery), never to wait for events."""
    time.sleep(seconds)
