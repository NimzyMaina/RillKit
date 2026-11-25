import logging
import signal
import sys
import threading
import time
from collections.abc import Iterable, Sequence
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Optional

from .ack import AckPolicy
from .clients import CLIENT_ERRORS, RESPONSE_ERRORS, Client, is_cluster, run_together
from .config import Control
from .consumer import Subscriber, _text, replay_entry
from .exceptions import ConfigurationError, PublishError
from .keys import HashTagKeys, KeyBuilder, PlainKeys
from .middleware import BaseMiddleware, PublishCommand
from .settings import Settings
from .stream import Stream, StreamLike, as_stream

if TYPE_CHECKING:
    from .controlplane import ControlPlane

log = logging.getLogger("rillkit")

Hook = Callable[[], Any]


class Publisher:
    """Returned by ``broker.publisher(stream)``. Use it as a decorator to publish a handler's return
    value, or call ``.publish(body)`` directly."""

    def __init__(self, broker: "Broker", stream: Stream, maxlen: Optional[int] = None) -> None:
        self.broker = broker
        self.stream = stream
        self.maxlen = maxlen

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        publishers = list(getattr(func, "_rillkit_publishers", ()))
        publishers.append(self)
        func._rillkit_publishers = publishers  # type: ignore[attr-defined]
        return func

    def publish(self, body: Any) -> str:
        return self.broker.publish(self.stream, body, maxlen=self.maxlen)


class Broker:
    """Owns the client, the subscribers and the lifecycle hooks.

        broker = Broker.from_env()

        @broker.subscribe(Stream("orders", group="workers"))
        def handle(order: dict): ...

        broker.publish("orders", {"id": 1})
        broker.run()

    ``client`` can be any redis-py or valkey-py client, standalone or cluster. It is used as is;
    ``stop()`` closes it. ``keys`` picks the physical key layout; by default it comes from
    ``settings.key_layout``, else plain keys on a standalone client and hash-tagged keys on a
    cluster client.

    ``control`` connects the broker to a RillKit control plane (see ``Control``). When it is not
    given it comes from RILLKIT_CONTROL and the config file, and is off unless one of them turns
    it on. Off, the broker is the plain library and ``broker.control`` is None.
    """

    def __init__(
        self,
        url: Optional[str] = None,
        *,
        client: Optional[Client] = None,
        settings: Optional[Settings] = None,
        middlewares: Sequence[BaseMiddleware] = (),
        keys: Optional[KeyBuilder] = None,
        control: Optional[Control] = None,
    ) -> None:
        if control is None:
            control = Control.from_env()
        if control.mode == "relay":
            try:
                import websockets.sync.client  # noqa: F401
            except ImportError as e:
                raise ConfigurationError(
                    'control mode "relay" needs the Relay client: pip install "rillkit[control]"'
                ) from e
        if client is None:
            settings = settings or Settings(url=url)
            client = settings.client()
        self.client = client
        if keys is None:
            if settings is not None and settings.has_key_settings:
                keys = settings.key_builder()
            else:
                keys = HashTagKeys() if is_cluster(client) else PlainKeys()
        self.keys: KeyBuilder = keys
        self.middlewares: list[BaseMiddleware] = []
        self.control_settings = control
        self.control: Optional[ControlPlane] = None
        if control.enabled:
            # imported only when used, so a broker with the control plane off loads none of it
            from . import controlplane
            from .logs import LogCapture
            from .tracing import TraceContext

            self.control = controlplane.ControlPlane(self, control)
            own = [type(m) for m in middlewares]
            extras: list[type[BaseMiddleware]] = [TraceContext]
            if control.capture_logs:
                extras.append(LogCapture)
            for extra in extras:
                if extra not in own:
                    self.use(extra())
        for mw in middlewares:
            self.use(mw)
        self.subscribers: list[Subscriber] = []
        self._hooks: dict[str, list[Hook]] = {
            "on_startup": [], "after_startup": [], "on_shutdown": [], "after_shutdown": [],
        }
        self._stop = threading.Event()
        self._stop_lock = threading.Lock()
        self._stopped = False

    @classmethod
    def from_env(
        cls, prefix: str = "REDIS_", *, config: Optional[str] = None, **kwargs: Any
    ) -> "Broker":
        """A broker from RILLKIT_* variables, over the config file (``config`` or
        RILLKIT_CONFIG) if one is named. Keyword arguments are passed to ``Broker``."""
        if "control" not in kwargs:
            kwargs["control"] = Control.from_env(config=config)
        return cls(settings=Settings.from_env(prefix, config=config), **kwargs)

    # middleware

    def use(self, middleware: BaseMiddleware) -> BaseMiddleware:
        middleware.broker = self
        self.middlewares.append(middleware)
        return middleware

    # subscribing and publishing

    def subscribe(
        self,
        stream: StreamLike,
        *,
        group: Optional[str] = None,
        ack_policy: AckPolicy = AckPolicy.NACK_ON_ERROR,
        max_retries: Optional[int] = 3,
        claim_idle_ms: int = 60_000,
        backoff: float = 1.0,
        max_backoff_ms: int = 15 * 60_000,
        block_ms: int = 5000,
        count: int = 10,
        middlewares: Sequence[BaseMiddleware] = (),
        consumer: Optional[str] = None,
        heartbeat: bool = True,
        heartbeat_ttl: int = 60,
        group_start_id: str = "0",
        concurrency: int = 1,
        idle_timeout: Optional[float] = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a handler for a stream. The handler is returned unchanged.

        ``concurrency`` runs that many consumer loops (threads) for the stream, named
        ``<consumer>``, ``<consumer>-1``, ... A control plane can change it at run time.
        ``idle_timeout`` stops the subscriber cleanly after that many seconds with no messages
        and nothing in flight; a nacked message waiting for its retry does not keep it alive."""
        resolved = as_stream(stream)
        if group:
            resolved = Stream(resolved.name, group, resolved.dlq, resolved.maxlen, resolved.codec)
        own = list(middlewares)
        for m in own:
            m.broker = self

        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.subscribers.append(Subscriber(
                self, resolved, func, ack_policy=ack_policy, max_retries=max_retries,
                claim_idle_ms=claim_idle_ms, backoff=backoff, max_backoff_ms=max_backoff_ms,
                block_ms=block_ms, count=count, middlewares=own,
                consumer=consumer, heartbeat=heartbeat, heartbeat_ttl=heartbeat_ttl,
                group_start_id=group_start_id, concurrency=concurrency,
                idle_timeout=idle_timeout,
            ))
            return func

        return decorator

    def publisher(self, stream: StreamLike, *, maxlen: Optional[int] = None) -> Publisher:
        return Publisher(self, as_stream(stream), maxlen)

    def publish(
        self,
        stream: StreamLike,
        body: Any,
        *,
        maxlen: Optional[int] = None,
        approximate: bool = True,
    ) -> str:
        """Add one entry to the stream and return its id. Raises ``PublishError`` on failure."""
        resolved = as_stream(stream)
        cmd = PublishCommand(
            resolved, body, maxlen if maxlen is not None else resolved.maxlen, approximate
        )
        call: Callable[[PublishCommand], str] = self._xadd
        for mw in reversed(self.middlewares):
            call = partial(mw.publish_scope, call)
        return call(cmd)

    def _xadd(self, cmd: PublishCommand) -> str:
        key, fields = self._entry(cmd)
        try:
            entry_id = self.client.xadd(
                key, fields, maxlen=cmd.maxlen, approximate=cmd.approximate,
            )
        except CLIENT_ERRORS + (TypeError, ValueError) as e:
            raise PublishError(f"could not publish to {cmd.stream.name}: {e}") from e
        return _text(entry_id)

    def _entry(self, cmd: PublishCommand) -> tuple[str, dict[Any, Any]]:
        """The physical key and encoded fields of one command (what ``XADD`` receives)."""
        try:
            payload = cmd.stream.codec.encode(cmd.body)
            fields: dict[Any, Any] = {"data": payload, **cmd.headers}
        except (TypeError, ValueError) as e:
            raise PublishError(f"could not publish to {cmd.stream.name}: {e}") from e
        return self.keys.stream(cmd.stream), fields

    def publish_many(
        self,
        stream: StreamLike,
        bodies: Iterable[Any],
        *,
        maxlen: Optional[int] = None,
        approximate: bool = True,
    ) -> list[str]:
        """Add several entries in one pipeline and return their ids, in the order given.

        Every middleware's ``publish_scope`` runs for every body, so ``TraceContext`` still gives
        each entry its own ``traceparent`` (same trace, its own span), and other headers are set
        per entry. Only the writes are batched: one network round trip instead of one per message.
        The entries are encoded before the pipeline runs, so an encoding error publishes nothing.

        Middleware sees ``""`` as the entry id inside ``publish_scope`` during a batch; ids only
        exist after the pipeline. The built-in middleware does not use that value. A failed
        pipeline raises ``PublishError``; the commands may have reached Redis.
        """
        resolved = as_stream(stream)
        entries: list[tuple[str, dict[Any, Any], Optional[int], bool]] = []

        def collect(cmd: PublishCommand) -> str:
            key, fields = self._entry(cmd)
            entries.append((key, fields, cmd.maxlen, cmd.approximate))
            return ""

        call: Callable[[PublishCommand], str] = collect
        for mw in reversed(self.middlewares):
            call = partial(mw.publish_scope, call)
        for body in bodies:
            call(PublishCommand(
                resolved, body, maxlen if maxlen is not None else resolved.maxlen, approximate
            ))
        if not entries:
            return []
        pipe = self.client.pipeline(transaction=False)
        for key, fields, entry_maxlen, entry_approximate in entries:
            pipe.xadd(key, fields, maxlen=entry_maxlen, approximate=entry_approximate)
        try:
            ids = pipe.execute()
        except CLIENT_ERRORS + (TypeError, ValueError) as e:
            raise PublishError(f"could not publish to {resolved.name}: {e}") from e
        return [_text(entry_id) for entry_id in ids]

    # lifecycle hooks

    def _hook(self, name: str) -> Callable[[Hook], Hook]:
        def register(func: Hook) -> Hook:
            self._hooks[name].append(func)
            return func
        return register

    def on_startup(self, func: Hook) -> Hook:
        """Runs first, before the connection is checked."""
        return self._hook("on_startup")(func)

    def after_startup(self, func: Hook) -> Hook:
        """Runs once groups exist and the consumer threads are running."""
        return self._hook("after_startup")(func)

    def on_shutdown(self, func: Hook) -> Hook:
        """Runs after the consumers finished their current message, before the client closes."""
        return self._hook("on_shutdown")(func)

    def after_shutdown(self, func: Hook) -> Hook:
        """Runs last, after the client is closed."""
        return self._hook("after_shutdown")(func)

    def _run_hooks(self, name: str) -> None:
        for func in self._hooks[name]:
            func()

    # running

    def start(self) -> None:
        """Start all consumers in background threads and return."""
        self._stop.clear()
        self._stopped = False
        self._run_hooks("on_startup")
        self.client.ping()
        for sub in self.subscribers:
            sub.setup()
        if self.control is not None:
            # before any polling, so a new worker honours PAUSED or MAINTENANCE from the start
            self.control.hydrate()
        for sub in self.subscribers:
            sub.start()
        if self.control is not None:
            self.control.start()
        self._run_hooks("after_startup")

    def stop(self, timeout: Optional[float] = None) -> None:
        """Ask the consumers to stop, wait for the current messages to finish, then clean up.

        Safe to call more than once or from another thread while ``run()`` is blocking: the shutdown
        hooks and the client close happen only once per start."""
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
            for sub in self.subscribers:
                sub.stop()
            for sub in self.subscribers:
                sub.join(timeout)
            if self.control is not None:
                self.control.stop(timeout)
            try:
                self._run_hooks("on_shutdown")
            finally:
                self.client.close()
                self._run_hooks("after_shutdown")
                self._stop.set()

    def run(self, handle_signals: bool = True, shutdown_timeout: Optional[float] = None) -> None:
        """Start the consumers and block until SIGINT/SIGTERM (or ``stop()``) shuts them down.

        The first signal finishes the current message and exits cleanly. A second signal exits now.
        """
        requested = threading.Event()
        previous = {}

        def on_signal(signum: int, frame: Any) -> None:
            if requested.is_set():
                log.warning("second shutdown signal received, exiting now")
                sys.exit(1)
            log.info("shutdown requested, finishing the current message before exiting")
            requested.set()

        if handle_signals and threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGINT, signal.SIGTERM):
                previous[sig] = signal.signal(sig, on_signal)
        try:
            self.start()
            while not requested.wait(0.5):
                if not any(s.is_alive() for s in self.subscribers):
                    break
            self.stop(shutdown_timeout)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)

    # operations

    def active_workers(self, stream: StreamLike, group: Optional[str] = None) -> list[str]:
        """Consumer names with a live heartbeat. Unlike XINFO CONSUMERS this includes idle workers
        that have not received a message yet.

        Reads the consumer registry (``broker.keys.consumers``), one sorted set per stream and
        group, so it needs no ``SCAN`` and works for ACL users that may not list keys. Expiry is
        judged by the workers' and caller's clocks, which should agree within the heartbeat TTL.
        Workers older than 0.3, which write no registry entry, are not listed."""
        s = as_stream(stream)
        key = self.keys.consumers(s, str(group or s.group))
        members: Any = self.client.zrangebyscore(key, int(time.time() * 1000), "+inf")
        return sorted(_text(m) for m in members)

    def replay_dlq(self, stream: StreamLike, count: int = 100) -> int:
        """Move up to ``count`` entries from the DLQ back onto the stream. Returns how many.

        Each move is one transaction when the stream and DLQ keys share a slot (always outside a
        cluster). Otherwise the entry is added to the stream first and then deleted from the DLQ,
        so a failure in between can replay it twice but never drops it."""
        s = as_stream(stream)
        stream_key, dlq_key = self.keys.stream(s), self.keys.dlq(s)
        moved = 0
        rows: Any = self.client.xrange(dlq_key, count=count)
        for entry_id, raw in rows:
            fields: dict[Any, Any] = replay_entry({_text(k): _text(v) for k, v in raw.items()})
            run_together(
                self.client, [("xadd", (stream_key, fields)), ("xdel", (dlq_key, entry_id))]
            )
            moved += 1
        return moved

    def replay_dlq_entries(
        self, stream: StreamLike, ids: Sequence[str], data: Optional[dict[str, str]] = None,
    ) -> tuple[int, list[str]]:
        """Move the DLQ entries ``ids`` back onto the stream, each as one transaction where the
        keys allow it (see ``replay_dlq``). ``data`` replaces the ``data`` field of the entries
        it names, to fix a payload before it is handled again. Returns how many were moved and
        the ids that were not in the DLQ."""
        s = as_stream(stream)
        stream_key, dlq_key = self.keys.stream(s), self.keys.dlq(s)
        data = data or {}
        moved, missing = 0, []
        for entry_id in ids:
            rows: Any = self.client.xrange(dlq_key, entry_id, entry_id)
            if not rows:
                missing.append(entry_id)
                continue
            fields: dict[Any, Any] = replay_entry({_text(k): _text(v)
                                                   for k, v in rows[0][1].items()})
            if entry_id in data:
                fields["data"] = data[entry_id]
            run_together(
                self.client, [("xadd", (stream_key, fields)), ("xdel", (dlq_key, entry_id))]
            )
            moved += 1
        return moved, missing

    def delete_dlq_entries(self, stream: StreamLike, ids: Sequence[str]) -> int:
        """Delete the DLQ entries ``ids``. Returns how many existed."""
        return int(self.client.xdel(self.keys.dlq(as_stream(stream)), *ids))

    def purge_dlq(self, stream: StreamLike) -> int:
        """Delete the whole DLQ of ``stream``. Returns how many entries it held."""
        key = self.keys.dlq(as_stream(stream))
        count = int(self.client.xlen(key))
        self.client.delete(key)
        return count

    def create_group(self, stream: StreamLike, group: str, start: str = "$") -> bool:
        """Create ``stream`` (if needed) and its consumer group ``group``, starting at ``start``:
        ``$`` for new messages only, ``0`` for everything already in the stream. Returns False
        if the group already existed."""
        try:
            self.client.xgroup_create(self.keys.stream(as_stream(stream)), group, id=start,
                                      mkstream=True)
        except RESPONSE_ERRORS as e:
            if "BUSYGROUP" not in str(e):
                raise
            return False
        return True
