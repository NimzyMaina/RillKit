import inspect
import logging
import math
import numbers
import os
import socket
import threading
import time
from collections.abc import Sequence
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    get_type_hints,
)

from .ack import AckPolicy
from .clients import RESPONSE_ERRORS, Client, run_together, socket_timeout
from .exceptions import AckMessage, DecodeError, NackMessage, RejectMessage
from .keys import HEARTBEAT_PREFIX
from .message import Message
from .middleware import BaseMiddleware
from .stream import Stream

if TYPE_CHECKING:
    from .broker import Broker

log = logging.getLogger("rillkit")

RETRY_BACKOFF_SECONDS = 5
RUNNING = "RUNNING"
__all__ = ["HEARTBEAT_PREFIX", "Subscriber"]


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _fields(raw: dict[Any, Any]) -> dict[str, str]:
    return {_text(k): _text(v) for k, v in raw.items()}


def _published_ms(msg_id: str) -> Optional[int]:
    """The millisecond timestamp in a stream entry id (``<ms>-<seq>``), or None."""
    ms = msg_id.partition("-")[0]
    return int(ms) if ms.isascii() and ms.isdigit() else None


# The data-plane rules, as pure functions so the conformance vectors can test them
# (spec/conformance/data_plane.json). Every SDK must compute the same values.

DLQ_ERROR_LIMIT = 1000
TRACE_TTL_SECONDS = 3600
TRACE_MAX_RECORDS = 1000


def entry_size(fields: dict[str, str]) -> int:
    """The size of a stream entry in bytes: its field names and values, UTF-8 encoded."""
    return sum(len(k.encode()) + len(v.encode()) for k, v in fields.items())


def lane_consumer_name(consumer: str, index: int) -> str:
    """The consumer name of lane ``index``: ``consumer`` itself, then ``<consumer>-1``, ..."""
    return consumer if index == 0 else f"{consumer}-{index}"


def retry_wait_ms(
    claim_idle_ms: int, backoff: float, max_backoff_ms: int, times_delivered: int
) -> float:
    """How long a pending message must be idle before it is claimed and retried:
    ``claim_idle_ms * backoff ** (times_delivered - 1)``, capped at ``max_backoff_ms``."""
    wait = claim_idle_ms * (backoff ** max(times_delivered - 1, 0))
    return min(wait, max_backoff_ms)


def retries_exhausted(max_retries: Optional[int], deliveries: int) -> bool:
    """Whether a message delivered ``deliveries`` times has no retries left (None: unlimited)."""
    return max_retries is not None and deliveries > max_retries


def queue_wait_ms(published_ms: float, started_ms: float) -> float:
    """How long a message waited between being published and its handler starting, in
    milliseconds (data-plane 9.1). Never negative: clock skew counts as no wait."""
    return max(0.0, started_ms - published_ms)


def idle_expired(remaining_s: Optional[float], in_flight: int) -> bool:
    """Whether an idle subscription should stop now: its idle window is used up and no lane
    has work in flight. ``remaining_s`` is None when ``idle_timeout`` is off (vectors)."""
    return remaining_s is not None and remaining_s <= 0 and in_flight == 0


def idle_read_block_ms(remaining_s: float, block_ms: int, idle_timeout_s: float) -> int:
    """The ``BLOCK`` for one read while ``idle_timeout`` is on (vectors): never 0, which blocks
    forever in Redis, and never past the remaining idle time, or the full timeout while another
    lane still has work in flight."""
    cap_s = remaining_s if remaining_s > 0 else idle_timeout_s
    return max(1, min(block_ms, int(cap_s * 1000)))


def dead_letter_entry(
    fields: dict[str, str], msg_id: str, stream: str, group: str, error: str, deliveries: int
) -> dict[str, str]:
    """The DLQ entry for a message: its own fields plus the ``x-`` context fields. A field of
    the same name in the original entry is overwritten."""
    entry = dict(fields)
    entry.update({
        "x-original-id": msg_id,
        "x-stream": stream,
        "x-group": group,
        "x-error": error[:DLQ_ERROR_LIMIT],
        "x-deliveries": str(deliveries),
    })
    return entry


def replay_entry(fields: dict[str, str]) -> dict[str, str]:
    """The entry a DLQ entry is replayed as: every field except the ``x-`` ones."""
    return {k: v for k, v in fields.items() if not k.startswith("x-")}


def _is_model(annotation: Any) -> bool:
    return inspect.isclass(annotation) and hasattr(annotation, "model_validate")


def _plan(handler: Callable[..., Any]) -> list[tuple[str, str, Any]]:
    """Decide what each handler parameter receives: the ``Message``, the plain body, or a model."""
    try:
        hints = get_type_hints(handler)
    except Exception:
        hints = getattr(handler, "__annotations__", {})
    plan: list[tuple[str, str, Any]] = []
    bodies = 0
    for name, param in inspect.signature(handler).parameters.items():
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            continue
        ann = hints.get(name, param.annotation)
        if ann is Message or (ann is param.empty and name in ("msg", "message")):
            plan.append((name, "message", None))
        elif _is_model(ann):
            plan.append((name, "model", ann))
            bodies += 1
        else:
            plan.append((name, "body", None))
            bodies += 1
    if bodies > 1:
        raise TypeError(f"{handler!r} takes more than one body parameter")
    return plan


class Subscriber:
    """Runs one consumer loop for one stream and group: claim stuck messages, then read new ones."""

    def __init__(
        self,
        broker: "Broker",
        stream: Stream,
        handler: Callable[..., Any],
        *,
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
    ) -> None:
        if not stream.group:
            raise ValueError(f"stream {stream.name!r} needs a group to be consumed")
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if block_ms < 1:
            raise ValueError("block_ms must be at least 1")
        if idle_timeout is not None and (
            isinstance(idle_timeout, bool)
            or not isinstance(idle_timeout, numbers.Real)
            or not math.isfinite(idle_timeout)
            or idle_timeout <= 0
        ):
            raise ValueError("idle_timeout must be positive")
        self.broker = broker
        self.stream = stream
        self.group: str = stream.group
        self.handler = handler
        self.ack_policy = AckPolicy(ack_policy)
        self.max_retries = max_retries
        self.claim_idle_ms = claim_idle_ms
        self.backoff = backoff
        self.max_backoff_ms = max_backoff_ms
        self.block_ms = block_ms
        self._block_ms = block_ms  # what is actually used, see setup()
        self._max_block_ms: Optional[int] = None  # the most the socket timeout allows
        self.count = count
        self.own_middlewares = list(middlewares)
        self.consumer = consumer or f"{stream.group}-{socket.gethostname()}-{os.getpid()}"
        self.heartbeat = heartbeat
        self.heartbeat_ttl = heartbeat_ttl
        # per consumer (lane) name -> monotonic time of the last heartbeat write
        self._last_beat: dict[str, float] = {}
        self.group_start_id = group_start_id
        self.concurrency = concurrency
        self.idle_timeout = idle_timeout
        self._plan = _plan(handler)
        self._stop = threading.Event()
        # Lanes are consumer loops, each with its own consumer name (lane 0 uses ``consumer``).
        # Their number is ``concurrency``, or what the control plane asks for.
        self._lanes: dict[int, threading.Thread] = {}
        self._threads: list[threading.Thread] = []  # every lane thread started, for join()
        self._first: Optional[threading.Thread] = None
        self._lanes_lock = threading.Lock()
        self._target = 0
        self._counter_lock = threading.Lock()
        self._last_activity: Optional[float] = None
        self.in_flight = 0
        self.processed = 0
        # counters since start, reported in telemetry (spec 5.1): each handler run counts once
        self.succeeded_total = 0
        self.failed_total = 0
        self.dead_lettered_total = 0
        self.bytes_total = 0
        # attempt timings since start (data-plane 9.1): milliseconds, reported in telemetry
        self.attempts_total = 0
        self.wait_ms_total = 0.0
        self.work_ms_total = 0.0

    @property
    def client(self) -> Client:
        return self.broker.client

    @property
    def stream_key(self) -> str:
        """The physical key of the stream, from the broker's key builder."""
        return self.broker.keys.stream(self.stream)

    @property
    def dlq_key(self) -> str:
        return self.broker.keys.dlq(self.stream)

    @property
    def middlewares(self) -> list[BaseMiddleware]:
        """Broker-wide middleware first (outermost), then this subscriber's own."""
        return [*self.broker.middlewares, *self.own_middlewares]

    @property
    def heartbeat_key(self) -> str:
        return self.broker.keys.heartbeat(self.stream, self.group, self.consumer)

    @property
    def registry_key(self) -> str:
        """The sorted set of live consumers of this stream and group (``active_workers``)."""
        return self.broker.keys.consumers(self.stream, self.group)

    def lane_consumer(self, index: int) -> str:
        """The consumer name of lane ``index``: ``consumer``, then ``<consumer>-1``, ..."""
        return lane_consumer_name(self.consumer, index)

    @property
    def _thread(self) -> Optional[threading.Thread]:
        """The first lane's thread (the only one unless ``concurrency`` is above 1)."""
        return self._first

    @property
    def lanes(self) -> int:
        """How many lanes (consumer loops) are running now."""
        with self._lanes_lock:
            return sum(1 for t in self._lanes.values() if t.is_alive())

    def is_alive(self) -> bool:
        """Whether this subscriber is still consuming, or (under a control plane) waiting to."""
        if self._stop.is_set():
            return False
        if self.broker.control is not None:
            return True
        return any(t.is_alive() for t in list(self._threads))

    def _desired(self) -> tuple[str, int, int, int]:
        """(state, lanes, block_ms, count) to use now: the control plane's, else the code's."""
        control = self.broker.control
        if control is None:
            return RUNNING, self.concurrency, self._block_ms, self.count
        d = control.desired(self)
        block = self._block_ms
        if d.block_ms is not None:
            top = self._max_block_ms
            block = d.block_ms if top is None else min(d.block_ms, top)
        lanes = d.concurrency if d.concurrency is not None else self.concurrency
        count = d.batch_size if d.batch_size is not None else self.count
        return d.state, lanes, block, count

    # lifecycle

    def setup(self) -> None:
        # A blocking read that outlasts the client's socket timeout fails and is retried by
        # redis-py, so keep the block time a second under the timeout.
        timeout = socket_timeout(self.client)
        if timeout is not None:
            self._max_block_ms = max(100, int((timeout - 1) * 1000))
        if timeout is not None and self.block_ms / 1000 > timeout - 1:
            self._block_ms = max(100, int((timeout - 1) * 1000))
            log.warning(
                "client socket_timeout is %.1f s, shorter than block_ms=%d; using %d ms",
                timeout, self.block_ms, self._block_ms,
            )
        try:
            self.client.xgroup_create(
                self.stream_key, self.group, id=self.group_start_id, mkstream=True
            )
            log.debug("created group %s on %s", self.group, self.stream_key)
        except RESPONSE_ERRORS as e:
            if "BUSYGROUP" not in str(e):
                raise

    def _touch_activity(self) -> None:
        with self._counter_lock:
            self._last_activity = time.monotonic()

    def _idle_state(self) -> tuple[Optional[float], int]:
        """Remaining idle seconds before exit (None if ``idle_timeout`` is off) and in-flight."""
        with self._counter_lock:
            if self.idle_timeout is None:
                return None, self.in_flight
            now = time.monotonic()
            if self._last_activity is None:
                self._last_activity = now
            return self.idle_timeout - (now - self._last_activity), self.in_flight

    def start(self) -> None:
        self._stop.clear()
        self._touch_activity()
        self.reconcile()

    def reconcile(self) -> None:
        """Start or retire lanes to match the desired number. Retired lanes finish the messages
        they already read, then deregister. ``DRAINING`` means zero lanes."""
        state, lanes, _, _ = self._desired()
        with self._lanes_lock:
            had_live = any(t.is_alive() for t in self._lanes.values())
            self._target = 0 if state == "DRAINING" or self._stop.is_set() else lanes
            if self._target > 0 and not had_live:
                # Exiting DRAINING or starting fresh gives the subscriber a full idle window.
                self._touch_activity()
            for index in range(self._target):
                thread = self._lanes.get(index)
                if thread is not None and thread.is_alive():
                    continue
                thread = threading.Thread(
                    target=self._run, args=(index,), daemon=True,
                    name=f"rillkit-{self.stream.name}" + (f"-{index}" if index else ""),
                )
                self._lanes[index] = thread
                self._threads = [t for t in self._threads if t.is_alive()] + [thread]
                if index == 0:
                    self._first = thread
                thread.start()

    def stop(self) -> None:
        self._stop.set()
        control = self.broker.control
        if control is not None:
            control.wake()

    def join(self, timeout: Optional[float] = None) -> None:
        deadline = None if timeout is None else time.monotonic() + timeout
        for thread in list(self._threads):
            left = None if deadline is None else max(0.0, deadline - time.monotonic())
            thread.join(left)

    def _retire(self, index: int) -> bool:
        """Whether lane ``index`` should stop. If so it is removed, and its heartbeat with it,
        under the lanes lock so a restarted lane of the same index never loses its heartbeat."""
        with self._lanes_lock:
            if not self._stop.is_set() and index < self._target:
                return False
            if self._lanes.get(index) is threading.current_thread():
                self._unregister(self.lane_consumer(index))
                del self._lanes[index]
            return True

    def _unregister(self, consumer: str) -> None:
        if not self.heartbeat:
            return
        with self._counter_lock:
            self._last_beat.pop(consumer, None)
        try:
            pipe = self.client.pipeline(transaction=False)
            pipe.delete(self.broker.keys.heartbeat(self.stream, self.group, consumer))
            pipe.zrem(self.registry_key, consumer)
            pipe.execute()
        except Exception as e:
            log.error("could not remove heartbeat key: %s", e)

    def _run(self, index: int = 0) -> None:
        consumer = self.lane_consumer(index)
        log.info(
            "consumer started | stream: %s | group: %s | consumer: %s | policy: %s"
            " | max retries: %s",
            self.stream.name, self.group, consumer, self.ack_policy.value,
            self.max_retries,
        )
        control = self.broker.control
        try:
            while not self._retire(index):
                self._beat(consumer)
                version = control.version if control is not None else 0
                state, _, block_ms, count = self._desired()
                if state != RUNNING:
                    # paused, in maintenance or draining: in-flight messages are done, so wait
                    # for the next change, beating now and then to stay visible
                    assert control is not None
                    self._touch_activity()
                    control.wait(version, max(1.0, self.heartbeat_ttl / 3))
                    self._touch_activity()
                    continue
                try:
                    read_block_ms: Optional[int] = block_ms
                    if self.idle_timeout is not None:
                        remaining, in_flight = self._idle_state()
                        assert remaining is not None
                        if idle_expired(remaining, in_flight):
                            if self._drain_or_stop(consumer, count):
                                break
                            continue
                        read_block_ms = idle_read_block_ms(
                            remaining, block_ms, self.idle_timeout
                        )
                    for msg in self._claim(consumer, count):
                        self._process(msg)
                    for msg in self._read(consumer, count, read_block_ms):
                        if self._stop.is_set():
                            break  # unread leftovers stay pending, claimed after the idle time
                        self._process(msg)
                except RESPONSE_ERRORS as e:
                    if "NOGROUP" in str(e):
                        self.setup()
                    else:
                        self._loop_error(e)
                except Exception as e:  # client or other loop-level error: keep the worker alive
                    self._loop_error(e)
        finally:
            with self._lanes_lock:
                if self._lanes.get(index) is threading.current_thread():
                    self._unregister(consumer)
                    del self._lanes[index]
            log.info(
                "consumer stopped | stream: %s | consumer: %s", self.stream.name, consumer
            )

    def _drain_or_stop(self, consumer: str, count: int) -> bool:
        """Final non-blocking poll when idle; sets ``_stop`` and returns True if still empty."""
        if self._stop.is_set():
            return True
        claimed = self._claim(consumer, count)
        for msg in claimed:
            self._process(msg)
        if self._stop.is_set():
            return True
        read = self._read(consumer, count, block_ms=0)
        for msg in read:
            if self._stop.is_set():
                break
            self._process(msg)
        if claimed or read:
            return False
        remaining, in_flight = self._idle_state()
        if idle_expired(remaining, in_flight):
            if not self._stop.is_set():
                log.info(
                    "consumer idle timeout reached (%.3fs) | stream: %s | consumer: %s",
                    self.idle_timeout, self.stream.name, consumer,
                )
                self._stop.set()
            return True
        return False

    def _loop_error(self, e: Exception) -> None:
        log.error("consumer loop error, retrying in %s s: %s", RETRY_BACKOFF_SECONDS, e)
        self._stop.wait(RETRY_BACKOFF_SECONDS)

    def _beat(self, consumer: Optional[str] = None) -> None:
        if not self.heartbeat:
            return
        consumer = consumer or self.consumer
        mono = time.monotonic()
        # Heartbeat writes are 3 Redis commands, so coalesce them: often enough to stay alive,
        # never once per message. With the default TTL of 60 s that is one write every 5 s.
        interval = min(5.0, max(0.5, self.heartbeat_ttl / 3))
        with self._counter_lock:
            last = self._last_beat.get(consumer)
            if last is not None and mono < last + interval:
                return
            self._last_beat[consumer] = mono
        now = time.time()
        try:
            pipe = self.client.pipeline(transaction=False)
            key = self.broker.keys.heartbeat(self.stream, self.group, consumer)
            pipe.set(key, int(now), ex=self.heartbeat_ttl)
            # the registry: this consumer is alive until now + ttl; drop entries already expired
            pipe.zadd(self.registry_key, {consumer: int((now + self.heartbeat_ttl) * 1000)})
            pipe.zremrangebyscore(self.registry_key, "-inf", int(now * 1000))
            pipe.execute()
        except Exception as e:
            log.error("heartbeat failed: %s", e)

    # reading

    def _message(
        self, id: Any, raw: dict[Any, Any], deliveries: int, consumer: Optional[str] = None
    ) -> Message:
        return Message(
            _text(id), self.stream, _fields(raw), deliveries, consumer or self.consumer,
            self.client, key=self.stream_key,
        )

    def _read(
        self, consumer: Optional[str] = None, count: Optional[int] = None,
        block_ms: Optional[int] = None,
    ) -> list[Message]:
        consumer = consumer or self.consumer
        # BLOCK 0 in Redis/Valkey means block forever; pass block=None for a non-blocking poll.
        block = (
            None if block_ms is not None and block_ms <= 0
            else (self._block_ms if block_ms is None else block_ms)
        )
        result: Any = self.client.xreadgroup(
            self.group, consumer, {self.stream_key: ">"},
            count=count or self.count, block=block,
        )
        messages = [
            self._message(i, f, 1, consumer) for _, entries in result or [] for i, f in entries
        ]
        if messages:
            self._touch_activity()
        return messages

    def _required_idle_ms(self, times_delivered: int) -> float:
        return retry_wait_ms(self.claim_idle_ms, self.backoff, self.max_backoff_ms,
                             times_delivered)

    def _exhausted(self, deliveries: int) -> bool:
        return retries_exhausted(self.max_retries, deliveries)

    def _claim(self, consumer: Optional[str] = None, count: Optional[int] = None) -> list[Message]:
        """Take over pending messages idle long enough, honouring the retry backoff."""
        consumer = consumer or self.consumer
        pending: Any = self.client.xpending_range(
            self.stream_key, self.group, "-", "+", count or self.count, idle=self.claim_idle_ms
        )
        due = {
            _text(p["message_id"]): p["times_delivered"]
            for p in pending
            if p["time_since_delivered"] >= self._required_idle_ms(p["times_delivered"])
        }
        if not due:
            return []
        claimed: Any = self.client.xclaim(
            self.stream_key, self.group, consumer, self.claim_idle_ms, list(due)
        )
        if claimed:
            self._touch_activity()
        messages = []
        for id, raw in claimed:
            if raw is None:  # entry was trimmed from the stream while pending
                self.client.xack(self.stream_key, self.group, id)
                continue
            attempts = due[_text(id)]
            if self._exhausted(attempts):
                # not run again, so the delivery count is the attempts it already had
                self._dead_letter(self._message(id, raw, attempts, consumer), "retries exhausted")
                continue
            msg = self._message(id, raw, attempts + 1, consumer)
            messages.append(msg)
        return messages

    # processing

    def _invoke(self, msg: Message) -> Any:
        kwargs: dict[str, Any] = {}
        for name, kind, model in self._plan:
            if kind == "message":
                kwargs[name] = msg
            elif kind == "model":
                try:
                    kwargs[name] = model.model_validate(msg.body)
                except DecodeError:
                    raise
                except Exception as e:
                    raise DecodeError(f"body failed validation: {e}") from e
            else:
                kwargs[name] = msg.body
        result = self.handler(**kwargs)
        publishers = getattr(self.handler, "_rillkit_publishers", ())
        if result is not None:
            for publisher in publishers:
                publisher.publish(result)
        return result

    def _chain(self) -> Callable[[Message], Any]:
        call = self._invoke
        for mw in reversed(self.middlewares):
            call = partial(mw.consume_scope, call)
        return call

    def _process(self, msg: Message) -> None:
        control = self.broker.control
        with self._counter_lock:
            self.in_flight += 1
        ok = dead = False
        timing: Optional[tuple[float, float, float]] = None
        try:
            ok, dead, timing = self._handle(msg)
        finally:
            size = entry_size(msg.raw)
            with self._counter_lock:
                self.in_flight -= 1
                self.processed += 1
                self._last_activity = time.monotonic()
                self.bytes_total += size
                if ok:
                    self.succeeded_total += 1
                else:
                    self.failed_total += 1
                if timing is not None:
                    self.attempts_total += 1
                    self.wait_ms_total += timing[2]
                    self.work_ms_total += timing[1]
            if control is not None and control.settings.trace_records:
                self._trace_record(msg, timing, ok, dead)

    def _handle(self, msg: Message) -> tuple[bool, bool, Optional[tuple[float, float, float]]]:
        """Run the message through middleware and the handler.

        Returns ``(ok, dead_lettered, timing)``. ``ok`` is True if the handler finished without
        raising and the message was acknowledged, not nacked or rejected. ``dead_lettered`` is
        True if this attempt moved the message to the DLQ. ``timing`` is ``(started_at_seconds,
        work_ms, wait_ms)`` while a control plane is on, else None (data-plane 9.1)."""
        control = self.broker.control
        started_wall = 0.0
        work_ms = 0.0
        policy = self.ack_policy
        exc: Optional[BaseException] = None
        dead = False
        if policy is AckPolicy.ACK_FIRST:
            msg.ack()
        try:
            # Time the handler itself, not the acknowledgement: a slow Redis must not look like
            # a slow handler on the bottleneck chart.
            try:
                started_wall = time.time() if control is not None else 0.0
                handler_started = time.monotonic() if control is not None else 0.0
                for mw in self.middlewares:
                    mw.on_receive(msg)
                self._chain()(msg)
            finally:
                if control is not None:
                    work_ms = (time.monotonic() - handler_started) * 1000
        except AckMessage:
            msg.ack()
        except NackMessage as e:
            msg.nack()
            exc = e
        except RejectMessage as e:
            msg.reject(str(e) or "rejected")
            exc = e
        except DecodeError as e:
            log.error("message %s cannot be decoded, sending to DLQ: %s", msg.id, e)
            exc = e
            dead = True
            self._dead_letter(msg, str(e))
        except Exception as e:
            log.error("handler failed for message %s (delivery %s): %s", msg.id, msg.deliveries, e)
            exc = e
            msg.error = e
            if msg.rejected:
                pass  # the handler already asked to skip retries
            elif policy is AckPolicy.ACK_ALWAYS:
                msg.ack()
            elif policy in (AckPolicy.NACK_ON_ERROR, AckPolicy.MANUAL):
                msg.nack()
        else:
            auto_ack = policy in (AckPolicy.NACK_ON_ERROR, AckPolicy.ACK_ALWAYS)
            if auto_ack and not msg.nacked and not msg.rejected:
                msg.ack()

        timing: Optional[tuple[float, float, float]] = None
        if control is not None:
            published = _published_ms(msg.id)
            wait_ms = 0.0 if published is None else queue_wait_ms(published, started_wall * 1000)
            timing = (started_wall, work_ms, wait_ms)

        if msg.rejected:
            reason = msg.reject_reason or (str(exc) or type(exc).__name__ if exc else "rejected")
            dead = True
            self._dead_letter(msg, reason)
        elif msg.nacked and not msg.acked and self._exhausted(msg.deliveries):
            dead = True
            self._dead_letter(msg, (str(exc) or type(exc).__name__) if exc else "nacked")
        self._after(msg, exc)
        self._beat(msg.consumer)
        ok = exc is None and msg.acked and not msg.rejected
        return ok, dead, timing

    def _trace_record(self, msg: Message, timing: Optional[tuple[float, float, float]],
                      ok: bool, dead: bool) -> None:
        """Append this attempt to the trace key (data-plane 9.2). A failure here is logged and
        never fails or retries the message."""
        if timing is None:
            return
        try:
            from . import tracing

            parent = tracing.parse_traceparent(msg.raw.get(tracing.FIELD))
            span = tracing.parse_traceparent(msg.traceparent)
            control = self.broker.control
            if parent is None or span is None or control is None:
                return
            started_wall, work_ms, _ = timing
            started_ms = int(round(started_wall * 1000))
            published = _published_ms(msg.id)
            record = {
                "trace_id": span.trace_id,
                "span_id": span.span_id,
                "parent_span_id": parent.span_id,
                "stream": self.stream.name,
                "group": self.group,
                "message_id": msg.id,
                "consumer": msg.consumer,
                "node_id": control.node_id,
                "delivery": str(max(1, msg.deliveries)),
                "published_at": str(started_ms if published is None else published),
                "started_at": str(started_ms),
                "started_us": str(int(round(started_wall * 1_000_000))),
                "finished_at": str(started_ms + int(round(work_ms))),
                "outcome": "dead_letter" if dead else ("ok" if ok else "error"),
            }
            key = self.broker.keys.trace(span.trace_id)
            pipe = self.client.pipeline(transaction=False)
            pipe.xadd(key, record, maxlen=TRACE_MAX_RECORDS, approximate=True)
            pipe.expire(key, TRACE_TTL_SECONDS)
            pipe.execute()
        except Exception as e:
            log.debug("could not write the trace record for message %s: %s", msg.id, e)

    def _after(self, msg: Message, exc: Optional[BaseException]) -> None:
        for mw in self.middlewares:
            try:
                mw.after_processed(msg, exc)
            except Exception as e:
                log.error("after_processed failed in %s: %s", type(mw).__name__, e)

    def _dead_letter(self, msg: Message, error: str) -> None:
        """Copy the entry to the DLQ stream with context, then ack it so it stops being retried.

        Both happen in one transaction when the stream and DLQ keys share a slot (always outside a
        cluster). Otherwise the copy runs first: if the ack then fails the message stays pending,
        is retried and can reach the DLQ twice, but it is never lost.
        """
        with self._counter_lock:
            self.dead_lettered_total += 1
        log.warning("moving message %s to %s: %s", msg.id, self.dlq_key, error)
        entry: dict[Any, Any] = dead_letter_entry(
            msg.raw, msg.id, self.stream.name, self.group, error, msg.deliveries
        )
        run_together(self.client, [
            ("xadd", (self.dlq_key, entry)),
            ("xack", (self.stream_key, self.group, msg.id)),
        ])
        msg.acked = True
        msg.nacked = False
