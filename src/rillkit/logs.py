"""Per-message log capture.

``LogCapture`` records every log record emitted while a handler runs, in the thread that runs
it, and writes them to a stream of their own: the log key for ``(stream, message id)`` from the
broker's key builder (``rillkit:logs:<stream>:<id>`` or ``rk:{<stream>}:logs:<id>``), which
expires after ``ttl`` seconds. Brokers with a control plane add it automatically.

Records are captured through a handler on the root logger, so the usual logging levels decide
what is emitted: a logger whose effective level is WARNING produces no INFO records to capture.
"""

import logging
import threading
import time
import traceback
from contextvars import ContextVar
from typing import Any, Callable, Optional

from .message import Message
from .middleware import BaseMiddleware
from .tracing import current_traceparent

log = logging.getLogger("rillkit")

_buffer: ContextVar[Optional[list[dict[str, str]]]] = ContextVar("rillkit_logs", default=None)
_MESSAGE_LIMIT = 4000


def _record(level: str, logger: str, message: str, exc: Optional[str] = None) -> dict[str, str]:
    fields = {
        "ts": str(int(time.time() * 1000)),
        "level": level,
        "logger": logger,
        "message": message[:_MESSAGE_LIMIT],
    }
    traceparent = current_traceparent()
    if traceparent:
        fields["traceparent"] = traceparent
    if exc:
        fields["exc"] = exc[:_MESSAGE_LIMIT]
    return fields


class _CaptureHandler(logging.Handler):
    """Appends records to the buffer of the message being handled in this context, if any."""

    def __init__(self) -> None:
        super().__init__(logging.NOTSET)
        self.limit = 200

    def emit(self, record: logging.LogRecord) -> None:
        buffer = _buffer.get()
        if buffer is None or len(buffer) >= self.limit:
            return
        try:
            exc = None
            if record.exc_info and record.exc_info[0] is not None:
                exc = "".join(traceback.format_exception(*record.exc_info))
            buffer.append(_record(record.levelname, record.name, record.getMessage(), exc))
        except Exception:  # never let logging break a handler
            self.handleError(record)


_handler: Optional[_CaptureHandler] = None
_install_lock = threading.Lock()


def _install() -> _CaptureHandler:
    global _handler
    with _install_lock:
        if _handler is None:
            _handler = _CaptureHandler()
            logging.getLogger().addHandler(_handler)
        return _handler


class LogCapture(BaseMiddleware):
    """Store the log records of each handler run in a TTL-bounded stream per message.

    ``ttl`` is in seconds. ``max_records`` caps how many records one message run keeps; a
    redelivery appends to the same stream. Failing to write the logs never fails the message.
    """

    def __init__(self, ttl: int = 24 * 3600, max_records: int = 200) -> None:
        self.ttl = ttl
        self.max_records = max_records
        _install().limit = max(_install().limit, max_records)

    def consume_scope(self, call_next: Callable[[Message], Any], msg: Message) -> Any:
        buffer: list[dict[str, str]] = []
        token = _buffer.set(buffer)
        try:
            return call_next(msg)
        except Exception as e:
            if len(buffer) < self.max_records:
                buffer.append(_record(
                    "ERROR", "rillkit", f"handler raised {type(e).__name__}: {e}",
                    "".join(traceback.format_exception(type(e), e, e.__traceback__)),
                ))
            raise
        finally:
            _buffer.reset(token)
            self._flush(msg, buffer[: self.max_records])

    def _flush(self, msg: Message, records: list[dict[str, str]]) -> None:
        if not records:
            return
        key = self.broker.keys.logs(msg.stream, msg.id)
        try:
            pipe = self.broker.client.pipeline(transaction=False)
            for fields in records:
                fields = {**fields, "consumer": msg.consumer, "delivery": str(msg.deliveries)}
                pipe.xadd(key, fields, maxlen=self.max_records * 4, approximate=True)
            pipe.expire(key, self.ttl)
            pipe.execute()
        except Exception as e:
            log.warning("could not store the logs of message %s: %s", msg.id, e)


def read_logs(broker: Any, stream: Any, msg_id: str) -> list[dict[str, str]]:
    """The records ``LogCapture`` stored for one message of ``stream``, oldest first."""
    from .consumer import _fields
    from .stream import as_stream

    rows: Any = broker.client.xrange(broker.keys.logs(as_stream(stream), msg_id))
    return [_fields(raw) for _, raw in rows]
