"""Per-message log capture.

``LogCapture`` records the log records emitted while a handler runs, in the thread that runs it,
and writes them to a stream of their own: the log key for ``(stream, message id)`` from the
broker's key builder (``rillkit:logs:<stream>:<id>`` or ``rk:{<stream>}:logs:<id>``), which
expires after ``ttl`` seconds.

Captured logs are stored in Redis or Valkey, and a control plane may show them to anyone with
dashboard access, so capture is opt-in: add the middleware, or set ``Control(capture_logs=True)``.
Handler logs often hold message data, so narrow what is kept with ``loggers`` and ``redact``.
Exception messages and tracebacks are left out unless ``exceptions=True``.

Records are captured through a handler on the root logger, so the usual logging levels decide
what is emitted: a logger whose effective level is WARNING produces no INFO records to capture.
"""

import logging
import threading
import time
import traceback
from collections.abc import Sequence
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .message import Message
from .middleware import BaseMiddleware
from .tracing import current_traceparent

log = logging.getLogger("rillkit")

_MESSAGE_LIMIT = 4000
Record = dict[str, str]
Redactor = Callable[[Record], Optional[Record]]


@dataclass
class _Capture:
    """What is being captured for the message handled in this context."""

    limit: int
    loggers: Optional[tuple[str, ...]]
    exceptions: bool
    records: list[Record] = field(default_factory=list)

    def wants(self, logger: str) -> bool:
        if self.loggers is None:
            return True
        return any(logger == name or logger.startswith(f"{name}.") for name in self.loggers)

    def add(self, level: str, logger: str, message: str,
            exc: Optional[BaseException] = None) -> None:
        if len(self.records) >= self.limit:
            return
        fields = {
            "ts": str(int(time.time() * 1000)),
            "level": level,
            "logger": logger,
            "message": message[:_MESSAGE_LIMIT],
        }
        traceparent = current_traceparent()
        if traceparent:
            fields["traceparent"] = traceparent
        if exc is not None:
            fields["exc_type"] = type(exc).__name__
            if self.exceptions:
                text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
                fields["exc"] = text[:_MESSAGE_LIMIT]
        self.records.append(fields)


_current: ContextVar[Optional[_Capture]] = ContextVar("rillkit_logs", default=None)


class _CaptureHandler(logging.Handler):
    """Hands each record to the capture of the message handled in this context, if any."""

    def emit(self, record: logging.LogRecord) -> None:
        capture = _current.get()
        if capture is None or not capture.wants(record.name):
            return
        try:
            exc = record.exc_info[1] if record.exc_info else None
            capture.add(record.levelname, record.name, record.getMessage(), exc)
        except Exception:  # never let logging break a handler
            self.handleError(record)


_handler: Optional[_CaptureHandler] = None
_install_lock = threading.Lock()


def _install() -> None:
    global _handler
    with _install_lock:
        if _handler is None:
            _handler = _CaptureHandler(logging.NOTSET)
            logging.getLogger().addHandler(_handler)


class LogCapture(BaseMiddleware):
    """Store the log records of each handler run in a TTL-bounded stream per message.

    - ``ttl``: seconds the records are kept.
    - ``max_records``: how many records one handler run keeps; a redelivery appends to the same
      stream.
    - ``loggers``: capture only these loggers and their children (``["app"]`` takes
      ``app.orders`` too). Default: every logger.
    - ``redact``: called with each record (a dict of strings) before it is stored; return a
      changed record, or None to drop it.
    - ``exceptions``: also store exception messages and tracebacks (``exc``). Off by default,
      because they often contain message data; the class name (``exc_type``) is always kept.

    Failing to write the logs, or a ``redact`` that raises, never fails the message: the
    records of that run are dropped and a warning is logged.
    """

    def __init__(
        self,
        ttl: int = 24 * 3600,
        max_records: int = 200,
        *,
        loggers: Optional[Sequence[str]] = None,
        redact: Optional[Redactor] = None,
        exceptions: bool = False,
    ) -> None:
        self.ttl = ttl
        self.max_records = max_records
        self.loggers = None if loggers is None else tuple(loggers)
        self.redact = redact
        self.exceptions = exceptions
        _install()

    def consume_scope(self, call_next: Callable[[Message], Any], msg: Message) -> Any:
        capture = _Capture(self.max_records, self.loggers, self.exceptions)
        token = _current.set(capture)
        try:
            return call_next(msg)
        except Exception as e:
            capture.add("ERROR", "rillkit", f"handler raised {type(e).__name__}", e)
            raise
        finally:
            _current.reset(token)
            self._flush(msg, capture.records)

    def _flush(self, msg: Message, records: list[Record]) -> None:
        if not records:
            return
        key = self.broker.keys.logs(msg.stream, msg.id)
        try:
            kept = []
            for fields in records:
                fields = {**fields, "consumer": msg.consumer, "delivery": str(msg.deliveries)}
                if self.redact is not None:
                    redacted = self.redact(dict(fields))
                    if redacted is None:
                        continue
                    fields = {str(k): str(v) for k, v in redacted.items()}
                kept.append(fields)
            if not kept:
                return
            pipe = self.broker.client.pipeline(transaction=False)
            for fields in kept:
                pipe.xadd(key, fields, maxlen=self.max_records * 4, approximate=True)
            pipe.expire(key, self.ttl)
            pipe.execute()
        except Exception as e:
            log.warning("could not store the logs of message %s: %s", msg.id, e)


def read_logs(broker: Any, stream: Any, msg_id: str) -> list[Record]:
    """The records ``LogCapture`` stored for one message of ``stream``, oldest first."""
    from .consumer import _fields
    from .stream import as_stream

    rows: Any = broker.client.xrange(broker.keys.logs(as_stream(stream), msg_id))
    return [_fields(raw) for _, raw in rows]
