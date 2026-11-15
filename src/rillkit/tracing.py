"""W3C trace context (``traceparent``) carried across streams.

``TraceContext`` reads the ``traceparent`` field of each delivered entry, opens a child span for
the handler, and writes a ``traceparent`` on everything published while the handler runs (or
while ``use_traceparent`` is active), so one trace follows a request through every stream it
crosses. Brokers with a control plane add it automatically; add it yourself otherwise::

    broker = Broker(middlewares=[TraceContext()])

It only propagates ids. Exporting spans (for example to OpenTelemetry) is up to the caller:
``current_traceparent()`` returns the active header.
"""

import contextlib
import re
import secrets
from collections.abc import Iterator
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Callable, Optional

from .message import Message
from .middleware import BaseMiddleware, PublishCommand

FIELD = "traceparent"
_HEADER = re.compile(r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})(-.*)?$")


@dataclass(frozen=True)
class SpanContext:
    trace_id: str
    span_id: str
    flags: str = "01"

    @property
    def traceparent(self) -> str:
        return f"00-{self.trace_id}-{self.span_id}-{self.flags}"

    def child(self) -> "SpanContext":
        return SpanContext(self.trace_id, new_span_id(), self.flags)


def new_trace_id() -> str:
    return secrets.token_hex(16)


def new_span_id() -> str:
    return secrets.token_hex(8)


def parse_traceparent(value: Optional[str]) -> Optional[SpanContext]:
    """The span a ``traceparent`` header names, or None if it is missing or invalid."""
    if not value:
        return None
    match = _HEADER.match(value.strip().lower())
    if match is None:
        return None
    version, trace_id, span_id, flags, rest = match.groups()
    if version == "ff" or (version == "00" and rest):
        return None
    if trace_id == "0" * 32 or span_id == "0" * 16:
        return None
    return SpanContext(trace_id, span_id, flags)


_current: ContextVar[Optional[SpanContext]] = ContextVar("rillkit_span", default=None)


def current_span() -> Optional[SpanContext]:
    return _current.get()


def current_traceparent() -> Optional[str]:
    """The ``traceparent`` of the active span: the message being handled, or ``use_traceparent``.
    """
    span = _current.get()
    return None if span is None else span.traceparent


@contextlib.contextmanager
def use_traceparent(traceparent: Optional[str]) -> Iterator[Optional[SpanContext]]:
    """Make ``traceparent`` (for example from an incoming HTTP request) the parent of what is
    published inside the block. An invalid or missing header starts a new trace."""
    parent = parse_traceparent(traceparent)
    span = parent.child() if parent else SpanContext(new_trace_id(), new_span_id())
    token = _current.set(span)
    try:
        yield span
    finally:
        _current.reset(token)


class TraceContext(BaseMiddleware):
    """Propagate W3C ``traceparent`` through published and consumed messages.

    ``originate``: whether a publish outside any trace starts a new one (default True). With
    False, only messages published inside a trace carry a ``traceparent``.
    """

    def __init__(self, originate: bool = True) -> None:
        self.originate = originate

    def consume_scope(self, call_next: Callable[[Message], Any], msg: Message) -> Any:
        parent = parse_traceparent(msg.raw.get(FIELD))
        span = parent.child() if parent else SpanContext(new_trace_id(), new_span_id())
        msg.traceparent = span.traceparent
        token = _current.set(span)
        try:
            return call_next(msg)
        finally:
            _current.reset(token)

    def publish_scope(self, call_next: Callable[[PublishCommand], str], cmd: PublishCommand) -> str:
        if FIELD not in cmd.headers:
            span = _current.get()
            if span is not None:
                cmd.headers[FIELD] = span.child().traceparent
            elif self.originate:
                cmd.headers[FIELD] = SpanContext(new_trace_id(), new_span_id()).traceparent
        return call_next(cmd)
