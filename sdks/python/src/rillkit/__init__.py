"""rillkit: simple publish and consume for Redis and Valkey streams."""

from .ack import AckPolicy
from .broker import Broker, Publisher
from .codec import Codec, JsonCodec
from .config import Control
from .exceptions import (
    AckMessage,
    ConfigurationError,
    DecodeError,
    NackMessage,
    PublishError,
    RejectMessage,
    RillkitError,
)
from .keys import HashTagKeys, KeyBuilder, PlainKeys
from .logs import LogCapture, read_logs
from .message import Message
from .middleware import BaseMiddleware, Idempotent, PublishCommand
from .settings import Settings
from .stream import Stream
from .tracing import TraceContext, current_traceparent, use_traceparent

__version__ = "0.4.1"

__all__ = [
    "AckMessage", "AckPolicy", "BaseMiddleware", "Broker", "Codec", "ConfigurationError",
    "Control", "DecodeError", "HashTagKeys", "Idempotent", "JsonCodec", "KeyBuilder",
    "LogCapture", "Message", "NackMessage", "PlainKeys", "PublishCommand", "PublishError",
    "Publisher", "RejectMessage", "RillkitError", "Settings", "Stream", "TraceContext",
    "current_traceparent", "read_logs", "use_traceparent",
]
