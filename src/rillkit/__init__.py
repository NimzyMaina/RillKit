"""rillkit: simple publish and consume for Redis and Valkey streams."""

from .ack import AckPolicy
from .broker import Broker, Publisher
from .codec import Codec, JsonCodec
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
from .message import Message
from .middleware import BaseMiddleware, Idempotent, PublishCommand
from .settings import Settings
from .stream import Stream

__version__ = "0.2.0"

__all__ = [
    "AckMessage", "AckPolicy", "BaseMiddleware", "Broker", "Codec", "ConfigurationError",
    "DecodeError", "HashTagKeys", "Idempotent", "JsonCodec", "KeyBuilder", "Message",
    "NackMessage", "PlainKeys", "PublishCommand", "PublishError", "Publisher", "RejectMessage",
    "RillkitError", "Settings", "Stream",
]
