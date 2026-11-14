import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields
from typing import Any, Optional, Union
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from .clients import BACKENDS, MODES, Client, create_client, parse_node
from .exceptions import ConfigurationError
from .keys import KEY_LAYOUTS, HashTagKeys, KeyBuilder, PlainKeys, key_builder

_SECRET_WORDS = ("password", "secret", "token", "credential")


def _seconds(value: Optional[str], default: Optional[float]) -> Optional[float]:
    if value is None or value == "":
        return default
    return None if value.strip().lower() in ("none", "off", "0") else float(value)


def _nodes(value: Optional[str]) -> tuple[str, ...]:
    return tuple(n.strip() for n in (value or "").split(",") if n.strip())


def _is_secret(name: str) -> bool:
    return any(w in name.lower() for w in _SECRET_WORDS)


def redact_url(url: str) -> str:
    """``url`` with the password, a lone username and secret query values replaced by ``***``."""
    parsed = urlparse(url)
    netloc = parsed.netloc
    if "@" in netloc:
        host = netloc.rpartition("@")[2]
        user = parsed.username or ""
        netloc = f"{user}:***@{host}" if parsed.password is not None else f"***@{host}"
    query = urlencode(
        [(k, "***" if _is_secret(k) else v) for k, v in parse_qsl(parsed.query, True)],
        safe="*",
    )
    return urlunparse(parsed._replace(netloc=netloc, query=query))


@dataclass
class Settings:
    """Connection settings: which server, which client library, and how to connect.

    ``Settings.from_env()`` reads REDIS_URL, or REDIS_HOST/PORT/DB/USERNAME/PASSWORD/SSL, plus
    REDIS_SOCKET_TIMEOUT (seconds, or ``none``), and the RILLKIT_URL, RILLKIT_BACKEND,
    RILLKIT_MODE, RILLKIT_CLUSTER_NODES and RILLKIT_KEY_LAYOUT variables.

    ``socket_timeout`` is set explicitly because consumers block in ``XREADGROUP`` for up to
    ``block_ms`` and the client must wait longer than that. redis-py 8 changed its own default to
    5 s, which is shorter than rillkit's default block time.

    With a ``url``, the host, port, db and ssl fields are ignored, and ``username``/``password``
    are only used when the url carries no credentials of its own.
    """

    url: Optional[str] = None
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    ssl: bool = False
    socket_timeout: Optional[float] = 15.0
    backend: str = "redis"
    mode: str = "standalone"
    username: Optional[str] = None
    cluster_nodes: Sequence[Union[str, tuple[str, int]]] = ()
    key_layout: Optional[str] = None
    client_kwargs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.backend = self.backend.strip().lower()
        self.mode = self.mode.strip().lower()
        if self.backend not in BACKENDS:
            raise ConfigurationError(
                f"unknown backend {self.backend!r}, expected one of {BACKENDS}"
            )
        if self.mode not in MODES:
            raise ConfigurationError(f"unknown mode {self.mode!r}, expected one of {MODES}")
        if self.key_layout is not None and self.key_layout not in KEY_LAYOUTS:
            raise ConfigurationError(
                f"unknown key_layout {self.key_layout!r}, expected one of {KEY_LAYOUTS}"
            )
        if isinstance(self.cluster_nodes, str):
            self.cluster_nodes = _nodes(self.cluster_nodes)
        self.cluster_nodes = tuple(self.cluster_nodes)
        for node in self.cluster_nodes:
            parse_node(node)
        self.client_kwargs = dict(self.client_kwargs)

    @classmethod
    def from_env(cls, prefix: str = "REDIS_", **overrides: Any) -> "Settings":
        """Settings from environment variables. Keyword arguments override what the env says.

        ``RILLKIT_URL`` wins over ``<prefix>URL``. Without either, the host/port/db/password
        variables are used. When a url is found, ``<prefix>USERNAME`` and ``<prefix>PASSWORD`` are
        not read, as before: put credentials in the url.
        """
        env = os.environ
        url = env.get("RILLKIT_URL") or env.get(f"{prefix}URL") or None
        values: dict[str, Any] = {
            "url": url,
            "host": env.get(f"{prefix}HOST", "localhost"),
            "port": int(env.get(f"{prefix}PORT", 6379)),
            "db": int(env.get(f"{prefix}DB", 0)),
            "password": None if url else env.get(f"{prefix}PASSWORD"),
            "username": None if url else env.get(f"{prefix}USERNAME"),
            "ssl": env.get(f"{prefix}SSL", "").lower() in ("1", "true", "yes"),
            "socket_timeout": _seconds(env.get(f"{prefix}SOCKET_TIMEOUT"), 15.0),
            "backend": env.get("RILLKIT_BACKEND") or "redis",
            "mode": env.get("RILLKIT_MODE") or "standalone",
            "cluster_nodes": _nodes(env.get("RILLKIT_CLUSTER_NODES")),
            "key_layout": env.get("RILLKIT_KEY_LAYOUT") or None,
        }
        values.update(overrides)
        return cls(**values)

    def client(self) -> Client:
        """Build a new client: ``redis.Redis``, ``redis.RedisCluster``, ``valkey.Valkey`` or
        ``valkey.ValkeyCluster``, depending on ``backend`` and ``mode``."""
        return create_client(self)

    def key_builder(self) -> KeyBuilder:
        """``key_layout`` if set, otherwise plain keys standalone and hash-tagged on a cluster."""
        if self.key_layout is not None:
            return key_builder(self.key_layout)
        return HashTagKeys() if self.mode == "cluster" else PlainKeys()

    def __repr__(self) -> str:
        parts = []
        for f in fields(self):
            value = getattr(self, f.name)
            if f.name == "password" and value is not None:
                value = "***"
            elif f.name == "url" and value:
                value = redact_url(value)
            elif f.name == "client_kwargs":
                value = {k: "***" if _is_secret(k) else v for k, v in value.items()}
            parts.append(f"{f.name}={value!r}")
        return f"Settings({', '.join(parts)})"
