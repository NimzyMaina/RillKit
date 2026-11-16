import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, fields
from typing import Any, Optional, Union
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from .clients import BACKENDS, MODES, Client, create_client, parse_node
from .config import config_path, load_config
from .exceptions import ConfigurationError
from .keys import KEY_LAYOUTS, HashTagKeys, KeyBuilder, PlainKeys, key_builder

log = logging.getLogger("rillkit")

_SECRET_WORDS = ("password", "secret", "token", "credential")
# Fields a url replaces, so their variables are not used when one is set.
_URL_REPLACES = ("HOST", "PORT", "DB", "USERNAME", "PASSWORD", "SSL")


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

    ``Settings.from_env()`` reads RILLKIT_URL, or RILLKIT_HOST/PORT/DB/USERNAME/PASSWORD/SSL,
    plus RILLKIT_SOCKET_TIMEOUT (seconds, or ``none``), RILLKIT_MAX_CONNECTIONS, RILLKIT_BACKEND,
    RILLKIT_MODE, RILLKIT_CLUSTER_NODES, RILLKIT_KEY_LAYOUT, RILLKIT_KEY_PREFIX and RILLKIT_TENANT.
    Each connection variable falls back to its REDIS_ name (REDIS_URL, REDIS_HOST, ...), so 0.1
    configurations keep working. A config file (``config=`` or RILLKIT_CONFIG) fills in whatever
    the environment does not set.

    ``max_connections`` caps the client's connection pool. redis-py 8 defaults that pool to 100,
    which a producer with many threads, or a consumer with more than 100 lanes, can exhaust:
    the next command then fails with ``Too many connections`` instead of waiting. Raise it to
    the peak number of concurrent commands plus headroom (``RILLKIT_MAX_CONNECTIONS``). Leave it
    None to use the client library's own default (valkey-py's is effectively unlimited).

    ``tenant`` (and ``key_prefix``) imply hash-tagged keys, ``HashTagKeys(prefix, tenant)`` with
    the default prefix ``rk``; combining either with ``key_layout="plain"`` is an error.

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
    max_connections: Optional[int] = None
    backend: str = "redis"
    mode: str = "standalone"
    username: Optional[str] = None
    cluster_nodes: Sequence[Union[str, tuple[str, int]]] = ()
    key_layout: Optional[str] = None
    client_kwargs: Mapping[str, Any] = field(default_factory=dict)
    tenant: Optional[str] = None
    key_prefix: Optional[str] = None

    def __post_init__(self) -> None:
        self.backend = self.backend.strip().lower()
        self.mode = self.mode.strip().lower()
        if self.backend not in BACKENDS:
            raise ConfigurationError(
                f"unknown backend {self.backend!r}, expected one of {BACKENDS}"
            )
        if self.mode not in MODES:
            raise ConfigurationError(f"unknown mode {self.mode!r}, expected one of {MODES}")
        if self.max_connections is not None:
            if (isinstance(self.max_connections, bool)
                    or not isinstance(self.max_connections, int) or self.max_connections < 1):
                raise ConfigurationError("max_connections must be a positive integer or None")
            if "max_connections" in self.client_kwargs:
                raise ConfigurationError(
                    "set max_connections on Settings, not in client_kwargs"
                )
        if self.key_layout is not None and self.key_layout not in KEY_LAYOUTS:
            raise ConfigurationError(
                f"unknown key_layout {self.key_layout!r}, expected one of {KEY_LAYOUTS}"
            )
        if self.key_layout == "plain" and (self.tenant is not None or self.key_prefix is not None):
            what = "tenant" if self.tenant is not None else "key_prefix"
            raise ConfigurationError(
                f"{what} needs hash-tagged keys and cannot be combined with key_layout='plain'"
            )
        if self.tenant is not None or self.key_prefix is not None:
            # validates the tenant and prefix now rather than on first use
            HashTagKeys(prefix="rk" if self.key_prefix is None else self.key_prefix,
                        tenant=self.tenant)
        if isinstance(self.cluster_nodes, str):
            self.cluster_nodes = _nodes(self.cluster_nodes)
        self.cluster_nodes = tuple(self.cluster_nodes)
        for node in self.cluster_nodes:
            parse_node(node)
        self.client_kwargs = dict(self.client_kwargs)

    @classmethod
    def from_env(
        cls, prefix: str = "REDIS_", *, config: Optional[str] = None, **overrides: Any
    ) -> "Settings":
        """Settings from environment variables. Keyword arguments override what the env says.

        Every connection variable has a ``RILLKIT_`` name and an older ``<prefix>`` name
        (``REDIS_`` by default); a non-empty ``RILLKIT_`` value wins. A url (``RILLKIT_URL``, else
        ``<prefix>URL``) wins over the separate host, port, db, username, password and ssl
        variables, which are then not read; setting one of their ``RILLKIT_`` names alongside a
        url logs a warning.

        ``config`` (else ``RILLKIT_CONFIG``) names a config file whose ``[connection]`` and
        ``[keys]`` sections fill in what the environment does not set.
        """
        env = os.environ
        path = config_path(config)
        loaded = load_config(path) if path else {}
        file: dict[str, Any] = {**loaded.get("connection", {}), **loaded.get("keys", {})}

        def get(name: str, default: Optional[str] = None) -> Optional[str]:
            value = env.get(f"RILLKIT_{name}")
            if value:
                return value
            value = env.get(f"{prefix}{name}")
            if value is not None:
                return value
            field_name = name.lower()
            if field_name in file:
                return file[field_name]  # type: ignore[no-any-return]
            return default

        url = get("URL") or None
        if url:
            ignored = [f"RILLKIT_{n}" for n in _URL_REPLACES if env.get(f"RILLKIT_{n}")]
            if ignored:
                log.warning("%s ignored: a url is set and provides them", ", ".join(ignored))
        ssl = get("SSL", "")
        nodes = env.get("RILLKIT_CLUSTER_NODES") or file.get("cluster_nodes")
        timeout = get("SOCKET_TIMEOUT")
        max_connections = get("MAX_CONNECTIONS")
        values: dict[str, Any] = {
            "url": url,
            "host": get("HOST", "localhost"),
            "port": int(get("PORT", "6379") or 6379),
            "db": int(get("DB", "0") or 0),
            "password": None if url else get("PASSWORD"),
            "username": None if url else get("USERNAME"),
            "ssl": ssl if isinstance(ssl, bool) else (ssl or "").lower() in ("1", "true", "yes"),
            "socket_timeout": (timeout if isinstance(timeout, (int, float))
                               else _seconds(timeout, 15.0)),
            "max_connections": (
                int(max_connections) if isinstance(max_connections, str) and max_connections
                else max_connections if isinstance(max_connections, int) else None
            ),
            "backend": env.get("RILLKIT_BACKEND") or file.get("backend") or "redis",
            "mode": env.get("RILLKIT_MODE") or file.get("mode") or "standalone",
            "cluster_nodes": nodes if isinstance(nodes, (list, tuple)) else _nodes(nodes),
            "key_layout": env.get("RILLKIT_KEY_LAYOUT") or file.get("key_layout") or None,
            "key_prefix": env.get("RILLKIT_KEY_PREFIX") or file.get("key_prefix"),
            "tenant": env.get("RILLKIT_TENANT") or file.get("tenant") or None,
        }
        values.update(overrides)
        return cls(**values)

    def client(self) -> Client:
        """Build a new client: ``redis.Redis``, ``redis.RedisCluster``, ``valkey.Valkey`` or
        ``valkey.ValkeyCluster``, depending on ``backend`` and ``mode``."""
        return create_client(self)

    @property
    def has_key_settings(self) -> bool:
        """Whether any field picks the key layout (``key_layout``, ``tenant``, ``key_prefix``)."""
        return any(v is not None for v in (self.key_layout, self.tenant, self.key_prefix))

    def key_builder(self) -> KeyBuilder:
        """``HashTagKeys(key_prefix, tenant)`` if either is set, else ``key_layout`` if set,
        otherwise plain keys standalone and hash-tagged on a cluster."""
        if self.tenant is not None or self.key_prefix is not None:
            prefix = "rk" if self.key_prefix is None else self.key_prefix
            return HashTagKeys(prefix=prefix, tenant=self.tenant)
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
