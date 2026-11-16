"""Building Redis or Valkey clients, and the small differences between the two libraries.

rillkit talks to whichever client it is given through the commands both libraries share. This
module holds the parts that are not shared: the client classes, the options each constructor
accepts, the exception classes, and how to reach a single node of a cluster.
"""

import importlib
import inspect
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import parse_qsl, unquote, urlparse

import redis
from redis.crc import key_slot

from .exceptions import ConfigurationError

if TYPE_CHECKING:
    from .settings import Settings

BACKENDS = ("redis", "valkey")
MODES = ("standalone", "cluster")

# Options that pick the server or the credentials. They have their own Settings fields, so they are
# refused in client_kwargs instead of quietly fighting with those fields.
RESERVED_OPTIONS = {
    "host": "host", "port": "port", "db": "db", "url": "url", "startup_nodes": "cluster_nodes",
    "username": "username", "password": "password", "ssl": "ssl",
}

# A client instance. Typed loosely because it is one of four classes from two libraries.
Client = Any


@dataclass(frozen=True)
class Backend:
    """The classes and option lists of one client library."""

    name: str
    standalone: type
    cluster: type
    cluster_node: type
    standalone_options: frozenset[str]
    cluster_options: frozenset[str]


def _constructor_options(cls: type) -> set[str]:
    params = inspect.signature(cls.__init__).parameters.values()  # type: ignore[misc]
    return {p.name for p in params if p.kind not in (p.VAR_KEYWORD, p.VAR_POSITIONAL)} - {"self"}


def _load(name: str) -> Backend:
    try:
        module = importlib.import_module(name)
        cluster_module = importlib.import_module(f"{name}.cluster")
    except ImportError as e:
        raise ConfigurationError(
            f'backend "{name}" needs the {name} package: pip install "rillkit[{name}]"'
        ) from e
    standalone = module.Redis if name == "redis" else module.Valkey
    cluster = module.RedisCluster if name == "redis" else module.ValkeyCluster
    # Cluster clients forward only the keys in this list to each node and silently drop the rest,
    # so it is the real list of what works, on top of the cluster constructor's own arguments.
    list_name = "REDIS_ALLOWED_KEYS" if name == "redis" else "VALKEY_ALLOWED_KEYS"
    forwarded: Iterable[str] = getattr(cluster_module, list_name, ())
    disabled = set(getattr(cluster_module, "KWARGS_DISABLED_KEYS", ()))
    cluster_options = (_constructor_options(cluster) | (set(forwarded) - disabled)) - {"db"}
    return Backend(
        name=name,
        standalone=standalone,
        cluster=cluster,
        cluster_node=cluster_module.ClusterNode,
        standalone_options=frozenset(_constructor_options(standalone) - set(RESERVED_OPTIONS)),
        cluster_options=frozenset(cluster_options - set(RESERVED_OPTIONS)),
    )


_backends: dict[str, Backend] = {}


def backend(name: str) -> Backend:
    """The client library for ``name`` ("redis" or "valkey"), imported on first use."""
    if name not in BACKENDS:
        raise ConfigurationError(f"unknown backend {name!r}, expected one of {BACKENDS}")
    if name not in _backends:
        _backends[name] = _load(name)
    return _backends[name]


# exceptions


def _exception_classes(*names: str) -> tuple[type[Exception], ...]:
    """The named exception classes from redis-py, plus valkey-py's when it is installed."""
    found: list[type[Exception]] = []
    modules = [redis.exceptions]
    try:
        modules.append(importlib.import_module("valkey.exceptions"))
    except ImportError:
        pass
    for module in modules:
        for name in names:
            cls = getattr(module, name, None)
            if cls is not None and cls not in found:
                found.append(cls)
    return tuple(found)


# Any error raised by either client, including cluster errors, which do not derive from the
# library's base error class.
CLIENT_ERRORS = _exception_classes(
    "RedisError", "RedisClusterException", "ValkeyError", "ValkeyClusterException"
)
# The server answered with an error (BUSYGROUP, NOGROUP, WRONGTYPE, MOVED, ...).
RESPONSE_ERRORS = _exception_classes("ResponseError")
# The server could not be reached or did not answer in time.
CONNECTION_ERRORS = _exception_classes("ConnectionError", "TimeoutError")
# A WATCHed key changed before EXEC, so the transaction was not applied.
WATCH_ERRORS = _exception_classes("WatchError")
# The cluster topology changed under a command, so it may succeed once the slots are reloaded.
CLUSTER_REDIRECT_ERRORS = _exception_classes(
    "MovedError", "AskError", "TryAgainError", "ClusterDownError"
)


# building clients


def _url_credentials(url: str) -> bool:
    """Whether the url carries a username or password, in the userinfo or the query string."""
    parsed = urlparse(url)
    query = {k.lower() for k, _ in parse_qsl(parsed.query, True)}
    return bool(parsed.username or parsed.password or query & {"username", "password"})


def _url_db(url: str) -> int:
    path = urlparse(url).path.strip("/")
    if not path:
        return 0
    try:
        return int(unquote(path))
    except ValueError as e:
        raise ConfigurationError("the database in the url path must be a number") from e


def _check_options(options: Mapping[str, Any], allowed: Iterable[str], where: str) -> None:
    allowed = set(allowed)
    for key in options:
        if key in RESERVED_OPTIONS:
            raise ConfigurationError(
                f"client_kwargs[{key!r}] is not allowed: set Settings.{RESERVED_OPTIONS[key]}"
            )
        if key not in allowed:
            raise ConfigurationError(f"client_kwargs[{key!r}] is not an option of {where}")


def parse_node(node: Any) -> tuple[str, int]:
    """``"host:port"``, ``("host", port)`` or ``"host"`` (port 6379) as a (host, port) pair."""
    if isinstance(node, str):
        host, sep, port = node.strip().rpartition(":")
        if not sep:
            host, port = port, "6379"
        host = host.strip("[]")  # [::1]:7000
    elif isinstance(node, Sequence) and len(node) == 2:
        host, port = node
    else:
        raise ConfigurationError(f"cluster node {node!r} is not 'host:port' or (host, port)")
    try:
        return str(host), int(port)
    except ValueError as e:
        raise ConfigurationError(f"cluster node {node!r} has an invalid port") from e


def create_client(settings: "Settings") -> Client:
    """Build the client ``settings`` describe. Raises ``ConfigurationError`` for bad settings."""
    lib = backend(settings.backend)
    options: dict[str, Any] = {"socket_timeout": settings.socket_timeout}
    options.update(settings.client_kwargs)
    if settings.max_connections is not None:
        options["max_connections"] = settings.max_connections
    url = settings.url
    if url is None or not _url_credentials(url):
        # Credentials embedded in the url win; the separate fields fill in when it has none.
        if settings.username is not None:
            options["username"] = settings.username
        if settings.password is not None:
            options["password"] = settings.password
    if settings.mode == "cluster":
        return _cluster_client(lib, settings, options)
    return _standalone_client(lib, settings, options)


def _standalone_client(lib: Backend, settings: "Settings", options: dict[str, Any]) -> Client:
    _check_options(settings.client_kwargs, lib.standalone_options, lib.standalone.__name__)
    if settings.cluster_nodes:
        raise ConfigurationError("cluster_nodes is set but mode is 'standalone'")
    cls: Any = lib.standalone
    if settings.url:
        client = cls.from_url(settings.url, **options)
    else:
        client = cls(
            host=settings.host, port=settings.port, db=settings.db, ssl=settings.ssl, **options
        )
    pool = client.connection_pool
    try:
        # Build (without connecting) one connection now, so an option the connection class does
        # not accept fails here rather than on the first command.
        pool.connection_class(**pool.connection_kwargs)
    except TypeError as e:
        client.close()
        raise ConfigurationError(f"invalid client option: {e}") from e
    return client


def _cluster_client(lib: Backend, settings: "Settings", options: dict[str, Any]) -> Client:
    _check_options(settings.client_kwargs, lib.cluster_options, lib.cluster.__name__)
    if settings.db != 0 or (settings.url and _url_db(settings.url) != 0):
        raise ConfigurationError("cluster mode only has database 0")
    node_cls: Any = lib.cluster_node
    cls: Any = lib.cluster
    nodes = [node_cls(*parse_node(n)) for n in settings.cluster_nodes]
    if settings.url:
        # The url is one seed node plus credentials and TLS; cluster_nodes adds more seeds.
        return cls.from_url(settings.url, startup_nodes=nodes or None, **options)
    if not nodes:
        nodes = [node_cls(settings.host, settings.port)]
    return cls(startup_nodes=nodes, ssl=settings.ssl, **options)


# working with clients of either library and mode


def is_cluster(client: Client) -> bool:
    return hasattr(client, "nodes_manager") and hasattr(client, "get_node_from_key")


def node_client_for(client: Client, key: str) -> Client:
    """A client for the server that holds ``key``: ``client`` itself outside a cluster."""
    if not is_cluster(client):
        return client
    return _node_client(client.get_node_from_key(key))


def _node_client(node: Any) -> Client:
    conn = getattr(node, "redis_connection", None) or getattr(node, "valkey_connection", None)
    if conn is None:
        raise ConfigurationError(f"cluster node {node!r} has no client")
    return conn


def socket_timeout(client: Client) -> Optional[float]:
    """The read timeout a connection from this client will use, or None for no timeout."""
    try:
        if is_cluster(client):
            client = _node_client(client.get_primaries()[0])
        pool = client.connection_pool
        conn = pool.connection_class(**pool.connection_kwargs)  # built, never connected
    except Exception:
        return None
    return getattr(conn, "socket_timeout", None) or getattr(conn, "_socket_timeout", None)


def same_slot(*keys: str) -> bool:
    return len({key_slot(k.encode()) for k in keys}) == 1


Command = tuple[str, tuple[Any, ...]]


def run_together(client: Client, commands: Sequence[Command]) -> list[Any]:
    """Run ``commands`` (method name, args) atomically where the keys allow it, else in order.

    The first argument of every command is its key. Standalone clients use MULTI/EXEC. On a
    cluster, keys in one hash slot also use MULTI/EXEC, sent to the node that owns the slot.
    Keys in different slots cannot be in one transaction, so the commands run one after another
    and a failure leaves the earlier ones applied: callers order them so that is safe to retry.
    """
    if not is_cluster(client):
        pipe = client.pipeline(transaction=True)
        for name, args in commands:
            getattr(pipe, name)(*args)
        return list(pipe.execute())
    keys = [args[0] for _, args in commands]
    if not same_slot(*keys):
        return [getattr(client, name)(*args) for name, args in commands]
    return _node_transaction(client, keys[0], commands)


def _node_transaction(client: Client, key: str, commands: Sequence[Command]) -> list[Any]:
    # Not every cluster client supports transactional pipelines (valkey-py 6.1 refuses them), so
    # run MULTI/EXEC on the owning node directly. If the slot moved, the queued commands fail
    # with MOVED, EXEC applies nothing, and one retry after reloading the slot map follows it.
    for attempt in (1, 2):
        pipe = _node_client(client.get_node_from_key(key)).pipeline(transaction=True)
        for name, args in commands:
            getattr(pipe, name)(*args)
        try:
            return list(pipe.execute())
        except CLUSTER_REDIRECT_ERRORS + CONNECTION_ERRORS:
            if attempt == 2:
                raise
            client.nodes_manager.initialize()
    raise AssertionError("unreachable")
