"""The optional config file, and the control-plane switch (``Control``).

The file is read only when ``RILLKIT_CONFIG=<path>`` or ``config=<path>`` names it; nothing is
discovered implicitly. It has three sections, mapped onto the existing settings::

    [connection]            # Settings fields, same names as the RILLKIT_* variables
    url = "rediss://cache.internal:6379/0"
    backend = "valkey"

    [keys]
    layout = "hashtag"      # or prefix = "shop", tenant = "t-42"

    [control]
    mode = "valkey"         # off (default), valkey or relay
    api_key_env = "RILLKIT_API_KEY"

Secrets are referenced, never stored: ``password``, ``api_key`` and a ``url`` that carries a
password are refused, use ``<name>_env`` (the variable holding it) or ``<name>_file`` (a mounted
secret) instead. An unknown section or field raises ``ConfigurationError``.

Precedence, strongest first: code, environment, config file, defaults.
"""

import json
import os
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlparse

from .exceptions import ConfigurationError

CONTROL_MODES = ("off", "valkey", "relay")

# section -> {file field: target field}
_FIELDS: dict[str, dict[str, str]] = {
    "connection": {
        "url": "url", "host": "host", "port": "port", "db": "db", "username": "username",
        "password": "password", "ssl": "ssl", "socket_timeout": "socket_timeout",
        "backend": "backend", "mode": "mode", "cluster_nodes": "cluster_nodes",
    },
    "keys": {"layout": "key_layout", "prefix": "key_prefix", "tenant": "tenant"},
    "control": {
        "mode": "mode", "url": "url", "api_key": "api_key", "node_id": "node_id",
        "heartbeat_interval": "heartbeat_interval", "refresh_interval": "refresh_interval",
        "capture_logs": "capture_logs",
    },
}
_SECRETS = {"password", "api_key"}
_REFERENCE_ONLY_OK = {"url"}  # may be given literally (without a password) or by reference


def _parse(path: str) -> Any:
    ext = os.path.splitext(path)[1].lower()
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except OSError as e:
        raise ConfigurationError(f"cannot read config file {path}: {e}") from e
    try:
        if ext == ".json":
            return json.loads(raw)
        if ext == ".toml":
            try:
                import tomllib  # type: ignore[import-not-found,unused-ignore]
            except ImportError:  # Python < 3.11
                import tomli as tomllib  # type: ignore[import-not-found,no-redef,unused-ignore]
            return tomllib.loads(raw.decode())
        if ext in (".yaml", ".yml"):
            try:
                import yaml  # type: ignore[import-untyped,unused-ignore]
            except ImportError as e:
                raise ConfigurationError(
                    'YAML config files need PyYAML: pip install "rillkit[yaml]"'
                ) from e
            return yaml.safe_load(raw) or {}
    except ConfigurationError:
        raise
    except Exception as e:
        raise ConfigurationError(f"config file {path} is not valid: {e}") from e
    raise ConfigurationError(
        f"config file {path}: unknown extension {ext!r}, expected .toml, .json, .yaml or .yml"
    )


def _secret(section: str, name: str, how: str, ref: Any) -> str:
    if not isinstance(ref, str) or not ref:
        raise ConfigurationError(f"[{section}] {name}_{how} must be a non-empty string")
    if how == "env":
        value = os.environ.get(ref)
        if not value:
            raise ConfigurationError(f"[{section}] {name}_env: variable {ref} is not set")
        return value
    try:
        with open(ref, encoding="utf-8") as f:
            return f.read().strip()
    except OSError as e:
        raise ConfigurationError(f"[{section}] {name}_file: cannot read {ref}: {e}") from e


def load_config(path: str) -> dict[str, dict[str, Any]]:
    """Read and validate a config file. Returns ``{section: {target field: value}}`` with every
    secret reference resolved. Target fields are ``Settings`` fields for ``connection`` and
    ``keys``, and ``Control`` fields for ``control``."""
    data = _parse(path)
    if not isinstance(data, Mapping):
        raise ConfigurationError(f"config file {path} must hold a table of sections")
    out: dict[str, dict[str, Any]] = {}
    for section, body in data.items():
        if section not in _FIELDS:
            raise ConfigurationError(
                f"config file {path}: unknown section [{section}], expected one of "
                f"{tuple(_FIELDS)}"
            )
        if not isinstance(body, Mapping):
            raise ConfigurationError(f"config file {path}: [{section}] must be a table")
        fields = _FIELDS[section]
        values: dict[str, Any] = {}
        for key, value in body.items():
            name, how = key, None
            for suffix in ("_env", "_file"):
                if key.endswith(suffix) and key[: -len(suffix)] in fields:
                    name, how = key[: -len(suffix)], suffix[1:]
            if name not in fields:
                raise ConfigurationError(f"config file {path}: unknown field {key!r} in "
                                         f"[{section}]")
            if how is not None:
                if name not in _SECRETS | _REFERENCE_ONLY_OK:
                    raise ConfigurationError(
                        f"config file {path}: [{section}] {name} cannot be given by reference"
                    )
                value = _secret(section, name, how, value)
            elif name in _SECRETS:
                raise ConfigurationError(
                    f"config file {path}: [{section}] {name} must not be stored in the file, "
                    f"use {name}_env or {name}_file"
                )
            elif name == "url" and isinstance(value, str) and urlparse(value).password:
                raise ConfigurationError(
                    f"config file {path}: [{section}] url carries a password, use url_env or "
                    "url_file"
                )
            if fields[name] in values:
                raise ConfigurationError(f"config file {path}: [{section}] {name} is set twice")
            values[fields[name]] = value
        out[section] = values
    return out


def config_path(config: Optional[str]) -> Optional[str]:
    """The config file to read: ``config`` if given, else ``RILLKIT_CONFIG``, else none."""
    return config or os.environ.get("RILLKIT_CONFIG") or None


def default_node_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


@dataclass
class Control:
    """Whether and how this worker is managed by a RillKit control plane.

    ``mode``:

    - ``off`` (the default): the plain library. No control keys are read or written, no extra
      connection or thread is opened.
    - ``valkey``: before polling, the worker reads its desired state from the control keys and
      then listens on the control channels for changes. Something with access to the server
      (``rillkit-server``, or ``broker.control.apply(...)``) writes the state.
    - ``relay``: ``valkey`` plus an outbound connection to ``url`` with ``api_key``. The Relay
      client ships in a later release; this release refuses to start in ``relay`` mode.

    ``node_id`` names this process (default ``<hostname>-<pid>``). ``heartbeat_interval`` is how
    often the node heartbeat is written, ``refresh_interval`` how often the desired state is
    re-read even without a wakeup (the fallback if Pub/Sub drops a message).

    ``capture_logs`` adds ``LogCapture`` with its defaults, storing each handler run's logs in
    Redis or Valkey for a day (exception text excluded). It is off by default because handler
    logs often contain message data; add ``LogCapture(...)`` yourself to filter or redact.

    ``Control.from_env()`` reads RILLKIT_CONTROL, RILLKIT_CONTROL_URL, RILLKIT_API_KEY,
    RILLKIT_NODE_ID and RILLKIT_CAPTURE_LOGS, over the ``[control]`` section of the config file.
    """

    mode: str = "off"
    url: Optional[str] = None
    api_key: Optional[str] = None
    node_id: Optional[str] = None
    heartbeat_interval: float = 10.0
    refresh_interval: float = 30.0
    capture_logs: bool = False

    def __post_init__(self) -> None:
        self.mode = str(self.mode).strip().lower()
        if self.mode not in CONTROL_MODES:
            raise ConfigurationError(
                f"unknown control mode {self.mode!r}, expected one of {CONTROL_MODES}"
            )
        if self.node_id is not None and not self.node_id:
            raise ConfigurationError("node_id must be a non-empty string or None")
        if isinstance(self.capture_logs, str):
            self.capture_logs = self.capture_logs.strip().lower() in ("1", "true", "yes", "on")
        if self.heartbeat_interval <= 0 or self.refresh_interval <= 0:
            raise ConfigurationError("control intervals must be positive")
        if self.mode == "relay":
            if not self.url:
                raise ConfigurationError("control mode 'relay' needs a url (RILLKIT_CONTROL_URL)")
            if not self.api_key:
                raise ConfigurationError("control mode 'relay' needs an api_key (RILLKIT_API_KEY)")
            if urlparse(self.url).scheme not in ("wss", "https"):
                raise ConfigurationError("the control url must use wss:// or https://")

    @property
    def enabled(self) -> bool:
        return self.mode != "off"

    @classmethod
    def from_env(cls, config: Optional[str] = None, **overrides: Any) -> "Control":
        """Control from the environment over the config file. Keyword arguments win."""
        values: dict[str, Any] = {}
        path = config_path(config)
        if path:
            values.update(load_config(path).get("control", {}))
        env = os.environ
        for var, name in (("RILLKIT_CONTROL", "mode"), ("RILLKIT_CONTROL_URL", "url"),
                          ("RILLKIT_API_KEY", "api_key"), ("RILLKIT_NODE_ID", "node_id"),
                          ("RILLKIT_CAPTURE_LOGS", "capture_logs")):
            if env.get(var):
                values[name] = env[var]
        values.update(overrides)
        return cls(**values)

    def __repr__(self) -> str:
        key = None if self.api_key is None else "***"
        return (f"Control(mode={self.mode!r}, url={self.url!r}, api_key={key!r}, "
                f"node_id={self.node_id!r})")
