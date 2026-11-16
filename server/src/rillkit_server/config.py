"""Server settings, from RILLKIT_SERVER_* environment variables."""

import os
import secrets
import socket
from dataclasses import dataclass, field
from typing import Any


class SettingsError(ValueError):
    """The server cannot start with these settings."""


def _replica_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{secrets.token_hex(2)}"


@dataclass
class ServerSettings:
    """
    - ``database_url``: Postgres (RILLKIT_SERVER_DATABASE_URL, else DATABASE_URL).
    - ``admin_token``: the bearer token for the HTTP API and dashboard
      (RILLKIT_SERVER_ADMIN_TOKEN, at least 16 characters). Required.
    - ``public_url``: how workers reach this server, shown in the connection snippet
      (RILLKIT_SERVER_PUBLIC_URL).
    - ``static_dir``: the built dashboard to serve at ``/`` (RILLKIT_SERVER_STATIC_DIR).
    - ``bootstrap_tenant`` / ``bootstrap_api_key``: create this tenant and API key at startup
      if missing. For local demos only (RILLKIT_SERVER_BOOTSTRAP_TENANT, ..._API_KEY).
    """

    database_url: str
    admin_token: str
    public_url: str = "http://localhost:8080"
    static_dir: str | None = None
    replica_id: str = field(default_factory=_replica_id)
    telemetry_interval_sec: float = 5.0
    autoscale_interval_sec: float = 5.0
    node_ttl_sec: float = 30.0
    directive_timeout_sec: float = 15.0
    bootstrap_tenant: str | None = None
    bootstrap_api_key: str | None = None

    def __post_init__(self) -> None:
        if not self.database_url:
            raise SettingsError("RILLKIT_SERVER_DATABASE_URL (or DATABASE_URL) is required")
        if not self.admin_token or len(self.admin_token) < 16:
            raise SettingsError("RILLKIT_SERVER_ADMIN_TOKEN is required, at least 16 characters")
        if (self.bootstrap_tenant is None) != (self.bootstrap_api_key is None):
            raise SettingsError("set both RILLKIT_SERVER_BOOTSTRAP_TENANT and _API_KEY, or neither")

    @classmethod
    def from_env(cls, **overrides: Any) -> "ServerSettings":
        env = os.environ

        def number(name: str, default: float) -> float:
            value = env.get(f"RILLKIT_SERVER_{name}")
            return float(value) if value else default

        values: dict[str, Any] = {
            "database_url": env.get("RILLKIT_SERVER_DATABASE_URL") or env.get("DATABASE_URL", ""),
            "admin_token": env.get("RILLKIT_SERVER_ADMIN_TOKEN", ""),
            "public_url": env.get("RILLKIT_SERVER_PUBLIC_URL") or "http://localhost:8080",
            "static_dir": env.get("RILLKIT_SERVER_STATIC_DIR") or None,
            "telemetry_interval_sec": number("TELEMETRY_INTERVAL_SEC", 5.0),
            "autoscale_interval_sec": number("AUTOSCALE_INTERVAL_SEC", 5.0),
            "node_ttl_sec": number("NODE_TTL_SEC", 30.0),
            "bootstrap_tenant": env.get("RILLKIT_SERVER_BOOTSTRAP_TENANT") or None,
            "bootstrap_api_key": env.get("RILLKIT_SERVER_BOOTSTRAP_API_KEY") or None,
        }
        if env.get("RILLKIT_SERVER_REPLICA_ID"):
            values["replica_id"] = env["RILLKIT_SERVER_REPLICA_ID"]
        values.update(overrides)
        return cls(**values)

    def __repr__(self) -> str:
        return (f"ServerSettings(public_url={self.public_url!r}, replica_id={self.replica_id!r}, "
                f"admin_token='***', database_url='***')")
