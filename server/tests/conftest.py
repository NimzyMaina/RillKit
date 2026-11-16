"""Fixtures: a real Postgres, a real Redis or Valkey, and the server running in uvicorn.

    RILLKIT_SERVER_TEST_DATABASE   admin url of a Postgres; a fresh database is created per run
                                   (default postgresql://postgres:postgres@localhost:5439/postgres)
    REDIS_URL                      the Redis or Valkey workers use (default redis://localhost:6390/15)
"""

import os
import socket
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import psycopg
import pytest
import uvicorn
from rillkit import Broker, Control, HashTagKeys, Settings

from rillkit_server.app import create_app
from rillkit_server.config import ServerSettings

ADMIN_DB = os.environ.get("RILLKIT_SERVER_TEST_DATABASE",
                          "postgresql://postgres:postgres@localhost:5439/postgres")
TOKEN = "test-admin-token-0123456789"


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    name = f"rillkit_test_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(ADMIN_DB, autocommit=True) as conn:
        conn.execute(f"create database {name}")
    yield ADMIN_DB.rsplit("/", 1)[0] + f"/{name}"
    with psycopg.connect(ADMIN_DB, autocommit=True) as conn:
        conn.execute(f"drop database {name} with (force)")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class Running:
    """A server replica running in uvicorn in a background thread."""

    def __init__(self, database_url: str, **overrides: Any) -> None:
        self.port = _free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        settings = ServerSettings(
            database_url=database_url, admin_token=TOKEN, public_url=self.url,
            telemetry_interval_sec=0.3, autoscale_interval_sec=0.5, node_ttl_sec=5,
            directive_timeout_sec=5, **overrides,
        )
        self.app = create_app(settings)
        self.server = uvicorn.Server(uvicorn.Config(self.app, host="127.0.0.1", port=self.port,
                                                    log_level="warning", ws_ping_interval=None))
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        deadline = time.monotonic() + 30
        while not self.server.started:
            if time.monotonic() > deadline or not self.thread.is_alive():
                raise RuntimeError("the server did not start")
            time.sleep(0.05)
        self.http = httpx.Client(base_url=self.url, headers={"Authorization": f"Bearer {TOKEN}"},
                                 timeout=15)

    @property
    def state(self) -> Any:
        return self.app.state.server

    def stop(self) -> None:
        self.http.close()
        self.server.should_exit = True
        self.thread.join(15)


@pytest.fixture(scope="session")
def server(database_url: str) -> Iterator[Running]:
    running = Running(database_url, replica_id="replica-a")
    yield running
    running.stop()


@pytest.fixture
def api(server: Running) -> httpx.Client:
    return server.http


@pytest.fixture
def tenant(api: httpx.Client) -> str:
    """A fresh tenant per test, so tests never see each other's nodes or keys."""
    tenant_id = f"t-{uuid.uuid4().hex[:8]}"
    assert api.post("/api/tenants", json={"id": tenant_id, "name": tenant_id}).status_code == 201
    return tenant_id


@pytest.fixture
def api_key(api: httpx.Client, tenant: str) -> str:
    response = api.post(f"/api/tenants/{tenant}/api-keys", json={"label": "test"})
    assert response.status_code == 201
    return str(response.json()["key"])


@pytest.fixture
def redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6390/15")


@pytest.fixture
def worker(redis_url: str, server: Running) -> Iterator[Callable[..., Broker]]:
    """Factory for workers connected to the test server over the Relay. Stopped on teardown."""
    made: list[Broker] = []

    def make(api_key: str, tenant: str, *, url: str = "", keys: Any = None,
             **control: Any) -> Broker:
        control.setdefault("node_id", f"node-{uuid.uuid4().hex[:6]}")
        control.setdefault("heartbeat_interval", 0.5)
        broker = Broker(
            settings=Settings(url=redis_url),
            keys=keys or HashTagKeys(tenant=tenant),
            control=Control(mode="relay", url=url or server.url, api_key=api_key, **control),
        )
        made.append(broker)
        return broker

    yield make
    for broker in made:
        try:
            broker.stop(5)
        except Exception:
            pass
