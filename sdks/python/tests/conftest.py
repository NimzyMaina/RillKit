"""Fixtures. The same tests run against every server type, picked with environment variables:

    RILLKIT_TEST_BACKEND   redis (default) or valkey: which client library to use
    RILLKIT_TEST_MODE      standalone (default) or cluster
    REDIS_URL              standalone server, default redis://localhost:6390/15
    RILLKIT_TEST_CLUSTER_NODES   cluster seed nodes, default 127.0.0.1:7101

Only database 15 of a standalone server is ever flushed. A cluster has no databases, so cluster
runs rely on the unique stream name of each test instead.
"""

import os
import uuid
from collections.abc import Iterator
from typing import Any, Callable

import pytest

from helpers import BACKEND, MODE
from rillkit import Broker, Settings

TEST_DB = 15


def pytest_report_header(config: Any) -> str:
    return f"rillkit backend: {BACKEND}, mode: {MODE}"


def pytest_collection_modifyitems(config: Any, items: list) -> None:
    for item in items:
        if MODE == "cluster" and item.get_closest_marker("standalone_only"):
            item.add_marker(pytest.mark.skip(reason="standalone-only test"))
        if MODE != "cluster" and item.get_closest_marker("cluster_only"):
            item.add_marker(pytest.mark.skip(reason="needs RILLKIT_TEST_MODE=cluster"))


@pytest.fixture(scope="session")
def redis_url() -> str:
    return os.environ.get("REDIS_URL", f"redis://localhost:6390/{TEST_DB}")


@pytest.fixture(scope="session")
def server_settings(redis_url: str) -> Callable[..., Settings]:
    """Settings for the server under test. Keyword arguments override fields."""

    def make(**overrides: Any) -> Settings:
        base: dict[str, Any] = {"backend": BACKEND, "mode": MODE}
        if MODE == "cluster":
            base["cluster_nodes"] = os.environ.get(
                "RILLKIT_TEST_CLUSTER_NODES", "127.0.0.1:7101"
            )
        else:
            base["url"] = redis_url
        base.update(overrides)
        return Settings(**base)

    return make


@pytest.fixture
def r(server_settings: Callable[..., Settings]) -> Iterator[Any]:
    """A raw client for arranging and inspecting state."""
    client = server_settings(socket_timeout=None).client()
    if MODE != "cluster" and client.connection_pool.connection_kwargs.get("db") == TEST_DB:
        client.flushdb()
    yield client
    client.close()


@pytest.fixture
def name() -> str:
    """A unique stream name for the test."""
    return f"t-{uuid.uuid4().hex[:10]}"


@pytest.fixture
def make_broker(
    server_settings: Callable[..., Settings], r: Any
) -> Iterator[Callable[..., Broker]]:
    """Factory for brokers. Consumer threads are always stopped on teardown."""
    created = []

    def factory(**kwargs: Any) -> Broker:
        broker = Broker(settings=server_settings(), **kwargs)
        created.append(broker)
        return broker

    yield factory
    for broker in created:
        for sub in broker.subscribers:
            sub.stop()
        for sub in broker.subscribers:
            sub.join(10)
        if broker.control is not None:
            broker.control.stop(5)
        broker.client.close()


@pytest.fixture
def broker(make_broker: Callable[..., Broker]) -> Broker:
    return make_broker()
