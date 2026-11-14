"""A whole worker running as a tenant ACL user that may touch only its own keys and not list any.

This is the isolation model for several tenants on one server or cluster: hash-tagged keys with a
tenant, and an allowlist ACL user per tenant. It proves the allowlist is enough for everything
rillkit does, and that it hides other tenants' keys, including their names.
"""

import logging
import uuid
from collections.abc import Iterator
from typing import Any, Callable

import pytest

from helpers import MODE, fast, wait_until
from rillkit import Broker, HashTagKeys, Idempotent, Message, Settings, Stream
from rillkit.clients import CLIENT_ERRORS

PREFIX = "acl"


def tenant_rule(tenant: str, password: str) -> list[str]:
    """The per-tenant ACL rule documented in the Keys guide."""
    scope = f"{PREFIX}:{{{tenant}:*"
    return [
        "on", f">{password}", "resetkeys", "resetchannels", f"~{scope}", f"&{scope}",
        "-@all", "+@connection", "+@stream", "+@string", "+@hash", "+@sortedset", "+@transaction",
        "+del", "+exists",
        "+ssubscribe", "+sunsubscribe", "+spublish", "+subscribe", "+unsubscribe", "+publish",
        "+cluster|slots", "+cluster|shards", "+cluster|nodes", "+readonly", "+readwrite",
    ]


def _every_node(r: Any) -> list[Any]:
    """ACL users are per server: on a cluster every node, replicas included, needs the user."""
    if MODE != "cluster":
        return [r]
    return [
        getattr(n, "redis_connection", None) or n.valkey_connection
        for n in r.get_nodes()
    ]


@pytest.fixture
def tenant_user(r: Any) -> Iterator[Callable[[str], tuple[str, str]]]:
    created: list[str] = []

    def make(tenant: str) -> tuple[str, str]:
        user, password = f"rk-{tenant}-{uuid.uuid4().hex[:6]}", uuid.uuid4().hex
        for node in _every_node(r):
            node.execute_command("ACL", "SETUSER", user, *tenant_rule(tenant, password))
        created.append(user)
        return user, password

    yield make
    for user in created:
        for node in _every_node(r):
            node.execute_command("ACL", "DELUSER", user)


def test_a_full_worker_runs_as_a_tenant_acl_user(server_settings, tenant_user, r, caplog):
    tenant = f"t-{uuid.uuid4().hex[:6]}"
    user, password = tenant_user(tenant)
    keys = HashTagKeys(prefix=PREFIX, tenant=tenant)
    settings: Settings = server_settings(username=user, password=password)
    broker = Broker(settings=settings, keys=keys,
                    middlewares=[Idempotent(key=lambda m: m.body["rid"])])
    orders = Stream("orders", group="billing")
    handled: list = []
    attempts: list = []

    @broker.subscribe(orders, consumer="worker-7", max_retries=1, **fast())
    def handle(order: dict, msg: Message):
        attempts.append(order["rid"])
        if order.get("fail"):
            raise ValueError("bad order")
        handled.append(order["rid"])

    with caplog.at_level(logging.ERROR, logger="rillkit"):
        broker.start()
        broker.publish(orders, {"rid": "a"})
        broker.publish(orders, {"rid": "a"})  # duplicate, skipped by Idempotent
        broker.publish(orders, {"rid": "b", "fail": True})  # retried once, then dead-lettered
        assert wait_until(lambda: broker.client.xlen(keys.dlq(orders)) == 1, 15)
        assert wait_until(lambda: broker.active_workers(orders) == ["worker-7"])
        assert handled == ["a"]
        assert attempts.count("b") == 2
        assert broker.client.exists(keys.idempotency(orders, "a"))
        broker.stop(5)
    unexpected = [rec.getMessage() for rec in caplog.records
                  if "bad order" not in rec.getMessage()]
    assert unexpected == []  # no permission errors in the loop, heartbeats or DLQ moves

    replayer = Broker(settings=server_settings(username=user, password=password), keys=keys)
    try:
        assert replayer.replay_dlq(orders) == 1
        assert replayer.active_workers(orders) == []  # removed from the registry on stop
    finally:
        replayer.client.close()


def test_a_tenant_user_cannot_see_other_tenants(server_settings, tenant_user, r):
    other = HashTagKeys(prefix=PREFIX, tenant="other-tenant")
    secret = other.stream(Stream("payroll"))
    r.xadd(secret, {"data": "{}"})
    user, password = tenant_user(f"t-{uuid.uuid4().hex[:6]}")
    client = server_settings(username=user, password=password).client()
    try:
        for attempt in (lambda: client.xlen(secret), lambda: list(client.scan_iter("*")),
                        lambda: client.randomkey()):
            with pytest.raises(CLIENT_ERRORS, match="(?i)noperm|permission"):
                attempt()
    finally:
        client.close()
        r.delete(secret)
