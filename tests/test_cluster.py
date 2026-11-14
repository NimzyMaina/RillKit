"""Behaviour that only exists on a cluster. Run with RILLKIT_TEST_MODE=cluster."""

import pytest

from helpers import BACKEND, dk, entries, fast, pending, sk, wait_until
from rillkit import Broker, HashTagKeys, Message, PlainKeys, Stream, clients
from rillkit.clients import CONNECTION_ERRORS, run_together, same_slot

pytestmark = pytest.mark.cluster_only


def _node_client(node):
    return getattr(node, "redis_connection", None) or node.valkey_connection


def test_injected_cluster_client_gets_hash_tagged_keys(r):
    assert isinstance(Broker(client=r).keys, HashTagKeys)


def test_stream_and_dlq_share_a_slot_and_move_in_one_transaction(make_broker, r, name, monkeypatch):
    assert same_slot(sk(name), dk(name))
    calls = []
    real = clients._node_transaction
    monkeypatch.setattr(
        clients, "_node_transaction", lambda *a: calls.append(a[1]) or real(*a)
    )
    broker = make_broker()

    @broker.subscribe(name, group="g", max_retries=0, **fast())
    def handle(body):
        raise ValueError("bad")

    broker.start()
    broker.publish(name, {"a": 1})
    assert wait_until(lambda: r.xlen(dk(name)) == 1 and pending(r, name) == 0)
    assert calls == [dk(name)]
    broker.stop(5)
    replayer = make_broker()
    assert replayer.replay_dlq(name) == 1
    assert calls == [dk(name), sk(name)]
    assert r.xlen(dk(name)) == 0


def test_plain_keys_on_a_cluster_dead_letter_in_two_ordered_steps(
    server_settings, r, name, monkeypatch
):
    broker = Broker(settings=server_settings(key_layout="plain"))
    assert isinstance(broker.keys, PlainKeys)
    assert not same_slot(name, f"{name}-dlq")
    real_xack = type(broker.client).xack
    fail = {"on": True}

    def flaky_xack(self, *args, **kwargs):
        if fail["on"]:
            fail["on"] = False
            raise clients.CONNECTION_ERRORS[0]("lost the connection before the ack")
        return real_xack(self, *args, **kwargs)

    monkeypatch.setattr(type(broker.client), "xack", flaky_xack)
    seen = []

    @broker.subscribe(name, group="g", max_retries=0, block_ms=50, claim_idle_ms=100)
    def handle(msg: Message):
        seen.append(msg.deliveries)
        raise ValueError("bad")

    try:
        broker.start()
        broker.publish(name, {"a": 1})
        # the copy happened, the ack failed: still pending, retried, dead-lettered again
        assert wait_until(
            lambda: r.xlen(f"{name}-dlq") >= 2 and r.xpending(name, "g")["pending"] == 0, 20
        )
        assert r.xlen(f"{name}-dlq") == 2  # at least once, never lost
    finally:
        broker.stop(5)


def test_active_workers_scans_every_primary(server_settings, r, name):
    broker = Broker(settings=server_settings(key_layout="plain"))
    consumers = [f"c{i}" for i in range(12)]
    for c in consumers:
        broker.subscribe(name, group="g", consumer=c, **fast())(lambda body: None)
    try:
        broker.start()
        assert wait_until(lambda: broker.active_workers(name, "g") == sorted(consumers))
        owners = {r.get_node_from_key(broker.keys.heartbeat(Stream(name), "g", c)).name
                  for c in consumers}
        assert len(owners) > 1  # the keys really are spread over several primaries
    finally:
        broker.stop(5)
    assert broker.active_workers(name, "g") == []


def test_active_workers_on_an_empty_cluster_stream(broker, name):
    assert broker.active_workers(name, "g") == []


def test_node_transaction_follows_a_moved_slot(r, name, monkeypatch):
    key = sk(name)
    owner = r.get_node_from_key(key)
    other = next(n for n in r.get_primaries() if n.name != owner.name)
    answers = iter([other, owner])
    monkeypatch.setattr(r, "get_node_from_key", lambda k, *a, **kw: next(answers))
    run_together(r, [("xadd", (key, {"a": "1"})), ("xadd", (dk(name), {"a": "1"}))])
    assert r.xlen(key) == 1 and r.xlen(dk(name)) == 1


def _migrate_slot(r, key: str) -> None:
    """Move the slot of ``key`` to another primary with the same commands redis-cli uses."""
    slot = r.keyslot(key)
    src = r.get_node_from_key(key)
    dst = next(n for n in r.get_primaries() if n.name != src.name)
    s, d = _node_client(src), _node_client(dst)
    src_id = s.execute_command("CLUSTER MYID")
    dst_id = d.execute_command("CLUSTER MYID")
    src_id = src_id.decode() if isinstance(src_id, bytes) else src_id
    dst_id = dst_id.decode() if isinstance(dst_id, bytes) else dst_id
    d.execute_command("CLUSTER SETSLOT", slot, "IMPORTING", src_id)
    s.execute_command("CLUSTER SETSLOT", slot, "MIGRATING", dst_id)
    while True:
        keys = s.execute_command("CLUSTER GETKEYSINSLOT", slot, 100)
        if not keys:
            break
        s.execute_command("MIGRATE", dst.host, dst.port, "", 0, 5000, "KEYS", *keys)
    for node in r.get_primaries():
        _node_client(node).execute_command("CLUSTER SETSLOT", slot, "NODE", dst_id)


def test_consuming_and_dead_lettering_survive_a_slot_migration(make_broker, r, name):
    broker = make_broker()
    got: list = []

    @broker.subscribe(name, group="g", consumer="w", max_retries=0, **fast())
    def handle(body):
        if body.get("fail"):
            raise ValueError("bad")
        got.append(body["n"])

    broker.start()
    broker.publish(name, {"n": 1})
    assert wait_until(lambda: got == [1])
    before = r.get_node_from_key(sk(name)).name
    _migrate_slot(r, sk(name))
    r.nodes_manager.initialize()
    assert r.get_node_from_key(sk(name)).name != before
    broker.publish(name, {"n": 2})
    broker.publish(name, {"fail": True})
    assert wait_until(lambda: got == [1, 2] and r.xlen(dk(name)) == 1 and pending(r, name) == 0,
                      20)
    assert broker.active_workers(name, "g") == ["w"]
    assert [f["x-error"] for _, f in entries(r, dk(name))] == ["bad"]


def test_paused_node_raises_then_recovers(server_settings, r, name):
    """A node that stops answering makes calls fail loudly, and work resumes when it is back."""
    if BACKEND == "redis":  # redis-py 6+ configures cluster retries with a Retry object
        from redis.backoff import NoBackoff
        from redis.retry import Retry

        retries = {"retry": Retry(NoBackoff(), 1)}
    else:
        retries = {"cluster_error_retry_attempts": 1}
    broker = Broker(settings=server_settings(socket_timeout=0.5, client_kwargs=retries))
    got = []
    broker.subscribe(name, group="g", consumer="w", block_ms=100, claim_idle_ms=200)(
        lambda body: got.append(body)
    )
    try:
        broker.start()
        assert wait_until(lambda: broker.active_workers(name, "g") == ["w"])
        owner = _node_client(r.get_node_from_key(sk(name)))
        owner.execute_command("CLIENT PAUSE", 3000, "ALL")
        with pytest.raises(CONNECTION_ERRORS + clients.CLIENT_ERRORS):
            broker.active_workers(name, "g")
        assert wait_until(lambda: owner.ping(), 10)
        broker.publish(name, {"after": "pause"})
        assert wait_until(lambda: got == [{"after": "pause"}] and pending(r, name) == 0, 20)
    finally:
        broker.stop(5)
