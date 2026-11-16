"""End to end: real rillkit workers connect over the Relay, and the dashboard's API drives them."""

import json
import logging
import threading
import time
import uuid
from typing import Any

import httpx
import pytest
from rillkit import HashTagKeys, Message, Stream

from conftest import Running
from helpers import wait_until


def overview(api: httpx.Client, tenant: str) -> dict:
    return api.get(f"/api/tenants/{tenant}/overview").json()


def directive(api: httpx.Client, tenant: str, **body: Any) -> dict:
    response = api.post(f"/api/tenants/{tenant}/directives", json=body)
    assert response.status_code == 201, response.text
    return response.json()


def status_of(api: httpx.Client, tenant: str, directive_id: str) -> dict:
    for d in api.get(f"/api/tenants/{tenant}/directives", params={"limit": 200}).json()["items"]:
        if d["id"] == directive_id:
            return d
    raise AssertionError(f"directive {directive_id} not listed")


def applied(api: httpx.Client, tenant: str, d: dict, timeout: float = 10) -> dict:
    assert wait_until(lambda: status_of(api, tenant, d["id"])["status"] not in ("PENDING", "SENT"),
                      timeout), status_of(api, tenant, d["id"])
    return status_of(api, tenant, d["id"])


def subscribe(broker: Any, stream: str, got: list, **opts: Any) -> None:
    @broker.subscribe(Stream(stream, group="g"), consumer="w", block_ms=100,
                      claim_idle_ms=500, **opts)
    def handle(body: dict, msg: Message) -> None:
        logging.getLogger("app.e2e").warning("handled %s", body["n"])
        got.append((body["n"], msg.id))


def test_a_worker_appears_in_the_overview(api, tenant, api_key, worker):
    broker = worker(api_key, tenant, node_id="node-1")
    subscribe(broker, "orders", [])
    broker.start()
    assert wait_until(lambda: overview(api, tenant)["totals"]["nodes_online"] == 1)
    assert wait_until(lambda: overview(api, tenant)["streams"])
    view = overview(api, tenant)
    [node] = view["nodes"]
    assert (node["node_id"], node["connected"], node["state"], node["active"]) == (
        "node-1", True, "RUNNING", 1)
    assert node["sdk"]["language"] == "python"
    assert node["keys"] == {"layout": "hashtag", "prefix": "rk", "tenant": tenant}
    [stream] = view["streams"]
    assert (stream["stream"], stream["group"], stream["concurrency"], stream["nodes"]) == (
        "orders", "g", 1, 1)
    assert stream["stream_key"] == HashTagKeys(tenant=tenant).stream(Stream("orders"))
    assert api.get(f"/api/tenants/{tenant}").json()["nodes_online"] == 1

    broker.stop(5)
    assert wait_until(lambda: overview(api, tenant)["totals"]["nodes_online"] == 0)
    assert overview(api, tenant)["nodes"][0]["connected"] is False


def test_pause_resume_and_scale_from_the_api(api, tenant, api_key, worker):
    broker = worker(api_key, tenant)
    got: list = []
    subscribe(broker, "orders", got)
    broker.start()
    assert wait_until(lambda: overview(api, tenant)["totals"]["nodes_online"] == 1)

    d = applied(api, tenant, directive(api, tenant, cmd="SET_STATE", params={"state": "PAUSED"}))
    assert d["status"] == "APPLIED"
    assert d["ack"]["current_state"]["state"] == "PAUSED"
    assert d["source"] == "user" and d["sent_to"] == broker.control.node_id
    assert broker.control.desired().state == "PAUSED"
    assert wait_until(lambda: overview(api, tenant)["state"] == "PAUSED")

    broker.publish("orders", {"n": 1})
    d = applied(api, tenant, directive(api, tenant, cmd="SET_CONCURRENCY", scope="stream",
                                       target={"stream": "orders", "group": "g"},
                                       params={"concurrency": 3}))
    assert d["status"] == "APPLIED"
    applied(api, tenant, directive(api, tenant, cmd="SET_STATE", params={"state": "RUNNING"}))
    assert wait_until(lambda: [n for n, _ in got] == [1])
    assert wait_until(lambda: broker.subscribers[0].lanes == 3)
    assert wait_until(lambda: overview(api, tenant)["streams"][0]["concurrency"] == 3)


def test_node_directives_reach_only_their_node(api, tenant, api_key, worker):
    one, two = worker(api_key, tenant, node_id="n-1"), worker(api_key, tenant, node_id="n-2")
    for b in (one, two):
        subscribe(b, "orders", [])
        b.start()
    assert wait_until(lambda: overview(api, tenant)["totals"]["nodes_online"] == 2)
    d = applied(api, tenant, directive(api, tenant, cmd="SET_STATE", scope="node",
                                       target={"node_id": "n-2"}, params={"state": "DRAINING"}))
    assert (d["status"], d["sent_to"]) == ("APPLIED", "n-2")
    assert wait_until(lambda: two.subscribers[0].lanes == 0)
    assert one.subscribers[0].lanes == 1
    missing = api.post(f"/api/tenants/{tenant}/directives",
                       json={"cmd": "SET_STATE", "scope": "node", "target": {"node_id": "n-9"},
                             "params": {"state": "PAUSED"}})
    assert missing.json()["error"]["code"] == "NO_NODE"


def test_message_logs_through_the_relay(api, tenant, api_key, worker):
    broker = worker(api_key, tenant, capture_logs=True)
    got: list = []
    subscribe(broker, "orders", got)
    broker.start()
    assert wait_until(lambda: overview(api, tenant)["totals"]["nodes_online"] == 1)
    broker.publish("orders", {"n": 42})
    assert wait_until(lambda: len(got) == 1)
    msg_id = got[0][1]
    body = api.get(f"/api/tenants/{tenant}/logs/orders/{msg_id}").json()
    assert [r["message"] for r in body["records"]] == ["handled 42"]
    assert api.get(f"/api/tenants/{tenant}/logs/orders/0-1").json()["records"] == []


def test_cloud_tenants_require_their_own_keys(api, worker, caplog):
    tenant = f"t-{uuid.uuid4().hex[:8]}"
    api.post("/api/tenants", json={"id": tenant, "name": tenant, "enforce_keys": True})
    key = api.post(f"/api/tenants/{tenant}/api-keys", json={}).json()["key"]
    with caplog.at_level(logging.ERROR, logger="rillkit"):
        wrong = worker(key, tenant, keys=HashTagKeys(tenant="someone-else"))
        subscribe(wrong, "orders", [])
        wrong.start()
        assert wait_until(lambda: "TENANT_MISMATCH" in caplog.text)
    assert overview(api, tenant)["nodes"] == []
    right = worker(key, tenant)
    subscribe(right, "orders", [])
    right.start()
    assert wait_until(lambda: overview(api, tenant)["totals"]["nodes_online"] == 1)


def test_a_revoked_key_disconnects_its_workers(api, tenant, worker, caplog):
    created = api.post(f"/api/tenants/{tenant}/api-keys", json={}).json()
    broker = worker(created["key"], tenant)
    got: list = []
    subscribe(broker, "orders", got)
    broker.start()
    assert wait_until(lambda: overview(api, tenant)["totals"]["nodes_online"] == 1)
    with caplog.at_level(logging.ERROR, logger="rillkit"):
        api.delete(f"/api/tenants/{tenant}/api-keys/{created['id']}")
        assert wait_until(lambda: not broker.control.relay.connected.is_set())
        assert wait_until(lambda: "revoked" in caplog.text or "UNAUTHORIZED" in caplog.text)
    assert wait_until(lambda: overview(api, tenant)["totals"]["nodes_online"] == 0)
    broker.publish("orders", {"n": 1})  # the worker keeps processing: it fails open
    assert wait_until(lambda: len(got) == 1)


def test_unknown_keys_are_refused(server, worker, caplog):
    with caplog.at_level(logging.ERROR, logger="rillkit"):
        broker = worker("rk_not_a_real_key", "t-whatever")
        broker.start()
        assert wait_until(lambda: "UNAUTHORIZED" in caplog.text)


def test_live_events(server, api, tenant, api_key, worker):
    events: list = []
    done = threading.Event()

    ticket = api.post(f"/api/tenants/{tenant}/events/ticket").json()["ticket"]

    def listen() -> None:
        with httpx.stream("GET", f"{server.url}/api/tenants/{tenant}/events",
                          params={"ticket": ticket}, timeout=30) as response:
            kind = None
            for line in response.iter_lines():
                if line.startswith("event: "):
                    kind = line[7:]
                elif line.startswith("data: "):
                    events.append((kind, json.loads(line[6:])))
                if done.is_set():
                    return

    thread = threading.Thread(target=listen, daemon=True)
    thread.start()
    assert wait_until(lambda: events and events[0][0] == "overview")
    broker = worker(api_key, tenant)
    subscribe(broker, "orders", [])
    broker.start()
    assert wait_until(lambda: any(k == "overview" and d["totals"]["nodes_online"] == 1
                                  for k, d in events))
    d = directive(api, tenant, cmd="MAINTENANCE_ON")
    assert wait_until(lambda: any(k == "directive" and e["id"] == d["id"]
                                  and e["status"] == "APPLIED" for k, e in events))
    done.set()


def test_a_second_replica_routes_through_postgres(database_url, api, tenant, api_key, worker):
    other = Running(database_url, replica_id="replica-b")
    try:
        broker = worker(api_key, tenant, url=other.url)  # connected to replica B
        subscribe(broker, "orders", [])
        broker.start()
        assert wait_until(lambda: overview(api, tenant)["totals"]["nodes_online"] == 1)
        d = applied(api, tenant, directive(api, tenant, cmd="SET_STATE",  # posted to replica A
                                           params={"state": "PAUSED"}))
        assert d["status"] == "APPLIED"
        assert broker.control.desired().state == "PAUSED"
        got: list = []
        broker2 = worker(api_key, tenant, url=other.url, capture_logs=True)
        subscribe(broker2, "logs", got)
        broker2.start()
        applied(api, tenant, directive(api, tenant, cmd="SET_STATE", params={"state": "RUNNING"}))
        broker2.publish("logs", {"n": 5})
        assert wait_until(lambda: len(got) == 1)
        records = api.get(f"/api/tenants/{tenant}/logs/logs/{got[0][1]}").json()["records"]
        assert [r["message"] for r in records] == ["handled 5"]
        leaders = [httpx.get(f"{u}/api/health").json()["leader"] for u in (api.base_url, other.url)]
        assert sorted(leaders) == [False, True]  # exactly one autoscaler leader
    finally:
        other.stop()


@pytest.mark.timeout(60)
def test_the_autoscaler_follows_lag(api, tenant, api_key, worker):
    broker = worker(api_key, tenant)
    subscribe(broker, "orders", [])
    broker.start()
    assert wait_until(lambda: overview(api, tenant)["totals"]["nodes_online"] == 1)
    applied(api, tenant, directive(api, tenant, cmd="SET_STATE", params={"state": "PAUSED"}))
    time.sleep(0.3)  # a poll already blocked when the pause landed (block_ms=100) returns first
    for n in range(40):
        broker.publish("orders", {"n": n})
    assert wait_until(lambda: (overview(api, tenant)["streams"][0]["lag"] or 0) >= 40, 10)
    api.put(f"/api/tenants/{tenant}/policies/orders/g",
            json={"min_workers": 1, "max_workers": 4, "target_lag_per_worker": 10,
                  "upscale_cooldown_sec": 0, "downscale_cooldown_sec": 0})
    assert wait_until(lambda: broker.subscribers[0].lanes == 4, 15)
    scaled = api.get(f"/api/tenants/{tenant}/directives",
                     params={"source": "autoscaler"}).json()["items"]
    assert scaled and scaled[0]["params"] == {"concurrency": 4}
    assert scaled[0]["reason"].startswith("AUTOSCALE: lag")
    policy = api.get(f"/api/tenants/{tenant}/policies").json()[0]
    assert policy["last_scaled_at"] is not None
    samples = api.get(f"/api/tenants/{tenant}/streams/orders/g/samples").json()
    assert samples and samples[-1]["concurrency"] >= 1

    applied(api, tenant, directive(api, tenant, cmd="SET_STATE", params={"state": "RUNNING"}))
    assert wait_until(lambda: (overview(api, tenant)["streams"][0]["lag"] or 0) == 0, 15)
    assert wait_until(lambda: broker.subscribers[0].lanes == 1, 15)  # scaled back down
