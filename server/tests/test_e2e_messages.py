"""End to end: browsing messages, managing the DLQ, creating streams and throughput metrics,
through the dashboard's API and real workers on the Relay."""

import time
from typing import Any

import httpx
from rillkit import HashTagKeys, Stream

from helpers import wait_until
from test_e2e import applied, directive, overview


def start(worker: Any, api: httpx.Client, tenant: str, api_key: str, **control: Any) -> Any:
    """A worker on stream ``orders`` that fails every message whose body says so."""
    broker = worker(api_key, tenant, **control)

    @broker.subscribe(Stream("orders", group="g"), max_retries=0, block_ms=100,
                      claim_idle_ms=500)
    def handle(order: dict) -> None:
        if order.get("fail"):
            raise ValueError(f"card declined for order {order['id']}")

    broker.start()
    assert wait_until(lambda: overview(api, tenant)["totals"]["nodes_online"] >= 1)
    return broker


def messages(api: httpx.Client, tenant: str, **params: Any) -> dict:
    response = api.get(f"/api/tenants/{tenant}/streams/orders/g/messages", params=params)
    assert response.status_code == 200, response.text
    return response.json()


def test_browsing_is_private_unless_the_worker_allows_it(api, tenant, api_key, worker):
    broker = start(worker, api, tenant, api_key)
    broker.publish("orders", {"id": 1})
    assert wait_until(lambda: messages(api, tenant)["total"] == 1)
    page = messages(api, tenant)
    [entry] = page["messages"]
    assert page["payloads"] is False and entry["fields"] is None
    assert entry["size"] > 0 and entry["traceparent"].startswith("00-")
    assert wait_until(lambda: messages(api, tenant)["messages"][0]["status"] == "acked")
    assert wait_until(lambda: overview(api, tenant)["streams"])
    caps = overview(api, tenant)["streams"][0]["capabilities"]
    assert caps == {"inspect_messages": False, "capture_logs": False, "trace_records": False}


def test_newest_first_appending_and_lookup(api, tenant, api_key, worker):
    broker = start(worker, api, tenant, api_key, inspect_messages=True)
    for n in range(5):
        broker.publish("orders", {"id": n})
    assert wait_until(lambda: messages(api, tenant)["total"] == 5)
    first = messages(api, tenant, count=3)
    assert [m["fields"]["data"] for m in first["messages"]] == [
        '{"id": 4}', '{"id": 3}', '{"id": 2}']
    assert first["payloads"] is True and first["as_of"] and first["node_id"]
    older = messages(api, tenant, count=3, before=first["next_before"])
    assert [m["fields"]["data"] for m in older["messages"]] == ['{"id": 1}', '{"id": 0}']
    assert older["next_before"] is None

    newest = first["messages"][0]["id"]
    assert messages(api, tenant, after=newest)["messages"] == []
    broker.publish("orders", {"id": 5})
    broker.publish("orders", {"id": 6})
    fresh = messages(api, tenant, after=newest)
    assert [m["fields"]["data"] for m in fresh["messages"]] == ['{"id": 6}', '{"id": 5}']
    assert fresh["more_after"] is False
    one = api.get(f"/api/tenants/{tenant}/streams/orders/g/messages/{newest}").json()
    assert one["id"] == newest and one["fields"]["data"] == '{"id": 4}'
    missing = api.get(f"/api/tenants/{tenant}/streams/orders/g/messages/1-0")
    assert missing.json()["error"]["code"] == "NOT_FOUND"


def test_managing_the_dlq(api, tenant, api_key, worker):
    broker = start(worker, api, tenant, api_key, inspect_messages=True)
    for n in range(4):
        broker.publish("orders", {"id": n, "fail": True})
    assert wait_until(lambda: messages(api, tenant, source="dlq")["total"] == 4, 15)
    dlq = messages(api, tenant, source="dlq")
    newest = dlq["messages"][0]
    assert newest["dead_letter"]["error"] == "card declined for order 3"
    assert newest["dead_letter"]["deliveries"] == 1 and newest["dead_letter"]["original_id"]
    assert not any(k.startswith("x-") for k in newest["fields"])
    ids = [m["id"] for m in dlq["messages"]]
    target = {"stream": "orders", "group": "g"}

    # fix one payload so it succeeds, replay it with one left unchanged (which fails again)
    fixed = '{"id": 3, "fail": false}'
    d = applied(api, tenant, directive(api, tenant, cmd="EXECUTE_REPLAY", scope="stream",
                                       target=target, params={"ids": ids[:2],
                                                              "data": {ids[0]: fixed}}))
    assert (d["status"], d["ack"]["result"]) == ("APPLIED", {"replayed": 2, "missing": []})
    assert wait_until(lambda: messages(api, tenant, source="dlq")["total"] == 3, 15)
    errors = [m["dead_letter"]["error"] for m in messages(api, tenant, source="dlq")["messages"]]
    assert errors.count("card declined for order 2") == 1  # the unchanged one came back
    assert "card declined for order 3" not in errors  # the fixed one was handled

    remaining = [m["id"] for m in messages(api, tenant, source="dlq")["messages"]]
    d = applied(api, tenant, directive(api, tenant, cmd="DELETE_DLQ", scope="stream",
                                       target=target, params={"ids": remaining[:1]}))
    assert d["ack"]["result"] == {"deleted": 1}
    d = applied(api, tenant, directive(api, tenant, cmd="DELETE_DLQ", scope="stream",
                                       target=target, params={"all": True}))
    assert d["ack"]["result"] == {"deleted": 2}
    assert messages(api, tenant, source="dlq")["total"] == 0


def test_creating_a_stream_through_a_worker(api, tenant, api_key, worker, redis_url):
    broker = start(worker, api, tenant, api_key)
    created = api.post(f"/api/tenants/{tenant}/streams",
                       json={"stream": "invoices", "group": "mailer"}).json()
    assert created["directive"]["cmd"] == "CREATE_STREAM"
    d = applied(api, tenant, created["directive"])
    assert d["ack"]["result"] == {"created": True}
    key = HashTagKeys(tenant=tenant).stream(Stream("invoices"))
    assert [g["name"] for g in broker.client.xinfo_groups(key)] == [b"mailer"]
    names = [(s["stream"], s["nodes"]) for s in overview(api, tenant)["streams"]]
    assert ("invoices", 0) in names and ("orders", 1) in names


def test_throughput_metrics(api, tenant, api_key, worker):
    broker = start(worker, api, tenant, api_key, inspect_messages=True, capture_logs=True)
    time.sleep(1.2)  # let the autoscaler record a baseline of the worker's counters
    for n in range(9):
        broker.publish("orders", {"id": n, "fail": n == 0})
    assert wait_until(lambda: api.get(f"/api/tenants/{tenant}/metrics",
                                      params={"window_sec": 300}).json()["current"]["succeeded"]
                      == 8, 15)
    m = api.get(f"/api/tenants/{tenant}/metrics", params={"window_sec": 300}).json()
    current = m["current"]
    assert (current["failed"], current["dead_lettered"]) == (1, 1)
    assert current["success_rate"] == round(8 / 9, 4) and current["bytes"] > 0
    assert current["dlq_length"] == 1
    assert sum(p["succeeded"] for p in m["series"]) == 8
    assert wait_until(lambda: overview(api, tenant)["streams"])
    s = overview(api, tenant)["streams"][0]
    assert s["dlq_length"] == 1
    assert s["capabilities"] == {"inspect_messages": True, "capture_logs": True,
                                 "trace_records": False}
    ranged = api.get(f"/api/tenants/{tenant}/streams/orders/g/samples",
                     params={"window_sec": 300}).json()
    timed = [x for x in ranged if x["attempts"]]
    assert timed and all(x["wait_ms"] is not None and x["work_ms"] is not None for x in timed)
    assert all(x["wait_ms"] >= 0 and x["work_ms"] >= 0 for x in timed)
    assert all(p["wait_ms"] is None for p in m["series"] if not p["attempts"])


def test_a_trace_is_read_across_streams(api, tenant, api_key, worker):
    broker = start(worker, api, tenant, api_key, trace_records=True)
    broker.publish("orders", {"id": 1})
    assert wait_until(lambda: messages(api, tenant)["total"] == 1)
    [entry] = messages(api, tenant)["messages"]
    trace_id = entry["traceparent"].split("-")[1]
    assert wait_until(lambda: api.get(
        f"/api/tenants/{tenant}/traces/{trace_id}").json()["records"], 10)
    body = api.get(f"/api/tenants/{tenant}/traces/{trace_id}").json()
    assert body["trace_id"] == trace_id and body["node_id"]
    [record] = body["records"]
    assert (record["stream"], record["group"], record["message_id"]) == (
        "orders", "g", entry["id"])
    assert (record["delivery"], record["outcome"]) == (1, "ok")
    assert record["finished_at"] >= record["started_at"] and record["published_at"] > 0
    assert wait_until(lambda: overview(api, tenant)["streams"])
    caps = overview(api, tenant)["streams"][0]["capabilities"]
    assert caps["trace_records"] is True

    empty = api.get(f"/api/tenants/{tenant}/traces/{'0' * 32}")
    assert empty.status_code == 200 and empty.json()["records"] == []
    bad = api.get(f"/api/tenants/{tenant}/traces/not-a-trace")
    assert bad.status_code == 422 and bad.json()["error"]["code"] == "INVALID"
    assert api.get("/api/tenants/nope/traces/" + "0" * 32).status_code == 404
