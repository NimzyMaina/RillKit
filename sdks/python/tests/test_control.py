"""The worker side of the control plane (spec 2.1, 2.2, milestone 1): hydration, wakeups,
directive dedup, lanes, states and node heartbeats, against a real server."""

import json
import threading
import time
import uuid
from typing import Any, Callable

import pytest

from helpers import MODE, entries, fast, keep_server_awake, settle, wait_until
from rillkit import Broker, Control, HashTagKeys, KeyBuilder, Message, PlainKeys, Stream
from rillkit.consumer import entry_size
from rillkit.tracing import parse_traceparent


@pytest.fixture
def keys() -> KeyBuilder:
    """A namespace of its own per test: a cluster is never flushed between tests."""
    if MODE == "cluster":
        return HashTagKeys(prefix=f"c{uuid.uuid4().hex[:8]}")
    return PlainKeys()


def ctl(node_id: str = "", **kw: Any) -> Control:
    opts: dict[str, Any] = {"heartbeat_interval": 0.5, "refresh_interval": 30.0}
    opts.update(kw)
    return Control(mode="valkey", node_id=node_id or f"n-{uuid.uuid4().hex[:6]}", **opts)


@pytest.fixture
def node(make_broker, keys) -> Callable[..., Broker]:
    def make(node_id: str = "", **kw: Any) -> Broker:
        return make_broker(control=ctl(node_id, **kw), keys=keys)

    return make


@pytest.fixture
def admin(node) -> Broker:
    """A broker that is never started: it only applies directives, like the server would."""
    return node("admin")


def directive(cmd: str, scope: str = "global", **params: Any) -> dict:
    target = params.pop("target", {})
    return {"type": "DIRECTIVE", "directive_id": f"dir_{uuid.uuid4().hex[:8]}", "cmd": cmd,
            "scope": scope, "target": target, "params": params}


def apply_ok(admin: Broker, d: dict) -> dict:
    assert admin.control is not None
    ack = admin.control.apply(d)
    assert ack["status"] == "APPLIED", ack
    return ack


def collecting(broker: Broker, stream: Stream, got: list, **opts: Any) -> None:
    @broker.subscribe(stream, consumer=f"w-{uuid.uuid4().hex[:4]}", **fast(**opts))
    def handle(body: dict) -> None:
        got.append(body["n"])


def test_off_writes_no_control_key_and_starts_no_control_thread(make_broker, r, name):
    broker = make_broker()
    got: list = []
    collecting(broker, Stream(name, group="g"), got)
    broker.start()
    broker.publish(name, {"n": 1})
    assert wait_until(lambda: got == [1])
    assert broker.control is None
    assert not [t for t in threading.enumerate() if t.name == "rillkit-control"]
    broker.stop(5)
    if MODE != "cluster":
        names = {k.decode() for k in r.keys("*")}
        assert names == {name}, names  # the stream; heartbeat and registry are gone after stop
    keys = broker.keys
    for key in (keys.state_global(), keys.nodes(), keys.node("x")):
        assert not r.exists(key)


def test_a_paused_namespace_is_honoured_before_the_first_poll(node, admin, r, name):
    apply_ok(admin, directive("SET_STATE", state="PAUSED"))
    worker = node()
    got: list = []
    collecting(worker, Stream(name, group="g"), got)
    worker.publish(name, {"n": 1})
    with keep_server_awake(r):
        worker.start()
        settle(0.5)
        assert got == []
        apply_ok(admin, directive("SET_STATE", state="RUNNING"))  # wakes the worker
        assert wait_until(lambda: got == [1], 5)


def test_a_directive_is_applied_once(admin, keys):
    d = directive("MAINTENANCE_ON")
    assert admin.control is not None
    assert admin.control.apply(d)["status"] == "APPLIED"
    again = admin.control.apply(json.dumps(d))
    assert again["status"] == "DUPLICATE"
    assert again["directive_id"] == d["directive_id"]
    assert admin.client.hget(keys.state_global(), "state") == b"MAINTENANCE"
    assert admin.client.hget(keys.state_global(), "directive_id").decode() == d["directive_id"]
    assert 0 < admin.client.ttl(keys.dedup(d["directive_id"])) <= 60
    assert admin.control.desired().state == "MAINTENANCE"
    apply_ok(admin, directive("MAINTENANCE_OFF"))
    assert admin.control.desired().state == "RUNNING"


def test_concurrency_scales_lanes_up_and_down(node, admin, r, name):
    worker = node()
    stream = Stream(name, group="g")
    seen: set = set()

    @worker.subscribe(stream, consumer="w", **fast())
    def handle(body: dict, msg: Message) -> None:
        seen.add(msg.consumer)
        time.sleep(0.05)

    sub = worker.subscribers[0]
    with keep_server_awake(r):
        worker.start()
        assert sub.lanes == 1
        apply_ok(admin, directive("SET_CONCURRENCY", "stream", concurrency=3,
                                  target={"stream": name, "group": "g"}))
        assert wait_until(lambda: sub.lanes == 3, 5)
        assert wait_until(lambda: worker.active_workers(stream) == ["w", "w-1", "w-2"], 5)
        for n in range(30):
            worker.publish(stream, {"n": n})
        assert wait_until(lambda: len(seen) == 3, 10)

        apply_ok(admin, directive("SET_CONCURRENCY", "stream", concurrency=1,
                                  target={"stream": name, "group": "g"}))
        assert wait_until(lambda: sub.lanes == 1, 5)
        assert wait_until(lambda: worker.active_workers(stream) == ["w"], 5)
        assert wait_until(lambda: int(r.xpending(worker.keys.stream(stream), "g")["pending"])
                          == 0, 10)


def test_draining_finishes_in_flight_messages_then_releases_consumers(node, admin, r, name):
    worker = node()
    stream = Stream(name, group="g")
    started, release, done = threading.Event(), threading.Event(), []

    @worker.subscribe(stream, consumer="w", **fast())
    def handle(body: dict) -> None:
        started.set()
        release.wait(5)
        done.append(body["n"])

    sub = worker.subscribers[0]
    with keep_server_awake(r):
        worker.start()
        worker.publish(stream, {"n": 1})
        assert started.wait(5)
        apply_ok(admin, directive("SET_STATE", state="DRAINING"))
        settle(0.3)
        assert sub.in_flight == 1  # still running its message
        release.set()
        assert wait_until(lambda: sub.lanes == 0, 5)
        assert done == [1]
        assert int(r.xpending(worker.keys.stream(stream), "g")["pending"]) == 0
        assert worker.active_workers(stream) == []
        assert sub.is_alive()  # the node stays up, reporting DRAINING

        worker.publish(stream, {"n": 2})
        settle(0.3)
        assert done == [1]
        apply_ok(admin, directive("SET_STATE", state="RUNNING"))
        assert wait_until(lambda: done == [1, 2], 5)
        assert sub.lanes == 1


def test_paused_lanes_retire_at_once_when_draining(node, admin, r, name):
    worker = node()
    collecting(worker, Stream(name, group="g"), [], concurrency=3)
    sub = worker.subscribers[0]
    with keep_server_awake(r):
        worker.start()
        assert wait_until(lambda: sub.lanes == 3)
        apply_ok(admin, directive("SET_STATE", state="PAUSED"))
        assert wait_until(lambda: worker.control.desired().state == "PAUSED", 5)
        settle(0.3)  # every lane is now waiting for a change (for up to 20 s)
        apply_ok(admin, directive("SET_STATE", state="DRAINING"))
        assert wait_until(lambda: sub.lanes == 0, 3)
        assert worker.active_workers(name) == []


def test_a_stream_pause_leaves_other_streams_running(node, admin, r, name):
    worker = node()
    a, b = Stream(f"{name}-a", group="g"), Stream(f"{name}-b", group="g")
    got_a: list = []
    got_b: list = []
    collecting(worker, a, got_a)
    collecting(worker, b, got_b)
    with keep_server_awake(r):
        worker.start()
        apply_ok(admin, directive("SET_STATE", "stream", state="PAUSED",
                                  target={"stream": a.name, "group": "g"}))
        assert wait_until(lambda: worker.control.desired(worker.subscribers[0]).state
                          == "PAUSED", 5)
        settle(0.2)  # a poll already blocked when the pause arrived may still return a message
        worker.publish(a, {"n": 1})
        worker.publish(b, {"n": 2})
        assert wait_until(lambda: got_b == [2], 5)
        settle(0.3)
        assert got_a == []


def test_a_node_directive_reaches_only_that_node(node, admin, r, name):
    one, two = node("node-1"), node("node-2")
    stream = Stream(name, group="g")
    got_one: list = []
    got_two: list = []
    collecting(one, stream, got_one)
    collecting(two, stream, got_two)
    with keep_server_awake(r):
        one.start()
        two.start()
        apply_ok(admin, directive("SET_STATE", "node", state="PAUSED",
                                  target={"node_id": "node-1"}))
        assert wait_until(lambda: one.control.desired().state == "PAUSED", 5)
        assert two.control.desired().state == "RUNNING"
        settle(0.2)  # let node-1 finish any poll it was blocked in
        for n in range(5):
            one.publish(stream, {"n": n})
        assert wait_until(lambda: len(got_two) == 5, 5)
        assert got_one == []


def test_node_directive_for_one_subscription(node, admin, r, name):
    worker = node("node-s")
    a, b = Stream(f"{name}-a", group="g"), Stream(f"{name}-b", group="g")
    collecting(worker, a, [])
    collecting(worker, b, [])
    with keep_server_awake(r):
        worker.start()
        apply_ok(admin, directive("SET_CONCURRENCY", "node", concurrency=2, batch_size=5,
                                  poll_ms=40, target={"node_id": "node-s", "stream": a.name,
                                                      "group": "g"}))
        sub_a, sub_b = worker.subscribers
        assert wait_until(lambda: sub_a.lanes == 2, 5)
        assert sub_b.lanes == 1
        assert sub_a._desired()[1:] == (2, 40, 5)
        assert sub_b._desired()[1:] == (1, sub_b._block_ms, sub_b.count)


def test_poll_config_and_most_specific_scope_wins(admin, node, name):
    worker = node("node-p")
    collecting(worker, Stream(name, group="g"), [])
    sub = worker.subscribers[0]
    sub.setup()
    apply_ok(admin, directive("SET_POLL_CONFIG", block_ms=300, batch_size=7))
    apply_ok(admin, directive("SET_POLL_CONFIG", "stream", batch_size=3,
                              target={"stream": name, "group": "g"}))
    assert worker.control is not None
    worker.control.hydrate()
    assert sub._desired() == ("RUNNING", 1, 300, 3)


def test_block_ms_stays_under_the_socket_timeout(make_broker, keys, admin, name, server_settings):
    worker = Broker(settings=server_settings(socket_timeout=2), keys=keys, control=ctl())
    try:
        collecting(worker, Stream(name, group="g"), [])
        sub = worker.subscribers[0]
        sub.setup()
        apply_ok(admin, directive("SET_POLL_CONFIG", block_ms=60_000))
        worker.control.hydrate()
        assert sub._desired()[2] == 1000
    finally:
        worker.client.close()


def test_state_is_refreshed_even_without_a_wakeup(node, keys, r, name):
    worker = node(refresh_interval=0.2)
    got: list = []
    collecting(worker, Stream(name, group="g"), got)
    with keep_server_awake(r):
        worker.start()
        r.hset(keys.state_global(), "state", "PAUSED")  # written without a publish
        assert wait_until(lambda: worker.control.desired().state == "PAUSED", 5)
        settle(0.2)
        worker.publish(name, {"n": 1})
        settle(0.4)
        assert got == []
        r.hset(keys.state_global(), "state", "RUNNING")
        assert wait_until(lambda: got == [1], 5)


def test_execute_replay_moves_the_dlq_once(node, admin, r, name):
    stream = Stream(name, group="g")
    worker = node()

    @worker.subscribe(stream, max_retries=0, **fast())
    def handle(body: dict) -> None:
        raise ValueError("no")

    with keep_server_awake(r):
        worker.start()
        worker.publish(stream, {"n": 1})
        assert wait_until(lambda: r.xlen(worker.keys.dlq(stream)) == 1, 5)
        worker.stop(5)
    d = directive("EXECUTE_REPLAY", "stream", count=10, target={"stream": name, "group": "g"})
    ack = apply_ok(admin, d)
    assert ack["result"] == {"replayed": 1}
    assert admin.control.apply(d)["status"] == "DUPLICATE"
    assert r.xlen(worker.keys.dlq(stream)) == 0
    assert [f["data"] for _, f in entries(r, worker.keys.stream(stream))] == ['{"n": 1}'] * 2

    ack = apply_ok(admin, directive("EXECUTE_REPLAY", "stream", set_id="0",
                                    target={"stream": name, "group": "g"}))
    assert ack["result"] == {"set_id": "0"}


@pytest.mark.parametrize("bad, error", [
    ({"cmd": "SET_STATE", "params": {"state": "ASLEEP"}}, "state must be"),
    ({"cmd": "REBOOT"}, "unknown cmd"),
    ({"cmd": "SET_STATE", "scope": "galaxy", "params": {"state": "PAUSED"}}, "unknown scope"),
    ({"cmd": "SET_CONCURRENCY", "params": {"concurrency": 0}}, "between 1 and"),
    ({"cmd": "SET_CONCURRENCY", "params": {"concurrency": "many"}}, "integer"),
    ({"cmd": "SET_POLL_CONFIG", "params": {}}, "needs block_ms"),
    ({"cmd": "SET_STATE", "scope": "node", "params": {"state": "PAUSED"}}, "node_id"),
    ({"cmd": "SET_STATE", "scope": "stream", "params": {"state": "PAUSED"}}, "target.stream"),
    ({"cmd": "SET_STATE", "target": {"stream": "s"}, "params": {"state": "PAUSED"}},
     "both stream and group"),
    ({"cmd": "EXECUTE_REPLAY"}, "target.stream"),
    ({"cmd": "SET_STATE", "directive_id": None}, "directive_id"),
    ({"cmd": "SET_STATE", "type": "TELEMETRY_REPORT"}, "type must be"),
])
def test_bad_directives_are_rejected(admin, bad, error):
    d = {"type": "DIRECTIVE", "directive_id": f"dir_{uuid.uuid4().hex[:8]}", **bad}
    ack = admin.control.apply(d)
    assert ack["status"] == "REJECTED"
    assert error in ack["error"]
    assert admin.control.apply("not json")["status"] == "REJECTED"


def test_node_heartbeat_registry_and_telemetry(node, keys, r, name):
    worker = node("node-hb")
    stream = Stream(name, group="g", dlq="dead")
    collecting(worker, stream, [])
    with keep_server_awake(r):
        worker.start()
        assert worker.control.nodes() == ["node-hb"]
        beat = {k.decode(): v.decode() for k, v in r.hgetall(keys.node("node-hb")).items()}
        assert beat["state"] == "RUNNING"
        assert 0 < r.ttl(keys.node("node-hb")) <= 3
        report = json.loads(beat["telemetry"])
        assert report["type"] == "TELEMETRY_REPORT"
        assert report["node_id"] == "node-hb"
        assert report["keys"] == keys.describe()
        assert report["workers"]["active"] == 1
        [s] = report["streams"]
        assert (s["stream"], s["group"], s["stream_key"], s["dlq_key"]) == (
            name, "g", keys.stream(stream), keys.dlq(stream))
        worker.publish(stream, {"n": 1})
        assert wait_until(lambda: worker.control.telemetry()["streams"][0]["pending"] == 0
                          and r.xlen(keys.stream(stream)) == 1, 5)
        worker.stop(5)
    assert not r.exists(keys.node("node-hb"))
    assert r.zscore(keys.nodes(), "node-hb") is None


def test_control_brokers_trace_and_capture_logs_only_when_asked(node):
    assert [type(m).__name__ for m in node().middlewares] == ["TraceContext"]
    worker = node(capture_logs=True)
    assert [type(m).__name__ for m in worker.middlewares] == ["TraceContext", "LogCapture"]


@pytest.mark.cluster_only
def test_plain_keys_on_a_cluster_use_the_ordered_fallback(make_broker, name):
    admin = make_broker(control=ctl("admin"), keys=PlainKeys())
    d = directive("SET_STATE", "stream", state="PAUSED", target={"stream": name, "group": "g"})
    assert admin.control.apply(d)["status"] == "APPLIED"
    assert admin.control.apply(d)["status"] == "DUPLICATE"
    key = PlainKeys().state_stream(Stream(name), "g")
    assert admin.client.hget(key, "state") == b"PAUSED"
    admin.client.delete(key)


# DLQ management, stream creation, browsing and counters (the dashboard's stream page)


def _dead_letters(worker, r, stream, n, stop=True):
    """Run ``n`` messages that always fail through ``worker`` so they end in the DLQ.

    ``stop=False`` leaves it running for a caller that still needs its client: on a cluster a
    closed client cannot be reused."""
    with keep_server_awake(r):
        worker.start()
        for i in range(n):
            worker.publish(stream, {"n": i})
        assert wait_until(lambda: r.xlen(worker.keys.dlq(stream)) == n, 10)
        if stop:
            worker.stop(5)
    return [e[0] for e in entries(r, worker.keys.dlq(stream))]


def failing(node, name, **kw):
    worker = node(**kw)
    stream = Stream(name, group="g")

    @worker.subscribe(stream, max_retries=0, **fast())
    def handle(body: dict) -> None:
        raise ValueError(f"bad order {body['n']}")

    return worker, stream


def test_counters_and_telemetry(node, r, name):
    worker = node()
    stream = Stream(name, group="g")

    @worker.subscribe(stream, max_retries=0, **fast())
    def handle(body: dict) -> None:
        if body["n"] == 2:
            raise ValueError("no")

    with keep_server_awake(r):
        worker.start()
        for n in range(3):
            worker.publish(stream, {"n": n})
        sub = worker.subscribers[0]
        assert wait_until(lambda: sub.succeeded_total == 2 and sub.dead_lettered_total == 1, 10)
        assert sub.failed_total == 1
        # every field of each handled entry (managed brokers add a traceparent header)
        handled = entries(r, worker.keys.stream(stream))
        assert sub.bytes_total == sum(entry_size(f) for _, f in handled)
        [s] = worker.control.telemetry()["streams"]
        assert (s["succeeded_total"], s["failed_total"], s["dead_lettered_total"],
                s["dlq_length"]) == (2, 1, 1, 1)


def test_attempt_timings_are_counted_and_reported(node, r, name):
    worker = node()
    stream = Stream(name, group="g")
    collecting(worker, stream, [])
    with keep_server_awake(r):
        worker.start()
        worker.publish(stream, {"n": 1})
        sub = worker.subscribers[0]
        assert wait_until(lambda: sub.attempts_total == 1, 5)
        assert sub.wait_ms_total >= 0 and sub.work_ms_total >= 0
        [s] = worker.control.telemetry()["streams"]
        assert s["attempts_total"] == 1
        assert s["wait_ms_total"] == round(sub.wait_ms_total, 3)
        assert s["work_ms_total"] == round(sub.work_ms_total, 3)
        assert worker.control.capabilities()["trace_records"] is False
        worker.stop(5)


def test_trace_records_are_opt_in(node, r, name):
    worker = node()
    traces: list = []

    @worker.subscribe(Stream(name, group="g"), **fast())
    def handle(body: dict, msg: Message) -> None:
        traces.append(msg.raw.get("traceparent"))

    with keep_server_awake(r):
        worker.start()
        worker.publish(name, {"n": 1})
        assert wait_until(lambda: len(traces) == 1, 5)
        trace_id = parse_traceparent(traces[0]).trace_id
        assert not r.exists(worker.keys.trace(trace_id))
        worker.stop(5)


def test_trace_records_follow_a_trace_across_streams(node, r, name):
    worker = node(trace_records=True)
    first, second = Stream(name, group="g"), Stream(f"{name}-2", group="g")
    out = worker.publisher(second)
    ids: dict[str, str] = {}
    traces: dict[str, str] = {}
    entry_traces: dict[str, str] = {}

    @worker.subscribe(first, **fast())
    @out
    def step(body: dict, msg: Message) -> dict:
        ids["first"], traces["first"] = msg.id, msg.traceparent
        entry_traces["first"] = msg.raw.get("traceparent")
        return body

    @worker.subscribe(second, **fast())
    def end(body: dict, msg: Message) -> None:
        ids["second"], traces["second"] = msg.id, msg.traceparent

    with keep_server_awake(r):
        worker.start()
        worker.publish(first, {"n": 1})
        assert wait_until(lambda: len(ids) == 2, 10)
        trace_id = parse_traceparent(traces["first"]).trace_id
        assert wait_until(lambda: len(worker.control.trace(trace_id)["records"]) == 2, 5)
        records = worker.control.trace(trace_id)["records"]
        assert [(x["stream"], x["message_id"]) for x in records] == [
            (name, ids["first"]), (f"{name}-2", ids["second"])]
        assert [x["outcome"] for x in records] == ["ok", "ok"]
        assert all(x["trace_id"] == trace_id for x in records)
        assert records[0]["parent_span_id"] == parse_traceparent(
            entry_traces["first"]).span_id
        worker.stop(5)


def test_trace_records_capture_each_attempt_and_its_outcome(node, r, name):
    worker = node(trace_records=True)
    stream = Stream(name, group="g")
    traces: dict[int, str] = {}

    @worker.subscribe(stream, max_retries=1, **fast())
    def handle(body: dict, msg: Message) -> None:
        traces[body["n"]] = parse_traceparent(msg.raw.get("traceparent")).trace_id
        if body.get("fail"):
            raise ValueError("no")

    with keep_server_awake(r):
        worker.start()
        worker.publish(stream, {"n": 1})
        worker.publish(stream, {"n": 2, "fail": True})

        def recorded() -> bool:
            # The dead-letter trace record is written after the DLQ entry: wait for both.
            trace_id = traces.get(2)
            return (
                trace_id is not None
                and r.xlen(worker.keys.dlq(stream)) == 1
                and len(worker.control.trace(trace_id)["records"]) == 2
            )

        assert wait_until(recorded, 10)
        records = worker.control.trace(traces[2])["records"]
        assert [(x["delivery"], x["outcome"]) for x in records] == [
            (1, "error"), (2, "dead_letter")]
        assert 0 < r.ttl(worker.keys.trace(traces[2])) <= 3600
        worker.stop(5)


def test_dlq_directives(node, admin, r, name):
    worker, stream = failing(node, name)
    ids = _dead_letters(worker, r, stream, 4)
    target = {"stream": name, "group": "g"}
    edited = '{"n": 100}'
    ack = apply_ok(admin, directive("EXECUTE_REPLAY", "stream", target=target,
                                    ids=[ids[0], ids[1], "9-9"], data={ids[1]: edited}))
    assert ack["result"] == {"replayed": 2, "missing": ["9-9"]}
    replayed = [f for _, f in entries(r, worker.keys.stream(stream))][-2:]
    assert [f["data"] for f in replayed] == ['{"n": 0}', edited]
    assert not any(k.startswith("x-") for f in replayed for k in f)
    ack = apply_ok(admin, directive("DELETE_DLQ", "stream", target=target, ids=[ids[2]]))
    assert ack["result"] == {"deleted": 1}
    assert [e[0] for e in entries(r, worker.keys.dlq(stream))] == [ids[3]]
    ack = apply_ok(admin, directive("DELETE_DLQ", "stream", target=target, all=True))
    assert ack["result"] == {"deleted": 1}
    assert r.xlen(worker.keys.dlq(stream)) == 0


def test_create_stream_directive(admin, keys, r, name):
    target = {"stream": name, "group": "fresh"}
    assert apply_ok(admin, directive("CREATE_STREAM", "stream", target=target))["result"] == {
        "created": True}
    groups = r.xinfo_groups(keys.stream(Stream(name)))
    assert [g["name"] for g in groups] == [b"fresh"]
    again = apply_ok(admin, directive("CREATE_STREAM", "stream", target=target, start="0"))
    assert again["result"] == {"created": False}


def test_browsing_hides_payloads_unless_inspection_is_allowed(node, r, name):
    worker, stream = failing(node, name)
    _dead_letters(worker, r, stream, 2, stop=False)
    hidden = worker.control.browse(name, "g", "dlq")
    assert hidden["payloads"] is False and hidden["total"] == 2
    entry = hidden["messages"][0]
    assert entry["fields"] is None and entry["dead_letter"]["error"] is None
    assert entry["dead_letter"]["deliveries"] == 1 and entry["dead_letter"]["group"] == "g"
    assert entry["size"] > 0
    worker.stop(5)  # a closed cluster client cannot be reused

    shown = node(inspect_messages=True)
    page = shown.control.browse(name, "g", "dlq")
    assert page["payloads"] is True
    newest = page["messages"][0]
    assert newest["fields"]["data"] == '{"n": 1}'  # newest first
    assert not any(k.startswith("x-") for k in newest["fields"])  # DLQ context is metadata
    assert newest["traceparent"] == newest["fields"]["traceparent"]
    assert newest["dead_letter"]["error"] == "bad order 1"
    assert shown.control.capabilities() == {"inspect_messages": True, "capture_logs": False,
                                            "trace_records": False}


def test_browsing_pages_and_statuses(node, r, name):
    worker = node(inspect_messages=True)
    stream = Stream(name, group="g")
    release = threading.Event()

    @worker.subscribe(stream, **fast())
    def handle(body: dict) -> None:
        if body["n"] == 1:
            release.wait(5)

    with keep_server_awake(r):
        worker.publish(stream, {"n": 0})
        worker.publish(stream, {"n": 1})
        worker.start()
        assert wait_until(lambda: worker.subscribers[0].in_flight == 1, 5)
        worker.subscribers[0].stop()  # nothing more is read; message 1 stays in flight
        worker.publish(stream, {"n": 2})
        page = worker.control.browse(name, "g", count=2)
        assert [m["fields"]["data"] for m in page["messages"]] == ['{"n": 2}', '{"n": 1}']
        waiting, busy = page["messages"]
        assert waiting["status"] == "waiting"
        assert (busy["status"], busy["deliveries"]) == ("pending", 1)
        assert busy["consumer"] == worker.subscribers[0].consumer
        older = worker.control.browse(name, "g", count=2, before=page["next_before"])
        assert [m["status"] for m in older["messages"]] == ["acked"]
        assert older["next_before"] is None and older["total"] == 3
        oldest_id = older["messages"][0]["id"]
        newer = worker.control.browse(name, "g", count=1, after=oldest_id)
        assert [m["fields"]["data"] for m in newer["messages"]] == ['{"n": 1}']  # oldest first
        assert newer["more_after"] is True and newer["next_before"] is None
        rest = worker.control.browse(name, "g", count=5, after=oldest_id)
        assert [m["fields"]["data"] for m in rest["messages"]] == ['{"n": 2}', '{"n": 1}']
        assert rest["more_after"] is False and rest["as_of"].endswith("+00:00")
        exact = worker.control.browse(name, "g", ids=[busy["id"]])
        assert [m["id"] for m in exact["messages"]] == [busy["id"]]
        release.set()


def test_trace_order_follows_parent_spans():
    from rillkit.controlplane import _trace_order

    child = {"span_id": "b", "parent_span_id": "a", "started_at": 10, "published_at": 10}
    parent = {"span_id": "a", "parent_span_id": "caller", "started_at": 10,
              "published_at": 10}
    sibling = {"span_id": "c", "parent_span_id": "caller", "started_at": 5,
               "published_at": 5}
    # A child can be appended before its parent and share its millisecond.
    assert [r["span_id"] for r in _trace_order([child, parent, sibling])] == [
        "c", "a", "b"]
    # Unrelated roots in the same millisecond fall back to the microsecond start.
    slow = {"span_id": "d", "parent_span_id": "caller", "started_at": 10,
            "started_us": 10_000_900, "published_at": 10}
    quick = {"span_id": "e", "parent_span_id": "caller", "started_at": 10,
             "started_us": 10_000_100, "published_at": 10}
    assert [r["span_id"] for r in _trace_order([slow, quick])] == ["e", "d"]


def test_idle_timeout_excludes_paused_and_resets_after_draining(node, admin, r, name):
    worker = node()
    stream = Stream(name, group="g")
    got: list[int] = []

    @worker.subscribe(stream, consumer="w", block_ms=5000, idle_timeout=0.35)
    def handle(body: dict) -> None:
        got.append(body["n"])

    sub = worker.subscribers[0]
    with keep_server_awake(r):
        t = threading.Thread(target=worker.run, kwargs={"handle_signals": False}, daemon=True)
        t.start()
        assert wait_until(lambda: worker.active_workers(stream) == ["w"], 5)

        apply_ok(admin, directive("SET_STATE", state="PAUSED"))
        assert wait_until(lambda: worker.control.desired().state == "PAUSED", 5)
        settle(0.5)  # longer than idle_timeout; paused workers stay alive
        assert t.is_alive()
        assert sub.is_alive()

        apply_ok(admin, directive("SET_STATE", state="DRAINING"))
        assert wait_until(lambda: sub.lanes == 0, 5)
        settle(0.45)  # longer than idle_timeout; drained workers wait for RUNNING
        assert t.is_alive()
        assert sub.is_alive()

        apply_ok(admin, directive("SET_STATE", state="RUNNING"))
        assert wait_until(lambda: sub.lanes == 1, 5)
        worker.publish(stream, {"n": 42})
        assert wait_until(lambda: got == [42], 5)
        t.join(5)
        assert not t.is_alive()
        assert not sub.is_alive()
