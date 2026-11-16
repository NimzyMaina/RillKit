"""The Relay client (spec 5.4) against a scripted control plane on a local WebSocket server."""

import json
import logging
import queue
import threading
import uuid
from collections.abc import Iterator
from typing import Any, Callable, Optional

import pytest
from websockets.sync.server import ServerConnection, serve

from helpers import MODE, fast, keep_server_awake, wait_until
from rillkit import (
    Broker,
    ConfigurationError,
    Control,
    HashTagKeys,
    KeyBuilder,
    Message,
    PlainKeys,
    Stream,
)
from rillkit.relay import relay_url
from rillkit.tracing import parse_traceparent


class FakeServer:
    """A control plane that records every frame and lets the test script its answers."""

    def __init__(self, on_hello: Optional[Callable[[dict], dict]] = None) -> None:
        self.frames: queue.Queue[dict] = queue.Queue()
        self.received: list[dict] = []
        self.headers: list[Any] = []
        self.connections: list[ServerConnection] = []
        self.on_hello = on_hello or (lambda hello: {
            "type": "WELCOME", "protocol_version": 1, "tenant_id": "t-1",
            "telemetry_interval_sec": 0.2,
        })
        self._server = serve(self._handle, "127.0.0.1", 0)
        self.port = self._server.socket.getsockname()[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    def _handle(self, ws: ServerConnection) -> None:
        self.headers.append(ws.request.headers if ws.request else {})
        self.connections.append(ws)
        hello = json.loads(ws.recv())
        self._record(hello)
        answer = self.on_hello(hello)
        ws.send(json.dumps(answer))
        if answer["type"] == "ERROR":
            ws.close(4001, answer["code"])
            return
        for raw in ws:
            self._record(json.loads(raw))

    def _record(self, frame: dict) -> None:
        self.received.append(frame)
        self.frames.put(frame)

    def send(self, frame: dict) -> None:
        self.connections[-1].send(json.dumps(frame))

    def next(self, kind: str, timeout: float = 5.0) -> dict:
        """The next frame of type ``kind``, skipping others (telemetry arrives all the time)."""
        while True:
            frame = self.frames.get(timeout=timeout)
            if frame.get("type") == kind:
                return frame

    def close(self) -> None:
        self._server.shutdown()
        self._thread.join(5)


@pytest.fixture
def fake() -> Iterator[FakeServer]:
    server = FakeServer()
    yield server
    server.close()


@pytest.fixture
def keys() -> KeyBuilder:
    if MODE == "cluster":
        return HashTagKeys(prefix=f"c{uuid.uuid4().hex[:8]}")
    return PlainKeys()


def relay_control(url: str, **kw: Any) -> Control:
    node_id = kw.pop("node_id", "n-1")
    return Control(mode="relay", url=url, api_key="rk_test_secret", node_id=node_id,
                   heartbeat_interval=0.5, **kw)


@pytest.fixture
def worker(make_broker, keys, fake, name) -> Iterator[Broker]:
    broker = make_broker(control=relay_control(fake.url, capture_logs=True, trace_records=True),
                         keys=keys)
    got: list = []
    traces: list = []
    broker.got = got  # type: ignore[attr-defined]
    broker.traces = traces  # type: ignore[attr-defined]

    @broker.subscribe(Stream(name, group="g"), consumer="w", **fast())
    def handle(body: dict, msg: Message) -> None:
        logging.getLogger("app").warning("handled %s", body["n"])
        got.append((body["n"], msg.id))
        traces.append(msg.raw.get("traceparent"))

    yield broker


# urls and settings


@pytest.mark.parametrize("url, expect", [
    ("wss://cloud.rillkit.dev", "wss://cloud.rillkit.dev/relay/v1"),
    ("https://cloud.rillkit.dev/", "wss://cloud.rillkit.dev/relay/v1"),
    ("http://localhost:8080", "ws://localhost:8080/relay/v1"),
    ("wss://example.com/custom/path", "wss://example.com/custom/path"),
])
def test_relay_url(url, expect):
    assert relay_url(url) == expect


def test_insecure_urls_need_loopback_or_an_explicit_opt_in():
    Control(mode="relay", url="ws://127.0.0.1:8080", api_key="k")
    Control(mode="relay", url="http://localhost:8080", api_key="k")
    with pytest.raises(ConfigurationError, match="insecure"):
        Control(mode="relay", url="ws://rillkit-server:8080", api_key="k")
    assert Control(mode="relay", url="ws://rillkit-server:8080", api_key="k", insecure="1").insecure


def test_relay_mode_needs_the_control_extra(monkeypatch):
    import sys

    monkeypatch.setitem(sys.modules, "websockets.sync.client", None)
    with pytest.raises(ConfigurationError, match=r"rillkit\[control\]"):
        Broker(control=Control(mode="relay", url="wss://x", api_key="k"))


# the session


def test_handshake_and_telemetry(worker, fake, keys, r):
    with keep_server_awake(r):
        worker.start()
        hello = fake.next("HELLO")
        assert hello["node_id"] == "n-1"
        assert hello["protocol_version"] == 1
        assert hello["keys"] == keys.describe()
        assert hello["sdk"]["language"] == "python"
        assert fake.headers[0]["Authorization"] == "Bearer rk_test_secret"
        assert fake.headers[0]["User-Agent"].startswith("rillkit-python/")
        report = fake.next("TELEMETRY_REPORT")
        assert report["node_id"] == "n-1"
        [stream] = report["streams"]
        assert (stream["concurrency"], stream["state"]) == (1, "RUNNING")
        fake.next("TELEMETRY_REPORT")  # again after the interval the server asked for
        assert worker.control.relay.connected.is_set()
        assert worker.control.relay.tenant_id == "t-1"


def test_directives_are_applied_and_acknowledged(worker, fake, r, name):
    with keep_server_awake(r):
        worker.start()
        fake.next("HELLO")
        pause = {"type": "DIRECTIVE", "directive_id": f"d-{uuid.uuid4().hex[:6]}",
                 "cmd": "SET_STATE", "scope": "global", "params": {"state": "PAUSED"}}
        fake.send(pause)
        ack = fake.next("DIRECTIVE_ACK")
        assert (ack["directive_id"], ack["status"]) == (pause["directive_id"], "APPLIED")
        assert ack["current_state"]["state"] == "PAUSED"
        assert worker.control.desired().state == "PAUSED"

        fake.send(pause)  # delivered twice, e.g. by two replicas: applied once
        assert fake.next("DIRECTIVE_ACK")["status"] == "DUPLICATE"

        fake.send({"type": "DIRECTIVE", "directive_id": "d-bad", "cmd": "REBOOT"})
        assert fake.next("DIRECTIVE_ACK")["status"] == "REJECTED"

        fake.send({"type": "DIRECTIVE", "directive_id": f"d-{uuid.uuid4().hex[:6]}",
                   "cmd": "SET_CONCURRENCY", "scope": "stream",
                   "target": {"stream": name, "group": "g"}, "params": {"concurrency": 3}})
        assert fake.next("DIRECTIVE_ACK")["current_state"]["concurrency"] == 3
        assert wait_until(lambda: worker.subscribers[0].lanes == 3, 5)

        fake.send({"type": "SOMETHING_NEW"})  # unknown frames are ignored
        fake.send({"type": "DIRECTIVE", "directive_id": f"d-{uuid.uuid4().hex[:6]}",
                   "cmd": "SET_STATE", "params": {"state": "RUNNING"}})
        assert fake.next("DIRECTIVE_ACK")["status"] == "APPLIED"
        worker.publish(name, {"n": 1})
        assert wait_until(lambda: [n for n, _ in worker.got] == [1], 5)


def test_logs_are_served_on_request(worker, fake, r, name):
    with keep_server_awake(r):
        worker.start()
        fake.next("HELLO")
        worker.publish(name, {"n": 7})
        assert wait_until(lambda: len(worker.got) == 1, 5)
        msg_id = worker.got[0][1]
        fake.send({"type": "LOGS_REQUEST", "request_id": "q1", "stream": name, "msg_id": msg_id})
        response = fake.next("LOGS_RESPONSE")
        assert response["request_id"] == "q1"
        assert [rec["message"] for rec in response["records"]] == ["handled 7"]
        fake.send({"type": "LOGS_REQUEST", "request_id": "q2", "stream": name, "msg_id": "0-1"})
        assert fake.next("LOGS_RESPONSE")["records"] == []


def test_a_refused_worker_keeps_consuming_and_retries(make_broker, keys, r, name, caplog):
    attempts: list = []

    def refuse(hello: dict) -> dict:
        attempts.append(hello)
        return {"type": "ERROR", "code": "UNAUTHORIZED", "message": "unknown API key"}

    server = FakeServer(on_hello=refuse)
    try:
        broker = make_broker(control=relay_control(server.url), keys=keys)
        got: list = []

        @broker.subscribe(Stream(name, group="g"), **fast())
        def handle(body: dict) -> None:
            got.append(body["n"])

        with keep_server_awake(r), caplog.at_level(logging.ERROR, logger="rillkit"):
            broker.start()
            assert wait_until(lambda: len(attempts) >= 2, 10)  # retried with backoff
            assert "unknown API key" in caplog.text
            broker.publish(name, {"n": 1})
            assert wait_until(lambda: got == [1], 5)
            assert not broker.control.relay.connected.is_set()
    finally:
        server.close()


def test_the_worker_reconnects_after_the_server_drops_it(worker, fake, r):
    with keep_server_awake(r):
        worker.start()
        fake.next("HELLO")
        fake.connections[-1].close(1012, "restarting")
        fake.next("HELLO", timeout=10)
        assert len(fake.connections) == 2
        assert wait_until(lambda: worker.control.relay.connected.is_set(), 5)


def test_processing_fails_open_without_a_server(make_broker, keys, r, name):
    broker = make_broker(control=relay_control("ws://127.0.0.1:9"), keys=keys)  # nothing there
    got: list = []

    @broker.subscribe(Stream(name, group="g"), **fast())
    def handle(body: dict) -> None:
        got.append(body["n"])

    with keep_server_awake(r):
        broker.start()
        broker.publish(name, {"n": 1})
        assert wait_until(lambda: got == [1], 5)


def test_stop_closes_the_connection(worker, fake, r):
    with keep_server_awake(r):
        worker.start()
        fake.next("HELLO")
        worker.stop(5)
        assert not worker.control.relay.connected.is_set()
        assert not [t for t in threading.enumerate() if t.name == "rillkit-relay" and t.is_alive()]


def test_frames_match_the_protocol_schemas(worker, fake, r, name):
    jsonschema = pytest.importorskip("jsonschema")
    referencing = pytest.importorskip("referencing")
    from test_conformance import PROTOCOL

    schemas = {p.stem: json.loads(p.read_text()) for p in PROTOCOL.glob("*.json")}
    registry = referencing.Registry().with_resources(
        (s["$id"], referencing.Resource.from_contents(s)) for s in schemas.values())

    def check(kind: str, frame: dict) -> None:
        jsonschema.Draft202012Validator(schemas[kind], registry=registry).validate(frame)

    with keep_server_awake(r):
        worker.start()
        hello = fake.next("HELLO")
        check("hello", hello)
        assert hello["capabilities"] == {"inspect_messages": False, "capture_logs": True,
                                         "trace_records": True}
        check("telemetry_report", fake.next("TELEMETRY_REPORT"))
        fake.send({"type": "DIRECTIVE", "directive_id": "d-schema", "cmd": "MAINTENANCE_ON"})
        check("directive_ack", fake.next("DIRECTIVE_ACK"))
        fake.send({"type": "LOGS_REQUEST", "request_id": "q", "stream": name, "msg_id": "0-1"})
        check("logs_response", fake.next("LOGS_RESPONSE"))
        fake.send({"type": "MESSAGES_REQUEST", "request_id": "m", "stream": name, "group": "g"})
        check("messages_response", fake.next("MESSAGES_RESPONSE"))
        fake.send({"type": "TRACE_REQUEST", "request_id": "t",
                   "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"})
        check("trace_response", fake.next("TRACE_RESPONSE"))


def test_messages_are_served_on_request(worker, fake, r, name):
    with keep_server_awake(r):
        worker.start()
        fake.next("HELLO")
        worker.publish(name, {"n": 3})
        assert wait_until(lambda: len(worker.got) == 1, 5)
        fake.send({"type": "MESSAGES_REQUEST", "request_id": "m1", "stream": name, "group": "g",
                   "count": 10})
        page = fake.next("MESSAGES_RESPONSE")
        assert page["request_id"] == "m1" and page["total"] == 1
        assert page["payloads"] is False  # inspect_messages is off for this worker
        [entry] = page["messages"]
        assert (entry["id"], entry["status"], entry["fields"]) == (worker.got[0][1], "acked", None)
        fake.send({"type": "MESSAGES_REQUEST", "request_id": "m2", "stream": name, "group": "g",
                   "count": 0})
        assert "count" in fake.next("MESSAGES_RESPONSE")["error"]


def test_trace_records_are_served_on_request(worker, fake, r, name):
    with keep_server_awake(r):
        worker.start()
        fake.next("HELLO")
        worker.publish(name, {"n": 11})
        assert wait_until(lambda: len(worker.got) == 1, 5)
        trace_id = parse_traceparent(worker.traces[0]).trace_id
        assert wait_until(lambda: int(worker.client.xlen(worker.keys.trace(trace_id))) == 1, 5)
        fake.send({"type": "TRACE_REQUEST", "request_id": "t1", "trace_id": trace_id})
        response = fake.next("TRACE_RESPONSE")
        assert (response["request_id"], response["trace_id"]) == ("t1", trace_id)
        [record] = response["records"]
        assert (record["stream"], record["group"], record["message_id"]) == (
            name, "g", worker.got[0][1])
        assert (record["delivery"], record["outcome"]) == (1, "ok")
        assert record["parent_span_id"] == parse_traceparent(worker.traces[0]).span_id
        assert record["started_at"] <= record["finished_at"]
        assert "data" not in json.dumps(record)
        assert 0 < worker.client.ttl(worker.keys.trace(trace_id)) <= 3600
        fake.send({"type": "TRACE_REQUEST", "request_id": "t2", "trace_id": "nope"})
        assert "trace_id" in fake.next("TRACE_RESPONSE")["error"]
