"""W3C traceparent propagation and per-message log capture."""

import logging
from typing import Any

import pytest

from helpers import entries, fast, sk, wait_until
from rillkit import (
    LogCapture,
    Message,
    Stream,
    TraceContext,
    current_traceparent,
    read_logs,
    use_traceparent,
)
from rillkit.tracing import parse_traceparent

PARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


@pytest.mark.parametrize("value, ok", [
    (PARENT, True),
    (PARENT.upper(), False),  # W3C: lowercase hex only
    ("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra", True),
    ("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra", False),
    ("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", False),
    ("00-00000000000000000000000000000000-00f067aa0ba902b7-01", False),
    ("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", False),
    ("00-xyz-00f067aa0ba902b7-01", False),
    ("", False),
    (None, False),
])
def test_parse_traceparent(value, ok):
    assert (parse_traceparent(value) is not None) is ok


def test_use_traceparent_sets_and_resets_the_context():
    assert current_traceparent() is None
    with use_traceparent(PARENT) as span:
        assert span.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
        assert span.span_id != "00f067aa0ba902b7"
        assert current_traceparent() == span.traceparent
    assert current_traceparent() is None
    with use_traceparent("garbage") as span:  # a new trace
        assert len(span.trace_id) == 32


def test_a_trace_follows_a_message_through_two_streams(make_broker, r, name):
    broker = make_broker(middlewares=[TraceContext()])
    first, second = Stream(name, group="g"), Stream(f"{name}-2", group="g")
    seen: list = []
    out = broker.publisher(second)

    @broker.subscribe(first, **fast())
    @out
    def relay(body: dict, msg: Message) -> dict:
        seen.append(("first", msg.traceparent, current_traceparent()))
        return body

    @broker.subscribe(second, **fast())
    def end(body: dict, msg: Message) -> None:
        seen.append(("second", msg.traceparent, msg.raw.get("traceparent")))

    broker.start()
    with use_traceparent(PARENT):
        broker.publish(first, {"n": 1})
    assert wait_until(lambda: len(seen) == 2)
    trace = "4bf92f3577b34da6a3ce929d0e0e4736"
    for _, own, other in seen:
        assert own.split("-")[1] == trace and other.split("-")[1] == trace
    assert seen[0][1] == seen[0][2]  # inside the handler the context is the message's span
    assert current_traceparent() is None
    broker.stop(5)


def test_publishing_outside_a_trace(make_broker, r, name):
    tracing = make_broker(middlewares=[TraceContext()])
    tracing.publish(name, {"n": 1})
    quiet = make_broker(middlewares=[TraceContext(originate=False)])
    quiet.publish(name, {"n": 2})
    plain = make_broker()
    plain.publish(name, {"n": 3})
    fields = [f for _, f in entries(r, sk(name))]
    assert parse_traceparent(fields[0]["traceparent"]) is not None
    assert fields[1] == {"data": '{"n": 2}'}
    assert fields[2] == {"data": '{"n": 3}'}  # without the middleware, nothing is added


def test_log_capture_stores_each_message_logs(make_broker, r, name):
    broker = make_broker(middlewares=[TraceContext(), LogCapture(ttl=120)])
    stream = Stream(name, group="g")
    ids: list = []
    app = logging.getLogger("app.orders")
    app.setLevel(logging.INFO)

    @broker.subscribe(stream, max_retries=1, **fast())
    def handle(body: dict, msg: Message) -> None:
        ids.append(msg.id)
        app.info("handling order %s", body["n"])
        if body.get("fail"):
            raise ValueError("bad order")

    broker.start()
    broker.publish(stream, {"n": 1})
    broker.publish(stream, {"n": 2, "fail": True})
    assert wait_until(lambda: r.xlen(broker.keys.dlq(stream)) == 1, 10)
    app.info("outside any message")  # not captured anywhere

    ok = read_logs(broker, stream, ids[0])
    assert [(x["level"], x["logger"], x["message"]) for x in ok] == [
        ("INFO", "app.orders", "handling order 1")]
    assert ok[0]["delivery"] == "1"
    assert parse_traceparent(ok[0]["traceparent"]) is not None
    assert 0 < r.ttl(broker.keys.logs(stream, ids[0])) <= 120

    failed = read_logs(broker, stream, ids[1])
    assert [x["level"] for x in failed] == ["INFO", "ERROR"] * 2  # two deliveries
    assert "ValueError: bad order" in failed[1]["exc"]
    assert [x["delivery"] for x in failed] == ["1", "1", "2", "2"]
    broker.stop(5)


def test_log_capture_failure_never_fails_the_message(make_broker, r, name, caplog):
    broker = make_broker(middlewares=[LogCapture()])
    got: list = []

    class DownClient:
        """The broker's client, except that writing the logs fails."""

        def __init__(self, client: Any) -> None:
            self._client = client

        def pipeline(self, *a: Any, **k: Any) -> Any:
            raise ConnectionError("down")

        def __getattr__(self, attr: str) -> Any:
            return getattr(self._client, attr)

    @broker.subscribe(Stream(name, group="g"), **fast())
    def handle(body: dict) -> None:
        logging.getLogger("app").warning("hi")
        broker.client = DownClient(real)
        got.append(body)

    real = broker.client
    broker.start()
    broker.publish(name, {"n": 1})
    assert wait_until(lambda: got == [{"n": 1}])
    assert wait_until(lambda: "could not store the logs" in caplog.text)
    broker.client = real
    assert wait_until(lambda: int(r.xpending(sk(name), "g")["pending"]) == 0)
    broker.stop(5)
