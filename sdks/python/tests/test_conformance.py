"""The shared conformance vectors in spec/conformance, run against the Python SDK.

Every SDK runs the same files. A vector failing here means the Python SDK and the spec disagree:
fix whichever is wrong, and if it is the spec, every other SDK must follow.
"""

import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from redis.crc import key_slot

from rillkit import ConfigurationError, Control, HashTagKeys, KeyBuilder, PlainKeys, Stream
from rillkit.consumer import (
    dead_letter_entry,
    lane_consumer_name,
    queue_wait_ms,
    replay_entry,
    retries_exhausted,
    retry_wait_ms,
)
from rillkit.controlplane import DirectiveError, plan_directive, resolve_desired
from rillkit.tracing import parse_traceparent


def _find_spec() -> Path:
    """spec/ at the repository root (sdks/python/tests -> ../../../spec), or next to tests/
    in an sdist, which ships a copy."""
    here = Path(__file__).resolve()
    for candidate in (here.parents[3] / "spec", here.parents[1] / "spec"):
        if (candidate / "conformance").is_dir():
            return candidate
    raise FileNotFoundError("cannot find the spec/ directory with the conformance vectors")


SPEC = _find_spec()
VECTORS = SPEC / "conformance"
PROTOCOL = SPEC / "protocol" / "v1"
RUNNERS = {"keys.json", "desired_state.json", "directives.json", "traceparent.json",
           "data_plane.json"}


def cases(name: str) -> list[dict[str, Any]]:
    """The ``cases`` of a vector file (data_plane.json groups its cases by rule instead)."""
    return json.loads((VECTORS / name).read_text())["cases"]  # type: ignore[no-any-return]


def case_id(case: dict[str, Any]) -> str:
    return str(case.get("name") or case.get("expect") or case.get("expect_error"))[:80]


def builder(spec: dict[str, Any]) -> KeyBuilder:
    if spec["layout"] == "plain":
        return PlainKeys()
    return HashTagKeys(**{k: spec[k] for k in ("prefix", "tenant") if k in spec})


def test_every_vector_file_has_a_runner():
    assert {p.name for p in VECTORS.glob("*.json")} == RUNNERS


# keys (spec 4.2)

STREAM_KINDS = {"stream", "dlq", "heartbeat", "consumers", "idempotency", "logs", "cordon",
                "cordon_buffer", "state_stream"}


def build_key(keys: KeyBuilder, kind: str, args: dict[str, Any]) -> str:
    if kind in STREAM_KINDS:
        s = Stream(args["stream"]["name"], dlq=args["stream"].get("dlq"))
        rest = [args[a] for a in ("group", "consumer", "key", "msg_id") if a in args]
        return getattr(keys, kind)(s, *rest)  # type: ignore[no-any-return]
    rest = [args[a] for a in ("node_id", "directive_id", "trace_id") if a in args]
    return getattr(keys, kind)(*rest)  # type: ignore[no-any-return]


@pytest.mark.parametrize("case", cases("keys.json"), ids=case_id)
def test_keys(case):
    if "expect_error" in case:
        with pytest.raises(ConfigurationError):
            build_key(builder(case["builder"]), case["kind"], case["args"])
        return
    key = build_key(builder(case["builder"]), case["kind"], case["args"])
    assert key == case["expect"]
    assert key_slot(key.encode()) == case["slot"]


# desired state (spec 2.2)


@pytest.mark.parametrize("case", cases("desired_state.json"), ids=case_id)
def test_desired_state(case):
    sub = case["subscription"]
    got = resolve_desired(case["global"], case["stream"], case["node"],
                          None if sub is None else (sub["stream"], sub["group"]))
    assert {"state": got.state, "concurrency": got.concurrency, "block_ms": got.block_ms,
            "batch_size": got.batch_size} == case["expect"]


# directives (spec 2.1, 5.2)


@pytest.mark.parametrize("case", cases("directives.json"), ids=case_id)
def test_directives(case):
    keys = builder(case["builder"])
    expect = case["expect"]
    if expect["status"] == "REJECTED":
        with pytest.raises((DirectiveError, ConfigurationError)):
            plan_directive(keys, case["directive"])
        return
    plan = plan_directive(keys, case["directive"])
    got = {"status": "ACCEPTED", "cmd": plan["cmd"]}
    if plan["cmd"] in ("EXECUTE_REPLAY", "DELETE_DLQ", "CREATE_STREAM"):
        got.update(stream=plan["stream"], group=plan["group"], action=plan["action"])
    else:
        got.update(key=plan["key"], channel=plan["channel"], fields=plan["fields"])
    assert got == expect


# traceparent


@pytest.mark.parametrize("case", cases("traceparent.json"), ids=lambda c: repr(c["traceparent"]))
def test_traceparent(case):
    span = parse_traceparent(case["traceparent"])
    got = None if span is None else {"trace_id": span.trace_id, "span_id": span.span_id,
                                     "flags": span.flags}
    assert got == case["expect"]


# data plane (spec/data-plane.md)

DATA_PLANE = json.loads((VECTORS / "data_plane.json").read_text())


@pytest.mark.parametrize("case", DATA_PLANE["retry_wait"], ids=str)
def test_retry_wait(case):
    got = retry_wait_ms(case["claim_idle_ms"], case["backoff"], case["max_backoff_ms"],
                        case["times_delivered"])
    assert got == pytest.approx(case["expect_ms"])


@pytest.mark.parametrize("case", DATA_PLANE["retries_exhausted"], ids=str)
def test_retries_exhausted(case):
    assert retries_exhausted(case["max_retries"], case["deliveries"]) is case["expect"]


@pytest.mark.parametrize("case", DATA_PLANE["dead_letter"], ids=case_id)
def test_dead_letter_entry(case):
    got = dead_letter_entry(case["fields"], case["msg_id"], case["stream"], case["group"],
                            case["error"], case["deliveries"])
    assert got == case["expect"]


@pytest.mark.parametrize("case", DATA_PLANE["replay"], ids=str)
def test_replay_entry(case):
    assert replay_entry(case["fields"]) == case["expect"]


@pytest.mark.parametrize("case", DATA_PLANE["lane_consumer"], ids=str)
def test_lane_consumer_name(case):
    assert lane_consumer_name(case["consumer"], case["index"]) == case["expect"]


@pytest.mark.parametrize("case", DATA_PLANE["queue_wait"], ids=str)
def test_queue_wait(case):
    got = queue_wait_ms(case["published_ms"], case["started_ms"])
    assert got == pytest.approx(case["expect_ms"])


# protocol schemas (spec 5)


@pytest.fixture(scope="module")
def validator() -> Any:
    jsonschema = pytest.importorskip("jsonschema")
    referencing = pytest.importorskip("referencing")
    schemas = {p.name: json.loads(p.read_text()) for p in PROTOCOL.glob("*.json")}
    registry = referencing.Registry().with_resources(
        (s["$id"], referencing.Resource.from_contents(s)) for s in schemas.values()
    )

    def validate(name: str, document: Any) -> None:
        schema = schemas[f"{name}.json"]
        jsonschema.Draft202012Validator.check_schema(schema)
        jsonschema.Draft202012Validator(schema, registry=registry).validate(document)

    return validate


@pytest.mark.parametrize("name", ["telemetry_report", "directive", "directive_ack", "hello",
                                  "welcome", "error", "logs_request", "logs_response",
                                  "trace_request", "trace_response"])
def test_schema_examples_are_valid(validator, name):
    validator(name, _example(name))


def _example(name: str) -> dict[str, Any]:
    return json.loads((PROTOCOL / "examples" / f"{name}.json").read_text())  # type: ignore[no-any-return]


def _bad_documents() -> list[tuple[str, str, Any]]:
    d, ack, report = _example("directive"), _example("directive_ack"), _example("telemetry_report")
    state = {"type": "DIRECTIVE", "directive_id": "d", "cmd": "SET_STATE"}
    return [
        ("directive", "a non-integer concurrency", {**d, "params": {"concurrency": 2.5}}),
        ("directive", "a lowercase cmd", {**d, "cmd": "set_concurrency"}),
        ("directive", "SET_STATE without a state", state),
        ("directive", "node scope without node_id",
         {**state, "scope": "node", "params": {"state": "PAUSED"}}),
        ("directive", "SET_POLL_CONFIG with nothing to set",
         {**state, "cmd": "SET_POLL_CONFIG", "params": {}}),
        ("directive", "a stream without a group",
         {**state, "target": {"stream": "s"}, "params": {"state": "PAUSED"}}),
        ("directive_ack", "REJECTED without an error", {**ack, "status": "REJECTED"}),
        ("telemetry_report", "a lowercase state",
         {**report, "workers": {**report["workers"], "state": "paused"}}),
        ("telemetry_report", "an incomplete keys descriptor",
         {**report, "keys": {"layout": "plain"}}),
        ("trace_request", "an uppercase trace id",
         {**_example("trace_request"), "trace_id": "4BF92F3577B34DA6A3CE929D0E0E4736"}),
        ("trace_response", "an unknown outcome",
         {**_example("trace_response"), "records": [
             {**_example("trace_response")["records"][0], "outcome": "retried"}]}),
    ]


@pytest.mark.parametrize("name, why, document", _bad_documents(), ids=lambda v: str(v))
def test_schemas_reject_invalid_messages(validator, name, why, document):
    jsonschema = pytest.importorskip("jsonschema")
    with pytest.raises(jsonschema.ValidationError):
        validator(name, document)


def test_accepted_directive_vectors_match_the_schema(validator):
    for case in cases("directives.json"):
        if case["expect"]["status"] == "ACCEPTED":
            validator("directive", case["directive"])


def test_real_messages_match_the_schemas(validator, make_broker, name):
    worker = make_broker(control=Control(mode="valkey", node_id=f"n-{uuid.uuid4().hex[:6]}"),
                         keys=HashTagKeys(prefix=f"c{uuid.uuid4().hex[:8]}"))

    @worker.subscribe(Stream(name, group="g"))
    def handle(body: Any) -> None: ...

    worker.subscribers[0].setup()
    assert worker.control is not None
    validator("telemetry_report", worker.control.telemetry())
    for directive in (
        {"type": "DIRECTIVE", "directive_id": "d-1", "cmd": "MAINTENANCE_ON"},
        {"type": "DIRECTIVE", "directive_id": "d-1", "cmd": "MAINTENANCE_ON"},  # duplicate
        {"type": "DIRECTIVE", "directive_id": "d-2", "cmd": "REBOOT"},  # rejected
        {"cmd": "SET_STATE"},  # rejected without an id
        {"type": "DIRECTIVE", "directive_id": "d-3", "cmd": "EXECUTE_REPLAY", "scope": "stream",
         "target": {"stream": name, "group": "g"}},
    ):
        validator("directive_ack", worker.control.apply(directive))
