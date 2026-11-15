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
from rillkit.controlplane import DirectiveError, plan_directive, resolve_desired
from rillkit.tracing import parse_traceparent

SPEC = Path(__file__).resolve().parents[1] / "spec"
VECTORS = SPEC / "conformance"
PROTOCOL = SPEC / "protocol" / "v1"
RUNNERS = {"keys.json", "desired_state.json", "directives.json", "traceparent.json"}


def cases(name: str) -> list[dict[str, Any]]:
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
    rest = [args[a] for a in ("node_id", "directive_id") if a in args]
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
    if plan["cmd"] == "EXECUTE_REPLAY":
        got.update(stream=plan["stream"], group=plan["group"], replay=plan["replay"])
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


@pytest.mark.parametrize("name", ["telemetry_report", "directive", "directive_ack"])
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
