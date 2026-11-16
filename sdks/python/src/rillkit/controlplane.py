"""The worker side of the RillKit control plane: desired state, wakeups, heartbeats, directives.

Loaded only when a broker's control mode is not ``off``. ``ControlPlane`` (``broker.control``):

- reads the desired state from the state keys before any stream is polled (``hydrate``), so a
  booting worker honours ``PAUSED`` and ``MAINTENANCE`` from its first poll,
- listens on the broadcast channel and its node's channel and re-reads the state on every
  wakeup, and every ``refresh_interval`` seconds in case a wakeup was lost,
- writes a node heartbeat (a hash with a TTL plus the node registry),
- applies directives (``apply``): deduplicated by directive id, stored in the matching state key,
  then announced on the matching channel. Any node can apply any directive; every node that it
  concerns converges on the wakeup.

Desired state lives in hashes at three scopes: global, per stream and group, and per node. A
node hash may also hold per-subscription fields, ``<field>:<stream>:<group>``. Each worker
combines them: the most restrictive ``state`` wins (any scope can pause), and for
``concurrency``, ``block_ms`` and ``batch_size`` the most specific scope wins over the code's own
values.
"""

import json
import logging
import os
import random
import re
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional, Union

from .clients import WATCH_ERRORS, is_cluster, node_client_for, same_slot
from .config import Control, default_node_id
from .consumer import TRACE_MAX_RECORDS, Subscriber, _text
from .keys import _escape_plain
from .stream import Stream

if TYPE_CHECKING:
    from .broker import Broker

log = logging.getLogger("rillkit")

STATES = ("RUNNING", "PAUSED", "MAINTENANCE", "DRAINING")
# how restrictive each state is; the most restrictive scope wins
_RANK = {"RUNNING": 0, "PAUSED": 1, "MAINTENANCE": 2, "DRAINING": 3}
SCOPES = ("global", "stream", "node")
COMMANDS = (
    "SET_STATE", "SET_CONCURRENCY", "SET_POLL_CONFIG", "MAINTENANCE_ON", "MAINTENANCE_OFF",
    "EXECUTE_REPLAY", "DELETE_DLQ", "CREATE_STREAM",
)
# one-off actions on a stream, as opposed to desired state: deduplicated, then run once
ACTIONS = ("EXECUTE_REPLAY", "DELETE_DLQ", "CREATE_STREAM")
MAX_IDS = 1000
MAX_DATA_BYTES = 1024 * 1024
_ENTRY_ID = re.compile(r"^[0-9]+-[0-9]+$")
DEDUP_TTL = 60
MAX_CONCURRENCY = 256
MAX_BLOCK_MS = 600_000
MAX_BATCH_SIZE = 10_000
MAX_REPLAY_COUNT = 100_000
# the tunable settings and the largest value each accepts
TUNABLES = {"concurrency": MAX_CONCURRENCY, "block_ms": MAX_BLOCK_MS, "batch_size": MAX_BATCH_SIZE}


@dataclass(frozen=True)
class Desired:
    """The state and settings one subscription should run with. None means the code's value."""

    state: str = "RUNNING"
    concurrency: Optional[int] = None
    block_ms: Optional[int] = None
    batch_size: Optional[int] = None


class DirectiveError(ValueError):
    """The directive is malformed or names something that does not exist."""


def _positive(value: Any, name: str, top: int, strings: bool = False) -> int:
    """``value`` as an integer from 1 to ``top``. Directive parameters must be JSON integers;
    with ``strings`` (values read from a state hash) a string of ASCII digits is accepted too.
    Booleans, floats and anything else are refused rather than converted."""
    if isinstance(value, int) and not isinstance(value, bool):
        number = value
    elif strings and isinstance(value, str) and value.isascii() and value.isdigit():
        number = int(value)
    else:
        raise DirectiveError(f"{name} must be an integer")
    if not 1 <= number <= top:
        raise DirectiveError(f"{name} must be between 1 and {top}")
    return number


def _entry_ids(value: Any) -> list[str]:
    """A list of 1 to MAX_IDS stream entry ids (``<ms>-<seq>``), duplicates removed in order."""
    if not isinstance(value, list) or not 1 <= len(value) <= MAX_IDS:
        raise DirectiveError(f"ids must be a list of 1 to {MAX_IDS} entry ids")
    out: list[str] = []
    for item in value:
        if not isinstance(item, str) or not _ENTRY_ID.match(item):
            raise DirectiveError(f"{item!r} is not a stream entry id like 1718000000000-0")
        if item not in out:
            out.append(item)
    return out


def _sub_suffix(stream: str, group: str) -> str:
    return f":{_escape_plain(stream)}:{group}"


def plan_directive(keys: Any, directive: Any) -> dict[str, Any]:
    """Validate a directive and work out what applying it writes, without touching a server.

    Returns ``cmd`` and, for state commands, the state ``key``, the wakeup ``channel`` and the
    hash ``fields`` to write; for ``EXECUTE_REPLAY``, the ``stream``, ``group`` and ``params``.
    Raises ``DirectiveError`` for a malformed directive. ``keys`` is the key builder."""
    if not isinstance(directive, Mapping):
        raise DirectiveError("a directive must be a JSON object")
    directive_id = directive.get("directive_id")
    if not directive_id or not isinstance(directive_id, str):
        raise DirectiveError("directive_id is required")
    if directive.get("type", "DIRECTIVE") != "DIRECTIVE":
        raise DirectiveError("type must be DIRECTIVE")
    cmd = directive.get("cmd")
    if cmd not in COMMANDS:
        raise DirectiveError(f"unknown cmd {cmd!r}, expected one of {COMMANDS}")
    scope = directive.get("scope", "global")
    if scope not in SCOPES:
        raise DirectiveError(f"unknown scope {scope!r}, expected one of {SCOPES}")
    target = directive.get("target") or {}
    params = directive.get("params") or {}
    if not isinstance(target, Mapping) or not isinstance(params, Mapping):
        raise DirectiveError("target and params must be objects")
    stream, group = target.get("stream"), target.get("group")
    node_id = target.get("node_id")
    if (stream is None) != (group is None):
        raise DirectiveError("target needs both stream and group, or neither")
    for name, value in (("stream", stream), ("group", group), ("node_id", node_id)):
        if value is not None and (not isinstance(value, str) or not value):
            raise DirectiveError(f"target.{name} must be a non-empty string")

    if cmd in ACTIONS:
        if not stream:
            raise DirectiveError(f"{cmd} needs target.stream and target.group")
        return {"cmd": cmd, "stream": stream, "group": group, "action": _plan_action(cmd, params)}

    fields: dict[str, str] = {}
    if cmd in ("SET_STATE", "MAINTENANCE_ON", "MAINTENANCE_OFF"):
        state = {"MAINTENANCE_ON": "MAINTENANCE", "MAINTENANCE_OFF": "RUNNING"}.get(
            cmd, params.get("state"))
        if state not in STATES:
            raise DirectiveError(f"state must be one of {STATES}")
        fields["state"] = state
    if cmd == "SET_CONCURRENCY":
        fields["concurrency"] = str(_positive(params.get("concurrency"), "concurrency",
                                              MAX_CONCURRENCY))
    if cmd in ("SET_CONCURRENCY", "SET_POLL_CONFIG"):
        block = params.get("block_ms", params.get("poll_ms"))
        if block is not None:
            fields["block_ms"] = str(_positive(block, "block_ms", MAX_BLOCK_MS))
        if params.get("batch_size") is not None:
            fields["batch_size"] = str(_positive(params["batch_size"], "batch_size",
                                                     MAX_BATCH_SIZE))
        if cmd == "SET_POLL_CONFIG" and not fields:
            raise DirectiveError("SET_POLL_CONFIG needs block_ms (or poll_ms) or batch_size")

    if scope == "global":
        key, channel = keys.state_global(), keys.broadcast_channel()
    elif scope == "stream":
        if not stream:
            raise DirectiveError("scope 'stream' needs target.stream and target.group")
        key = keys.state_stream(Stream(stream), group)
        channel = keys.broadcast_channel()
    else:
        if not node_id:
            raise DirectiveError("scope 'node' needs target.node_id")
        key, channel = keys.state_node(node_id), keys.node_channel(node_id)
        if stream and group:
            suffix = _sub_suffix(stream, group)
            fields = {f"{name}{suffix}": value for name, value in fields.items()}
    return {"cmd": cmd, "key": key, "channel": channel, "fields": fields,
            "target": {"stream": stream, "group": group}}


def _plan_action(cmd: str, params: Mapping[str, Any]) -> dict[str, Any]:
    """What an action directive does, validated (see ``plan_directive``)."""
    if cmd == "CREATE_STREAM":
        start = params.get("start", "$")
        if start not in ("$", "0"):
            raise DirectiveError("start must be \"$\" (new messages only) or \"0\" (everything)")
        return {"start": start}
    if cmd == "DELETE_DLQ":
        if params.get("all") is True and params.get("ids") is None:
            return {"all": True}
        if params.get("all") is True:
            raise DirectiveError("DELETE_DLQ takes ids or all: true, not both")
        if params.get("all") not in (None, False):
            raise DirectiveError("all must be true")
        if params.get("ids") is None:
            raise DirectiveError("DELETE_DLQ needs ids or all: true")
        return {"ids": _entry_ids(params["ids"])}
    # EXECUTE_REPLAY: the oldest count entries, chosen entries (optionally edited), or set_id
    given = [k for k in ("count", "ids", "set_id") if params.get(k) is not None]
    if len(given) > 1:
        raise DirectiveError("EXECUTE_REPLAY takes one of count, ids or set_id")
    if params.get("data") is not None and "ids" not in given:
        raise DirectiveError("data can only be given with ids")
    if "set_id" in given:
        if not isinstance(params["set_id"], str) or not params["set_id"]:
            raise DirectiveError("set_id must be a non-empty string")
        return {"set_id": params["set_id"]}
    if "ids" in given:
        ids = _entry_ids(params["ids"])
        data = params.get("data") or {}
        if not isinstance(data, Mapping):
            raise DirectiveError("data must map entry ids to new data")
        for key, value in data.items():
            if key not in ids:
                raise DirectiveError(f"data names {key!r}, which is not in ids")
            if not isinstance(value, str) or len(value.encode()) > MAX_DATA_BYTES:
                raise DirectiveError("each data value must be a string of at most 1 MB")
        return {"ids": ids, "data": dict(data)}
    return {"count": _positive(params.get("count", 100), "count", MAX_REPLAY_COUNT)}


def resolve_desired(
    global_state: Optional[Mapping[str, str]],
    stream_state: Optional[Mapping[str, str]],
    node_state: Optional[Mapping[str, str]],
    subscription: Optional[tuple[str, str]] = None,
) -> Desired:
    """Combine the state hashes that apply to one subscription (``(stream, group)``), or to the
    node as a whole without one.

    The most restrictive ``state`` across all of them wins. For each tunable the most specific
    valid value wins: a node field for this subscription (``<field>:<stream>:<group>``), then a
    plain node field, then the stream's, then the global one. Invalid values are ignored."""
    node = dict(node_state or {})
    layers = [dict(global_state or {})]
    if subscription is not None:
        layers.append(dict(stream_state or {}))
    layers.append(node)
    if subscription is not None:
        suffix = _sub_suffix(*subscription)
        layers.append({
            name[: -len(suffix)]: value for name, value in node.items() if name.endswith(suffix)
        })
    state = "RUNNING"
    values: dict[str, Optional[int]] = dict.fromkeys(TUNABLES)
    for layer in layers:
        wanted = layer.get("state", "RUNNING")
        if wanted in _RANK and _RANK[wanted] > _RANK[state]:
            state = wanted
        for name, top in TUNABLES.items():
            if name in layer:
                try:
                    values[name] = _positive(layer[name], name, top, strings=True)
                except DirectiveError:
                    log.warning("ignoring invalid %s=%r in the desired state", name, layer[name])
    return Desired(state, values["concurrency"], values["block_ms"], values["batch_size"])


def _trace_order(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Order attempts by their place in the trace, then by start time.

    Two attempts can start in the same millisecond, and a child can finish before its parent,
    so timestamps alone are not enough. ``parent_span_id`` gives the causal order: a handler's
    record follows the record whose span published its message.
    """
    spans = {r["span_id"] for r in records if r["span_id"]}
    parents = {r["span_id"]: r["parent_span_id"] for r in records if r["span_id"]}

    def depth(span_id: str) -> int:
        seen: set[str] = set()
        depth = 0
        while span_id in parents and parents[span_id] in spans and span_id not in seen:
            seen.add(span_id)
            span_id = parents[span_id]
            depth += 1
        return depth

    return sorted(records, key=lambda r: (depth(r["span_id"]),
                                          r.get("started_us", r["started_at"] * 1000),
                                          r["published_at"]))


class ControlPlane:
    def __init__(self, broker: "Broker", settings: Control) -> None:
        self.broker = broker
        self.settings = settings
        self.node_id = settings.node_id or default_node_id()
        self.version = 0
        self._hashes: dict[str, dict[str, str]] = {}
        self._cond = threading.Condition()
        self._stopping = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._next_beat = 0.0
        self._started_at = int(time.time())
        self._rate_mark: dict[int, tuple[float, int]] = {}
        self.relay: Optional[Any] = None
        if settings.mode == "relay":
            from .relay import Relay

            self.relay = Relay(self, settings)
        self._cpu_mark: Optional[tuple[float, float]] = None

    @property
    def client(self) -> Any:
        return self.broker.client

    @property
    def keys(self) -> Any:
        return self.broker.keys

    # desired state

    def _state_keys(self) -> list[str]:
        keys = [self.keys.state_global(), self.keys.state_node(self.node_id)]
        for sub in self.broker.subscribers:
            key = self.keys.state_stream(sub.stream, sub.group)
            if key not in keys:
                keys.append(key)
        return keys

    def hydrate(self) -> bool:
        """Re-read every state key that concerns this node. Returns whether anything changed.

        Lanes waiting for a change wake up, and subscribers start or retire lanes to match."""
        hashes = {}
        for key in self._state_keys():
            raw: Any = self.client.hgetall(key)
            hashes[key] = {_text(k): _text(v) for k, v in raw.items()}
        with self._cond:
            changed = hashes != self._hashes
            if changed:
                self._hashes = hashes
                self.version += 1
                self._cond.notify_all()
        if changed and self._running:
            for sub in self.broker.subscribers:
                sub.reconcile()
            # lanes woken above may have gone back to waiting before reconcile() lowered
            # their number; wake them again so retired lanes stop now
            self.wake()
        return changed

    def desired(self, sub: Optional[Subscriber] = None) -> Desired:
        """What ``sub`` (or, without one, the node) should do now, from the last hydrate."""
        with self._cond:
            hashes = self._hashes
        stream = None if sub is None else hashes.get(self.keys.state_stream(sub.stream, sub.group))
        return resolve_desired(
            hashes.get(self.keys.state_global()),
            stream,
            hashes.get(self.keys.state_node(self.node_id)),
            None if sub is None else (sub.stream.name, sub.group),
        )

    def wait(self, version: int, timeout: float) -> None:
        """Block until the desired state changes from ``version``, ``wake()`` or ``timeout``."""
        with self._cond:
            if self.version == version and not self._stopping.is_set():
                self._cond.wait(timeout)

    def wake(self) -> None:
        with self._cond:
            self._cond.notify_all()

    # running

    def start(self) -> None:
        self._stopping.clear()
        self._running = True
        self._beat()
        self._thread = threading.Thread(target=self._listen, name="rillkit-control", daemon=True)
        self._thread.start()
        if self.relay is not None:
            self.relay.start()

    def stop(self, timeout: Optional[float] = None) -> None:
        self._running = False
        self._stopping.set()
        self.wake()
        if self.relay is not None:
            self.relay.stop(timeout)
        if self._thread is not None:
            self._thread.join(timeout)
        try:
            pipe = self.client.pipeline(transaction=False)
            pipe.delete(self.keys.node(self.node_id))
            pipe.zrem(self.keys.nodes(), self.node_id)
            pipe.execute()
        except Exception as e:
            log.error("could not remove the node heartbeat: %s", e)

    def _channels(self) -> list[str]:
        return [self.keys.broadcast_channel(), self.keys.node_channel(self.node_id)]

    def _subscribe(self) -> Any:
        if is_cluster(self.client):
            # sharded Pub/Sub: the channels carry the control tag, so they live on one shard
            pubsub = self.client.pubsub()
            pubsub.ssubscribe(*self._channels())
        else:
            pubsub = self.client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(*self._channels())
        return pubsub

    def _next_message(self, pubsub: Any) -> Optional[Mapping[str, Any]]:
        """The next wakeup, waiting up to about a second. Subscribe confirmations are None."""
        if not is_cluster(self.client):
            msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            return msg if isinstance(msg, Mapping) else None
        # Read each shard's connection directly (target_node). Without it, valkey-py 6.2
        # ignores the timeout and, with ignore_subscribe_messages, drops every message.
        nodes = {}
        for channel in self._channels():
            node = self.client.get_node_from_key(channel)
            nodes[node.name] = node
        timeout = 1.0 / len(nodes)
        for node in nodes.values():
            msg = pubsub.get_sharded_message(
                ignore_subscribe_messages=False, timeout=timeout, target_node=node
            )
            if isinstance(msg, Mapping) and _text(msg.get("type")) == "smessage":
                return msg
        return None

    def _listen(self) -> None:
        delay = 1.0
        while not self._stopping.is_set():
            pubsub = None
            try:
                pubsub = self._subscribe()
                self.hydrate()  # after subscribing, so no change can fall in between
                delay = 1.0
                next_refresh = time.monotonic() + self.settings.refresh_interval
                while not self._stopping.is_set():
                    msg = self._next_message(pubsub)
                    if msg is not None and msg.get("type") in ("message", "smessage"):
                        self.hydrate()
                        next_refresh = time.monotonic() + self.settings.refresh_interval
                    elif time.monotonic() >= next_refresh:
                        self.hydrate()
                        next_refresh = time.monotonic() + self.settings.refresh_interval
                    self._maybe_beat()
            except Exception as e:
                if self._stopping.is_set():
                    break
                wait = delay * random.uniform(0.5, 1.5)
                log.warning("control listener error, reconnecting in %.1f s: %s", wait, e)
                self._maybe_beat()
                self._stopping.wait(wait)
                delay = min(delay * 2, 30.0)
            finally:
                if pubsub is not None:
                    try:
                        pubsub.close()
                    except Exception:
                        pass

    def _maybe_beat(self) -> None:
        if time.monotonic() >= self._next_beat:
            self._beat()

    def _beat(self) -> None:
        """Write the node heartbeat: a hash with a TTL, and the node registry entry."""
        self._next_beat = time.monotonic() + self.settings.heartbeat_interval
        now = time.time()
        ttl = max(3, int(self.settings.heartbeat_interval * 3))
        try:
            report = self.telemetry()
        except Exception as e:
            log.debug("telemetry failed: %s", e)
            report = self._node_report()
        workers = report["workers"]
        try:
            key = self.keys.node(self.node_id)
            pipe = self.client.pipeline(transaction=False)
            pipe.hset(key, mapping={
                "state": workers["state"],
                "active_workers": workers["active"],
                "in_flight": workers["in_flight"],
                "started_at": self._started_at,
                "updated_at": int(now * 1000),
                "telemetry": json.dumps(report),
            })
            pipe.expire(key, ttl)
            pipe.zadd(self.keys.nodes(), {self.node_id: int((now + ttl) * 1000)})
            pipe.zremrangebyscore(self.keys.nodes(), "-inf", int(now * 1000))
            pipe.execute()
        except Exception as e:
            log.error("node heartbeat failed: %s", e)

    def nodes(self) -> list[str]:
        """Ids of the nodes with a live heartbeat in this namespace, from the node registry."""
        members: Any = self.client.zrangebyscore(self.keys.nodes(), int(time.time() * 1000), "+inf")
        return sorted(_text(m) for m in members)

    # telemetry (spec 5.1)

    def _node_report(self) -> dict[str, Any]:
        subs = self.broker.subscribers
        first = subs[0] if subs else None
        desired = self.desired(first) if first else self.desired()
        return {
            "type": "TELEMETRY_REPORT",
            "node_id": self.node_id,
            "timestamp": int(time.time()),
            "keys": self.keys.describe(),
            "workers": {
                "active": sum(s.lanes for s in subs),
                "in_flight": sum(s.in_flight for s in subs),
                "state": self.desired().state,
                "poll_ms": desired.block_ms or (first._block_ms if first else None),
                "batch_size": desired.batch_size or (first.count if first else None),
                "cpu_pct": self._cpu_pct(),
                "mem_mb": _mem_mb(),
            },
            "streams": [],
        }

    def telemetry(self) -> dict[str, Any]:
        """A ``TELEMETRY_REPORT`` for this node: workers, key layout, and per stream the
        physical keys, lag, pending count and ack rate."""
        report = self._node_report()
        for sub in self.broker.subscribers:
            lag = pending = None
            groups: Any = self.client.xinfo_groups(sub.stream_key)
            for group in groups:
                if _text(group.get("name")) == sub.group:
                    lag, pending = group.get("lag"), group.get("pending")
            report["streams"].append({
                "stream": sub.stream.name,
                "group": sub.group,
                "stream_key": sub.stream_key,
                "dlq_key": sub.dlq_key,
                "lag": lag,
                "pending": pending,
                "ack_rate_sec": self._rate(sub),
                "concurrency": sub.lanes,
                "state": self.desired(sub).state,
                "succeeded_total": sub.succeeded_total,
                "failed_total": sub.failed_total,
                "dead_lettered_total": sub.dead_lettered_total,
                "bytes_total": sub.bytes_total,
                "dlq_length": self.client.xlen(sub.dlq_key),
                "attempts_total": sub.attempts_total,
                "wait_ms_total": round(sub.wait_ms_total, 3),
                "work_ms_total": round(sub.work_ms_total, 3),
            })
        return report

    def _rate(self, sub: Subscriber) -> float:
        now, done = time.monotonic(), sub.processed
        then, before = self._rate_mark.get(id(sub), (now, done))
        self._rate_mark[id(sub)] = (now, done)
        return round((done - before) / (now - then), 2) if now > then else 0.0

    def _cpu_pct(self) -> Optional[float]:
        times = os.times()
        cpu, wall = times.user + times.system, time.monotonic()
        mark, self._cpu_mark = self._cpu_mark, (cpu, wall)
        if mark is None or wall <= mark[1]:
            return None
        return round(100 * (cpu - mark[0]) / (wall - mark[1]), 1)

    # directives (spec 5.2, 5.3)

    def apply(self, directive: Union[str, bytes, Mapping[str, Any]]) -> dict[str, Any]:
        """Apply a ``DIRECTIVE`` and return its ``DIRECTIVE_ACK``.

        The directive id is recorded for 60 s, so the same directive delivered twice (through
        two Relays, or retried) is applied once; the second ack has status ``DUPLICATE``. A
        malformed directive is ``REJECTED`` and an error while applying it ``FAILED``; both
        carry an ``error``.
        """
        directive_id = None
        try:
            if isinstance(directive, (str, bytes)):
                directive = json.loads(directive)
            if isinstance(directive, Mapping) and isinstance(directive.get("directive_id"), str):
                directive_id = directive["directive_id"]
            plan = plan_directive(self.keys, directive)
        except (DirectiveError, ValueError) as e:
            return self._ack(directive_id, "REJECTED", error=str(e))
        try:
            if plan["cmd"] in ACTIONS:
                if not self.client.set(self.keys.dedup(directive_id), "1", nx=True,
                                       ex=DEDUP_TTL):
                    return self._ack(directive_id, "DUPLICATE")
                result = self._act(plan)
                return self._ack(directive_id, "APPLIED", result=result)
            fields = {**plan["fields"], "directive_id": directive_id,
                      "updated_at": str(int(time.time() * 1000))}
            if not self._write_state(self.keys.dedup(directive_id), plan["key"], fields):
                return self._ack(directive_id, "DUPLICATE")
            self._publish(plan["channel"], json.dumps({"directive_id": directive_id}))
        except Exception as e:
            log.error("directive %s failed: %s", directive_id, e)
            return self._ack(directive_id, "FAILED", error=str(e))
        try:
            self.hydrate()  # this node at once; the others on the wakeup
        except Exception as e:
            log.warning("directive %s applied, but re-reading the state failed: %s",
                        directive_id, e)
        return self._ack(directive_id, "APPLIED", target=plan["target"])

    def _write_state(self, dedup: str, key: str, fields: Mapping[str, str]) -> bool:
        """Record the directive id and write the state, once. False if already applied.

        When both keys share a slot (always outside a cluster, and under ``HashTagKeys``) this
        is one optimistic transaction: WATCH the dedup key, check it, then MULTI/EXEC. Otherwise
        the dedup key is set first and the state written after, which is safe because writing
        the same desired state twice changes nothing."""
        if not same_slot(dedup, key) and is_cluster(self.client):
            if not self.client.set(dedup, "1", nx=True, ex=DEDUP_TTL):
                return False
            self.client.hset(key, mapping=dict(fields))
            return True
        pipe = node_client_for(self.client, dedup).pipeline(transaction=True)
        try:
            pipe.watch(dedup)
            if pipe.exists(dedup):
                return False
            pipe.multi()
            pipe.set(dedup, "1", ex=DEDUP_TTL)
            pipe.hset(key, mapping=dict(fields))
            pipe.execute()
            return True
        except WATCH_ERRORS:
            return False  # another node recorded the same directive id in the meantime
        finally:
            pipe.reset()

    def _publish(self, channel: str, message: str) -> None:
        if is_cluster(self.client):
            self.client.spublish(channel, message)
        else:
            self.client.publish(channel, message)

    def _act(self, plan: Mapping[str, Any]) -> dict[str, Any]:
        stream = self._stream(plan["stream"], plan["group"])
        action = plan["action"]
        if plan["cmd"] == "CREATE_STREAM":
            return {"created": self.broker.create_group(stream, plan["group"], action["start"])}
        if plan["cmd"] == "DELETE_DLQ":
            if action.get("all"):
                return {"deleted": self.broker.purge_dlq(stream)}
            return {"deleted": self.broker.delete_dlq_entries(stream, action["ids"])}
        if "set_id" in action:
            self.client.xgroup_setid(self.keys.stream(stream), plan["group"], action["set_id"])
            return {"set_id": action["set_id"]}
        if "ids" in action:
            replayed, missing = self.broker.replay_dlq_entries(stream, action["ids"],
                                                               action["data"])
            return {"replayed": replayed, "missing": missing}
        return {"replayed": self.broker.replay_dlq(stream, count=action["count"])}

    def capabilities(self) -> dict[str, bool]:
        """What this worker lets the control plane see (sent in HELLO)."""
        from .logs import LogCapture

        return {"inspect_messages": bool(self.settings.inspect_messages),
                "capture_logs": any(isinstance(m, LogCapture) for m in self.broker.middlewares),
                "trace_records": bool(self.settings.trace_records)}

    def browse(self, stream_name: str, group: str, source: str = "stream",
               before: Optional[str] = None, count: int = 50,
               ids: Optional[list[str]] = None, after: Optional[str] = None) -> dict[str, Any]:
        """A page of a stream's (or its DLQ's) entries, newest first, for the dashboard.

        No cursor: the newest ``count``. ``before``: the next older page. ``after``: only
        entries newer than that id, the oldest ``count`` of them (``more_after`` says whether
        newer ones are left), for appending to a list already shown.

        Payloads and DLQ error texts are included only with ``inspect_messages``. Entries of the
        stream carry their status in ``group``: waiting (not delivered yet), pending (delivered,
        not acknowledged, with its consumer and deliveries) or acked."""
        stream = self._stream(stream_name, group)
        key = self.keys.dlq(stream) if source == "dlq" else self.keys.stream(stream)
        payloads = bool(self.settings.inspect_messages)
        as_of = datetime.now(timezone.utc).isoformat()
        rows: list[Any] = []
        more_after = False
        if ids:
            for entry_id in ids:
                rows.extend(self.client.xrange(key, entry_id, entry_id))
        elif after:
            rows = list(self.client.xrange(key, min=f"({after}", max="+", count=count))
            more_after = len(rows) == count
            rows.reverse()  # newest first, like every page
        else:
            top = f"({before}" if before else "+"
            rows = list(self.client.xrevrange(key, max=top, min="-", count=count))
        messages = [self._entry(_text(i), {_text(k): _text(v) for k, v in raw.items()}, source,
                                payloads) for i, raw in rows]
        if source == "stream" and messages:
            self._add_status(key, group, messages)
        more = not ids and not after and len(rows) == count
        return {"messages": messages, "next_before": messages[-1]["id"] if more else None,
                "more_after": more_after, "total": int(self.client.xlen(key)),
                "payloads": payloads, "as_of": as_of}

    def trace(self, trace_id: str) -> dict[str, Any]:
        """The recorded attempts of one trace, oldest first (data-plane 9.2). Empty when this
        worker keeps no trace records, the trace is unknown, or its key expired."""
        records: list[dict[str, Any]] = []
        rows: Any = self.client.xrange(self.keys.trace(trace_id), count=TRACE_MAX_RECORDS)
        for _entry_id, raw in rows:
            f = {_text(k): _text(v) for k, v in raw.items()}
            try:
                record = {
                    "trace_id": f.get("trace_id", trace_id),
                    "span_id": f.get("span_id", ""),
                    "parent_span_id": f.get("parent_span_id", ""),
                    "stream": f.get("stream", ""),
                    "group": f.get("group", ""),
                    "message_id": f.get("message_id", ""),
                    "consumer": f.get("consumer", ""),
                    "node_id": f.get("node_id", ""),
                    "delivery": int(f.get("delivery", "1")),
                    "published_at": int(f.get("published_at", "0")),
                    "started_at": int(f.get("started_at", "0")),
                    "started_us": int(f.get("started_us", int(f.get("started_at", "0")) * 1000)),
                    "finished_at": int(f.get("finished_at", "0")),
                    "outcome": f.get("outcome", "ok"),
                }
            except (TypeError, ValueError):
                continue
            records.append(record)
        records = _trace_order(records)
        return {"type": "TRACE_RESPONSE", "trace_id": trace_id, "records": records}

    def _entry(self, entry_id: str, fields: dict[str, str], source: str,
               payloads: bool) -> dict[str, Any]:
        from .consumer import entry_size

        visible = {k: v for k, v in fields.items() if not k.startswith("x-")}
        entry: dict[str, Any] = {"id": entry_id, "size": entry_size(fields),
                                 "fields": visible if payloads else None,
                                 "traceparent": fields.get("traceparent")}
        if source == "dlq":
            try:
                deliveries = int(fields.get("x-deliveries", "0"))
            except ValueError:
                deliveries = 0
            entry["dead_letter"] = {
                "original_id": fields.get("x-original-id"), "stream": fields.get("x-stream"),
                "group": fields.get("x-group"), "deliveries": deliveries,
                "error": fields.get("x-error") if payloads else None,
            }
        return entry

    def _add_status(self, key: str, group: str, messages: list[dict[str, Any]]) -> None:
        def order(entry_id: Any) -> tuple[int, int]:
            ms, _, seq = _text(entry_id).partition("-")
            return int(ms), int(seq or 0)

        try:
            groups: Any = self.client.xinfo_groups(key)
        except Exception:
            groups = []
        info = next((g for g in groups if _text(g.get("name")) == group), None)
        if info is None:
            return  # no such group yet: statuses are unknown
        last = order(info.get("last-delivered-id") or "0-0")
        newest, oldest = messages[0]["id"], messages[-1]["id"]
        pending: Any = self.client.xpending_range(key, group, oldest, newest, len(messages))
        by_id = {_text(p["message_id"]): p for p in pending}
        for m in messages:
            p = by_id.get(m["id"])
            if p is not None:
                m.update(status="pending", deliveries=p["times_delivered"],
                         consumer=_text(p["consumer"]), idle_ms=p["time_since_delivered"])
            else:
                m.update(status="acked" if order(m["id"]) <= last else "waiting",
                         deliveries=None, consumer=None, idle_ms=None)

    def stream_named(self, name: str) -> Stream:
        """The subscribed ``Stream`` called ``name`` (any group), else a plain ``Stream``."""
        for sub in self.broker.subscribers:
            if sub.stream.name == name:
                return sub.stream
        return Stream(name)

    def _stream(self, name: str, group: str) -> Stream:
        """The subscribed ``Stream`` (with its DLQ name) for ``name`` and ``group``, if any."""
        for sub in self.broker.subscribers:
            if sub.stream.name == name and sub.group == group:
                return sub.stream
        return Stream(name, group)

    def _ack(self, directive_id: Optional[str], status: str, error: Optional[str] = None,
             target: Optional[Mapping[str, Any]] = None,
             result: Optional[Mapping[str, Any]] = None) -> dict[str, Any]:
        sub = None
        if target and target.get("stream"):
            sub = next((s for s in self.broker.subscribers if s.stream.name == target["stream"]
                        and s.group == target["group"]), None)
        desired = self.desired(sub)
        lanes = sub.lanes if sub else sum(s.lanes for s in self.broker.subscribers)
        ack: dict[str, Any] = {
            "type": "DIRECTIVE_ACK",
            "directive_id": directive_id,
            "node_id": self.node_id,
            "status": status,
            "current_state": {"concurrency": desired.concurrency or lanes,
                              "state": desired.state},
        }
        if error is not None:
            ack["error"] = error
        if result is not None:
            ack["result"] = dict(result)
        return ack


def _mem_mb() -> Optional[float]:
    """Resident memory in MB where the platform tells us cheaply (Linux), else None."""
    try:
        with open("/proc/self/statm") as f:
            pages = int(f.read().split()[1])
        return round(pages * os.sysconf("SC_PAGE_SIZE") / 1_048_576, 1)
    except (OSError, ValueError, IndexError, AttributeError):
        return None
