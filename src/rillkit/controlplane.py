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
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Union

from .clients import WATCH_ERRORS, is_cluster, node_client_for, same_slot
from .config import Control, default_node_id
from .consumer import Subscriber, _text
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
    "EXECUTE_REPLAY",
)
DEDUP_TTL = 60
MAX_CONCURRENCY = 256
MAX_BLOCK_MS = 600_000
MAX_BATCH_SIZE = 10_000
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


def _positive(value: Any, name: str, top: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as e:
        raise DirectiveError(f"{name} must be an integer") from e
    if isinstance(value, bool) or not 1 <= number <= top:
        raise DirectiveError(f"{name} must be between 1 and {top}")
    return number


def _sub_suffix(stream: str, group: str) -> str:
    return f":{_escape_plain(stream)}:{group}"


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

    def _layers(self, sub: Optional[Subscriber]) -> list[dict[str, str]]:
        """The state hashes for ``sub`` (or the node as a whole), least specific first."""
        node = self._hashes.get(self.keys.state_node(self.node_id), {})
        layers = [self._hashes.get(self.keys.state_global(), {})]
        if sub is None:
            return [*layers, node]
        layers.append(self._hashes.get(self.keys.state_stream(sub.stream, sub.group), {}))
        layers.append(node)
        suffix = _sub_suffix(sub.stream.name, sub.group)
        layers.append({
            name[: -len(suffix)]: value for name, value in node.items() if name.endswith(suffix)
        })
        return layers

    def desired(self, sub: Optional[Subscriber] = None) -> Desired:
        """What ``sub`` (or, without one, the node) should do now, from the last hydrate."""
        with self._cond:
            layers = self._layers(sub)
        state = "RUNNING"
        values: dict[str, Optional[int]] = dict.fromkeys(TUNABLES)
        for layer in layers:
            wanted = layer.get("state", "RUNNING").upper()
            if wanted in _RANK and _RANK[wanted] > _RANK[state]:
                state = wanted
            for name, top in TUNABLES.items():
                if name in layer:
                    try:
                        values[name] = _positive(layer[name], name, top)
                    except DirectiveError:
                        log.warning("ignoring invalid %s=%r in the desired state", name,
                                    layer[name])
        return Desired(state, values["concurrency"], values["block_ms"], values["batch_size"])

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

    def stop(self, timeout: Optional[float] = None) -> None:
        self._running = False
        self._stopping.set()
        self.wake()
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
            if not isinstance(directive, Mapping):
                raise DirectiveError("a directive must be a JSON object")
            directive_id = directive.get("directive_id")
            if not directive_id or not isinstance(directive_id, str):
                raise DirectiveError("directive_id is required")
            plan = self._plan(directive)
        except (DirectiveError, ValueError) as e:
            return self._ack(directive_id, "REJECTED", error=str(e))
        try:
            if plan["cmd"] == "EXECUTE_REPLAY":
                if not self.client.set(self.keys.dedup(directive_id), "1", nx=True,
                                       ex=DEDUP_TTL):
                    return self._ack(directive_id, "DUPLICATE")
                result = self._replay(plan)
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

    def _plan(self, directive: Mapping[str, Any]) -> dict[str, Any]:
        if directive.get("type", "DIRECTIVE") != "DIRECTIVE":
            raise DirectiveError("type must be DIRECTIVE")
        cmd = str(directive.get("cmd", "")).upper()
        if cmd not in COMMANDS:
            raise DirectiveError(f"unknown cmd {cmd!r}, expected one of {COMMANDS}")
        scope = str(directive.get("scope", "global")).lower()
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

        if cmd == "EXECUTE_REPLAY":
            if not stream:
                raise DirectiveError("EXECUTE_REPLAY needs target.stream and target.group")
            return {"cmd": cmd, "stream": stream, "group": group, "params": params}

        fields: dict[str, str] = {}
        if cmd in ("SET_STATE", "MAINTENANCE_ON", "MAINTENANCE_OFF"):
            state = {"MAINTENANCE_ON": "MAINTENANCE", "MAINTENANCE_OFF": "RUNNING"}.get(
                cmd, str(params.get("state", "")).upper())
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
            key, channel = self.keys.state_global(), self.keys.broadcast_channel()
        elif scope == "stream":
            if not stream:
                raise DirectiveError("scope 'stream' needs target.stream and target.group")
            key = self.keys.state_stream(Stream(stream), group)
            channel = self.keys.broadcast_channel()
        else:
            if not node_id:
                raise DirectiveError("scope 'node' needs target.node_id")
            key, channel = self.keys.state_node(node_id), self.keys.node_channel(node_id)
            if stream and group:
                suffix = _sub_suffix(stream, group)
                fields = {f"{name}{suffix}": value for name, value in fields.items()}
        return {"cmd": cmd, "key": key, "channel": channel, "fields": fields,
                "target": {"stream": stream, "group": group}}

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

    def _replay(self, plan: Mapping[str, Any]) -> dict[str, Any]:
        stream = self._stream(plan["stream"], plan["group"])
        params = plan["params"]
        if params.get("set_id") is not None:
            self.client.xgroup_setid(self.keys.stream(stream), plan["group"], str(params["set_id"]))
            return {"set_id": str(params["set_id"])}
        count = _positive(params.get("count", 100), "count", 100_000)
        return {"replayed": self.broker.replay_dlq(stream, count=count)}

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
