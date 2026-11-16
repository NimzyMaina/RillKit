"""The leader-elected autoscaler (spec 3.1).

One replica at a time holds a Postgres advisory lock on a dedicated connection and is the leader;
the lock goes with the connection, so a crashed leader is replaced within one interval. Each tick
the leader:

- marks directives nobody could deliver as UNDELIVERABLE,
- records a sample per stream and group for the dashboard charts,
- evaluates the enabled scaling policies and sends ``SET_CONCURRENCY`` where needed.

Scaling uses two signals: the group's lag (every node reports it, so it is deduplicated, not
summed) and the observed arrival rate (finished messages per second plus lag growth). Workers
needed for the backlog are ``ceil(lag / target_lag_per_worker)``; workers needed for the rate are
``ceil(arrival / (peak_per_worker * 0.75))``, which keeps 25 % headroom. The larger of the two,
clamped to ``min_workers``/``max_workers``, is spread evenly over the nodes consuming the stream
as a per-node concurrency (at least 1, at most 256), and sent as one stream-scoped directive so
nodes that join later get it too. Using the rate as well as the lag is what stops a healthy,
fully provisioned stream from scaling down to its minimum (and oscillating) just because its lag
reached zero for a moment.
"""

import asyncio
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import psycopg

from .store import COUNTERS, TIMINGS, Row, now

if TYPE_CHECKING:
    from .app import Server

log = logging.getLogger("rillkit_server")

LEADER_LOCK = 7_340_002
MAX_PER_NODE = 256
PRUNE_EVERY_SEC = 600


@dataclass(frozen=True)
class Decision:
    per_node: int | None   # the concurrency to send, None to do nothing
    text: str                 # shown on the dashboard as last_decision
    scaled: bool = False


def decide(policy: Row, summary: Row | None, at: datetime, arrival: float = 0.0,
           peak_per_worker: float = 0.0) -> Decision:
    """What a policy wants for one stream and group, given its summary from the overview.

    ``arrival`` is the observed message rate (finished per second plus backlog growth) and
    ``peak_per_worker`` the highest per-worker rate seen for this policy. Callers that do not
    pass them (tests, older callers) keep the lag-only behaviour."""
    if summary is None or not summary.get("nodes"):
        return Decision(None, "no nodes are consuming this stream")
    nodes = summary["nodes"]
    lag = summary.get("lag") or 0
    by_lag = math.ceil(lag / policy["target_lag_per_worker"])
    by_rate = (math.ceil(arrival / (peak_per_worker * 0.75))
               if arrival > 0 and peak_per_worker > 0 else 0)
    wanted = max(by_lag, by_rate)
    wanted = max(policy["min_workers"], min(policy["max_workers"], wanted))
    per_node = max(1, min(MAX_PER_NODE, math.ceil(wanted / nodes)))
    current = summary.get("concurrency") or 0
    current_per_node = math.ceil(current / nodes) if current else 0
    if per_node == current_per_node:
        return Decision(None, f"lag {lag}, {arrival:.0f}/s: {current} workers is right")
    cooldown = policy["upscale_cooldown_sec"] if per_node > current_per_node \
        else policy["downscale_cooldown_sec"]
    last = policy.get("last_scaled_at")
    if last is not None and at - last < timedelta(seconds=cooldown):
        left = int(cooldown - (at - last).total_seconds())
        return Decision(None, f"lag {lag}, {arrival:.0f}/s: would scale to "
                              f"{per_node * nodes} workers, cooling down for {left} s")
    return Decision(per_node, f"lag {lag}, {arrival:.0f}/s -> {per_node * nodes} workers "
                              f"({per_node} per node, was {current})", scaled=True)


def add_deltas(tenant_id: str, overview: Row,
               previous: dict[tuple[str, str, str, str], dict[str, float]]) -> None:
    """Give each stream summary a ``delta``: how much each counter grew since the previous tick,
    summed over its live nodes. A counter lower than before means the worker restarted, so its
    whole value is new. The first sight of a node counts as zero (no baseline yet)."""
    numbers = COUNTERS + TIMINGS
    for summary in overview["streams"]:
        summary["delta"] = dict.fromkeys(numbers, 0)
    by_key = {(s["stream"], s["group"]): s for s in overview["streams"]}
    for node in overview["nodes"]:
        if not node["connected"]:
            continue
        for s in node["streams"]:
            key = (tenant_id, node["node_id"], s["stream"], s["group"])
            # Workers that predate a counter (timings, for instance) report only some of them,
            # so each reported number grows on its own.
            current: dict[str, float] = {}
            for k in numbers:
                value = s.get(f"{k}_total")
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    current[k] = float(value)
            if not any(k in current for k in COUNTERS):
                continue  # a worker that does not report counters
            before = previous.get(key)
            previous[key] = current
            summary = by_key.get((s["stream"], s["group"]))
            if before is None or summary is None:
                continue
            for k, value in current.items():
                if k not in before:
                    continue
                grown = value - before[k]
                summary["delta"][k] += grown if grown >= 0 else value


def rate_state(rows: list[Row], target_lag: int) -> dict[str, Any] | None:
    """Seed ``arrival`` and per-worker capacity from the most recent sample rows.

    The rows come from the samples table, oldest first. Capacity is only learned from rows
    where the stream was keeping up (``lag <= target_lag``), the same rule as the live
    estimator. Returns None when there is nothing to learn from."""
    if not rows:
        return None
    last = rows[-1]
    lanes = max(1, int(last.get("concurrency") or 0))
    lag = int(last.get("lag") or 0)
    ack = float(last.get("ack_rate_sec") or 0)
    if len(rows) > 1:
        previous = rows[-2]
        dt = max(0.1, (last["ts"] - previous["ts"]).total_seconds())
        arrival = max(0.0, ack + (lag - int(previous.get("lag") or 0)) / dt)
    else:
        arrival = ack
    if lag <= target_lag:
        capacity = ack / lanes
    else:
        capacity = 0.0
    return {"at": last["ts"], "lag": lag, "arrival": arrival, "peak": capacity}


class Autoscaler:
    def __init__(self, server: "Server") -> None:
        self.server = server
        self.leader = False
        self._conn: psycopg.AsyncConnection[Any] | None = None
        self._last_prune = 0.0
        # the workers' counters at the previous tick, to turn totals into throughput
        self._counters: dict[tuple[str, str, str, str], dict[str, float]] = {}
        # per policy: the last lag, smoothed arrival rate and peak per-worker rate
        self._rates: dict[tuple[str, str, str], dict[str, Any]] = {}

    async def run(self, stop: asyncio.Event) -> None:
        interval = self.server.settings.autoscale_interval_sec
        while not stop.is_set():
            try:
                if await self._lead():
                    await self.tick()
            except Exception:
                log.exception("autoscaler tick failed")
                await self._resign()
            try:
                await asyncio.wait_for(stop.wait(), interval)
            except TimeoutError:
                pass
        await self._resign()

    async def _lead(self) -> bool:
        """Whether this replica is the leader, taking the lock if it is free."""
        if self._conn is not None:
            try:
                await self._conn.execute("select 1")
                return True
            except psycopg.Error:
                await self._resign()
        conn = await psycopg.AsyncConnection.connect(self.server.settings.database_url,
                                                     autocommit=True)
        cur = await conn.execute("select pg_try_advisory_lock(%s)", (LEADER_LOCK,))
        row = await cur.fetchone()
        if row and row[0]:
            self._conn, self.leader = conn, True
            log.info("replica %s is the autoscaler leader", self.server.settings.replica_id)
            return True
        await conn.close()
        return False

    async def _resign(self) -> None:
        if self._conn is not None:
            try:
                await self._conn.close()  # closing the session releases the lock
            except Exception:
                pass
        self._conn, self.leader = None, False

    async def tick(self) -> None:
        store = self.server.store
        for row in await store.expire_directives(self.server.settings.directive_timeout_sec):
            await self.server.notify("directive", tenant=row["tenant_id"], id=row["id"])
        policies: dict[str, list[Row]] = {}
        for policy in await store.all_enabled_policies():
            policies.setdefault(policy["tenant_id"], []).append(policy)
        for tenant in await store.list_tenants():
            overview = await store.overview(tenant)
            add_deltas(tenant["id"], overview, self._counters)
            await store.add_samples(tenant["id"], overview["streams"])
            summaries = {(s["stream"], s["group"]): s for s in overview["streams"]}
            for policy in policies.get(tenant["id"], []):
                await self._apply(policy, summaries.get((policy["stream"], policy["group"])))
        if time.monotonic() - self._last_prune > PRUNE_EVERY_SEC:
            self._last_prune = time.monotonic()
            await store.prune()

    async def _apply(self, policy: Row, summary: Row | None) -> None:
        at = now()
        key = (policy["tenant_id"], policy["stream"], policy["group"])
        if key not in self._rates and summary is not None:
            # A new leader has no in-memory history. The samples table already has the last
            # ticks, so start from those instead of relearning from zero.
            rows = await self.server.store.recent_samples(*key, 2)
            seeded = rate_state(rows, policy["target_lag_per_worker"])
            if seeded is not None:
                self._rates[key] = seeded
        arrival = peak = 0.0
        if summary is not None:
            lanes = max(1, int(summary.get("concurrency") or 0))
            ack = float(summary.get("ack_rate_sec") or 0)
            lag = int(summary.get("lag") or 0)
            target = policy["target_lag_per_worker"]
            previous = self._rates.get(key)
            if previous is None:
                arrival = ack
                peak = ack / lanes if lag <= target else 0.0
            else:
                dt = max(0.1, (at - previous["at"]).total_seconds())
                # The stream's arrival rate: what was finished, plus the backlog that grew.
                raw = max(0.0, ack + (lag - previous["lag"]) / dt)
                arrival = 0.5 * raw + 0.5 * previous["arrival"]  # a step is noisy; smooth it
                # A worker's real capacity only shows while the stream is keeping up. Learning it
                # during a backlog (when a lane is starved or retrying) overestimates capacity and
                # makes the controller undershoot on the next decision.
                peak = max(previous["peak"], ack / lanes) if lag <= target else previous["peak"]
            self._rates[key] = {"at": at, "lag": lag, "arrival": arrival, "peak": peak}
        decision = decide(policy, summary, at, arrival, peak)
        if decision.per_node is not None:
            body = {"cmd": "SET_CONCURRENCY", "scope": "stream",
                    "target": {"stream": policy["stream"], "group": policy["group"]},
                    "params": {"concurrency": decision.per_node},
                    "reason": f"AUTOSCALE: {decision.text}"}
            await self.server.create_directive(policy["tenant_id"], body, source="autoscaler")
            log.info("autoscaler, tenant %s, %s/%s: %s", policy["tenant_id"], policy["stream"],
                     policy["group"], decision.text)
        if decision.text != policy.get("last_decision") or decision.scaled:
            await self.server.store.policy_decision(policy, decision.text, decision.scaled)
