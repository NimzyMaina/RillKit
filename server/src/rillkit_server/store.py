"""Everything the server keeps in Postgres, and the views built from it (see API.md)."""

import hashlib
import secrets
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

STATES = ("RUNNING", "PAUSED", "MAINTENANCE", "DRAINING")
_RANK = {state: rank for rank, state in enumerate(STATES)}
RECENT = timedelta(minutes=10)  # nodes seen this recently are listed (connected or not)
COUNTERS = ("succeeded", "failed", "dead_lettered", "bytes")  # telemetry "<name>_total"
TIMINGS = ("attempts", "wait_ms", "work_ms")  # telemetry "<name>_total", per stream
NO_CAPABILITIES = {"inspect_messages": False, "capture_logs": False, "trace_records": False}

Row = dict[str, Any]


def now() -> datetime:
    return datetime.now(UTC)


def iso(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def most_restrictive(states: Iterable[str | None]) -> str:
    best = "RUNNING"
    for state in states:
        if state in _RANK and _RANK[state] > _RANK[best]:
            best = state
    return best


def hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


class Store:
    def __init__(self, pool: AsyncConnectionPool, node_ttl_sec: float = 30.0) -> None:
        self.pool = pool
        self.node_ttl = timedelta(seconds=node_ttl_sec)

    async def _all(self, sql: str, args: Any = ()) -> list[Row]:
        async with self.pool.connection() as conn:
            cur = await conn.execute(sql, args)
            if cur.description is None:  # a statement without RETURNING
                return []
            # rows are dicts: the pool's connections use row_factory=dict_row
            return cast(list[Row], await cur.fetchall())

    async def _one(self, sql: str, args: Any = ()) -> Row | None:
        rows = await self._all(sql, args)
        return rows[0] if rows else None

    # tenants

    def tenant_view(self, row: Row) -> Row:
        return {"id": row["id"], "name": row["name"], "enforce_keys": row["enforce_keys"],
                "created_at": iso(row["created_at"]), "nodes_online": row.get("nodes_online", 0)}

    async def list_tenants(self) -> list[Row]:
        rows = await self._all(
            """select t.*, (select count(*) from nodes n where n.tenant_id = t.id
                            and n.connected and n.last_seen > %s) as nodes_online
               from tenants t order by t.created_at""",
            (now() - self.node_ttl,),
        )
        return [self.tenant_view(r) for r in rows]

    async def get_tenant(self, tenant_id: str) -> Row | None:
        row = await self._one(
            """select t.*, (select count(*) from nodes n where n.tenant_id = t.id
                            and n.connected and n.last_seen > %s) as nodes_online
               from tenants t where t.id = %s""",
            (now() - self.node_ttl, tenant_id),
        )
        return None if row is None else self.tenant_view(row)

    async def create_tenant(self, tenant_id: str, name: str, enforce_keys: bool) -> Row | None:
        row = await self._one(
            "insert into tenants(id, name, enforce_keys) values (%s, %s, %s) "
            "on conflict do nothing returning *",
            (tenant_id, name, enforce_keys),
        )
        return None if row is None else self.tenant_view(row)

    # API keys

    def key_view(self, row: Row) -> Row:
        return {"id": row["id"], "label": row["label"], "prefix": row["prefix"],
                "created_at": iso(row["created_at"]), "revoked_at": iso(row["revoked_at"])}

    async def create_api_key(
        self, tenant_id: str, label: str | None, key: str | None = None
    ) -> tuple[Row, str]:
        key = key or f"rk_{secrets.token_urlsafe(32)}"
        row = await self._one(
            "insert into api_keys(id, tenant_id, label, prefix, hash) values (%s, %s, %s, %s, %s) "
            "returning *",
            (f"key_{uuid.uuid4().hex[:12]}", tenant_id, label, key[:10], hash_key(key)),
        )
        assert row is not None
        return self.key_view(row), key

    async def ensure_api_key(self, tenant_id: str, key: str, label: str) -> None:
        await self._all(
            "insert into api_keys(id, tenant_id, label, prefix, hash) values (%s, %s, %s, %s, %s) "
            "on conflict (hash) do nothing",
            (f"key_{uuid.uuid4().hex[:12]}", tenant_id, label, key[:10], hash_key(key)),
        )

    async def list_api_keys(self, tenant_id: str) -> list[Row]:
        rows = await self._all(
            "select * from api_keys where tenant_id = %s order by created_at", (tenant_id,)
        )
        return [self.key_view(r) for r in rows]

    async def revoke_api_key(self, tenant_id: str, key_id: str) -> Row | None:
        row = await self._one(
            "update api_keys set revoked_at = coalesce(revoked_at, now()) "
            "where tenant_id = %s and id = %s returning *",
            (tenant_id, key_id),
        )
        return None if row is None else self.key_view(row)

    async def authenticate(self, key: str) -> Row | None:
        """The live API key ``key`` with its tenant's settings, or None."""
        return await self._one(
            "select k.id as key_id, k.tenant_id, t.enforce_keys from api_keys k "
            "join tenants t on t.id = k.tenant_id where k.hash = %s and k.revoked_at is null",
            (hash_key(key),),
        )

    # nodes

    async def node_connected(self, tenant_id: str, node_id: str, replica_id: str, key_id: str,
                             keys: Row, sdk: Row, capabilities: Row) -> None:
        await self._all(
            """insert into nodes(tenant_id, node_id, replica_id, key_id, connected, keys, sdk,
                                 capabilities, connected_at, last_seen)
               values (%s, %s, %s, %s, true, %s, %s, %s, now(), now())
               on conflict (tenant_id, node_id) do update set replica_id = excluded.replica_id,
                 key_id = excluded.key_id, connected = true, keys = excluded.keys,
                 sdk = excluded.sdk, capabilities = excluded.capabilities,
                 connected_at = now(), last_seen = now()""",
            (tenant_id, node_id, replica_id, key_id, Jsonb(keys), Jsonb(sdk), Jsonb(capabilities)),
        )

    async def node_telemetry(self, tenant_id: str, node_id: str, telemetry: Row) -> None:
        await self._all(
            "update nodes set telemetry = %s, last_seen = now() "
            "where tenant_id = %s and node_id = %s",
            (Jsonb(telemetry), tenant_id, node_id),
        )

    async def node_disconnected(self, tenant_id: str, node_id: str, replica_id: str) -> None:
        await self._all(
            "update nodes set connected = false where tenant_id = %s and node_id = %s "
            "and replica_id = %s",
            (tenant_id, node_id, replica_id),
        )

    async def recent_nodes(self, tenant_id: str) -> list[Row]:
        return await self._all(
            "select * from nodes where tenant_id = %s and last_seen > %s "
            "order by connected desc, node_id",
            (tenant_id, now() - RECENT),
        )

    def is_live(self, node: Row) -> bool:
        return bool(node["connected"]) and node["last_seen"] > now() - self.node_ttl

    async def live_nodes(self, tenant_id: str) -> list[Row]:
        return [n for n in await self.recent_nodes(tenant_id) if self.is_live(n)]

    # directives

    def directive_view(self, row: Row) -> Row:
        return {"id": row["id"], "cmd": row["cmd"], "scope": row["scope"],
                "target": row["target"], "params": row["params"], "reason": row["reason"],
                "source": row["source"], "status": row["status"], "sent_to": row["sent_to"],
                "ack": row["ack"], "created_at": iso(row["created_at"]),
                "updated_at": iso(row["updated_at"])}

    async def create_directive(self, tenant_id: str, body: Row, source: str) -> Row:
        row = await self._one(
            "insert into directives(id, tenant_id, cmd, scope, target, params, reason, source) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s) returning *",
            (f"dir_{uuid.uuid4().hex[:16]}", tenant_id, body["cmd"], body.get("scope", "global"),
             Jsonb(body.get("target") or {}), Jsonb(body.get("params") or {}),
             body.get("reason"), source),
        )
        assert row is not None
        return row

    async def get_directive(self, directive_id: str) -> Row | None:
        return await self._one("select * from directives where id = %s", (directive_id,))

    async def list_directives(self, tenant_id: str, limit: int, offset: int = 0,
                              source: str | None = None) -> Row:
        """A page of the tenant's directives, newest first (API.md, Page<Directive>)."""
        where, args = "tenant_id = %s", [tenant_id]
        if source is not None:
            where, args = where + " and source = %s", [*args, source]
        total = await self._one(f"select count(*) as n from directives where {where}", args)
        rows = await self._all(
            f"select * from directives where {where} order by created_at desc, id desc "
            "limit %s offset %s", (*args, limit, offset),
        )
        return {"items": [self.directive_view(r) for r in rows],
                "total": total["n"] if total else 0, "limit": limit, "offset": offset}

    async def pending_directives(self, tenant_id: str) -> list[Row]:
        return await self._all(
            "select * from directives where tenant_id = %s and status = 'PENDING' "
            "order by created_at", (tenant_id,),
        )

    async def pending_directive_count(self, tenant_id: str) -> int:
        row = await self._one(
            "select count(*) as n from directives where tenant_id = %s "
            "and status in ('PENDING', 'SENT')", (tenant_id,),
        )
        return int(row["n"]) if row is not None else 0

    async def claim_directive(self, directive_id: str, replica_id: str,
                              node_id: str) -> Row | None:
        """Mark a pending directive as sent by this replica to ``node_id``. None if another
        replica claimed it first (or it is no longer pending)."""
        return await self._one(
            "update directives set status = 'SENT', delivered_by = %s, sent_to = %s, "
            "updated_at = now() where id = %s and status = 'PENDING' returning *",
            (replica_id, node_id, directive_id),
        )

    async def release_directive(self, directive_id: str) -> None:
        await self._all(
            "update directives set status = 'PENDING', delivered_by = null, sent_to = null, "
            "updated_at = now() where id = %s and status = 'SENT'", (directive_id,),
        )

    async def release_unacked(self, tenant_id: str, node_id: str) -> list[str]:
        """Directives sent to a node that disconnected before acknowledging: pending again."""
        rows = await self._all(
            "update directives set status = 'PENDING', delivered_by = null, sent_to = null, "
            "updated_at = now() where tenant_id = %s and sent_to = %s and status = 'SENT' "
            "returning id", (tenant_id, node_id),
        )
        return [r["id"] for r in rows]

    async def ack_directive(self, directive_id: str, ack: Row) -> Row | None:
        status = str(ack.get("status", "FAILED"))
        return await self._one(
            "update directives set status = %s, ack = %s, updated_at = now() "
            "where id = %s returning *",
            (status, Jsonb(ack), directive_id),
        )

    async def expire_directives(self, timeout_sec: float) -> list[Row]:
        return await self._all(
            "update directives set status = 'UNDELIVERABLE', updated_at = now() "
            "where status in ('PENDING', 'SENT') and created_at < %s returning *",
            (now() - timedelta(seconds=timeout_sec),),
        )

    # policies

    def policy_view(self, row: Row) -> Row:
        return policy_view(row)

    async def list_policies(self, tenant_id: str) -> list[Row]:
        return await self._all(
            'select * from policies where tenant_id = %s order by stream, "group"', (tenant_id,)
        )

    async def all_enabled_policies(self) -> list[Row]:
        return await self._all("select * from policies where enabled")

    async def put_policy(self, tenant_id: str, stream: str, group: str, p: Row) -> Row:
        row = await self._one(
            """insert into policies(tenant_id, stream, "group", enabled, min_workers, max_workers,
                 target_lag_per_worker, upscale_cooldown_sec, downscale_cooldown_sec)
               values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               on conflict (tenant_id, stream, "group") do update set enabled = excluded.enabled,
                 min_workers = excluded.min_workers, max_workers = excluded.max_workers,
                 target_lag_per_worker = excluded.target_lag_per_worker,
                 upscale_cooldown_sec = excluded.upscale_cooldown_sec,
                 downscale_cooldown_sec = excluded.downscale_cooldown_sec
               returning *""",
            (tenant_id, stream, group, p["enabled"], p["min_workers"], p["max_workers"],
             p["target_lag_per_worker"], p["upscale_cooldown_sec"], p["downscale_cooldown_sec"]),
        )
        assert row is not None
        return self.policy_view(row)

    async def delete_policy(self, tenant_id: str, stream: str, group: str) -> bool:
        rows = await self._all(
            'delete from policies where tenant_id = %s and stream = %s and "group" = %s '
            "returning stream", (tenant_id, stream, group),
        )
        return bool(rows)

    async def policy_decision(self, policy: Row, decision: str, scaled: bool) -> None:
        await self._all(
            "update policies set last_decision = %s, "
            "last_scaled_at = case when %s then now() else last_scaled_at end "
            'where tenant_id = %s and stream = %s and "group" = %s',
            (decision, scaled, policy["tenant_id"], policy["stream"], policy["group"]),
        )

    # streams created from the dashboard

    async def register_stream(self, tenant_id: str, stream: str, group: str) -> None:
        await self._all(
            'insert into streams(tenant_id, stream, "group") values (%s, %s, %s) '
            "on conflict do nothing", (tenant_id, stream, group),
        )

    async def registered_streams(self, tenant_id: str) -> list[Row]:
        return await self._all(
            'select * from streams where tenant_id = %s order by stream, "group"', (tenant_id,)
        )

    # samples

    async def add_samples(self, tenant_id: str, streams: list[Row]) -> None:
        """One row per stream and group: the summary's gauges plus ``delta`` counters (the
        throughput since the previous tick, filled in by the autoscaler)."""
        rows = [s for s in streams if s["nodes"]]
        if not rows:
            return
        async with self.pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    'insert into samples(tenant_id, stream, "group", lag, pending, concurrency, '
                    "ack_rate_sec, succeeded, failed, dead_lettered, bytes, dlq_length, "
                    "attempts, wait_ms, work_ms) "
                    "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    [(tenant_id, s["stream"], s["group"], s["lag"], s["pending"],
                      s["concurrency"], s["ack_rate_sec"],
                      *(s.get("delta", {}).get(k, 0) for k in COUNTERS), s["dlq_length"],
                      int(s.get("delta", {}).get("attempts", 0)),
                      *(s.get("delta", {}).get(k, 0) for k in TIMINGS[1:]))
                     for s in rows],
                )

    async def metrics(self, tenant_id: str, window_sec: int) -> Row:
        """Throughput and health over the last ``window_sec`` and the window before it, plus a
        series of ~60 buckets for charts (API.md, Metrics)."""
        rows = await self._all(
            "select ts, lag, pending, concurrency, succeeded, failed, dead_lettered, bytes, "
            "dlq_length, attempts, wait_ms, work_ms from samples where tenant_id = %s and ts > %s "
            "order by ts",
            (tenant_id, now() - timedelta(seconds=2 * window_sec)),
        )
        return build_metrics(rows, window_sec, now())

    async def samples(self, tenant_id: str, stream: str, group: str,
                      window_sec: int) -> list[Row]:
        rows = await self._all(
            'select * from samples where tenant_id = %s and stream = %s and "group" = %s '
            "and ts > %s order by ts",
            (tenant_id, stream, group, now() - timedelta(seconds=window_sec)),
        )
        return [{"ts": iso(r["ts"]), "lag": r["lag"], "pending": r["pending"],
                 "concurrency": r["concurrency"], "ack_rate_sec": r["ack_rate_sec"],
                 "attempts": r["attempts"], "wait_ms": _average(r["wait_ms"], r["attempts"]),
                 "work_ms": _average(r["work_ms"], r["attempts"])} for r in rows]

    async def recent_samples(self, tenant_id: str, stream: str, group: str,
                             count: int = 2) -> list[Row]:
        """The last raw sample rows for a stream, oldest first (autoscaler seeding)."""
        rows = await self._all(
            'select ts, lag, ack_rate_sec, concurrency from samples where tenant_id = %s '
            'and stream = %s and "group" = %s order by ts desc limit %s',
            (tenant_id, stream, group, count),
        )
        rows.reverse()
        return rows

    async def prune(self) -> None:
        await self._all("delete from samples where ts < %s", (now() - timedelta(days=1),))
        await self._all("delete from relay_responses where created_at < %s",
                        (now() - timedelta(minutes=5),))
        await self._all("delete from event_tickets where expires_at < %s", (now(),))

    # tickets for the live events stream

    async def create_ticket(self, tenant_id: str, ttl_sec: int) -> str:
        ticket = f"evt_{secrets.token_urlsafe(32)}"
        await self._all(
            "insert into event_tickets(hash, tenant_id, expires_at) values (%s, %s, %s)",
            (hash_key(ticket), tenant_id, now() + timedelta(seconds=ttl_sec)),
        )
        return ticket

    async def redeem_ticket(self, ticket: str) -> str | None:
        """The tenant a live, unused ticket was issued for; the ticket is used up either way."""
        row = await self._one(
            "delete from event_tickets where hash = %s returning tenant_id, expires_at",
            (hash_key(ticket),),
        )
        if row is None or row["expires_at"] <= now():
            return None
        return str(row["tenant_id"])

    # answers to Relay requests (logs, messages) relayed between replicas

    async def put_response(self, request_id: str, payload: Row) -> None:
        await self._all(
            "insert into relay_responses(request_id, payload) values (%s, %s) "
            "on conflict do nothing", (request_id, Jsonb(payload)),
        )

    async def take_response(self, request_id: str) -> Row | None:
        row = await self._one(
            "delete from relay_responses where request_id = %s returning payload", (request_id,)
        )
        return None if row is None else row["payload"]

    # the overview

    async def overview(self, tenant: Row) -> Row:
        nodes = await self.recent_nodes(tenant["id"])
        policies = await self.list_policies(tenant["id"])
        registered = await self.registered_streams(tenant["id"])
        return build_overview(tenant, nodes, policies, self.is_live, registered)


def _capabilities(reported: Any) -> Row:
    """What a node allows, from its HELLO; anything missing or unknown counts as not allowed."""
    allowed = dict(NO_CAPABILITIES)
    if isinstance(reported, dict):
        for name in allowed:
            allowed[name] = reported.get(name) is True
    return allowed


def _average(total: float | None, count: int | None) -> float | None:
    """Milliseconds per attempt, or None when nothing ran."""
    if not count:
        return None
    return round((total or 0.0) / count, 3)


def _empty_summary(stream: str, group: str) -> Row:
    return {"stream": stream, "group": group, "stream_key": None, "dlq_key": None,
            "lag": None, "pending": None, "ack_rate_sec": 0.0, "concurrency": 0, "nodes": 0,
            "states": [], "dlq_length": None, "capabilities": dict(NO_CAPABILITIES),
            "created_at": None}


def _totals(rows: list[Row], seconds: float) -> Row:
    """Throughput totals over sample rows (one per stream per tick)."""
    sums = {k: sum(r[k] or 0 for r in rows) for k in COUNTERS}
    ran = sums["succeeded"] + sums["failed"]
    ticks: dict[Any, int] = {}
    for r in rows:
        ticks[r["ts"]] = ticks.get(r["ts"], 0) + (r["lag"] or 0)
    last_ts = max((r["ts"] for r in rows), default=None)
    return {
        **sums,
        "success_rate": round(sums["succeeded"] / ran, 4) if ran else None,
        "per_sec": round(sums["succeeded"] / seconds, 3),
        "bytes_per_sec": round(sums["bytes"] / seconds, 1),
        "lag": round(sum(ticks.values()) / len(ticks), 1) if ticks else None,
        "dlq_length": sum(r["dlq_length"] or 0 for r in rows if r["ts"] == last_ts)
        if last_ts is not None else None,
    }


def build_metrics(rows: list[Row], window_sec: int, at: datetime, buckets: int = 60) -> Row:
    """Metrics from sample rows covering two windows (API.md, Metrics)."""
    start, middle = at - timedelta(seconds=2 * window_sec), at - timedelta(seconds=window_sec)
    current = [r for r in rows if r["ts"] > middle]
    previous = [r for r in rows if start < r["ts"] <= middle]
    width = window_sec / buckets
    series = []
    for i in range(buckets):
        lo = middle + timedelta(seconds=i * width)
        hi = lo + timedelta(seconds=width)
        inside = [r for r in current if lo < r["ts"] <= hi]
        by_tick: dict[Any, list[Row]] = {}
        for r in inside:
            by_tick.setdefault(r["ts"], []).append(r)
        last = by_tick[max(by_tick)] if by_tick else []
        attempts = sum(r["attempts"] or 0 for r in inside)
        series.append({
            "ts": iso(hi),
            **{k: sum(r[k] or 0 for r in inside) for k in COUNTERS},
            "attempts": attempts,
            "wait_ms": _average(sum(r["wait_ms"] or 0.0 for r in inside), attempts),
            "work_ms": _average(sum(r["work_ms"] or 0.0 for r in inside), attempts),
            # gauges: the last tick in the bucket, summed over streams
            "lag": sum(r["lag"] or 0 for r in last),
            "pending": sum(r["pending"] or 0 for r in last),
            "concurrency": sum(r["concurrency"] or 0 for r in last),
            "dlq_length": sum(r["dlq_length"] or 0 for r in last),
            "ticks": len(by_tick),
        })
    return {"window_sec": window_sec, "series": series,
            "current": _totals(current, window_sec), "previous": _totals(previous, window_sec)}


def policy_view(row: Row) -> Row:
    return {k: row[k] for k in (
        "stream", "group", "enabled", "min_workers", "max_workers", "target_lag_per_worker",
        "upscale_cooldown_sec", "downscale_cooldown_sec", "last_decision",
    )} | {"last_scaled_at": iso(row["last_scaled_at"])}


def _node_view(row: Row, live: bool) -> Row:
    t = row["telemetry"] or {}
    workers = t.get("workers") or {}
    return {
        "node_id": row["node_id"],
        "connected": live,
        "state": workers.get("state") or "RUNNING",
        "active": workers.get("active") or 0,
        "in_flight": workers.get("in_flight") or 0,
        "poll_ms": workers.get("poll_ms"),
        "batch_size": workers.get("batch_size"),
        "cpu_pct": workers.get("cpu_pct"),
        "mem_mb": workers.get("mem_mb"),
        "sdk": row["sdk"],
        "capabilities": _capabilities(row.get("capabilities")),
        "keys": row["keys"] or t.get("keys") or {"layout": "custom", "prefix": None,
                                                 "tenant": None},
        "connected_at": iso(row["connected_at"]),
        "last_seen": iso(row["last_seen"]),
        "streams": [
            {"stream": s.get("stream"), "group": s.get("group"),
             "stream_key": s.get("stream_key"), "dlq_key": s.get("dlq_key"),
             "lag": s.get("lag"), "pending": s.get("pending"),
             "ack_rate_sec": s.get("ack_rate_sec") or 0.0,
             "concurrency": s.get("concurrency"), "state": s.get("state"),
             **{f"{k}_total": s.get(f"{k}_total") for k in COUNTERS + TIMINGS},
             "dlq_length": s.get("dlq_length")}
            for s in t.get("streams") or []
        ],
    }


def build_overview(tenant: Row, node_rows: list[Row], policy_rows: list[Row],
                   is_live: Any, registered: list[Row] | None = None) -> Row:
    """The dashboard's view of a tenant from its nodes' latest telemetry (API.md, Overview)."""
    nodes = [_node_view(row, is_live(row)) for row in node_rows]
    live = [n for n in nodes if n["connected"]]
    policies = {(p["stream"], p["group"]): p for p in policy_rows}
    streams: dict[tuple[str, str], Row] = {}
    for node in live:
        for s in node["streams"]:
            key = (s["stream"], s["group"])
            agg = streams.setdefault(key, _empty_summary(s["stream"], s["group"]))
            agg["stream_key"], agg["dlq_key"] = s["stream_key"], s["dlq_key"]
            for name, allowed in node["capabilities"].items():
                agg["capabilities"][name] = agg["capabilities"][name] or allowed
            # lag, pending and the DLQ belong to the group: every node reports the same value
            for field in ("lag", "pending", "dlq_length"):
                if s[field] is not None:
                    agg[field] = max(agg[field] or 0, s[field])
            agg["ack_rate_sec"] += s["ack_rate_sec"] or 0.0
            agg["concurrency"] += s["concurrency"] or 0
            agg["nodes"] += 1
            agg["states"].append(s["state"] or node["state"])
    created = {(r["stream"], r["group"]): r["created_at"] for r in registered or []}
    for key in created:
        streams.setdefault(key, _empty_summary(*key))
    summaries = []
    for key, agg in sorted(streams.items()):
        agg["created_at"] = iso(created.get(key))
        states = agg.pop("states")
        agg["state"] = most_restrictive(states)
        agg["ack_rate_sec"] = round(agg["ack_rate_sec"], 2)
        policy = policies.get(key)
        agg["policy"] = None if policy is None else policy_view(policy)
        summaries.append(agg)
    totals = {
        "nodes_online": len(live),
        "active_workers": sum(n["active"] for n in live),
        "in_flight": sum(n["in_flight"] for n in live),
        "lag": sum(s["lag"] or 0 for s in summaries),
        "pending": sum(s["pending"] or 0 for s in summaries),
        "ack_rate_sec": round(sum(s["ack_rate_sec"] for s in summaries), 2),
    }
    return {
        "tenant": tenant | {"nodes_online": len(live)},
        "totals": totals,
        "state": most_restrictive(n["state"] for n in live),
        "nodes": nodes,
        "streams": summaries,
    }
