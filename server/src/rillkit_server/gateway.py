"""The Relay gateway (spec 5.4): workers' WebSocket sessions, and delivering directives to them.

Each replica holds some of the Relay connections. Directives are rows in Postgres; every replica
hears about a new one through NOTIFY, and the one holding a suitable connection claims it with a
conditional UPDATE (so exactly one replica sends it) and sends it. Worker-side dedup makes a
second delivery harmless anyway. Acks, telemetry and connection state go back into Postgres, so
any replica can answer the dashboard.
"""

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from fastapi import WebSocket, WebSocketDisconnect

from .store import Row

if TYPE_CHECKING:
    from .app import Server

log = logging.getLogger("rillkit_server")

PROTOCOL_VERSION = 1
HELLO_TIMEOUT = 10.0
REQUEST_TIMEOUT = 5.0
CLOUD_PREFIX = "rk"
CLOSE = {"UNAUTHORIZED": 4001, "TENANT_MISMATCH": 4003, "UNSUPPORTED_VERSION": 4006,
         "BAD_FRAME": 4400, "REPLACED": 4009}


class Refused(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(eq=False)
class Connection:
    ws: WebSocket
    tenant_id: str
    node_id: str
    key_id: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    closed: bool = False

    async def send(self, frame: Row) -> None:
        async with self.lock:
            await self.ws.send_text(json.dumps(frame))

    async def refuse(self, code: str, message: str) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            await self.send({"type": "ERROR", "code": code, "message": message})
            await self.ws.close(CLOSE[code], code)
        except Exception:
            pass


def expected_keys(tenant_id: str) -> Row:
    return {"layout": "hashtag", "prefix": CLOUD_PREFIX, "tenant": tenant_id}


def check_hello(hello: Any) -> Row:
    """Validate a HELLO frame (spec/protocol/v1/hello.json); raises Refused."""
    if not isinstance(hello, dict) or hello.get("type") != "HELLO":
        raise Refused("BAD_FRAME", "the first frame must be HELLO")
    version = hello.get("protocol_version")
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise Refused("UNSUPPORTED_VERSION", f"protocol version {version!r} is not supported")
    node_id = hello.get("node_id")
    if not isinstance(node_id, str) or not 0 < len(node_id) <= 200:
        raise Refused("BAD_FRAME", "node_id must be a string of 1 to 200 characters")
    keys, sdk = hello.get("keys"), hello.get("sdk")
    if not isinstance(keys, dict) or keys.get("layout") not in ("plain", "hashtag", "custom"):
        raise Refused("BAD_FRAME", "keys must describe the key layout")
    if not isinstance(sdk, dict):
        raise Refused("BAD_FRAME", "sdk must be an object")
    return hello


class Gateway:
    def __init__(self, server: "Server") -> None:
        self.server = server
        self.connections: dict[tuple[str, str], Connection] = {}
        self._waiting: dict[str, asyncio.Future[Row]] = {}   # log requests made here
        self._serving: set[str] = set()                      # log requests made elsewhere
        self._last_overview: dict[str, float] = {}

    @property
    def store(self) -> Any:
        return self.server.store

    @property
    def replica_id(self) -> str:
        return self.server.settings.replica_id

    # the WebSocket session

    async def session(self, ws: WebSocket) -> None:
        await ws.accept()
        auth = ws.headers.get("authorization", "")
        key = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        conn: Connection | None = None
        try:
            account = await self.store.authenticate(key) if key else None
            if account is None:
                raise Refused("UNAUTHORIZED", "missing, unknown or revoked API key")
            tenant_id = account["tenant_id"]
            try:
                raw = await asyncio.wait_for(ws.receive_text(), HELLO_TIMEOUT)
                hello = check_hello(json.loads(raw))
            except (TimeoutError, ValueError) as e:
                raise Refused("BAD_FRAME", f"expected a HELLO frame: {e}") from e
            if account["enforce_keys"] and hello["keys"] != expected_keys(tenant_id):
                raise Refused(
                    "TENANT_MISMATCH",
                    f"this API key belongs to tenant {tenant_id}: workers must use "
                    f"HashTagKeys(prefix={CLOUD_PREFIX!r}, tenant={tenant_id!r}) "
                    f"(RILLKIT_TENANT={tenant_id}), not {hello['keys']}",
                )
            conn = Connection(ws, tenant_id, hello["node_id"], account["key_id"])
            await self._register(conn, hello)
            await self._serve(conn)
        except Refused as e:
            log.info("Relay connection refused: %s %s", e.code, e)
            target = conn or Connection(ws, "", "", "")
            await target.refuse(e.code, str(e))
        except WebSocketDisconnect:
            pass
        finally:
            if conn is not None:
                await self._unregister(conn)

    async def _register(self, conn: Connection, hello: Row) -> None:
        key = (conn.tenant_id, conn.node_id)
        old = self.connections.get(key)
        self.connections[key] = conn
        if old is not None:
            await old.refuse("REPLACED", "a new connection with the same node id took over")
        await conn.send({
            "type": "WELCOME",
            "protocol_version": min(hello["protocol_version"], PROTOCOL_VERSION),
            "tenant_id": conn.tenant_id,
            "telemetry_interval_sec": self.server.settings.telemetry_interval_sec,
        })
        await self.store.node_connected(conn.tenant_id, conn.node_id, self.replica_id,
                                        conn.key_id, hello["keys"], hello["sdk"],
                                        hello.get("capabilities") or {})
        log.info("node %s of tenant %s connected", conn.node_id, conn.tenant_id)
        self.server.invalidate_overview(conn.tenant_id)
        await self.server.notify("overview", tenant=conn.tenant_id)
        asyncio.create_task(self.deliver_pending(conn.tenant_id))

    async def _unregister(self, conn: Connection) -> None:
        key = (conn.tenant_id, conn.node_id)
        if self.connections.get(key) is not conn:
            return  # replaced by a newer connection, which owns the node now
        del self.connections[key]
        try:
            await self.store.node_disconnected(conn.tenant_id, conn.node_id, self.replica_id)
            for directive_id in await self.store.release_unacked(conn.tenant_id, conn.node_id):
                await self.server.notify("directive", tenant=conn.tenant_id, id=directive_id)
            self.server.invalidate_overview(conn.tenant_id)
            await self.server.notify("overview", tenant=conn.tenant_id)
        except Exception:
            log.exception("cleaning up after node %s failed", conn.node_id)
        log.info("node %s of tenant %s disconnected", conn.node_id, conn.tenant_id)

    async def _serve(self, conn: Connection) -> None:
        while True:
            raw = await conn.ws.receive_text()
            try:
                frame = json.loads(raw)
            except ValueError as e:
                raise Refused("BAD_FRAME", "frames must be JSON objects") from e
            if not isinstance(frame, dict):
                raise Refused("BAD_FRAME", "frames must be JSON objects")
            kind = frame.get("type")
            if kind == "TELEMETRY_REPORT":
                await self.store.node_telemetry(conn.tenant_id, conn.node_id, frame)
                await self._overview_changed(conn.tenant_id)
            elif kind == "DIRECTIVE_ACK":
                await self._ack(conn, frame)
            elif kind in ("LOGS_RESPONSE", "MESSAGES_RESPONSE", "TRACE_RESPONSE"):
                await self._answered(frame)
            # anything else is ignored (spec 5.4)

    async def _overview_changed(self, tenant_id: str) -> None:
        """Tell dashboards the tenant changed, at most once a second per tenant."""
        now = time.monotonic()
        if now - self._last_overview.get(tenant_id, 0.0) >= 1.0:
            self._last_overview[tenant_id] = now
            await self.server.notify("overview", tenant=tenant_id)

    async def _ack(self, conn: Connection, ack: Row) -> None:
        directive_id = ack.get("directive_id")
        if not isinstance(directive_id, str):
            return
        row = await self.store.get_directive(directive_id)
        if row is None or row["tenant_id"] != conn.tenant_id:
            return  # never let one tenant's worker touch another tenant's directive
        await self.store.ack_directive(directive_id, ack)
        await self.server.notify("directive", tenant=conn.tenant_id, id=directive_id)

    # delivering directives

    def _pick(self, tenant_id: str, row: Row) -> Connection | None:
        if row["scope"] == "node":
            return self.connections.get((tenant_id, (row["target"] or {}).get("node_id", "")))
        local = [c for (t, _), c in self.connections.items() if t == tenant_id and not c.closed]
        return local[0] if local else None

    async def deliver(self, row: Row) -> None:
        """Send a pending directive if this replica holds a connection that can take it."""
        if row["status"] != "PENDING":
            return
        conn = self._pick(row["tenant_id"], row)
        if conn is None:
            return
        claimed = await self.store.claim_directive(row["id"], self.replica_id, conn.node_id)
        if claimed is None:
            return  # another replica sent it
        frame: Row = {"type": "DIRECTIVE", "directive_id": row["id"], "cmd": row["cmd"],
                      "scope": row["scope"], "target": row["target"] or {},
                      "params": row["params"] or {}}
        if row["reason"]:
            frame["reason"] = row["reason"]
        try:
            await conn.send(frame)
        except Exception as e:
            log.warning("sending directive %s to %s failed: %s", row["id"], conn.node_id, e)
            await self.store.release_directive(row["id"])
        await self.server.notify("directive", tenant=row["tenant_id"], id=row["id"])

    async def deliver_pending(self, tenant_id: str) -> None:
        try:
            for row in await self.store.pending_directives(tenant_id):
                await self.deliver(row)
        except Exception:
            log.exception("delivering pending directives failed")

    async def close_key(self, key_id: str) -> None:
        for conn in [c for c in self.connections.values() if c.key_id == key_id]:
            await conn.refuse("UNAUTHORIZED", "the API key was revoked")

    # requests answered by a worker (logs, messages)

    async def request(self, tenant_id: str, frame: Row, stream: str | None = None) -> Row:
        """Send ``frame`` (with a fresh ``request_id``) to a live node of the tenant, preferring
        one that consumes ``stream``, and return its answer. Raises LookupError when no node is
        connected and TimeoutError when none answers in time."""
        nodes = await self.store.live_nodes(tenant_id)
        if not nodes:
            raise LookupError("no worker is connected")

        def consumes(node: Row) -> bool:
            return any(s.get("stream") == stream
                       for s in (node["telemetry"] or {}).get("streams") or [])

        node = sorted(nodes, key=lambda n: not consumes(n))[0]
        request_id = f"req_{uuid.uuid4().hex[:16]}"
        frame = {**frame, "request_id": request_id}
        future: asyncio.Future[Row] = asyncio.get_running_loop().create_future()
        self._waiting[request_id] = future
        try:
            conn = self.connections.get((tenant_id, node["node_id"]))
            if conn is not None:
                await conn.send(frame)
            else:
                await self.server.notify("relay_request", replica=node["replica_id"],
                                         tenant=tenant_id, node=node["node_id"], frame=frame)
            answer = await asyncio.wait_for(future, REQUEST_TIMEOUT)
            return {**answer, "node_id": node["node_id"]}
        finally:
            self._waiting.pop(request_id, None)

    async def request_logs(self, tenant_id: str, stream: str, msg_id: str) -> Row:
        return await self.request(tenant_id, {"type": "LOGS_REQUEST", "stream": stream,
                                              "msg_id": msg_id}, stream)

    async def serve_remote(self, note: Row) -> None:
        """Another replica asked a node connected here."""
        if note.get("replica") != self.replica_id:
            return
        conn = self.connections.get((note["tenant"], note["node"]))
        if conn is None:
            return
        self._serving.add(note["frame"]["request_id"])
        await conn.send(note["frame"])

    async def _answered(self, frame: Row) -> None:
        request_id = str(frame.get("request_id"))
        future = self._waiting.get(request_id)
        if future is not None:
            if not future.done():
                future.set_result(frame)
        elif request_id in self._serving:
            self._serving.discard(request_id)
            await self.store.put_response(request_id, frame)
            await self.server.notify("relay_response", id=request_id)

    async def remote_answered(self, note: Row) -> None:
        future = self._waiting.get(str(note.get("id")))
        if future is None or future.done():
            return
        payload = await self.store.take_response(str(note["id"]))
        if payload is not None:
            future.set_result(payload)
