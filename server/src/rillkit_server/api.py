"""The HTTP API (server/API.md)."""

import asyncio
import hmac
import json
import re
from collections.abc import AsyncIterator
from typing import Any, Literal

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator
from rillkit import ConfigurationError, PlainKeys
from rillkit.controlplane import DirectiveError, plan_directive

from . import __version__
from .store import Row

TENANT_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")
PING_SEC = 15.0
TICKET_TTL_SEC = 60


class ApiError(Exception):
    STATUS = {"UNAUTHORIZED": 401, "NOT_FOUND": 404, "INVALID": 422, "CONFLICT": 409,
              "NO_NODE": 409, "TIMEOUT": 504}

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.status = self.STATUS[code]

    def response(self) -> JSONResponse:
        return JSONResponse({"error": {"code": self.code, "message": str(self)}},
                            status_code=self.status)


class Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NewTenant(Body):
    id: str
    name: str = Field(min_length=1, max_length=200)
    enforce_keys: bool = False


class NewKey(Body):
    label: str | None = Field(default=None, max_length=200)


class NewDirective(Body):
    cmd: str
    scope: str = "global"
    target: dict[str, Any] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    reason: str | None = Field(default=None, max_length=500)


class PolicyBody(Body):
    enabled: bool = True
    min_workers: int = Field(ge=0, le=100_000)
    max_workers: int = Field(ge=0, le=100_000)
    target_lag_per_worker: int = Field(gt=0)
    upscale_cooldown_sec: int = Field(default=30, ge=0, le=86_400)
    downscale_cooldown_sec: int = Field(default=120, ge=0, le=86_400)

    @model_validator(mode="after")
    def _range(self) -> "PolicyBody":
        if self.max_workers < self.min_workers:
            raise ValueError("max_workers must be at least min_workers")
        return self


def server_of(request: Request) -> Any:
    return request.app.state.server


def require_admin(request: Request) -> None:
    """The admin token, from the Authorization header only: never from the url, where it would
    end up in access logs and browser history."""
    expected = server_of(request).settings.admin_token
    auth = request.headers.get("authorization", "")
    given = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    if not given or not hmac.compare_digest(given.encode(), expected.encode()):
        raise ApiError("UNAUTHORIZED", "a valid admin token is required")


public = APIRouter(prefix="/api")
router = APIRouter(prefix="/api", dependencies=[Depends(require_admin)])


@public.get("/health")
async def health(request: Request) -> Row:
    server = server_of(request)
    return {"status": "ok", "version": __version__, "replica_id": server.settings.replica_id,
            "leader": server.autoscaler.leader}


async def tenant_or_404(request: Request, t: str) -> Row:
    tenant = await server_of(request).store.get_tenant(t)
    if tenant is None:
        raise ApiError("NOT_FOUND", f"no tenant {t!r}")
    return tenant


# tenants and keys


@router.get("/tenants")
async def list_tenants(request: Request) -> list[Row]:
    return await server_of(request).store.list_tenants()


@router.post("/tenants", status_code=201)
async def create_tenant(request: Request, body: NewTenant) -> Row:
    if not TENANT_ID.match(body.id):
        raise ApiError("INVALID", "tenant ids are 1 to 40 lowercase letters, digits and '-'")
    tenant = await server_of(request).store.create_tenant(body.id, body.name, body.enforce_keys)
    if tenant is None:
        raise ApiError("CONFLICT", f"tenant {body.id!r} already exists")
    return tenant


@router.get("/tenants/{t}")
async def get_tenant(request: Request, t: str) -> Row:
    return await tenant_or_404(request, t)


@router.get("/tenants/{t}/api-keys")
async def list_keys(request: Request, t: str) -> list[Row]:
    await tenant_or_404(request, t)
    return await server_of(request).store.list_api_keys(t)


@router.post("/tenants/{t}/api-keys", status_code=201)
async def create_key(request: Request, t: str, body: NewKey | None = None) -> Row:
    await tenant_or_404(request, t)
    view, key = await server_of(request).store.create_api_key(t, body.label if body else None)
    return view | {"key": key}


@router.delete("/tenants/{t}/api-keys/{key_id}")
async def revoke_key(request: Request, t: str, key_id: str) -> Row:
    server = server_of(request)
    view = await server.store.revoke_api_key(t, key_id)
    if view is None:
        raise ApiError("NOT_FOUND", f"no API key {key_id!r}")
    await server.notify("revoke", key_id=key_id)
    return view


@router.get("/tenants/{t}/connection")
async def connection(request: Request, t: str) -> Row:
    tenant = await tenant_or_404(request, t)
    url = server_of(request).settings.public_url
    env = {"RILLKIT_CONTROL": "relay", "RILLKIT_CONTROL_URL": url,
           "RILLKIT_API_KEY": "<your API key>"}
    if tenant["enforce_keys"]:
        env["RILLKIT_TENANT"] = tenant["id"]
    host = re.sub(r"^[a-z]+://", "", url).split("/")[0].split(":")[0]
    if url.startswith(("http://", "ws://")) and host not in ("localhost", "127.0.0.1"):
        env["RILLKIT_CONTROL_INSECURE"] = "1"
    return {"control_url": url, "env": env}


# fleet


@router.get("/tenants/{t}/overview")
async def overview(request: Request, t: str) -> Row:
    tenant = await tenant_or_404(request, t)
    return await server_of(request).overview(tenant)


@router.get("/metrics")
async def prometheus(request: Request) -> Response:
    """A Prometheus scrape: fleet numbers per tenant, not per stream (see server/API.md)."""
    server = server_of(request)
    tenants = await server.store.list_tenants()
    fields = [
        ("nodes_online", "Connected worker nodes."),
        ("active_workers", "Consumer lanes running."),
        ("in_flight", "Messages being handled now."),
        ("lag", "Messages waiting in their groups."),
        ("pending", "Delivered messages not acknowledged."),
        ("ack_rate_sec", "Messages finished per second."),
        ("streams", "Streams and groups consumed."),
        ("directives_pending", "Directives not yet acknowledged."),
    ]
    lines = [
        "# HELP rillkit_server_up 1 while this replica serves requests.",
        "# TYPE rillkit_server_up gauge",
        "rillkit_server_up 1",
        "# HELP rillkit_server_autoscaler_leader 1 on the replica holding the autoscaler lock.",
        "# TYPE rillkit_server_autoscaler_leader gauge",
        f"rillkit_server_autoscaler_leader {1 if server.autoscaler.leader else 0}",
        "# HELP rillkit_tenants Tenants known to this server.",
        "# TYPE rillkit_tenants gauge",
        f"rillkit_tenants {len(tenants)}",
    ]
    for name, help_text in fields:
        lines += [f"# HELP rillkit_tenant_{name} {help_text}",
                  f"# TYPE rillkit_tenant_{name} gauge"]
    for tenant in tenants:
        tenant_id = tenant["id"]
        view = await server.overview(tenant)
        totals = view["totals"]
        values = {
            "nodes_online": totals["nodes_online"],
            "active_workers": totals["active_workers"],
            "in_flight": totals["in_flight"],
            "lag": totals["lag"],
            "pending": totals["pending"],
            "ack_rate_sec": totals["ack_rate_sec"],
            "streams": len(view["streams"]),
            "directives_pending": await server.store.pending_directive_count(tenant_id),
        }
        for name, _ in fields:
            lines.append(f'rillkit_tenant_{name}{{tenant="{tenant_id}"}} {values[name]}')
    return Response("\n".join(lines) + "\n",
                    media_type="text/plain; version=0.0.4; charset=utf-8")


@router.get("/tenants/{t}/streams/{stream}/{group}/samples")
async def samples(request: Request, t: str, stream: str, group: str,
                  window_sec: int = Query(default=900, ge=10, le=86_400)) -> list[Row]:
    await tenant_or_404(request, t)
    return await server_of(request).store.samples(
        t, stream, group, window_sec)


# directives


@router.post("/tenants/{t}/directives", status_code=201)
async def create_directive(request: Request, t: str, body: NewDirective) -> Row:
    server = server_of(request)
    await tenant_or_404(request, t)
    directive = body.model_dump()
    try:
        plan_directive(PlainKeys(), {"type": "DIRECTIVE", "directive_id": "check", **directive})
    except (DirectiveError, ConfigurationError) as e:
        raise ApiError("INVALID", str(e)) from e
    live = await server.store.live_nodes(t)
    if body.scope == "node":
        live = [n for n in live if n["node_id"] == body.target.get("node_id")]
    if not live:
        what = f"node {body.target.get('node_id')!r}" if body.scope == "node" else "worker"
        raise ApiError("NO_NODE", f"no {what} is connected to deliver the directive to")
    return await server.create_directive(t, directive, source="user")


@router.get("/tenants/{t}/directives")
async def list_directives(request: Request, t: str,
                          limit: int = Query(default=25, ge=1, le=200),
                          offset: int = Query(default=0, ge=0),
                          source: Literal["user", "autoscaler"] | None = Query(default=None),
                          ) -> Row:
    await tenant_or_404(request, t)
    return await server_of(request).store.list_directives(t, limit, offset, source)


# streams, messages and metrics


class NewStream(Body):
    stream: str = Field(min_length=1, max_length=500)
    group: str = Field(min_length=1, max_length=200)
    start: Literal["$", "0"] = "$"


@router.post("/tenants/{t}/streams", status_code=201)
async def create_stream(request: Request, t: str, body: NewStream) -> Row:
    """Record the stream and, if a worker is connected, create it and its group there."""
    server = server_of(request)
    tenant = await tenant_or_404(request, t)
    await server.store.register_stream(t, body.stream, body.group)
    server.invalidate_overview(t)
    directive = None
    if await server.store.live_nodes(t):
        directive = await server.create_directive(
            t, {"cmd": "CREATE_STREAM", "scope": "stream",
                "target": {"stream": body.stream, "group": body.group},
                "params": {"start": body.start}}, source="user")
    overview = await server.overview(tenant)
    summary = next(s for s in overview["streams"]
                   if (s["stream"], s["group"]) == (body.stream, body.group))
    return {"stream": summary, "directive": directive}


ENTRY_ID = re.compile(r"^[0-9]+-[0-9]+$")
TRACE_ID = re.compile(r"^[0-9a-f]{32}$")


async def _messages(request: Request, t: str, stream: str, group: str, frame: Row) -> Row:
    await tenant_or_404(request, t)
    try:
        answer = await server_of(request).gateway.request(
            t, {"type": "MESSAGES_REQUEST", "stream": stream, "group": group, **frame}, stream)
    except LookupError as e:
        raise ApiError("NO_NODE", str(e)) from e
    except TimeoutError as e:
        raise ApiError("TIMEOUT", "no worker answered in time") from e
    if answer.get("error"):
        raise ApiError("INVALID", f"the worker could not read the stream: {answer['error']}")
    return answer


@router.get("/tenants/{t}/streams/{stream}/{group}/messages")
async def messages(request: Request, t: str, stream: str, group: str,
                   source: Literal["stream", "dlq"] = Query(default="stream"),
                   before: str | None = Query(default=None, pattern=ENTRY_ID.pattern),
                   after: str | None = Query(default=None, pattern=ENTRY_ID.pattern),
                   count: int = Query(default=50, ge=1, le=200)) -> Row:
    if before and after:
        raise ApiError("INVALID", "give before or after, not both")
    frame: Row = {"source": source, "count": count}
    if before:
        frame["before"] = before
    if after:
        frame["after"] = after
    answer = await _messages(request, t, stream, group, frame)
    return {k: answer.get(k) for k in ("messages", "next_before", "total", "payloads",
                                       "node_id", "as_of")} | {
        "more_after": bool(answer.get("more_after"))}


@router.get("/tenants/{t}/streams/{stream}/{group}/messages/{msg_id}")
async def message(request: Request, t: str, stream: str, group: str, msg_id: str,
                  source: Literal["stream", "dlq"] = Query(default="stream")) -> Row:
    if not ENTRY_ID.match(msg_id):
        raise ApiError("INVALID", f"{msg_id!r} is not a message id like 1718000000000-0")
    answer = await _messages(request, t, stream, group, {"source": source, "ids": [msg_id]})
    found: list[Row] = answer.get("messages") or []
    if not found:
        raise ApiError("NOT_FOUND", f"no message {msg_id} in {stream}"
                                    + (" (DLQ)" if source == "dlq" else ""))
    return found[0]


@router.get("/tenants/{t}/metrics")
async def metrics(request: Request, t: str,
                  window_sec: int = Query(default=3600, ge=300, le=86_400)) -> Row:
    await tenant_or_404(request, t)
    return await server_of(request).store.metrics(t, window_sec)


# policies


@router.get("/tenants/{t}/policies")
async def list_policies(request: Request, t: str) -> list[Row]:
    await tenant_or_404(request, t)
    store = server_of(request).store
    return [store.policy_view(p) for p in await store.list_policies(t)]


@router.put("/tenants/{t}/policies/{stream}/{group}")
async def put_policy(request: Request, t: str, stream: str, group: str, body: PolicyBody) -> Row:
    await tenant_or_404(request, t)
    server = server_of(request)
    policy = await server.store.put_policy(t, stream, group, body.model_dump())
    server.invalidate_overview(t)
    return policy


@router.delete("/tenants/{t}/policies/{stream}/{group}", status_code=204)
async def delete_policy(request: Request, t: str, stream: str, group: str) -> Response:
    await tenant_or_404(request, t)
    server = server_of(request)
    if not await server.store.delete_policy(t, stream, group):
        raise ApiError("NOT_FOUND", f"no policy for {stream}/{group}")
    server.invalidate_overview(t)
    return Response(status_code=204)


# logs


@router.get("/tenants/{t}/logs/{stream}/{msg_id}")
async def logs(request: Request, t: str, stream: str, msg_id: str) -> Row:
    await tenant_or_404(request, t)
    try:
        response = await server_of(request).gateway.request_logs(t, stream, msg_id)
    except LookupError as e:
        raise ApiError("NO_NODE", str(e)) from e
    except TimeoutError as e:
        raise ApiError("TIMEOUT", "no worker answered the log request in time") from e
    return {"records": response.get("records", []), "error": response.get("error")}


# traces


@router.get("/tenants/{t}/traces/{trace_id}")
async def trace(request: Request, t: str, trace_id: str) -> Row:
    if not TRACE_ID.match(trace_id):
        raise ApiError("INVALID", f"{trace_id!r} is not a trace id (32 lowercase hex digits)")
    await tenant_or_404(request, t)
    try:
        answer = await server_of(request).gateway.request(
            t, {"type": "TRACE_REQUEST", "trace_id": trace_id})
    except LookupError as e:
        raise ApiError("NO_NODE", str(e)) from e
    except TimeoutError as e:
        raise ApiError("TIMEOUT", "no worker answered the trace request in time") from e
    if answer.get("error"):
        raise ApiError("INVALID", f"the worker could not read the trace: {answer['error']}")
    return {"trace_id": trace_id, "records": answer.get("records", []),
            "node_id": answer.get("node_id")}


# live events


def sse(event: str, data: Any) -> bytes:
    """Render one server-sent event once; every subscriber sends the same bytes."""
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n".encode()


@router.post("/tenants/{t}/events/ticket", status_code=201)
async def events_ticket(request: Request, t: str) -> Row:
    """A single-use ticket for opening the events stream of ``t`` within a minute."""
    await tenant_or_404(request, t)
    ticket = await server_of(request).store.create_ticket(t, TICKET_TTL_SEC)
    return {"ticket": ticket, "expires_in_sec": TICKET_TTL_SEC}


@public.get("/tenants/{t}/events")
async def events(request: Request, t: str, ticket: str = Query(default="")) -> StreamingResponse:
    server = server_of(request)
    if not ticket or await server.store.redeem_ticket(ticket) != t:
        raise ApiError("UNAUTHORIZED", "a valid events ticket for this tenant is required")
    tenant = await tenant_or_404(request, t)

    async def stream() -> AsyncIterator[bytes]:
        queue = server.hub.subscribe(t)
        try:
            yield sse("overview", await server.overview(tenant))
            while not await request.is_disconnected():
                try:
                    yield await asyncio.wait_for(queue.get(), PING_SEC)
                except TimeoutError:
                    yield sse("ping", {})
        finally:
            server.hub.unsubscribe(t, queue)

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
