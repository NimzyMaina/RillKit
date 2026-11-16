"""The rillkit-server application: wiring, background tasks, and serving the dashboard."""

import asyncio
import contextlib
import logging
import time
from collections import defaultdict
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from psycopg_pool import AsyncConnectionPool
from starlette.exceptions import HTTPException

from . import db
from .api import ApiError, public, router, sse
from .autoscaler import Autoscaler
from .config import ServerSettings
from .gateway import Gateway
from .store import Row, Store

log = logging.getLogger("rillkit_server")


class Hub:
    """Fans events out to the dashboards connected to this replica (SSE)."""

    def __init__(self) -> None:
        self._queues: dict[str, set[asyncio.Queue[bytes]]] = defaultdict(set)
        self.dirty: set[str] = set()

    def subscribe(self, tenant_id: str) -> "asyncio.Queue[bytes]":
        queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=256)
        self._queues[tenant_id].add(queue)
        return queue

    def unsubscribe(self, tenant_id: str, queue: "asyncio.Queue[bytes]") -> None:
        self._queues[tenant_id].discard(queue)

    def watched(self, tenant_id: str) -> bool:
        return bool(self._queues.get(tenant_id))

    def publish(self, tenant_id: str, payload: bytes) -> None:
        for queue in list(self._queues.get(tenant_id, ())):
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:
                pass  # a stuck dashboard misses events; it refetches on reconnect


class Server:
    #: Overviews are assembled from every node, so API requests reuse one for a moment.
    OVERVIEW_TTL_SEC = 1.0

    def __init__(self, settings: ServerSettings) -> None:
        self.settings = settings
        self.pool: AsyncConnectionPool | None = None
        self._store: Store | None = None
        self.hub = Hub()
        self.gateway = Gateway(self)
        self.autoscaler = Autoscaler(self)
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[Any]] = []
        self._overviews: dict[str, tuple[float, Row]] = {}
        self._overview_locks: dict[str, asyncio.Lock] = {}

    @property
    def store(self) -> Store:
        if self._store is None:
            raise RuntimeError("the server has not started")
        return self._store

    async def start(self) -> None:
        self.pool = await db.open_pool(self.settings.database_url)
        await db.migrate(self.pool)
        self._store = Store(self.pool, self.settings.node_ttl_sec)
        await self._bootstrap()
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(db.listen(self.settings.database_url, self._on_note, self._stop)),
            asyncio.create_task(self.autoscaler.run(self._stop)),
            asyncio.create_task(self._publish_overviews()),
        ]
        log.info("rillkit-server replica %s started", self.settings.replica_id)

    async def stop(self) -> None:
        self._stop.set()
        for conn in list(self.gateway.connections.values()):
            with contextlib.suppress(Exception):
                await conn.ws.close(1001, "server stopping")
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.pool is not None:
            await self.pool.close()

    async def _bootstrap(self) -> None:
        tenant, key = self.settings.bootstrap_tenant, self.settings.bootstrap_api_key
        if tenant and key:
            await self.store.create_tenant(tenant, tenant, False)
            await self.store.ensure_api_key(tenant, key, "bootstrap")
            log.warning("bootstrap tenant %s and its API key are in place (demo setup only)",
                        tenant)

    async def notify(self, kind: str, **payload: Any) -> None:
        assert self.pool is not None
        await db.notify(self.pool, kind, **payload)

    async def overview(self, tenant: Row) -> Row:
        """The tenant overview, reused for up to :attr:`OVERVIEW_TTL_SEC`.

        The dashboard can ask many times a second while the fleet changes; building it walks
        every node, so a one-second cache keeps a large fleet from multiplying that work.
        """
        tenant_id = tenant["id"]
        cached = self._overviews.get(tenant_id)
        now = time.monotonic()
        if cached is not None and now - cached[0] < self.OVERVIEW_TTL_SEC:
            return cached[1]
        # A large fleet makes the build expensive; concurrent requests must wait for one build
        # rather than start a dozen of them when the cache entry expires.
        lock = self._overview_locks.setdefault(tenant_id, asyncio.Lock())
        async with lock:
            cached = self._overviews.get(tenant_id)
            now = time.monotonic()
            if cached is not None and now - cached[0] < self.OVERVIEW_TTL_SEC:
                return cached[1]
            view = await self.store.overview(tenant)
            self._overviews[tenant_id] = (now, view)
            return view

    def invalidate_overview(self, tenant_id: str | None = None) -> None:
        """Drop one tenant's cached overview, or all of them, after a write."""
        if tenant_id is None:
            self._overviews.clear()
        else:
            self._overviews.pop(tenant_id, None)

    async def create_directive(self, tenant_id: str, body: Row, source: str) -> Row:
        row = await self.store.create_directive(tenant_id, body, source)
        self.invalidate_overview(tenant_id)
        await self.notify("directive", tenant=tenant_id, id=row["id"])
        return self.store.directive_view(row)

    async def _on_note(self, note: Row) -> None:
        kind = note.get("kind")
        if kind == "directive":
            row = await self.store.get_directive(note["id"])
            if row is not None:
                await self.gateway.deliver(row)
                fresh = await self.store.get_directive(note["id"])
                self.hub.publish(row["tenant_id"],
                                 sse("directive", self.store.directive_view(fresh or row)))
        elif kind == "overview":
            self.hub.dirty.add(note["tenant"])
        elif kind == "revoke":
            await self.gateway.close_key(note["key_id"])
        elif kind == "relay_request":
            await self.gateway.serve_remote(note)
        elif kind == "relay_response":
            await self.gateway.remote_answered(note)

    async def _publish_overviews(self) -> None:
        """Send a fresh overview to dashboards of tenants that changed, once a second at most."""
        while not self._stop.is_set():
            await asyncio.sleep(1.0)
            dirty, self.hub.dirty = self.hub.dirty, set()
            for tenant_id in dirty:
                if not self.hub.watched(tenant_id):
                    continue
                try:
                    tenant = await self.store.get_tenant(tenant_id)
                    if tenant is not None:
                        self.hub.publish(tenant_id, sse("overview", await self.overview(tenant)))
                except Exception:
                    log.exception("publishing the overview of %s failed", tenant_id)


def create_app(settings: ServerSettings | None = None) -> FastAPI:
    settings = settings or ServerSettings.from_env()
    server = Server(settings)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await server.start()
        try:
            yield
        finally:
            await server.stop()

    app = FastAPI(title="rillkit-server", lifespan=lifespan, docs_url="/api/docs",
                  openapi_url="/api/openapi.json", redoc_url=None)
    app.state.server = server

    @app.exception_handler(ApiError)
    async def api_error(request: Request, exc: ApiError) -> JSONResponse:
        return exc.response()

    @app.exception_handler(RequestValidationError)
    async def invalid(request: Request, exc: RequestValidationError) -> JSONResponse:
        first = exc.errors()[0] if exc.errors() else {}
        where = ".".join(str(p) for p in first.get("loc", ()) if p != "body")
        message = f"{where}: {first.get('msg', 'invalid request')}" if where else \
            str(first.get("msg", "invalid request"))
        return ApiError("INVALID", message).response()

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException) -> JSONResponse:
        code = {401: "UNAUTHORIZED", 404: "NOT_FOUND", 409: "CONFLICT"}.get(exc.status_code,
                                                                           "INVALID")
        return JSONResponse({"error": {"code": code, "message": str(exc.detail)}},
                            status_code=exc.status_code)

    app.include_router(public)
    app.include_router(router)

    @app.websocket("/relay/v1")
    async def relay(ws: WebSocket) -> None:
        await server.gateway.session(ws)

    static = Path(settings.static_dir) if settings.static_dir else None
    if static is not None and (static / "index.html").is_file():
        root = static.resolve()

        @app.get("/{path:path}", include_in_schema=False)
        async def dashboard(path: str) -> Response:
            if path.startswith(("api/", "relay/")):
                return ApiError("NOT_FOUND", f"no route /{path}").response()
            target = (root / path).resolve()
            if path and target.is_file() and target.is_relative_to(root):
                # Vite's output names carry a content hash, so it never changes; everything else
                # (logo, screenshots) is short-lived.
                cache = ("public, max-age=31536000, immutable" if path.startswith("assets/")
                         else "public, max-age=3600")
                return FileResponse(target, headers={"Cache-Control": cache})
            if path.startswith("assets/"):
                # A tab opened before a rebuild asks for a chunk this build removed. The SPA
                # fallback would answer HTML, and the browser refuses to run it: 404 instead.
                return ApiError("NOT_FOUND", f"no asset /{path}").response()
            # index.html must be revalidated, or a stale tab keeps asking for removed chunks.
            return FileResponse(root / "index.html", headers={"Cache-Control": "no-cache"})
    return app
