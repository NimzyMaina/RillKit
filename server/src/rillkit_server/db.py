"""Postgres: the connection pool, migrations, and NOTIFY/LISTEN between replicas."""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from importlib import resources
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

log = logging.getLogger("rillkit_server")

CHANNEL = "rillkit_server"
_MIGRATION_LOCK = 7_340_001  # pg_advisory_lock key: one replica migrates at a time

Handler = Callable[[dict[str, Any]], Awaitable[None]]


async def open_pool(url: str) -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        url, min_size=1, max_size=10, open=False, kwargs={"row_factory": dict_row,
                                                         "autocommit": True},
    )
    await pool.open(wait=True, timeout=30)
    return pool


async def migrate(pool: AsyncConnectionPool) -> None:
    """Apply every migration in ``migrations/`` not yet applied, in order."""
    files = sorted(
        (f for f in resources.files("rillkit_server.migrations").iterdir()
         if f.name.endswith(".sql")),
        key=lambda f: f.name,
    )
    async with pool.connection() as conn:
        await conn.execute("select pg_advisory_lock(%s)", (_MIGRATION_LOCK,))
        try:
            await conn.execute(
                "create table if not exists schema_migrations ("
                "version text primary key, applied_at timestamptz not null default now())"
            )
            cur = await conn.execute("select version from schema_migrations")
            rows: list[Any] = await cur.fetchall()  # dict rows (row_factory=dict_row)
            done = {row["version"] for row in rows}
            for f in files:
                if f.name in done:
                    continue
                log.info("applying migration %s", f.name)
                async with conn.transaction():
                    await conn.execute(f.read_text())
                    await conn.execute(
                        "insert into schema_migrations(version) values (%s)", (f.name,)
                    )
        finally:
            await conn.execute("select pg_advisory_unlock(%s)", (_MIGRATION_LOCK,))


async def notify(pool: AsyncConnectionPool, kind: str, **payload: Any) -> None:
    """Tell every replica (including this one) that something happened."""
    body = json.dumps({"kind": kind, **payload}, default=str)
    async with pool.connection() as conn:
        await conn.execute("select pg_notify(%s, %s)", (CHANNEL, body))


async def listen(url: str, handler: Handler, stop: asyncio.Event) -> None:
    """Deliver every notification to ``handler`` until ``stop`` is set, reconnecting if the
    connection drops."""
    delay = 1.0
    while not stop.is_set():
        try:
            async with await psycopg.AsyncConnection.connect(url, autocommit=True) as conn:
                await conn.execute(f"listen {CHANNEL}")
                delay = 1.0
                while not stop.is_set():
                    # the generator ends after the timeout, so ``stop`` is checked every second
                    async for note in conn.notifies(timeout=1.0):
                        try:
                            await handler(json.loads(note.payload))
                        except Exception:
                            log.exception("notification handler failed")
        except (psycopg.OperationalError, OSError) as e:
            if stop.is_set():
                return
            log.warning("LISTEN connection lost (%s), reconnecting in %.0f s", e, delay)
            try:
                await asyncio.wait_for(stop.wait(), delay)
            except TimeoutError:
                pass
            delay = min(delay * 2, 30.0)
