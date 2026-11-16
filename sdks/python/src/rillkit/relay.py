"""The outbound Relay connection to rillkit-server (spec section 5.4), for ``relay`` mode.

One background thread holds one WebSocket connection: it says ``HELLO``, sends a
``TELEMETRY_REPORT`` every interval the server asks for, applies each ``DIRECTIVE`` through the
control plane (deduplicated in Redis or Valkey like any other directive) and answers with its
``DIRECTIVE_ACK``, and answers ``LOGS_REQUEST`` from the captured logs.

The worker never depends on the connection: if it fails, consumption carries on under the last
desired state stored in the server, and the thread reconnects with jittered exponential backoff.
Loaded only in ``relay`` mode; needs ``rillkit[control]`` (the ``websockets`` package).
"""

import json
import logging
import random
import re
import threading
import time
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urlparse, urlunparse

from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidURI
from websockets.sync.client import ClientConnection, connect

from .config import Control

if TYPE_CHECKING:
    from .controlplane import ControlPlane

log = logging.getLogger("rillkit")

PROTOCOL_VERSION = 1
RELAY_PATH = "/relay/v1"
HANDSHAKE_TIMEOUT = 10.0
MAX_LOG_RECORDS = 1000
_BACKOFF_START, _BACKOFF_MAX = 1.0, 30.0


class RelayRefused(Exception):
    """The server answered the handshake with an ERROR frame."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


def relay_url(url: str) -> str:
    """The Relay endpoint for a control url: ``https://x`` becomes ``wss://x/relay/v1``. A url
    that already has a path is used as is."""
    parsed = urlparse(url)
    scheme = {"https": "wss", "http": "ws"}.get(parsed.scheme, parsed.scheme)
    path = parsed.path if parsed.path not in ("", "/") else RELAY_PATH
    return urlunparse(parsed._replace(scheme=scheme, path=path))


class Relay:
    def __init__(self, control: "ControlPlane", settings: Control) -> None:
        self.control = control
        self.settings = settings
        self.url = relay_url(settings.url or "")
        self.connected = threading.Event()
        self.tenant_id: Optional[str] = None
        self._stopping = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ws: Optional[ClientConnection] = None
        self._ws_lock = threading.Lock()

    # lifecycle

    def start(self) -> None:
        self._stopping.clear()
        self._thread = threading.Thread(target=self._run, name="rillkit-relay", daemon=True)
        self._thread.start()

    def stop(self, timeout: Optional[float] = None) -> None:
        self._stopping.set()
        with self._ws_lock:
            ws = self._ws
        if ws is not None:
            try:
                ws.close(1000, "worker stopping")
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout)

    def _run(self) -> None:
        delay = _BACKOFF_START
        while not self._stopping.is_set():
            try:
                self._session()
            except RelayRefused as e:
                log.error("the control plane refused the Relay connection: %s", e)
            except (OSError, ConnectionClosed, InvalidHandshake, InvalidURI, TimeoutError,
                    ValueError) as e:
                if self._stopping.is_set():
                    break
                log.warning("Relay connection to %s failed: %s", self.url, e)
            except Exception as e:  # keep the worker alive whatever happens here
                log.exception("Relay error: %s", e)
            finally:
                if self.connected.is_set():
                    delay = _BACKOFF_START  # the session got past WELCOME before it ended
                self.connected.clear()
                with self._ws_lock:
                    self._ws = None
            if self._stopping.is_set():
                break
            wait = delay * random.uniform(0.5, 1.5)
            log.info("reconnecting to the control plane in %.1f s", wait)
            self._stopping.wait(wait)
            delay = min(delay * 2, _BACKOFF_MAX)

    # one session

    def _session(self) -> None:
        """Connect, handshake, then serve frames until the connection ends or the worker stops.
        """
        from . import __version__

        ws = connect(
            self.url,
            additional_headers={"Authorization": f"Bearer {self.settings.api_key}"},
            user_agent_header=f"rillkit-python/{__version__}",
            open_timeout=HANDSHAKE_TIMEOUT,
            max_size=4 * 1024 * 1024,
        )
        with self._ws_lock:
            self._ws = ws
        with ws:
            self._send(ws, {
                "type": "HELLO",
                "protocol_version": PROTOCOL_VERSION,
                "node_id": self.control.node_id,
                "keys": self.control.keys.describe(),
                "sdk": {"language": "python", "version": __version__},
                "capabilities": self.control.capabilities(),
            })
            welcome = json.loads(ws.recv(timeout=HANDSHAKE_TIMEOUT))
            if welcome.get("type") == "ERROR":
                raise RelayRefused(str(welcome.get("code")), str(welcome.get("message")))
            if welcome.get("type") != "WELCOME":
                raise ValueError(f"expected WELCOME, got {welcome.get('type')!r}")
            self.tenant_id = welcome.get("tenant_id")
            interval = float(welcome.get("telemetry_interval_sec") or 5.0)
            self.connected.set()
            log.info("connected to the control plane at %s as %s (tenant %s)", self.url,
                     self.control.node_id, self.tenant_id)
            next_report = 0.0
            while not self._stopping.is_set():
                now = time.monotonic()
                if now >= next_report:
                    self._send(ws, self._telemetry())
                    next_report = now + interval
                try:
                    raw = ws.recv(timeout=max(0.05, min(1.0, next_report - time.monotonic())))
                except TimeoutError:
                    continue
                self._handle(ws, raw)

    def _telemetry(self) -> dict[str, Any]:
        try:
            return self.control.telemetry()
        except Exception as e:  # report what we can without the server's stream data
            log.debug("telemetry failed: %s", e)
            return self.control._node_report()

    def _send(self, ws: ClientConnection, frame: dict[str, Any]) -> None:
        ws.send(json.dumps(frame))

    def _handle(self, ws: ClientConnection, raw: Any) -> None:
        try:
            frame = json.loads(raw)
        except ValueError:
            log.warning("ignoring a Relay frame that is not JSON")
            return
        if not isinstance(frame, dict):
            return
        kind = frame.get("type")
        if kind == "DIRECTIVE":
            self._send(ws, self.control.apply(frame))
        elif kind == "LOGS_REQUEST":
            self._send(ws, self._logs(frame))
        elif kind == "TRACE_REQUEST":
            self._send(ws, self._trace(frame))
        elif kind == "MESSAGES_REQUEST":
            self._send(ws, self._messages(frame))
        elif kind == "ERROR":
            raise RelayRefused(str(frame.get("code")), str(frame.get("message")))
        # unknown frame types are ignored (spec 5.4)

    def _logs(self, frame: dict[str, Any]) -> dict[str, Any]:
        from .logs import read_logs

        response: dict[str, Any] = {"type": "LOGS_RESPONSE",
                                    "request_id": str(frame.get("request_id")), "records": []}
        try:
            stream = self.control.stream_named(str(frame["stream"]))
            records = read_logs(self.control.broker, stream, str(frame["msg_id"]))
            response["records"] = records[:MAX_LOG_RECORDS]
        except Exception as e:
            response["error"] = str(e)
        return response

    def _trace(self, frame: dict[str, Any]) -> dict[str, Any]:
        response: dict[str, Any] = {"type": "TRACE_RESPONSE",
                                    "request_id": str(frame.get("request_id")),
                                    "trace_id": str(frame.get("trace_id")), "records": []}
        try:
            trace_id = str(frame.get("trace_id"))
            if not re.fullmatch(r"[0-9a-f]{32}", trace_id):
                raise ValueError("trace_id must be 32 lowercase hex digits")
            response.update(self.control.trace(trace_id))
            response["request_id"] = str(frame.get("request_id"))
        except Exception as e:
            response["error"] = str(e)
        return response

    def _messages(self, frame: dict[str, Any]) -> dict[str, Any]:
        response: dict[str, Any] = {"type": "MESSAGES_RESPONSE",
                                    "request_id": str(frame.get("request_id")),
                                    "messages": [], "next_before": None, "more_after": False,
                                    "total": 0, "payloads": False}
        try:
            stream, group = frame.get("stream"), frame.get("group")
            source = frame.get("source", "stream")
            count, before, ids = frame.get("count", 50), frame.get("before"), frame.get("ids")
            after = frame.get("after")
            if not isinstance(stream, str) or not stream or not isinstance(group, str):
                raise ValueError("stream and group are required")
            if source not in ("stream", "dlq"):
                raise ValueError("source must be stream or dlq")
            if not isinstance(count, int) or isinstance(count, bool) or not 1 <= count <= 200:
                raise ValueError("count must be between 1 and 200")
            for name, cursor in (("before", before), ("after", after)):
                if cursor is not None and not isinstance(cursor, str):
                    raise ValueError(f"{name} must be an entry id")
            if before is not None and after is not None:
                raise ValueError("give before or after, not both")
            if ids is not None and (not isinstance(ids, list) or len(ids) > 200
                                    or not all(isinstance(i, str) for i in ids)):
                raise ValueError("ids must be a list of at most 200 entry ids")
            response.update(self.control.browse(stream, group, source, before, count, ids, after))
        except Exception as e:
            response["error"] = str(e)
        return response
