"""``rillkit-server`` (or ``python -m rillkit_server``): run the server with uvicorn."""

import logging
import os
import sys

import uvicorn

from .config import ServerSettings, SettingsError


def main() -> None:
    logging.basicConfig(level=os.environ.get("RILLKIT_SERVER_LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        settings = ServerSettings.from_env()
    except SettingsError as e:
        sys.exit(f"rillkit-server: {e}")
    from .app import create_app

    uvicorn.run(create_app(settings), host=os.environ.get("RILLKIT_SERVER_HOST", "0.0.0.0"),
                port=int(os.environ.get("RILLKIT_SERVER_PORT", "8080")),
                proxy_headers=True, ws_ping_interval=20, ws_ping_timeout=20)


if __name__ == "__main__":
    main()
