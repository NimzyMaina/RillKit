import time
from collections.abc import Callable
from typing import Any


def wait_until(cond: Callable[[], Any], timeout: float = 10.0, interval: float = 0.05) -> bool:
    """Poll ``cond`` until it is truthy; return the final truthiness instead of raising."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return bool(cond())
