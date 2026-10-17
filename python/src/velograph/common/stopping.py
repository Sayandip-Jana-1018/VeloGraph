"""Graceful stop for long-running loops: console signals, or a sentinel file for windowless processes."""

from __future__ import annotations

import logging
import signal
import time
from pathlib import Path

log = logging.getLogger("velograph.stopping")


class StopFlag:
    """Set by Ctrl+C / SIGTERM (interactive runs) or by the sentinel file appearing (windowless runs).

    A windowless process on Windows receives no console signals, so the control scripts ask it to stop by
    creating the sentinel file instead.
    """

    def __init__(self, sentinel: Path | None = None) -> None:
        self._sentinel = sentinel
        self._signalled = False
        signal.signal(signal.SIGINT, self._handle)
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, self._handle)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum: int, _frame: object) -> None:
        log.info("signal %s received, stopping after the current step", signum)
        self._signalled = True

    def set(self) -> None:
        self._signalled = True

    def __bool__(self) -> bool:
        return self._signalled or (self._sentinel is not None and self._sentinel.exists())

    def sleep(self, seconds: float) -> None:
        """Sleeps up to ``seconds``, waking early once the flag is set."""
        deadline = time.monotonic() + seconds
        while not self and time.monotonic() < deadline:
            time.sleep(min(1.0, max(deadline - time.monotonic(), 0)))
