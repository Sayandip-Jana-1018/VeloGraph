"""Logging setup shared by every command-line entry point."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

_FORMAT = "%(asctime)s %(levelname)-7s %(name)s - %(message)s"


def configure_logging(name: str, *, log_dir: Path | None = None, level: int = logging.INFO) -> logging.Logger:
    """Log to the console and, when ``log_dir`` is given, to a size-rotated file ``<log_dir>/<name>.log``."""
    handlers: list[logging.Handler] = []
    # Windowless interpreters (pythonw.exe) have no stderr; a StreamHandler there would fail on every record.
    if sys.stderr is not None:
        handlers.append(logging.StreamHandler())
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        handlers.append(
            RotatingFileHandler(log_dir / f"{name}.log", maxBytes=2_000_000, backupCount=5, encoding="utf-8")
        )
    logging.basicConfig(level=level, format=_FORMAT, handlers=handlers, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger(f"velograph.{name}")
