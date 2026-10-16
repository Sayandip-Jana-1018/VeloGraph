"""Serving exports (ARCHITECTURE §10): the JSON files the website and the report read.

Every file has the envelope the Scala jobs write too (``velograph.common.ServingJson``): ``meta`` (schema,
version, generation time, producing job, optional source range and row count) and ``data``. A file is replaced
atomically, so a reader never sees half of one.
"""

from __future__ import annotations

import json
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def write_serving(
    path: Path,
    schema: str,
    job: str,
    data: Any,
    *,
    rows: int | None = None,
    source: dict[str, str] | None = None,
) -> Path:
    meta: dict[str, Any] = {
        "schema": f"velograph.{schema}",
        "version": 1,
        "generatedAt": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "job": job,
    }
    if source is not None:
        meta["source"] = source
    if rows is not None:
        meta["rows"] = rows
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=path.parent, delete=False, suffix=".tmp", encoding="utf-8"
    ) as tmp:
        # NaN and Infinity are not JSON: the website's parser would reject the whole file. Refuse them here.
        json.dump({"meta": meta, "data": data}, tmp, indent=2, allow_nan=False)
    return Path(tmp.name).replace(path)
