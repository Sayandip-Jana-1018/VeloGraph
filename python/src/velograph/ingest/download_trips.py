"""Download Citi Bike monthly trip archives into ``data/raw/citibike/<yyyymm>/``.

The archives on S3 are ~1 GB *stored* (uncompressed) zips holding CSV parts of about one million rows each.
The downloader is

* resumable  - an interrupted transfer continues from its ``.part`` file with an HTTP Range request,
* verified   - byte size always; MD5 as well when S3's ETag is a plain digest (single-part uploads),
* idempotent - months already extracted with matching sizes are skipped,
* recorded   - ``manifest.json`` lists every CSV with its size and row count, so jobs know what they read.

Usage (from ``python/``)::

    uv run python -m velograph.ingest.download_trips 202609 202606 202607 202608
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import time
import zipfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import httpx
import typer

from velograph.common.http import http_client, retrying
from velograph.common.logs import configure_logging
from velograph.common.settings import data_paths

ARCHIVE_URL = "https://s3.amazonaws.com/tripdata/{ym}-citibike-tripdata.zip"
CHUNK = 1 << 20  # 1 MiB
_MONTH = re.compile(r"^20\d{2}(0[1-9]|1[0-2])$")
_MD5 = re.compile(r"^[0-9a-f]{32}$")

log = logging.getLogger("velograph.download_trips")


@dataclass(frozen=True)
class RemoteArchive:
    url: str
    size: int
    etag: str

    @property
    def md5(self) -> str | None:
        """S3 ETags equal the MD5 digest only for single-part uploads (no ``-<parts>`` suffix)."""
        tag = self.etag.strip('"')
        return tag if _MD5.match(tag) else None


@dataclass(frozen=True)
class CsvPart:
    name: str
    bytes: int
    rows: int


class _Progress:
    """Logs every 10 % so a 1 GB transfer stays visible without flooding the log."""

    def __init__(self, label: str, total: int, already: int) -> None:
        self._label, self._total, self._done = label, total, already
        self._started, self._start_bytes = time.monotonic(), already
        self._next_pct = (already * 100 // total) // 10 * 10 + 10

    def advance(self, n: int) -> None:
        self._done += n
        pct = self._done * 100 // self._total
        if pct >= self._next_pct:
            elapsed = max(time.monotonic() - self._started, 1e-6)
            rate = (self._done - self._start_bytes) / elapsed / 1e6
            log.info(
                "%s %3d%% (%.0f / %.0f MB, %.1f MB/s)",
                self._label,
                pct,
                self._done / 1e6,
                self._total / 1e6,
                rate,
            )
            self._next_pct = pct // 10 * 10 + 10


@retrying(attempts=8, max_wait=60)
def probe(client: httpx.Client, url: str) -> RemoteArchive:
    response = client.head(url)
    response.raise_for_status()
    return RemoteArchive(
        url=url, size=int(response.headers["Content-Length"]), etag=response.headers.get("ETag", "")
    )


@retrying(attempts=8, max_wait=60)
def fetch_into(client: httpx.Client, remote: RemoteArchive, part: Path) -> None:
    """Append the missing byte range of ``remote`` to ``part``.

    Every retry re-reads the size of ``part``, so a dropped connection resumes instead of starting over.
    """
    have = part.stat().st_size if part.exists() else 0
    if have > remote.size:  # stale file from a different upload of the same name
        part.unlink()
        have = 0
    if have == remote.size:
        return
    headers = {"Range": f"bytes={have}-"} if have else {}
    with client.stream("GET", remote.url, headers=headers) as response:
        response.raise_for_status()
        if have and response.status_code != httpx.codes.PARTIAL_CONTENT:
            have = 0  # server ignored the Range header: restart cleanly
        progress = _Progress(part.name.removesuffix(".part"), remote.size, have)
        with part.open("ab" if have else "wb") as fh:
            for chunk in response.iter_bytes(CHUNK):
                fh.write(chunk)
                progress.advance(len(chunk))
    if part.stat().st_size != remote.size:
        raise httpx.ReadError(f"incomplete transfer: {part.stat().st_size} of {remote.size} bytes")


def verify(path: Path, remote: RemoteArchive) -> None:
    size = path.stat().st_size
    if size != remote.size:
        raise RuntimeError(f"{path.name}: expected {remote.size} bytes, found {size}")
    if remote.md5:
        digest = hashlib.md5(usedforsecurity=False)
        with path.open("rb") as fh:
            while block := fh.read(8 * CHUNK):
                digest.update(block)
        if digest.hexdigest() != remote.md5:
            raise RuntimeError(f"{path.name}: MD5 mismatch")


def extract_csvs(archive: Path, out_dir: Path) -> list[Path]:
    """Extract CSV entries by basename (archive paths are never trusted), skipping files already present."""
    out_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    with zipfile.ZipFile(archive) as zf:
        for info in zf.infolist():
            name = Path(info.filename).name
            if info.is_dir() or info.filename.startswith("__MACOSX") or not name.lower().endswith(".csv"):
                continue
            target = out_dir / name
            if not (target.exists() and target.stat().st_size == info.file_size):
                tmp = target.with_name(target.name + ".tmp")
                with zf.open(info) as src, tmp.open("wb") as dst:
                    shutil.copyfileobj(src, dst, CHUNK)
                tmp.replace(target)
            extracted.append(target)
    return sorted(extracted)


def count_rows(csv: Path) -> int:
    """Data rows in a CSV with one header line. Citi Bike fields never contain embedded newlines."""
    newlines, last = 0, b"\n"
    with csv.open("rb") as fh:
        while block := fh.read(8 * CHUNK):
            newlines += block.count(b"\n")
            last = block[-1:]
    lines = newlines + (0 if last == b"\n" else 1)
    return max(lines - 1, 0)


def _load_manifest(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"schema": "velograph.raw-citibike-manifest", "version": 1, "months": {}}


def _save_manifest(path: Path, manifest: dict[str, Any]) -> None:
    manifest["updatedAt"] = datetime.now(UTC).isoformat(timespec="seconds")
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    tmp.replace(path)


def _is_complete(entry: dict[str, Any] | None, remote: RemoteArchive, month_dir: Path) -> bool:
    if not entry or entry.get("etag") != remote.etag or entry.get("archiveBytes") != remote.size:
        return False
    return all(
        (month_dir / f["name"]).exists() and (month_dir / f["name"]).stat().st_size == f["bytes"]
        for f in entry["files"]
    )


def fetch_month(client: httpx.Client, ym: str, root: Path, *, keep_archive: bool) -> int:
    """Download, verify, extract and record one month. Returns its row count."""
    manifest_path = root / "manifest.json"
    month_dir = root / ym
    remote = probe(client, ARCHIVE_URL.format(ym=ym))

    manifest = _load_manifest(manifest_path)
    if _is_complete(manifest["months"].get(ym), remote, month_dir):
        rows = manifest["months"][ym]["rows"]
        log.info("%s already complete (%s rows), skipping", ym, f"{rows:,}")
        return rows

    downloads = root / "_downloads"
    downloads.mkdir(parents=True, exist_ok=True)
    archive = downloads / f"{ym}-citibike-tripdata.zip"
    if not (archive.exists() and archive.stat().st_size == remote.size):
        part = archive.with_name(archive.name + ".part")
        log.info("%s downloading %.0f MB from %s", ym, remote.size / 1e6, remote.url)
        fetch_into(client, remote, part)
        verify(part, remote)
        part.replace(archive)

    parts = [CsvPart(p.name, p.stat().st_size, count_rows(p)) for p in extract_csvs(archive, month_dir)]
    rows = sum(p.rows for p in parts)

    manifest = _load_manifest(manifest_path)  # re-read: another process may have recorded a different month
    manifest["months"][ym] = {
        "url": remote.url,
        "etag": remote.etag,
        "archiveBytes": remote.size,
        "rows": rows,
        "files": [asdict(p) for p in parts],
        "extractedAt": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    _save_manifest(manifest_path, manifest)
    if not keep_archive:
        archive.unlink()
    log.info("%s done: %d files, %s rows", ym, len(parts), f"{rows:,}")
    return rows


def main(
    months: Annotated[list[str], typer.Argument(help="Months as YYYYMM, processed in the order given.")],
    keep_archives: Annotated[bool, typer.Option(help="Keep each zip after extraction.")] = False,
) -> None:
    paths = data_paths()
    configure_logging("download_trips", log_dir=paths.logs)
    bad = [m for m in months if not _MONTH.match(m)]
    if bad:
        raise typer.BadParameter(f"not YYYYMM: {', '.join(bad)}")

    started = time.monotonic()
    total = 0
    with http_client(timeout=60) as client:
        for ym in months:
            total += fetch_month(client, ym, paths.citibike_raw, keep_archive=keep_archives)
    log.info("all done: %d months, %s rows in %.0f s", len(months), f"{total:,}", time.monotonic() - started)


if __name__ == "__main__":
    typer.run(main)
