"""Record the live Citi Bike GBFS feed to disk for offline replay and live backtesting.

Layout, one folder per New York calendar day::

    data/raw/gbfs_recordings/2026-10-15/
        station_status.jsonl.gz     one JSON line per poll: a full snapshot or a delta of changed stations
        weather_current.jsonl.gz    Open-Meteo current conditions + the next hours, every 15 minutes
        station_information.json    station metadata, captured once per day

Each poll is appended as its own gzip member. Concatenated members form a valid gzip stream, so ``gzip.open``
reads a day as one JSONL file and a crash can at worst lose the final line. Every day file starts with a
snapshot, so a single day can be replayed on its own.

Run it through ``scripts/recorder.ps1 start|stop|status``. Stop requests arrive as a sentinel file because a
windowless process on Windows receives no console signals.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from types import TracebackType
from typing import Annotated, Any
from zoneinfo import ZoneInfo

import httpx
import typer

from velograph.common.http import http_client, retrying
from velograph.common.logs import configure_logging
from velograph.common.settings import data_paths
from velograph.ingest.gbfs import GbfsClient, InventoryKey, select_changes

NYC = ZoneInfo("America/New_York")

# Central Park, the reference point for all NYC weather in VeloGraph.
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_PARAMS: dict[str, str | int | float] = {
    "latitude": 40.7812,
    "longitude": -73.9665,
    "timezone": "America/New_York",
    "current": "temperature_2m,apparent_temperature,precipitation,rain,wind_speed_10m,wind_gusts_10m,"
    "relative_humidity_2m,cloud_cover,weather_code",
    "hourly": "temperature_2m,apparent_temperature,precipitation,rain,wind_speed_10m,relative_humidity_2m,"
    "cloud_cover,weather_code",
    "forecast_hours": 3,
}

log = logging.getLogger("velograph.gbfs_recorder")


def append_jsonl_gz(path: Path, record: dict[str, Any]) -> int:
    """Append ``record`` as one gzip member holding one JSON line; returns the compressed size."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = gzip.compress((json.dumps(record, separators=(",", ":")) + "\n").encode("utf-8"))
    with path.open("ab") as fh:
        fh.write(payload)
        fh.flush()
        os.fsync(fh.fileno())
    return len(payload)


class AlreadyRunningError(RuntimeError):
    pass


class SingleInstance:
    """An OS-level file lock, released by the OS when the process dies: a crash never leaves a stale lock."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh: Any = None

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fh = self._path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt

                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            fh.close()
            raise AlreadyRunningError(f"another recorder holds {self._path}") from exc
        self._fh = fh

    def release(self) -> None:
        if self._fh is not None:
            self._fh.close()  # closing the handle releases the lock on both platforms
            self._fh = None

    def __enter__(self) -> SingleInstance:
        self.acquire()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.release()


@dataclass
class RecorderStats:
    polls: int = 0
    skipped: int = 0
    failures: int = 0
    bytes_written: int = 0


@dataclass
class Recorder:
    gbfs: GbfsClient
    http: httpx.Client
    out_root: Path
    snapshot_every: int = 360
    weather_every_s: float = 900.0
    stats: RecorderStats = field(default_factory=RecorderStats)
    _inventory: dict[str, InventoryKey] = field(default_factory=dict)
    _polls_since_snapshot: int = 0
    _last_feed_update: int | None = None
    _status_day: date | None = None
    _info_day: date | None = None
    _last_weather: float = float("-inf")

    def day_dir(self, now: datetime) -> Path:
        return self.out_root / now.astimezone(NYC).date().isoformat()

    def poll_status(self) -> None:
        payload = self.gbfs.fetch("station_status")
        now = datetime.now(UTC)
        updated = payload.get("last_updated")
        if updated is not None and updated == self._last_feed_update:
            self.stats.skipped += 1
            return
        today = now.astimezone(NYC).date()
        force = self._polls_since_snapshot >= self.snapshot_every or today != self._status_day
        stations = payload["data"]["stations"]
        kind, changed, self._inventory = select_changes(self._inventory, stations, force_snapshot=force)
        record = {
            "kind": kind,
            "fetched_at": now.isoformat(timespec="seconds"),
            "last_updated": updated,
            "ttl": payload.get("ttl"),
            "total_stations": len(stations),
            "count": len(changed),
            "stations": changed,
        }
        written = append_jsonl_gz(self.day_dir(now) / "station_status.jsonl.gz", record)
        self._polls_since_snapshot = 0 if kind == "snapshot" else self._polls_since_snapshot + 1
        self._last_feed_update, self._status_day = updated, today
        self.stats.polls += 1
        self.stats.bytes_written += written
        log.info("%-8s %4d/%d stations, %.1f KB", kind, len(changed), len(stations), written / 1024)

    def record_station_information(self) -> None:
        now = datetime.now(UTC)
        today = now.astimezone(NYC).date()
        if today == self._info_day:
            return
        target = self.day_dir(now) / "station_information.json"
        if not target.exists():
            payload = self.gbfs.fetch("station_information")
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_name(target.name + ".tmp")
            tmp.write_text(json.dumps(payload), encoding="utf-8")
            tmp.replace(target)
            log.info("station_information captured: %d stations", len(payload["data"]["stations"]))
        self._info_day = today

    def poll_weather(self) -> None:
        if time.monotonic() - self._last_weather < self.weather_every_s:
            return
        payload = self._fetch_weather()
        now = datetime.now(UTC)
        append_jsonl_gz(
            self.day_dir(now) / "weather_current.jsonl.gz",
            {"fetched_at": now.isoformat(timespec="seconds"), **payload},
        )
        self._last_weather = time.monotonic()
        log.info(
            "weather   %s C, %s mm", payload["current"]["temperature_2m"], payload["current"]["precipitation"]
        )

    @retrying()
    def _fetch_weather(self) -> dict[str, Any]:
        response = self.http.get(WEATHER_URL, params=WEATHER_PARAMS)
        response.raise_for_status()
        return response.json()


class _StopFlag:
    """Set by console signals (interactive runs) or by the sentinel file (windowless runs)."""

    def __init__(self, sentinel: Path) -> None:
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

    def __bool__(self) -> bool:
        return self._signalled or self._sentinel.exists()

    def sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self and time.monotonic() < deadline:
            time.sleep(min(1.0, max(deadline - time.monotonic(), 0)))


def main(
    interval: Annotated[
        float, typer.Option(help="Seconds between station_status polls (feed ttl is 60).")
    ] = 60.0,
    snapshot_every: Annotated[int, typer.Option(help="Polls between full snapshots.")] = 360,
    weather_every: Annotated[float, typer.Option(help="Seconds between weather polls.")] = 900.0,
) -> None:
    paths = data_paths()
    configure_logging("gbfs_recorder", log_dir=paths.logs)
    run_dir = paths.run
    pid_file, stop_file = run_dir / "gbfs_recorder.pid", run_dir / "gbfs_recorder.stop"

    lock = SingleInstance(run_dir / "gbfs_recorder.lock")
    try:
        lock.acquire()
    except AlreadyRunningError as exc:
        log.error("%s; refusing to start a second recorder", exc)
        raise typer.Exit(code=2) from exc

    stop_file.unlink(missing_ok=True)
    stop = _StopFlag(stop_file)
    pid_info = {
        "pid": os.getpid(),
        "startedAt": datetime.now(UTC).isoformat(timespec="seconds"),
        "interval": interval,
        "executable": sys.executable,
    }
    pid_file.write_text(json.dumps(pid_info), encoding="utf-8")
    log.info("recorder started (pid %d, every %.0f s) -> %s", os.getpid(), interval, paths.gbfs_recordings)
    try:
        with http_client(timeout=30) as http:
            recorder = Recorder(
                GbfsClient(http),
                http,
                paths.gbfs_recordings,
                snapshot_every=snapshot_every,
                weather_every_s=weather_every,
            )
            while not stop:
                started = time.monotonic()
                for step in (
                    recorder.poll_status,
                    recorder.record_station_information,
                    recorder.poll_weather,
                ):
                    try:
                        step()
                    except Exception:  # one failed poll must never end a multi-day recording
                        recorder.stats.failures += 1
                        log.exception("%s failed", step.__name__)
                stop.sleep(interval - (time.monotonic() - started))
            s = recorder.stats
            log.info(
                "recorder stopped: %d polls, %d unchanged, %d failures, %.1f MB written",
                s.polls,
                s.skipped,
                s.failures,
                s.bytes_written / 1e6,
            )
    finally:
        pid_file.unlink(missing_ok=True)
        stop_file.unlink(missing_ok=True)
        lock.release()


if __name__ == "__main__":
    typer.run(main)
