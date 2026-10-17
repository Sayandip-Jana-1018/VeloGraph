"""Citi Bike GBFS -> Kafka (roadmap P5.2, ARCHITECTURE §9.3).

Two sources produce the same messages:

* ``live`` (pull-to-push) polls ``station_status`` as often as the feed's ``ttl`` allows. It publishes only
  the stations whose inventory changed (change data capture), plus a full snapshot every ``--snapshot-every``
  publishes. Every ``--weather-every`` seconds it also publishes the NYC weather: current conditions and the
  next hours of the forecast.
* ``recorded`` (record & replay) plays back a day captured by the GBFS recorder, ``--speed`` times faster,
  keeping the recorded gaps. Its messages say ``"source": "replay"`` and keep their recorded times, so the
  scoring job and the website show them as a replay.

Station messages are keyed by the lake's station id, so all of one station's updates land in one partition and
stay in order. A record that cannot become a message goes to the dead-letter topic with the reason. The
producer is idempotent with ``acks=all`` (``velograph.common.kafka``) and flushes before it exits.

    uv run python -m velograph.ingest.gbfs_producer
    uv run python -m velograph.ingest.gbfs_producer --source recorded --day 2026-10-16 --start 07:30
"""

from __future__ import annotations

import gzip
import heapq
import json
import logging
import statistics
import time
from collections import Counter, deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any

import httpx
import typer

from velograph.common.http import http_client, retrying
from velograph.common.kafka import JsonProducer, Topics, now_iso
from velograph.common.logs import configure_logging
from velograph.common.serving import write_serving
from velograph.common.settings import data_paths
from velograph.common.stopping import StopFlag
from velograph.ingest.gbfs import GbfsClient, InventoryKey, StationInfo, select_changes, station_registry
from velograph.ingest.gbfs_recorder import NYC, WEATHER_PARAMS, WEATHER_URL

log = logging.getLogger("velograph.gbfs_producer")

STATUS_SCHEMA = "velograph.station-status"
WEATHER_SCHEMA = "velograph.weather"

# Open-Meteo name -> the lake's name (silver/weather_hourly), so live features line up with training ones.
WEATHER_FIELDS: dict[str, str] = {
    "temperature_2m": "temp_c",
    "apparent_temperature": "apparent_temp_c",
    "precipitation": "precip_mm",
    "wind_speed_10m": "wind_kmh",
    "relative_humidity_2m": "humidity_pct",
    "cloud_cover": "cloud_pct",
    "weather_code": "weather_code",
}
RAIN_MM = 0.1  # SilverWeatherJob's rule for is_raining


class Source(StrEnum):
    LIVE = "live"
    RECORDED = "recorded"


class PoisonRecordError(ValueError):
    """A feed record that cannot become a message."""


def status_message(
    raw: dict[str, Any],
    registry: dict[str, StationInfo],
    *,
    fetched_at: float,
    feed_updated: int | None,
    kind: str,
    source: str,
) -> dict[str, Any]:
    """One station's status as a Kafka message, joined to its lake id, name, position and capacity."""
    gbfs_id = raw.get("station_id")
    info = registry.get(gbfs_id) if isinstance(gbfs_id, str) else None
    if info is None:
        raise PoisonRecordError(f"station {gbfs_id!r} is not listed in station_information")
    try:
        return {
            "schema": STATUS_SCHEMA,
            "v": 1,
            "source": source,
            "kind": kind,
            "station_id": info.station_id,
            "gbfs_id": gbfs_id,
            "name": info.name,
            "lat": info.lat,
            "lon": info.lon,
            "capacity": info.capacity,
            "bikes": int(raw["num_bikes_available"]),
            "ebikes": int(raw.get("num_ebikes_available") or 0),
            "bikes_disabled": int(raw.get("num_bikes_disabled") or 0),
            "docks": int(raw["num_docks_available"]),
            "docks_disabled": int(raw.get("num_docks_disabled") or 0),
            "is_installed": bool(raw["is_installed"]),
            "is_renting": bool(raw["is_renting"]),
            "is_returning": bool(raw["is_returning"]),
            "last_reported": int(raw["last_reported"]),
            "fetched_at": round(fetched_at, 3),
            "feed_updated": feed_updated,
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise PoisonRecordError(f"station {gbfs_id}: {type(exc).__name__}: {exc}") from exc


def weather_message(payload: dict[str, Any], *, fetched_at: float, source: str) -> dict[str, Any]:
    """Open-Meteo current conditions and hourly forecast, in the lake's column names."""
    current = payload["current"]
    hourly = payload["hourly"]
    hours = []
    for i, hour in enumerate(hourly["time"]):
        values = {target: hourly[name][i] for name, target in WEATHER_FIELDS.items()}
        values["is_raining"] = (values["precip_mm"] or 0.0) >= RAIN_MM
        hours.append({"hour": hour, **values})
    return {
        "schema": WEATHER_SCHEMA,
        "v": 1,
        "source": source,
        "fetched_at": round(fetched_at, 3),
        "observed": current["time"],
        "current": {target: current.get(name) for name, target in WEATHER_FIELDS.items()},
        "hours": hours,
    }


def next_poll_delay(feed_updated: int | None, ttl: int | None, now: float, *, floor: float = 10.0) -> float:
    """Seconds until the feed is due to change again (``last_updated + ttl``), never less than ``floor``."""
    if feed_updated is None or not ttl:
        return 60.0
    return max(floor, feed_updated + ttl - now + 1.0)


@dataclass
class FeedClock:
    """This machine's clock corrected to the feed server's, from the ``Date`` header of each response.

    The laptop's clock can be minutes off (it was 92 s slow when this was written), while ``last_updated``
    and ``last_reported`` come from the server. Timing polls and stamping ``fetched_at`` on a corrected clock
    keeps every time in a message on one clock. The header has whole seconds, so the median offset is used.
    """

    _offsets: deque[float] = field(default_factory=lambda: deque(maxlen=15))

    def observe(self, server: float | None, sent: float, received: float) -> None:
        if server is not None:
            # The header is truncated to the second: on average the server's time was half a second later.
            self._offsets.append(server + 0.5 - (sent + received) / 2)

    @property
    def offset(self) -> float:
        return statistics.median(self._offsets) if self._offsets else 0.0

    def now(self) -> float:
        return time.time() + self.offset


def weather_key(source: str) -> str:
    """The weather topic keeps the latest value per key: a replay must not overwrite live weather."""
    return "nyc" if source == "live" else "nyc-replay"


@dataclass
class Publisher:
    """Turns full station lists into CDC messages: changed stations, and a full snapshot every N publishes."""

    producer: JsonProducer
    source: str
    registry: dict[str, StationInfo]
    snapshot_every: int = 30
    counts: Counter[str] = field(default_factory=Counter)
    _inventory: dict[str, InventoryKey] = field(default_factory=dict)
    _since_snapshot: int = 0

    def unknown(self, stations: list[dict[str, Any]]) -> int:
        return sum(1 for s in stations if s.get("station_id") not in self.registry)

    def publish_status(
        self, stations: list[dict[str, Any]], *, fetched_at: float, feed_updated: int | None
    ) -> tuple[str, int]:
        force = self._since_snapshot >= self.snapshot_every - 1
        kind, changed, self._inventory = select_changes(self._inventory, stations, force_snapshot=force)
        sent = 0
        for raw in changed:
            try:
                msg = status_message(
                    raw,
                    self.registry,
                    fetched_at=fetched_at,
                    feed_updated=feed_updated,
                    kind=kind,
                    source=self.source,
                )
            except PoisonRecordError as exc:
                self.producer.dead_letter(str(exc), json.dumps(raw), origin=Topics.STATION_STATUS)
                self.counts["dead_letters"] += 1
                continue
            self.producer.send(Topics.STATION_STATUS, msg["station_id"], msg)
            sent += 1
        self._since_snapshot = 0 if kind == "snapshot" else self._since_snapshot + 1
        self.counts[kind + "s"] += 1
        self.counts["station_messages"] += sent
        # Delivery reports are served by poll() and flush(): waiting here confirms this poll's messages before
        # the producer sleeps, so the measured ack latency is the brokers', not the time between polls.
        self.producer.flush(30)
        return kind, sent

    def publish_weather(self, payload: dict[str, Any], *, fetched_at: float) -> None:
        try:
            msg = weather_message(payload, fetched_at=fetched_at, source=self.source)
        except (KeyError, TypeError, IndexError) as exc:
            self.producer.dead_letter(f"weather: {type(exc).__name__}: {exc}", json.dumps(payload))
            self.counts["dead_letters"] += 1
            return
        self.producer.send(Topics.WEATHER, weather_key(self.source), msg)
        self.producer.flush(30)
        self.counts["weather_messages"] += 1


# ---- live ----------------------------------------------------------------------------------------------


@dataclass
class LiveFeed:
    gbfs: GbfsClient
    http: httpx.Client
    weather_every_s: float = 900.0
    clock: FeedClock = field(default_factory=FeedClock)
    _last_weather: float = float("-inf")
    _last_registry: float = float("-inf")
    _last_feed_update: int | None = None

    def registry(self, *, older_than: float) -> dict[str, StationInfo] | None:
        """A fresh station registry if the current one is older than ``older_than`` seconds, else None."""
        if time.monotonic() - self._last_registry < older_than:
            return None
        self._last_registry = time.monotonic()
        return station_registry(self.gbfs.fetch("station_information"))

    @retrying()
    def _weather(self) -> dict[str, Any]:
        response = self.http.get(WEATHER_URL, params=WEATHER_PARAMS)
        response.raise_for_status()
        return response.json()

    def weather(self) -> dict[str, Any] | None:
        if time.monotonic() - self._last_weather < self.weather_every_s:
            return None
        payload = self._weather()
        self._last_weather = time.monotonic()
        return payload

    def status(self) -> tuple[dict[str, Any] | None, float]:
        """The next station_status payload (None while unchanged) and the wait until the next poll."""
        sent = time.time()
        payload, server = self.gbfs.fetch_with_clock("station_status")
        self.clock.observe(server, sent, time.time())
        updated, ttl = payload.get("last_updated"), payload.get("ttl")
        if updated is not None and updated == self._last_feed_update:
            return None, 10.0
        self._last_feed_update = updated
        return payload, next_poll_delay(updated, ttl, self.clock.now())


def run_live(publisher: Publisher, feed: LiveFeed, stop: StopFlag, report: StatsReport) -> None:
    def refresh_registry() -> None:
        if (fresh := feed.registry(older_than=3600)) is not None:
            publisher.registry = fresh

    def weather() -> None:
        if (payload := feed.weather()) is not None:
            publisher.publish_weather(payload, fetched_at=feed.clock.now())

    delay = 30.0

    def status() -> None:
        nonlocal delay
        payload, delay = feed.status()
        if payload is None:
            publisher.counts["unchanged_polls"] += 1
            return
        stations = payload["data"]["stations"]
        # A station added since the last registry fetch: look again, at most every 5 minutes.
        if publisher.unknown(stations) and (fresh := feed.registry(older_than=300)) is not None:
            publisher.registry = fresh
        publisher.publish_status(
            stations, fetched_at=feed.clock.now(), feed_updated=payload.get("last_updated")
        )

    while not stop:
        delay = 30.0  # after a failed poll
        for step in (refresh_registry, weather, status):
            try:
                step()
            except Exception:  # one failed step must never end the stream; the next round retries it
                publisher.counts["failed_" + step.__name__] += 1
                log.exception("%s failed", step.__name__)
        report.maybe_log(publisher)
        stop.sleep(delay)


# ---- recorded ------------------------------------------------------------------------------------------


@dataclass(frozen=True, order=True)
class Recorded:
    at: float  # the recorded fetch time, epoch seconds
    order: int  # ties: weather before stations
    kind: str  # "status" or "weather"
    payload: dict[str, Any] = field(compare=False)


def _read_jsonl_gz(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield json.loads(line)


def _epoch(iso: str) -> float:
    return datetime.fromisoformat(iso).timestamp()


def recorded_events(day_dir: Path) -> Iterator[Recorded]:
    """A recorded day's polls and weather fetches, merged in fetch order."""
    status = (
        Recorded(_epoch(r["fetched_at"]), 1, "status", r)
        for r in _read_jsonl_gz(day_dir / "station_status.jsonl.gz")
    )
    weather = (
        Recorded(_epoch(r["fetched_at"]), 0, "weather", r)
        for r in _read_jsonl_gz(day_dir / "weather_current.jsonl.gz")
    )
    return heapq.merge(status, weather)


@dataclass
class Replay:
    """Rebuilds the full station list poll by poll: the publisher sees what a live poll would have given."""

    stations: dict[str, dict[str, Any]] = field(default_factory=dict)
    weather: dict[str, Any] | None = None

    def apply(self, event: Recorded) -> None:
        if event.kind == "weather":
            self.weather = event.payload
            return
        if event.payload["kind"] == "snapshot":
            self.stations = {}
        for s in event.payload["stations"]:
            self.stations[s["station_id"]] = s


def run_recorded(
    publisher: Publisher,
    day_dir: Path,
    *,
    start: datetime | None,
    until: datetime | None,
    speed: float,
    stop: StopFlag,
    report: StatsReport,
) -> None:
    replay = Replay()
    previous: float | None = None
    started = False
    for event in recorded_events(day_dir):
        if stop or (until is not None and event.at > until.timestamp()):
            break
        replay.apply(event)
        if start is not None and event.at < start.timestamp():
            continue  # fold the day up to the start time into the state, silently
        if not started:
            started = True
            if replay.weather is not None and event.kind != "weather":
                publisher.publish_weather(replay.weather, fetched_at=_epoch(replay.weather["fetched_at"]))
            log.info(
                "replaying %s from %s",
                day_dir.name,
                datetime.fromtimestamp(event.at, NYC).strftime("%H:%M:%S"),
            )
        if previous is not None:
            stop.sleep(max(0.0, event.at - previous) / speed)
        previous = event.at
        if event.kind == "weather":
            publisher.publish_weather(event.payload, fetched_at=event.at)
        else:
            publisher.publish_status(
                list(replay.stations.values()),
                fetched_at=event.at,
                feed_updated=event.payload.get("last_updated"),
            )
        report.maybe_log(publisher)
    log.info("replay finished")


# ---- reporting -----------------------------------------------------------------------------------------


@dataclass
class StatsReport:
    """Logs throughput every minute and keeps ``serving/live/producer-stats.json`` current for the gateway."""

    path: Path
    source: str
    clock: FeedClock | None = None  # live only
    every_s: float = 60.0
    started_at: str = field(default_factory=now_iso)
    _last: float = field(default_factory=time.monotonic)

    def maybe_log(self, publisher: Publisher, *, force: bool = False) -> None:
        if not force and time.monotonic() - self._last < self.every_s:
            return
        self._last = time.monotonic()
        publisher.producer.poll(0)
        s = publisher.producer.stats
        c = publisher.counts
        log.info(
            "%d snapshots, %d deltas, %d station + %d weather messages; delivered %d, failed %d, "
            "dead letters %d; ack p50 %.0f ms, p95 %.0f ms; clock offset %+.1f s",
            c["snapshots"],
            c["deltas"],
            c["station_messages"],
            c["weather_messages"],
            s.delivered,
            s.failed,
            c["dead_letters"],
            s.latency_ms(0.5),
            s.latency_ms(0.95),
            self.clock.offset if self.clock else 0.0,
        )
        write_serving(
            self.path,
            "producer-stats",
            "gbfs_producer",
            {
                "source": self.source,
                "startedAt": self.started_at,
                "counts": dict(c),
                "delivered": s.delivered,
                "failed": s.failed,
                "bytes": s.bytes,
                "byPartition": [
                    {"topic": t, "partition": p, "messages": n}
                    for (t, p), n in sorted(s.by_partition.items())
                ],
                "ackMs": {"p50": round(s.latency_ms(0.5), 1), "p95": round(s.latency_ms(0.95), 1)},
                "clockOffsetS": round(self.clock.offset, 1) if self.clock else None,
                "errors": dict(s.errors),
            },
        )


def _at(day: date, clock: str | None) -> datetime | None:
    if clock is None:
        return None
    hours, minutes = (int(x) for x in clock.split(":"))
    return datetime(day.year, day.month, day.day, tzinfo=NYC) + timedelta(hours=hours, minutes=minutes)


def main(
    source: Annotated[
        Source, typer.Option(help="live: the GBFS feed; recorded: a day from the recorder.")
    ] = (Source.LIVE),
    day: Annotated[str | None, typer.Option(help="Recorded day (New York date), e.g. 2026-10-16.")] = None,
    start: Annotated[str | None, typer.Option(help="Recorded: New York time to start from, HH:MM.")] = None,
    until: Annotated[str | None, typer.Option(help="Recorded: New York time to stop at, HH:MM.")] = None,
    speed: Annotated[float, typer.Option(help="Recorded: replay speed factor.")] = 10.0,
    snapshot_every: Annotated[int, typer.Option(help="Publishes between full snapshots.")] = 30,
    weather_every: Annotated[float, typer.Option(help="Live: seconds between weather fetches.")] = 900.0,
) -> None:
    if source is Source.RECORDED and day is None:
        raise typer.BadParameter("--day is required with --source recorded")
    paths = data_paths()
    configure_logging("gbfs_producer", log_dir=paths.logs)
    sentinel = paths.run / "gbfs_producer.stop"
    sentinel.unlink(missing_ok=True)
    stop = StopFlag(sentinel)
    label = "live" if source is Source.LIVE else "replay"
    producer = JsonProducer(f"gbfs-producer-{label}")
    report = StatsReport(paths.serving / "live" / "producer-stats.json", label)
    with http_client(timeout=30) as http:
        feed = LiveFeed(GbfsClient(http), http, weather_every_s=weather_every)
        report.clock = feed.clock if source is Source.LIVE else None
        if source is Source.LIVE:
            registry = feed.registry(older_than=0) or {}
        else:
            day_dir = paths.gbfs_recordings / str(day)
            registry = station_registry(
                json.loads((day_dir / "station_information.json").read_text(encoding="utf-8"))
            )
        publisher = Publisher(producer, label, registry, snapshot_every)
        log.info("%s: %d stations registered -> %s", label, len(registry), Topics.STATION_STATUS)
        try:
            if source is Source.LIVE:
                run_live(publisher, feed, stop, report)
            else:
                the_day = date.fromisoformat(str(day))
                run_recorded(
                    publisher,
                    day_dir,
                    start=_at(the_day, start),
                    until=_at(the_day, until),
                    speed=speed,
                    stop=stop,
                    report=report,
                )
        finally:
            left = producer.flush(30)
            report.maybe_log(publisher, force=True)
            log.info("producer stopped; %d messages unconfirmed at exit", left)


if __name__ == "__main__":
    typer.run(main)
