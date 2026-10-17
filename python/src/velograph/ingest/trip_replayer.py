"""Batch -> stream replay of real trips (roadmap P5.4, ARCHITECTURE §9.3).

Publishes the trips of one New York evening from ``silver/trips`` to ``velograph.trips.replay.v1``, each at
its start time, ``--speed`` times faster (default x60: an hour of trips in a minute), keeping the gaps between
them. Keyed by start station, so one station's trips stay in order within a partition.

To give the streaming job's watermark something to do, a share of trips (``--late-share``, default 2 %) is
held back and sent ``--late-min`` to ``--late-max`` minutes (5-50) after its start, as a dock that lost its
connection would. Spark drops a late trip only once its window has closed, that is when the 15-minute
watermark passes the *window's end*, so trips up to about half an hour late still count and the rest are
dropped and counted. Messages say ``"source": "replay"``; late ones carry ``late`` and ``late_by_s``.

    uv run python -m velograph.ingest.trip_replayer --date 2026-09-17 --start 16:00 --until 20:00
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any

import pyarrow.parquet as pq
import typer

from velograph.common.kafka import JsonProducer, Topics
from velograph.common.logs import configure_logging
from velograph.common.settings import data_paths
from velograph.common.stopping import StopFlag
from velograph.ingest.gbfs_recorder import NYC

log = logging.getLogger("velograph.trip_replayer")

TRIP_SCHEMA = "velograph.trip"
COLUMNS = [
    "ride_id",
    "rideable_type",
    "member_casual",
    "started_at",
    "ended_at",
    "start_station_id",
    "end_station_id",
    "duration_s",
]


@dataclass(frozen=True)
class Trip:
    ride_id: str
    rideable_type: str
    member_casual: str
    started_at: float  # epoch seconds
    ended_at: float
    start_station_id: str
    end_station_id: str
    duration_s: int


@dataclass(frozen=True)
class Scheduled:
    send_at: float  # event-time second at which the replay sends it
    trip: Trip
    late_by_s: float


def read_trips(partition: Path, start: datetime, until: datetime) -> list[Trip]:
    """One day's partition of ``silver/trips``, cut to [start, until) and ordered by start time."""
    table = pq.read_table(partition, columns=COLUMNS)
    # Spark stores instants; pyarrow reads them as naive UTC nanoseconds.
    started = table.column("started_at").cast("int64").to_pylist()
    ended = table.column("ended_at").cast("int64").to_pylist()
    rows = table.drop_columns(["started_at", "ended_at"]).to_pylist()
    lo, hi = start.timestamp(), until.timestamp()
    trips = [
        Trip(
            ride_id=r["ride_id"],
            rideable_type=r["rideable_type"],
            member_casual=r["member_casual"],
            started_at=s / 1e9,
            ended_at=e / 1e9,
            start_station_id=r["start_station_id"],
            end_station_id=r["end_station_id"],
            duration_s=int(r["duration_s"]),
        )
        for r, s, e in zip(rows, started, ended, strict=True)
        if lo <= s / 1e9 < hi
    ]
    trips.sort(key=lambda t: (t.started_at, t.ride_id))
    return trips


def schedule(
    trips: list[Trip], *, late_share: float, late_min: float, late_max: float, seed: int
) -> list[Scheduled]:
    """When each trip is sent, in event-time seconds: at its start, or later for the chosen late share."""
    # Seeded on purpose, so every run delays the same trips: a reproducible simulation, not a security use.
    rng = random.Random(seed)  # NOSONAR
    out = []
    for t in trips:
        late = rng.random() < late_share
        delay = rng.uniform(late_min, late_max) * 60 if late else 0.0
        out.append(Scheduled(t.started_at + delay, t, delay))
    out.sort(key=lambda s: (s.send_at, s.trip.ride_id))
    return out


def trip_message(s: Scheduled) -> dict[str, Any]:
    t = s.trip
    return {
        "schema": TRIP_SCHEMA,
        "v": 1,
        "source": "replay",
        "ride_id": t.ride_id,
        "rideable_type": t.rideable_type,
        "member_casual": t.member_casual,
        "started_at": t.started_at,
        "ended_at": t.ended_at,
        "start_station_id": t.start_station_id,
        "end_station_id": t.end_station_id,
        "duration_s": t.duration_s,
        "late": s.late_by_s > 0,
        "late_by_s": round(s.late_by_s, 1),
    }


def replay(
    planned: list[Scheduled], producer: JsonProducer, *, speed: float, stop: StopFlag, tick_s: float = 0.2
) -> int:
    """Sends each trip when its event time comes round on the sped-up clock; returns how many were sent."""
    if not planned:
        return 0
    origin_event, origin_wall = planned[0].send_at, time.monotonic()
    sent = 0
    last_log = time.monotonic()
    for s in planned:
        if stop:
            break
        wait = origin_wall + (s.send_at - origin_event) / speed - time.monotonic()
        if wait > tick_s:  # sleep in ticks: thousands of trips a minute need no timer each
            stop.sleep(wait)
        producer.send(Topics.TRIPS_REPLAY, s.trip.start_station_id, trip_message(s))
        sent += 1
        if time.monotonic() - last_log > 10:
            producer.poll(0)
            clock = datetime.fromtimestamp(s.send_at, NYC).strftime("%H:%M")
            log.info("%s New York time: %d trips sent, %d confirmed", clock, sent, producer.stats.delivered)
            last_log = time.monotonic()
    return sent


def _clock(day: date, hhmm: str) -> datetime:
    hours, minutes = (int(x) for x in hhmm.split(":"))
    return datetime(day.year, day.month, day.day, tzinfo=NYC) + timedelta(hours=hours, minutes=minutes)


def main(
    day: Annotated[
        str, typer.Option("--date", help="New York date of the trips, e.g. 2026-09-17.")
    ] = "2026-09-17",
    start: Annotated[str, typer.Option(help="New York time to start from, HH:MM.")] = "16:00",
    until: Annotated[str, typer.Option(help="New York time to stop at, HH:MM.")] = "20:00",
    speed: Annotated[float, typer.Option(help="Replay speed factor (60: an hour a minute).")] = 60.0,
    late_share: Annotated[float, typer.Option(help="Share of trips sent late.")] = 0.02,
    late_min: Annotated[float, typer.Option(help="Least delay of a late trip, minutes.")] = 5.0,
    late_max: Annotated[float, typer.Option(help="Most delay of a late trip, minutes.")] = 50.0,
    seed: Annotated[int, typer.Option(help="Seed for choosing the late trips.")] = 315,
) -> None:
    paths = data_paths()
    configure_logging("trip_replayer", log_dir=paths.logs)
    the_day = date.fromisoformat(day)
    # A trip is filed under the date it started on, and the evening's trips all start that day.
    partition = paths.lake / "silver" / "trips" / f"start_date={the_day.isoformat()}"
    trips = read_trips(partition, _clock(the_day, start), _clock(the_day, until))
    planned = schedule(trips, late_share=late_share, late_min=late_min, late_max=late_max, seed=seed)
    late = sum(1 for s in planned if s.late_by_s > 0)
    log.info("%d trips from %s %s-%s (%d sent late), at x%.0f", len(trips), day, start, until, late, speed)
    sentinel = paths.run / "trip_replayer.stop"
    sentinel.unlink(missing_ok=True)
    producer = JsonProducer("trip-replayer")
    sent = 0
    try:
        sent = replay(planned, producer, speed=speed, stop=StopFlag(sentinel))
    finally:
        left = producer.flush(30)
        log.info("replayed %d of %d trips; %d unconfirmed at exit", sent, len(planned), left)


if __name__ == "__main__":
    typer.run(main)
