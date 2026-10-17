import gzip
import json
from datetime import datetime
from pathlib import Path

import pytest

from velograph.common.kafka import Topics
from velograph.ingest.gbfs import StationInfo, normalize_station_id, station_registry
from velograph.ingest.gbfs_producer import (
    NYC,
    FeedClock,
    PoisonRecordError,
    Publisher,
    next_poll_delay,
    recorded_events,
    run_recorded,
    status_message,
    weather_message,
)


class FakeProducer:
    """Collects what would have been sent to Kafka."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict]] = []
        self.dead: list[str] = []

    def send(self, topic: str, key: str, value: dict) -> None:
        self.sent.append((topic, key, value))

    def dead_letter(self, error: str, raw: str, *, origin: str | None = None) -> None:
        self.dead.append(error)

    def flush(self, timeout: float = 30.0) -> int:
        return 0


class NoStop:
    """A stop flag that never fires and records the pacing instead of sleeping."""

    def __init__(self) -> None:
        self.slept: list[float] = []

    def __bool__(self) -> bool:
        return False

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)


class SilentReport:
    def maybe_log(self, publisher: Publisher, *, force: bool = False) -> None:
        pass


REGISTRY = {
    "uuid-a": StationInfo("5980.10", "W 12 St & Hudson St", 40.7, -74.0, 20),
    "uuid-b": StationInfo("6001.03", "Broadway & W 60 St", 40.77, -73.98, 30),
}


def _raw(gbfs_id: str, bikes: int, last_reported: int = 1_792_000_000) -> dict:
    return {
        "station_id": gbfs_id,
        "num_bikes_available": bikes,
        "num_ebikes_available": 1,
        "num_bikes_disabled": 0,
        "num_docks_available": 20 - bikes,
        "num_docks_disabled": 0,
        "is_installed": 1,
        "is_renting": 1,
        "is_returning": 1,
        "last_reported": last_reported,
    }


def test_station_ids_get_their_second_decimal_back():
    assert normalize_station_id("5980.1") == "5980.10"
    assert normalize_station_id("5980.10") == "5980.10"
    assert normalize_station_id("JC115") == "JC115"


def test_registry_maps_uuids_to_lake_ids_and_skips_stations_without_short_name():
    info = {
        "data": {
            "stations": [
                {"station_id": "u1", "short_name": "5980.1", "name": "A", "lat": 1, "lon": 2, "capacity": 9},
                {"station_id": "u2", "name": "no short name", "lat": 1, "lon": 2},
            ]
        }
    }
    registry = station_registry(info)
    assert list(registry) == ["u1"]
    assert registry["u1"].station_id == "5980.10"


def test_status_message_carries_lake_id_capacity_and_event_time():
    msg = status_message(
        _raw("uuid-a", 3),
        REGISTRY,
        fetched_at=1_792_000_060.5,
        feed_updated=1_792_000_055,
        kind="delta",
        source="live",
    )
    assert (msg["schema"], msg["v"], msg["station_id"], msg["capacity"]) == (
        "velograph.station-status",
        1,
        "5980.10",
        20,
    )
    assert (msg["bikes"], msg["docks"], msg["is_renting"], msg["last_reported"]) == (
        3,
        17,
        True,
        1_792_000_000,
    )


def test_unknown_or_malformed_stations_are_poison():
    with pytest.raises(PoisonRecordError, match="not listed"):
        status_message(
            _raw("uuid-x", 3), REGISTRY, fetched_at=0, feed_updated=None, kind="delta", source="live"
        )
    broken = _raw("uuid-a", 3)
    broken["num_bikes_available"] = "lots"
    with pytest.raises(PoisonRecordError, match="ValueError"):
        status_message(broken, REGISTRY, fetched_at=0, feed_updated=None, kind="delta", source="live")


def test_weather_uses_the_lake_column_names_and_rain_rule():
    payload = {
        "current": {"time": "2026-10-16T20:15", "temperature_2m": 19.7, "precipitation": 0.0},
        "hourly": {
            "time": ["2026-10-16T20:00", "2026-10-16T21:00"],
            "temperature_2m": [19.8, 19.1],
            "apparent_temperature": [20.7, 20.3],
            "precipitation": [0.3, 0.05],
            "wind_speed_10m": [12.7, 9.4],
            "relative_humidity_2m": [87, 88],
            "cloud_cover": [100, 90],
            "weather_code": [51, 3],
        },
    }
    msg = weather_message(payload, fetched_at=1.0, source="live")
    assert msg["current"]["temp_c"] == 19.7
    assert [h["is_raining"] for h in msg["hours"]] == [True, False]  # 0.1 mm, as in silver/weather_hourly
    assert msg["hours"][1] == {
        "hour": "2026-10-16T21:00",
        "temp_c": 19.1,
        "apparent_temp_c": 20.3,
        "precip_mm": 0.05,
        "wind_kmh": 9.4,
        "humidity_pct": 88,
        "cloud_pct": 90,
        "weather_code": 3,
        "is_raining": False,
    }


def test_polls_follow_the_feed_ttl():
    assert next_poll_delay(1000, 60, now=1030) == pytest.approx(31.0)
    assert next_poll_delay(1000, 60, now=1100) == 10.0  # overdue: poll again soon, but not in a tight loop
    assert next_poll_delay(None, None, now=0) == 60.0


def test_publisher_sends_changes_then_periodic_snapshots_and_routes_poison_to_the_dlq():
    producer = FakeProducer()
    publisher = Publisher(producer, "live", REGISTRY, snapshot_every=3)  # type: ignore[arg-type]
    poll = [_raw("uuid-a", 3), _raw("uuid-b", 5), _raw("uuid-x", 1)]
    assert publisher.publish_status(poll, fetched_at=1, feed_updated=1) == ("snapshot", 2)
    assert producer.dead == ["station 'uuid-x' is not listed in station_information"]
    poll[0] = _raw("uuid-a", 2)
    assert publisher.publish_status(poll, fetched_at=2, feed_updated=2) == ("delta", 1)
    assert publisher.publish_status(poll, fetched_at=3, feed_updated=3) == ("delta", 0)
    assert publisher.publish_status(poll, fetched_at=4, feed_updated=4)[0] == "snapshot"  # every 3rd publish
    keys = [key for topic, key, _ in producer.sent if topic == Topics.STATION_STATUS]
    assert keys[:3] == ["5980.10", "6001.03", "5980.10"]  # keyed by the lake id: one station, one partition


def _write_day(day: Path) -> None:
    def write(name: str, records: list[dict]) -> None:
        with gzip.open(day / name, "wt", encoding="utf-8") as fh:
            fh.writelines(json.dumps(r) + "\n" for r in records)

    day.mkdir(parents=True)
    write(
        "station_status.jsonl.gz",
        [
            {"kind": "snapshot", "fetched_at": "2026-10-16T11:00:00+00:00", "last_updated": 1,
             "stations": [_raw("uuid-a", 3), _raw("uuid-b", 5)]},
            {"kind": "delta", "fetched_at": "2026-10-16T11:01:00+00:00", "last_updated": 2,
             "stations": [_raw("uuid-a", 2)]},
            {"kind": "delta", "fetched_at": "2026-10-16T11:02:00+00:00", "last_updated": 3,
             "stations": [_raw("uuid-b", 4)]},
        ],
    )  # fmt: skip
    write(
        "weather_current.jsonl.gz",
        [{"fetched_at": "2026-10-16T11:00:30+00:00", "current": {"time": "t"}, "hourly": {"time": []}}],
    )


def test_recorded_events_merge_status_and_weather_in_time_order(tmp_path: Path):
    _write_day(tmp_path / "day")
    assert [e.kind for e in recorded_events(tmp_path / "day")] == ["status", "weather", "status", "status"]


def test_replay_folds_the_day_before_the_start_and_keeps_gaps_at_speed(tmp_path: Path):
    _write_day(tmp_path / "day")
    producer, stop = FakeProducer(), NoStop()
    publisher = Publisher(producer, "replay", REGISTRY)  # type: ignore[arg-type]
    start = datetime(2026, 10, 16, 7, 0, 45, tzinfo=NYC)  # 11:00:45 UTC: after the snapshot and the weather
    run_recorded(
        publisher, tmp_path / "day", start=start, until=None, speed=6.0, stop=stop, report=SilentReport()
    )  # type: ignore[arg-type]
    statuses = [v for topic, _, v in producer.sent if topic == Topics.STATION_STATUS]
    # The first published poll is a full snapshot of the folded state, already holding the 11:01 change.
    assert [(m["kind"], m["station_id"], m["bikes"]) for m in statuses] == [
        ("snapshot", "5980.10", 2),
        ("snapshot", "6001.03", 5),
        ("delta", "6001.03", 4),
    ]
    assert all(m["source"] == "replay" for m in statuses)
    assert stop.slept == [10.0]  # 60 recorded seconds at 6x
    assert producer.sent[0][0] == Topics.WEATHER  # the weather in force at the start goes out first


def test_the_feed_clock_follows_the_server_not_this_machine():
    clock = FeedClock()
    assert clock.offset == 0.0
    for local in (1000.0, 1060.0, 1120.0):  # this machine runs 92 s slow
        clock.observe(server=local + 92 - 0.4, sent=local - 0.1, received=local + 0.1)
    assert clock.offset == pytest.approx(92.1)
