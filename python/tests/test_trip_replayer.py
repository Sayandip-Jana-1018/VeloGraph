from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from velograph.common.kafka import Topics
from velograph.ingest import trip_replayer
from velograph.ingest.trip_replayer import NYC, Scheduled, Trip, read_trips, replay, schedule, trip_message


def _trip(ride: str, started: float) -> Trip:
    return Trip(ride, "electric_bike", "member", started, started + 600, "5980.10", "6001.03", 600)


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict]] = []

    def send(self, topic: str, key: str, value: dict) -> None:
        self.sent.append((topic, key, value))

    def poll(self, timeout: float = 0) -> None:
        pass


class FakeClock:
    """Stands in for time.monotonic; sleeping on the stop flag moves it on."""

    def __init__(self) -> None:
        self.now = 0.0
        self.slept = 0.0

    def monotonic(self) -> float:
        return self.now

    def __bool__(self) -> bool:  # the stop flag never fires
        return False

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        self.slept += seconds


def test_trips_are_read_for_the_window_in_start_order(tmp_path: Path):
    def ns(hh: int, mm: int) -> int:  # New York wall clock -> UTC nanoseconds, as Spark stores instants
        return int(datetime(2026, 9, 17, hh, mm, tzinfo=NYC).timestamp() * 1e9)

    table = pa.table(
        {
            "ride_id": ["late", "early", "outside"],
            "rideable_type": ["classic_bike", "electric_bike", "classic_bike"],
            "member_casual": ["member", "casual", "member"],
            "started_at": pa.array([ns(17, 30), ns(16, 5), ns(21, 0)], type=pa.timestamp("ns")),
            "ended_at": pa.array([ns(17, 45), ns(16, 20), ns(21, 10)], type=pa.timestamp("ns")),
            "start_station_id": ["a", "b", "c"],
            "end_station_id": ["b", "c", "a"],
            "duration_s": pa.array([900, 900, 600], type=pa.int32()),
        }
    )
    pq.write_table(table, tmp_path / "part-0.parquet")
    trips = read_trips(
        tmp_path, datetime(2026, 9, 17, 16, 0, tzinfo=NYC), datetime(2026, 9, 17, 20, 0, tzinfo=NYC)
    )
    assert [t.ride_id for t in trips] == ["early", "late"]
    assert trips[0].started_at == datetime(2026, 9, 17, 16, 5, tzinfo=NYC).timestamp()


def test_a_seeded_share_of_trips_is_sent_late_within_the_range():
    trips = [_trip(f"r{i}", 1000.0 + i) for i in range(2000)]
    planned = schedule(trips, late_share=0.05, late_min=5, late_max=30, seed=315)
    late = [s for s in planned if s.late_by_s > 0]
    assert 60 < len(late) < 140  # about 5 % of 2,000
    assert all(300 <= s.late_by_s <= 1800 for s in late)
    assert [s.send_at for s in planned] == sorted(s.send_at for s in planned)
    assert planned == schedule(trips, late_share=0.05, late_min=5, late_max=30, seed=315)  # reproducible


def test_messages_keep_the_event_time_and_flag_late_trips():
    msg = trip_message(Scheduled(1300.0, _trip("r1", 1000.0), 300.0))
    assert (msg["schema"], msg["source"], msg["started_at"], msg["late"], msg["late_by_s"]) == (
        "velograph.trip",
        "replay",
        1000.0,
        True,
        300.0,
    )


def test_replay_keeps_the_gaps_at_speed_and_keys_by_start_station(monkeypatch: pytest.MonkeyPatch):
    clock = FakeClock()
    monkeypatch.setattr(trip_replayer.time, "monotonic", clock.monotonic)
    planned = [Scheduled(t, _trip(f"r{t}", t), 0.0) for t in (0.0, 60.0, 600.0)]
    producer = FakeProducer()
    assert replay(planned, producer, speed=60.0, stop=clock) == 3  # type: ignore[arg-type]
    assert clock.slept == pytest.approx(10.0)  # 600 event seconds at x60
    assert {(topic, key) for topic, key, _ in producer.sent} == {(Topics.TRIPS_REPLAY, "5980.10")}
