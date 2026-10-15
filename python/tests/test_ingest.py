import gzip
import json
from pathlib import Path

from velograph.ingest.download_trips import RemoteArchive, count_rows
from velograph.ingest.download_weather import coverage
from velograph.ingest.gbfs import select_changes
from velograph.ingest.gbfs_recorder import append_jsonl_gz


def _station(station_id: str, bikes: int, last_reported: int = 0) -> dict:
    return {
        "station_id": station_id,
        "num_bikes_available": bikes,
        "num_docks_available": 10 - bikes,
        "last_reported": last_reported,
    }


def test_first_poll_is_a_snapshot():
    kind, written, state = select_changes({}, [_station("a", 3), _station("b", 4)], force_snapshot=False)
    assert kind == "snapshot"
    assert len(written) == 2
    assert set(state) == {"a", "b"}


def test_delta_contains_only_inventory_changes():
    _, _, state = select_changes({}, [_station("a", 3), _station("b", 4)], force_snapshot=False)
    # b's last_reported ticks over but its inventory is unchanged, so only a is written.
    kind, written, _ = select_changes(
        state, [_station("a", 2), _station("b", 4, last_reported=99)], force_snapshot=False
    )
    assert kind == "delta"
    assert [s["station_id"] for s in written] == ["a"]


def test_forced_snapshot_writes_everything():
    _, _, state = select_changes({}, [_station("a", 3)], force_snapshot=False)
    kind, written, _ = select_changes(state, [_station("a", 3)], force_snapshot=True)
    assert (kind, len(written)) == ("snapshot", 1)


def test_appended_gzip_members_read_back_as_one_jsonl_file(tmp_path: Path):
    target = tmp_path / "day" / "station_status.jsonl.gz"
    append_jsonl_gz(target, {"n": 1})
    append_jsonl_gz(target, {"n": 2})
    with gzip.open(target, "rt", encoding="utf-8") as fh:
        assert [json.loads(line)["n"] for line in fh] == [1, 2]


def test_count_rows_excludes_header_and_handles_missing_final_newline(tmp_path: Path):
    csv = tmp_path / "trips.csv"
    csv.write_bytes(b"ride_id,member_casual\nA,member\nB,casual")
    assert count_rows(csv) == 2


def test_weather_coverage_ignores_trailing_hours_without_values():
    payload = {"hourly": {"time": ["d0T00:00", "d0T01:00", "d0T02:00"], "temperature_2m": [20.1, 19.5, None]}}
    assert coverage(payload) == {
        "hoursReturned": 3,
        "hoursWithValues": 2,
        "firstHour": "d0T00:00",
        "lastHour": "d0T01:00",
    }


def test_md5_is_only_trusted_for_single_part_etags():
    assert (
        RemoteArchive("u", 1, '"0123456789abcdef0123456789abcdef"').md5 == "0123456789abcdef0123456789abcdef"
    )
    assert RemoteArchive("u", 1, '"0123456789abcdef0123456789abcdef-58"').md5 is None
