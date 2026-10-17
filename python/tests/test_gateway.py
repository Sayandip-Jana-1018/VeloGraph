import asyncio
import json
from pathlib import Path

from fastapi.testclient import TestClient

from velograph.api.cluster import mebibytes, parse_stats
from velograph.api.live import BatchAssembler, Broadcaster
from velograph.api.main import create_app


def _prediction(batch: int, risk: str) -> dict:
    return {"batch_id": batch, "as_of": f"t{batch}", "source": "live", "risk": risk}


def test_predictions_become_one_summary_per_batch():
    assembler = BatchAssembler(quiet_s=2.0)
    assert assembler.add(_prediction(7, "EMPTY"), now=0.0) is None
    assert assembler.add(_prediction(7, "OK"), now=0.1) is None
    done = assembler.add(_prediction(8, "FULL"), now=0.2)  # the next batch starts: 7 is complete
    assert done is not None
    assert done.event() == {
        "batchId": 7,
        "asOf": "t7",
        "source": "live",
        "stations": 2,
        "EMPTY": 1,
        "EMPTY_RISK": 0,
        "FULL": 0,
        "FULL_RISK": 0,
        "OFFLINE": 0,
        "OK": 1,
    }
    assert assembler.flush_if_quiet(now=1.0) is None  # batch 8 may still be arriving
    last = assembler.flush_if_quiet(now=2.5)
    assert last is not None and last.event()["stations"] == 1


def test_a_slow_subscriber_loses_its_oldest_events_not_the_stream():
    async def scenario() -> list[int]:
        broadcaster = Broadcaster(queue_size=2)
        broadcaster.bind(asyncio.get_running_loop())
        queue = broadcaster.subscribe()
        for n in range(3):
            broadcaster.publish({"event": "alert", "data": {"n": n}})
        await asyncio.sleep(0)  # let the loop run the hand-overs
        return [(await queue.get())["data"]["n"] for _ in range(2)]

    assert asyncio.run(scenario()) == [1, 2]


def test_live_files_are_served_and_missing_ones_say_so(tmp_path: Path):
    (tmp_path / "snapshot.json").write_text(
        json.dumps({"meta": {}, "data": {"batchId": 3}}), encoding="utf-8"
    )
    with TestClient(create_app(live_dir=tmp_path, start_consumer=False)) as client:
        snapshot = client.get("/api/live/snapshot")
        assert snapshot.status_code == 200
        assert snapshot.headers["cache-control"] == "no-store"
        assert snapshot.json()["data"]["batchId"] == 3
        assert client.get("/api/live/progress").status_code == 503  # not written yet
        assert client.get("/api/live/secrets").status_code == 404


def test_health_reports_what_is_missing_instead_of_failing(tmp_path: Path, monkeypatch):
    # Nothing listens on these ports in the test: the probes must time out into a "degraded" answer.
    monkeypatch.setattr("velograph.api.main.spark_master", lambda http: {"ok": False, "error": "refused"})
    monkeypatch.setattr(
        "velograph.api.main.kafka_cluster", lambda admin: {"ok": False, "error": "no brokers"}
    )
    with TestClient(create_app(live_dir=tmp_path, start_consumer=False)) as client:
        health = client.get("/api/health").json()
    assert health["status"] == "degraded"
    assert health["kafka"] == {"ok": False, "brokers": 0, "error": "no brokers"}
    assert health["snapshot"] == {"exists": False}


def test_docker_stats_become_mebibytes_per_project_container():
    assert (mebibytes("137.1MiB"), mebibytes("1.375GiB"), mebibytes("0B")) == (137.1, 1408.0, 0.0)
    lines = [
        '{"Name":"velograph-kafka-1","MemUsage":"454.9MiB / 640MiB","CPUPerc":"1.25%"}',
        '{"Name":"splitx-app","MemUsage":"1GiB / 2GiB","CPUPerc":"0.00%"}',
        "",
    ]
    assert parse_stats(lines) == [
        {"name": "kafka-1", "memUsedMb": 454.9, "memLimitMb": 640.0, "cpuPct": 1.25}
    ]
