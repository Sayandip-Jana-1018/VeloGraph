"""Resilience drills on the running stack (roadmap P5.6), recorded to ``serving/live/drills.json``.

``broker``
    Stops one Kafka broker for ``--down-s`` seconds while the producer and the scoring job run, sampling every
    few seconds: in-sync replicas and leaders per partition, the producer's confirmed and failed deliveries,
    and the scoring job's micro-batches. Then it starts the broker again, times how long the partitions take
    to be fully replicated (ISR 3) once more, and hands leadership back to the preferred replicas. With
    replication factor 3, ``min.insync.replicas = 2`` and ``acks=all``, losing one broker should cost no
    message and stop no batch.

``restart``
    Kills the live scoring driver (inside the Spark master container), starts the job again, and reads Spark's
    own offset log in the checkpoint. The first batch after the restart must start exactly where the last
    committed one ended. No gap means no loss; no overlap means nothing is counted twice.

    uv run python -m velograph.ops.drills broker --broker 2 --down-s 120
    uv run python -m velograph.ops.drills restart
"""

from __future__ import annotations

import json
import logging
import subprocess
import time
from pathlib import Path
from typing import Annotated, Any

import typer
from confluent_kafka.admin import AdminClient

from velograph.common.kafka import Topics, bootstrap_servers, now_iso
from velograph.common.logs import configure_logging
from velograph.common.serving import write_serving
from velograph.common.settings import REPO_ROOT, data_paths

log = logging.getLogger("velograph.drills")
app = typer.Typer(add_completion=False, help=__doc__)

JOB = "live-station-scoring"
WATCHED = (Topics.STATION_STATUS, Topics.PREDICTIONS, Topics.ALERTS)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))["data"]
    except (OSError, ValueError, KeyError):
        return None


def replication(admin: AdminClient) -> dict[str, Any]:
    """Partitions of the watched topics by ISR size, leaders per broker, and partitions without a leader."""
    md = admin.list_topics(timeout=5)
    isr: dict[int, int] = {}
    leaders: dict[int, int] = {}
    leaderless = 0
    for name in WATCHED:
        for p in md.topics[name].partitions.values():
            isr[len(p.isrs)] = isr.get(len(p.isrs), 0) + 1
            if p.leader < 0:
                leaderless += 1
            else:
                leaders[p.leader] = leaders.get(p.leader, 0) + 1
    return {
        "brokers": sorted(md.brokers),
        "isr": {str(k): v for k, v in sorted(isr.items())},
        "leaders": {str(k): v for k, v in sorted(leaders.items())},
        "leaderless": leaderless,
    }


def sample(admin: AdminClient, live: Path, started: float) -> dict[str, Any]:
    producer = _read_json(live / "producer-stats.json") or {}
    progress = _read_json(live / "stream-progress.json") or {}
    scoring = [b for b in progress.get("batches", []) if b.get("query") == JOB]
    last = scoring[-1] if scoring else {}
    try:
        kafka = replication(admin)
    except Exception as exc:  # a metadata timeout during the outage is itself worth recording
        kafka = {"error": str(exc)}
    return {
        "t": round(time.monotonic() - started, 1),
        **kafka,
        "delivered": producer.get("delivered"),
        "failed": producer.get("failed"),
        "ackP95Ms": (producer.get("ackMs") or {}).get("p95"),
        "lastBatch": last.get("batchId"),
        "latencyS": (last.get("scoring") or {}).get("latencyS"),
    }


def _docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True, text=True)


def _record(kind: str, result: dict[str, Any]) -> None:
    path = data_paths().serving / "live" / "drills.json"
    drills = _read_json(path) or {}
    drills[kind] = result
    write_serving(path, "resilience-drills", "drills", drills)
    log.info("recorded the %s drill in %s", kind, path)


@app.command()
def broker(
    broker: Annotated[int, typer.Option(help="Broker to stop: 1, 2 or 3.")] = 2,
    down_s: Annotated[float, typer.Option(help="Seconds to keep it stopped.")] = 120.0,
    every_s: Annotated[float, typer.Option(help="Seconds between samples.")] = 5.0,
) -> None:
    configure_logging("drills", log_dir=data_paths().logs)
    live = data_paths().serving / "live"
    admin = AdminClient({"bootstrap.servers": bootstrap_servers()})
    started = time.monotonic()
    samples = [sample(admin, live, started)]
    before = samples[0]
    log.info("before: %s", before)

    container = f"velograph-kafka-{broker}"
    stopped_at = now_iso()
    _docker("stop", container)
    log.info("%s stopped", container)
    while time.monotonic() - started < down_s:
        time.sleep(every_s)
        samples.append(sample(admin, live, started))
        log.info("down: %s", samples[-1])

    restarted_at = now_iso()
    restart_t = time.monotonic()
    _docker("start", container)
    log.info("%s started; waiting for every partition to be back in sync", container)
    recovered_s = None
    while time.monotonic() - restart_t < 300:
        time.sleep(every_s)
        s = sample(admin, live, started)
        samples.append(s)
        if s.get("isr") == {"3": sum(s["isr"].values())} and not s.get("leaderless"):
            recovered_s = round(time.monotonic() - restart_t, 1)
            break
    # The broker is back but leads nothing: hand each partition back to its preferred leader.
    _docker(
        "exec",
        "velograph-kafka-1",
        "/opt/kafka/bin/kafka-leader-election.sh",
        "--bootstrap-server",
        "kafka-1:19092",
        "--election-type",
        "preferred",
        "--all-topic-partitions",
    )
    time.sleep(every_s)
    samples.append(sample(admin, live, started))
    after = samples[-1]
    down = [s for s in samples if before["t"] < s["t"] <= down_s]
    batches = {s["lastBatch"] for s in down if s.get("lastBatch") is not None}
    result = {
        "broker": broker,
        "stoppedAt": stopped_at,
        "restartedAt": restarted_at,
        "downS": down_s,
        "recoveredS": recovered_s,
        "before": before,
        "after": after,
        "producerFailed": (after.get("failed") or 0) - (before.get("failed") or 0),
        "producerDelivered": (after.get("delivered") or 0) - (before.get("delivered") or 0),
        "batchesWhileDown": len(batches),
        "maxLatencyWhileDownS": max((s.get("latencyS") or 0 for s in down), default=None),
        "leaderlessWhileDown": max((s.get("leaderless") or 0 for s in down), default=0),
        "samples": samples,
        "passed": recovered_s is not None
        and (after.get("failed") or 0) == (before.get("failed") or 0)
        and len(batches) > 1,
    }
    _record("broker", result)
    log.info(
        "broker drill %s: %d deliveries, %d failed, %d batches while down, back in sync after %s s",
        "passed" if result["passed"] else "FAILED",
        result["producerDelivered"],
        result["producerFailed"],
        result["batchesWhileDown"],
        recovered_s,
    )


def offset_log(checkpoint: Path) -> dict[int, dict[str, Any]]:
    """Spark's offset log: per batch, the end offset of every partition (its start is the previous end)."""
    out: dict[int, dict[str, Any]] = {}
    commits = {int(p.name) for p in (checkpoint / "commits").glob("[0-9]*")}
    for path in (checkpoint / "offsets").glob("[0-9]*"):
        lines = path.read_text(encoding="utf-8").splitlines()
        meta, source = json.loads(lines[1]), json.loads(lines[2])
        out[int(path.name)] = {
            "end": source.get(Topics.STATION_STATUS, {}),
            "committed": int(path.name) in commits,
            "timestampMs": meta.get("batchTimestampMs"),
        }
    return dict(sorted(out.items()))


def total(offsets: dict[str, int]) -> int:
    return sum(int(v) for v in offsets.values())


@app.command()
def restart(
    wait_s: Annotated[
        float, typer.Option(help="Seconds to wait for the restarted job's new batches.")
    ] = 300.0,
    duration_min: Annotated[int, typer.Option(help="How long the restarted job runs.")] = 15,
) -> None:
    configure_logging("drills", log_dir=data_paths().logs)
    checkpoint = data_paths().checkpoints / f"{JOB}-live" / "scoring"
    log_before = offset_log(checkpoint)
    last = max(b for b, e in log_before.items() if e["committed"])
    log.info("last committed batch before the kill: %d, ends at %d", last, total(log_before[last]["end"]))

    killed_at = now_iso()
    subprocess.run(
        [
            "docker",
            "exec",
            "velograph-spark-master",
            "pkill",
            "-9",
            "-f",
            "velograph.streaming.LiveStationScoringJob",
        ],
        check=False,
    )
    log.info("driver killed (SIGKILL: no shutdown hook, no clean stop)")
    time.sleep(10)
    # -Command, not -File: only the former treats "--" as the end of submit.ps1's own parameters.
    script = REPO_ROOT / "scripts" / "submit.ps1"
    subprocess.Popen(
        [
            "powershell",
            "-NoProfile",
            "-Command",
            f"& '{script}' {JOB} -Mode cluster -- --duration-min {duration_min}",
        ],
        stdout=(data_paths().logs / "drill-restart.console.log").open("w"),
        stderr=subprocess.STDOUT,
    )
    restarted_at = now_iso()
    deadline = time.monotonic() + wait_s
    while time.monotonic() < deadline:
        time.sleep(5)
        log_after = offset_log(checkpoint)
        if len([b for b, e in log_after.items() if b > last and e["committed"]]) >= 2:
            break
    log_after = offset_log(checkpoint)
    resumed = [b for b in log_after if b > last]
    first = resumed[0] if resumed else None
    # Offsets logged but not committed when the driver died: that batch runs again with the same offsets.
    rerun = [b for b, e in log_before.items() if b > last and not e["committed"]]
    went_back = [
        b
        for b in resumed
        if b - 1 in log_after
        and any(int(log_after[b - 1]["end"].get(p, 0)) > int(v) for p, v in log_after[b]["end"].items())
    ]
    result = {
        "killedAt": killed_at,
        "restartedAt": restarted_at,
        "lastCommitted": last,
        "lastCommittedEnd": total(log_before[last]["end"]),
        "rerunBatches": rerun,
        "firstAfterRestart": first,
        "firstStart": total(log_after[first - 1]["end"])
        if first is not None and first - 1 in log_after
        else None,
        "firstEnd": total(log_after[first]["end"]) if first is not None else None,
        "batchesAfterRestart": len([b for b in resumed if log_after[b]["committed"]]),
        "offsetsWentBack": went_back,
        "passed": first is not None and not went_back and first - 1 in log_after and first - 1 >= last,
    }
    _record("restart", result)
    log.info(
        "restart drill %s: batch %s resumed at offset %s (last committed batch %d ended at %d)",
        "passed" if result["passed"] else "FAILED",
        first,
        result["firstStart"],
        last,
        result["lastCommittedEnd"],
    )


if __name__ == "__main__":
    app()
