"""Export the Spark history server's dashboards as JSON for the website (roadmap P6.3).

The Spark UI portal redraws Spark's own dashboards natively: the application list, the RDD job's timeline, the
stages behind the groupByKey vs reduceByKey comparison with their task-time distributions, a Spark SQL
physical plan as a graph, the Pregel supersteps of the GraphX job, and the executors. All of it comes from the
history server's REST API and is written to ``data/serving/spark-ui.json`` in the serving envelope.

Usage (history server on :18080; from ``python/``)::

    uv run python -m velograph.ops.spark_ui_export
"""

from __future__ import annotations

import json
import logging
import tempfile
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Annotated, Any

import typer

from velograph.common.logs import configure_logging
from velograph.common.settings import data_paths
from velograph.ops.ui_screenshots import HistoryApi

log = logging.getLogger("velograph.spark_ui_export")

PIPELINE = [
    "bronze-trips",
    "silver-trips",
    "silver-stations",
    "silver-weather",
    "silver-calendar",
    "gold-station-hour",
    "gold-routes",
    "rdd-fundamentals",
    "insights",
    "graph-metrics",
]
SHUFFLE_GROUPS = ["groupByKey", "reduceByKey", "join", "broadcast"]
QUANTILES = "0.0,0.25,0.5,0.75,1.0"


def epoch_ms(stamp: str) -> int:
    """History-server timestamps look like 2026-10-15T14:01:51.123GMT."""
    return int(datetime.strptime(stamp.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp() * 1000)


def applications(api: HistoryApi) -> list[dict[str, Any]]:
    """The latest attempt of every pipeline application, in pipeline order."""
    latest: dict[str, dict[str, Any]] = {}
    for app in api.get("/applications"):
        name = app["name"].removeprefix("velograph-")
        attempt = app["attempts"][0]
        newer = name not in latest or attempt["endTimeEpoch"] > latest[name]["end"]
        if name in PIPELINE and attempt["completed"] and newer:
            latest[name] = {
                "name": name,
                "id": app["id"],
                "start": attempt["startTimeEpoch"],
                "end": attempt["endTimeEpoch"],
                "durationMs": attempt["duration"],
                "sparkVersion": attempt.get("appSparkVersion", ""),
            }
    return [latest[n] for n in PIPELINE if n in latest]


def stage_view(api: HistoryApi, app: str, stage: dict[str, Any]) -> dict[str, Any]:
    sid, attempt = stage["stageId"], stage["attemptId"]
    summary = api.get(f"/applications/{app}/stages/{sid}/{attempt}/taskSummary?quantiles={QUANTILES}")
    return {
        "stageId": sid,
        "name": stage["name"],
        "numTasks": stage["numTasks"],
        "durationMs": epoch_ms(stage["completionTime"]) - epoch_ms(stage["submissionTime"]),
        "executorRunTimeMs": stage["executorRunTime"],
        "inputBytes": stage["inputBytes"],
        "inputRecords": stage["inputRecords"],
        "shuffleReadBytes": stage["shuffleReadBytes"],
        "shuffleReadRecords": stage["shuffleReadRecords"],
        "shuffleWriteBytes": stage["shuffleWriteBytes"],
        "shuffleWriteRecords": stage["shuffleWriteRecords"],
        "taskRunTimeMs": summary["executorRunTime"],
    }


def rdd_section(api: HistoryApi, app: str) -> dict[str, Any]:
    jobs = sorted(api.get(f"/applications/{app}/jobs"), key=lambda j: j["jobId"])
    stages = {
        (s["stageId"], s["attemptId"]): s for s in api.get(f"/applications/{app}/stages?status=complete")
    }
    t0 = min(epoch_ms(j["submissionTime"]) for j in jobs)
    timeline = [
        {
            "jobId": j["jobId"],
            "name": j["name"],
            "group": j.get("jobGroup"),
            "startMs": epoch_ms(j["submissionTime"]) - t0,
            "durationMs": epoch_ms(j["completionTime"]) - epoch_ms(j["submissionTime"]),
            "tasks": j["numTasks"],
            "stages": j["numCompletedStages"],
            "skippedStages": j["numSkippedStages"],
        }
        for j in jobs
        if j.get("completionTime")
    ]
    measured = []
    for group in SHUFFLE_GROUPS:
        job = api.job_for_group(app, group)
        ran = [stages[(sid, 0)] for sid in sorted(job["stageIds"]) if (sid, 0) in stages]
        measured.append(
            {"group": group, "jobId": job["jobId"], "stages": [stage_view(api, app, s) for s in ran]}
        )
    return {"appId": app, "jobs": timeline, "shuffles": measured, "executors": executors(api, app)}


def sql_section(api: HistoryApi, app: str) -> dict[str, Any]:
    executions = api.get(f"/applications/{app}/sql?details=true&planDescription=false")

    def exchange(execution: dict[str, Any]) -> int:  # shuffles in a plan: the most instructive one to draw
        return sum(n["nodeName"].startswith("Exchange") for n in execution["nodes"])

    richest = max(executions, key=exchange)
    nodes = [
        {
            "id": n["nodeId"],
            "name": n["nodeName"],
            "codegen": n.get("wholeStageCodegenId"),
            "metrics": [{"name": m["name"], "value": m["value"]} for m in n.get("metrics", [])[:4]],
        }
        for n in richest["nodes"]
    ]
    return {
        "appId": app,
        "executions": len(executions),
        "executionId": richest["id"],
        "description": richest["description"],
        "durationMs": richest["duration"],
        "exchanges": exchange(richest),
        "nodes": nodes,
        "edges": [{"from": e["fromId"], "to": e["toId"]} for e in richest["edges"]],
    }


def pregel_section(api: HistoryApi, app: str) -> dict[str, Any]:
    jobs = sorted(api.get(f"/applications/{app}/jobs"), key=lambda j: j["jobId"])
    steps = [j for j in jobs if j["name"].startswith("isEmpty at Pregel")]
    first_run = [steps[0]]
    for a, b in pairwise(steps):  # one Pregel run is a block of consecutive superstep jobs
        if b["jobId"] != a["jobId"] + 1:
            break
        first_run.append(b)
    return {
        "appId": app,
        "totalJobs": len(jobs),
        "superstepJobs": len(steps),
        "firstRun": [
            {
                "jobId": j["jobId"],
                "durationMs": epoch_ms(j["completionTime"]) - epoch_ms(j["submissionTime"]),
                "skippedStages": j["numSkippedStages"],
            }
            for j in first_run
        ],
        "executors": executors(api, app),
    }


def executors(api: HistoryApi, app: str) -> list[dict[str, Any]]:
    return [
        {
            "id": e["id"],
            "cores": e["totalCores"],
            "tasks": e["totalTasks"],
            "taskTimeMs": e["totalDuration"],
            "gcTimeMs": e["totalGCTime"],
            "inputBytes": e["totalInputBytes"],
            "shuffleReadBytes": e["totalShuffleRead"],
            "shuffleWriteBytes": e["totalShuffleWrite"],
            "maxMemoryBytes": e["maxMemory"],
        }
        for e in api.get(f"/applications/{app}/allexecutors")
    ]


def write_envelope(path: Path, data: dict[str, Any], rows: int) -> None:
    meta = {
        "schema": "velograph.spark-ui",
        "version": 1,
        "generatedAt": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "job": "spark-ui-export",
        "rows": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", dir=path.parent, delete=False, suffix=".tmp", encoding="utf-8"
    ) as tmp:
        json.dump({"meta": meta, "data": data}, tmp, indent=2)
    Path(tmp.name).replace(path)  # atomic: a reader never sees half a file


def main(base: Annotated[str, typer.Option(help="History server URL.")] = "http://localhost:18080") -> None:
    configure_logging("spark_ui_export")
    api = HistoryApi(base)
    apps = applications(api)
    by_name = {a["name"]: a["id"] for a in apps}
    data = {
        "historyServer": base,
        "applications": apps,
        "rdd": rdd_section(api, by_name["rdd-fundamentals"]),
        "sql": sql_section(api, by_name["insights"]),
        "pregel": pregel_section(api, by_name["graph-metrics"]),
    }
    target = data_paths().serving / "spark-ui.json"
    write_envelope(target, data, rows=len(apps))
    log.info(
        "wrote %s: %d applications, %d RDD jobs, SQL plan with %d nodes, %d Pregel superstep jobs",
        target,
        len(apps),
        len(data["rdd"]["jobs"]),
        len(data["sql"]["nodes"]),
        len(data["pregel"]["firstRun"]),
    )


if __name__ == "__main__":
    typer.run(main)
