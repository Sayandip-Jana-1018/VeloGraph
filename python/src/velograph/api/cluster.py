"""What ``/api/cluster`` and ``/api/health`` report: the Spark master and the Kafka cluster, as they are now.

Every probe has a short timeout and reports its own failure, so the website can show a cluster that is
half up (Kafka running, Spark stopped) instead of an error page.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from typing import Any

import httpx
from confluent_kafka import ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from velograph.common.kafka import Topics

log = logging.getLogger("velograph.api.cluster")

SPARK_MASTER_UI = os.environ.get("VELOGRAPH_SPARK_MASTER_UI", "http://localhost:8080")


def spark_master(http: httpx.Client, url: str = SPARK_MASTER_UI) -> dict[str, Any]:
    """The standalone master's own JSON (``/json/``): workers, cores, memory, applications."""
    try:
        response = http.get(f"{url}/json/", timeout=3.0)
        response.raise_for_status()
        d = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return {
        "ok": True,
        "url": d.get("url"),
        "status": d.get("status"),
        "aliveWorkers": d.get("aliveworkers", 0),
        "cores": d.get("cores", 0),
        "coresUsed": d.get("coresused", 0),
        "memoryMb": d.get("memory", 0),
        "memoryUsedMb": d.get("memoryused", 0),
        "workers": [
            {
                "id": w.get("id"),
                "host": w.get("host"),
                "cores": w.get("cores"),
                "coresUsed": w.get("coresused"),
                "memoryMb": w.get("memory"),
                "memoryUsedMb": w.get("memoryused"),
                "state": w.get("state"),
                "lastHeartbeat": w.get("lastheartbeat"),
            }
            for w in d.get("workers", [])
        ],
        "activeApps": [_app(a) for a in d.get("activeapps", [])],
        "completedApps": len(d.get("completedapps", [])),
        # Newest first: what the cluster ran since it started.
        "completed": [
            _app(a) for a in sorted(d.get("completedapps", []), key=lambda a: -a.get("starttime", 0))
        ][:12],
    }


def _app(a: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": a.get("id"),
        "name": a.get("name"),
        "cores": a.get("cores"),
        "memoryPerExecutorMb": a.get("memoryperexecutor") or a.get("memoryperslave"),
        "startTime": a.get("starttime"),
        "durationMs": a.get("duration"),
        "state": a.get("state"),
    }


_UNITS = {
    "B": 1 / 2**20,
    "KiB": 1 / 1024,
    "MiB": 1.0,
    "GiB": 1024.0,
    "kB": 1e3 / 2**20,
    "MB": 1e6 / 2**20,
    "GB": 1e9 / 2**20,
}


def mebibytes(size: str) -> float:
    """``docker stats`` sizes ("137.1MiB", "1.375GiB", "448MiB") in MiB."""
    match = re.fullmatch(r"([\d.]+)\s*([A-Za-z]+)", size.strip())
    if match is None:
        raise ValueError(f"not a size: {size!r}")
    return round(float(match[1]) * _UNITS[match[2]], 1)


def parse_stats(lines: list[str], prefix: str = "velograph-") -> list[dict[str, Any]]:
    """Per project container, from ``docker stats --format '{{json .}}'``: memory used and capped, CPU."""
    rows = []
    for line in lines:
        if not line.strip():
            continue
        d = json.loads(line)
        if not d["Name"].startswith(prefix):
            continue
        used, limit = (mebibytes(x.strip()) for x in d["MemUsage"].split("/"))
        rows.append(
            {
                "name": d["Name"].removeprefix(prefix),
                "memUsedMb": used,
                "memLimitMb": limit,
                "cpuPct": float(d["CPUPerc"].rstrip("%") or 0),
            }
        )
    return sorted(rows, key=lambda r: r["name"])


def containers(timeout: float = 10.0) -> dict[str, Any]:
    """Memory and CPU of every VeloGraph container, against the memory of the Docker VM (ADR 009)."""
    try:
        stats = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True,
        ).stdout
        total = subprocess.run(
            ["docker", "info", "--format", "{{.MemTotal}}"],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True,
        ).stdout
        rows = parse_stats(stats.splitlines())
        vm = round(int(total.strip()) / 2**20, 1)
    except (OSError, subprocess.SubprocessError, ValueError, KeyError) as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    return {"ok": True, "vmMemoryMb": vm, "containers": rows}


def kafka_cluster(admin: AdminClient, group: str = "velograph-gateway") -> dict[str, Any]:
    """Brokers, controller, each topic's partitions (leader, replicas, ISR) and the gateway's lag."""
    try:
        md = admin.list_topics(timeout=4)
    except KafkaException as exc:
        return {"ok": False, "error": str(exc)}
    topics = []
    for name in Topics.ALL:
        topic = md.topics.get(name)
        if topic is None:
            continue
        topics.append(
            {
                "name": name,
                "partitions": [
                    {"id": p.id, "leader": p.leader, "replicas": list(p.replicas), "isr": list(p.isrs)}
                    for p in sorted(topic.partitions.values(), key=lambda p: p.id)
                ],
            }
        )
    return {
        "ok": True,
        "clusterId": md.cluster_id,
        "controller": md.controller_id,
        "brokers": sorted(
            ({"id": b.id, "host": b.host, "port": b.port} for b in md.brokers.values()), key=lambda b: b["id"]
        ),
        "topics": topics,
        "gatewayLag": group_lag(admin, group, [Topics.ALERTS, Topics.PREDICTIONS], md),
    }


def group_lag(admin: AdminClient, group: str, topics: list[str], md: Any) -> dict[str, Any] | None:
    """Committed offset against the log end, per partition: how far the consumer group is behind."""
    partitions = [TopicPartition(t, p) for t in topics if t in md.topics for p in md.topics[t].partitions]
    try:
        committed = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group, partitions)])
        offsets = committed[group].result(timeout=4).topic_partitions
        ends = admin.list_offsets({tp: OffsetSpec.latest() for tp in partitions}, request_timeout=4)
        lag_by_topic: dict[str, int] = {}
        for tp in offsets:
            end = ends[TopicPartition(tp.topic, tp.partition)].result(timeout=4).offset
            lag = end - tp.offset if tp.offset >= 0 else 0  # no commit yet: nothing to be behind on
            lag_by_topic[tp.topic] = lag_by_topic.get(tp.topic, 0) + lag
        return {"group": group, "lag": lag_by_topic}
    except (KafkaException, KeyError) as exc:
        log.debug("lag of %s unavailable: %s", group, exc)
        return None
