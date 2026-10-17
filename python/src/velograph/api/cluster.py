"""What ``/api/cluster`` and ``/api/health`` report: the Spark master and the Kafka cluster, as they are now.

Every probe has a short timeout and reports its own failure, so the website can show a cluster that is
half up (Kafka running, Spark stopped) instead of an error page.
"""

from __future__ import annotations

import logging
import os
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
        "activeApps": [
            {
                "id": a.get("id"),
                "name": a.get("name"),
                "cores": a.get("cores"),
                "memoryPerExecutorMb": a.get("memoryperslave") or a.get("memoryperexecutor"),
                "startTime": a.get("starttime"),
                "durationMs": a.get("duration"),
                "state": a.get("state"),
            }
            for a in d.get("activeapps", [])
        ],
        "completedApps": len(d.get("completedapps", [])),
    }


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
