"""Records the cluster as it is now to ``serving/cluster.json`` (roadmap P6.7).

The cluster page follows the gateway's ``/api/cluster`` while the gateway runs. This snapshot is what it
shows when nothing does: the Spark master with its workers and applications, the Kafka brokers with every
partition's leader and in-sync replicas, and the memory of each container. Take it while a streaming job
runs, so the recording shows the cluster at work.

    uv run python -m velograph.ops.cluster_snapshot
"""

from __future__ import annotations

import httpx
import typer
from confluent_kafka.admin import AdminClient

from velograph.api.cluster import containers, kafka_cluster, spark_master
from velograph.common.kafka import bootstrap_servers
from velograph.common.logs import configure_logging
from velograph.common.serving import write_serving
from velograph.common.settings import data_paths


def main() -> None:
    log = configure_logging("cluster_snapshot")
    admin = AdminClient({"bootstrap.servers": bootstrap_servers(), "socket.timeout.ms": 4000})
    with httpx.Client() as http:
        data = {"spark": spark_master(http), "kafka": kafka_cluster(admin), "containers": containers()}
    path = write_serving(data_paths().serving / "cluster.json", "cluster", "cluster_snapshot", data)
    spark, kafka, docker = data["spark"], data["kafka"], data["containers"]
    log.info(
        "Spark: %s workers, %s apps running · Kafka: %s brokers, %s topics · %s containers -> %s",
        spark.get("aliveWorkers", "down"),
        len(spark.get("activeApps", [])),
        len(kafka.get("brokers", [])),
        len(kafka.get("topics", [])),
        len(docker.get("containers", [])),
        path,
    )


if __name__ == "__main__":
    typer.run(main)
