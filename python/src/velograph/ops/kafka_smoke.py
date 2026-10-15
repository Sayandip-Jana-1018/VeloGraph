"""Kafka round-trip smoke test from the host (roadmap P0.6).

Creates a throwaway topic replicated to all three brokers, produces keyed messages through their EXTERNAL
listeners with an idempotent producer (``acks=all``), reads every message back with a fresh consumer group,
then deletes the topic. Exit code 0 means the cluster accepts fully replicated writes and serves reads.

Usage (from ``python/``)::

    uv run python -m velograph.ops.kafka_smoke
"""

from __future__ import annotations

import json
import time
import uuid
from collections import Counter
from typing import Annotated

import typer
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from velograph.common.logs import configure_logging

BOOTSTRAP = "localhost:9092,localhost:9094,localhost:9096"


def main(
    messages: Annotated[int, typer.Option(help="Messages to round-trip.")] = 300,
    bootstrap: Annotated[str, typer.Option(help="Comma-separated bootstrap servers.")] = BOOTSTRAP,
    timeout: Annotated[float, typer.Option(help="Seconds allowed for each phase.")] = 30.0,
) -> None:
    log = configure_logging("kafka_smoke")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    brokers = sorted(admin.list_topics(timeout=10).brokers)
    log.info("brokers online: %s", brokers)
    if len(brokers) != 3:
        log.error("expected 3 brokers, found %d", len(brokers))
        raise typer.Exit(code=1)

    topic = f"velograph.smoke-{uuid.uuid4().hex[:8]}.v1"
    created = admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=3)])
    created[topic].result(timeout=timeout)
    try:
        partitions = admin.list_topics(topic, timeout=10).topics[topic].partitions.values()
        for p in sorted(partitions, key=lambda p: p.id):
            log.info("partition %d: leader %d, replicas %s, in-sync %s", p.id, p.leader, p.replicas, p.isrs)

        delivered: Counter[int] = Counter()
        failures: list[KafkaError] = []

        def on_delivery(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                failures.append(err)
            else:
                delivered[msg.partition()] += 1

        producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 5,
                "compression.type": "lz4",
            }
        )
        started = time.monotonic()
        for seq in range(messages):
            value = json.dumps({"seq": seq, "sent_at": time.time()})
            producer.produce(topic, key=f"station-{seq % 25}", value=value, on_delivery=on_delivery)
            producer.poll(0)
        producer.flush(timeout)
        produce_ms = (time.monotonic() - started) * 1000
        if failures or sum(delivered.values()) != messages:
            log.error("delivered %d of %d; errors: %s", sum(delivered.values()), messages, failures[:3])
            raise typer.Exit(code=1)
        log.info("produced %d messages in %.0f ms, per partition %s", messages, produce_ms, dict(delivered))

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"velograph-smoke-{uuid.uuid4().hex[:8]}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])
        seen: set[int] = set()
        started = time.monotonic()
        try:
            while len(seen) < messages and time.monotonic() - started < timeout:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                seen.add(json.loads(msg.value())["seq"])
        finally:
            consumer.close()
        if len(seen) != messages:
            log.error("consumed %d of %d messages within %.0f s", len(seen), messages, timeout)
            raise typer.Exit(code=1)
        log.info("consumed all %d messages in %.0f ms", messages, (time.monotonic() - started) * 1000)
    finally:
        admin.delete_topics([topic])[topic].result(timeout=timeout)
        log.info("deleted %s", topic)


if __name__ == "__main__":
    typer.run(main)
