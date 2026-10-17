"""Kafka plumbing shared by the producers, the gateway and the ops tools (ARCHITECTURE §9).

Topic names mirror ``infra/kafka/create-topics.sh``, which creates them; ``tests/test_kafka.py`` checks that
the two agree. Every value is a JSON object carrying ``schema`` and ``v``, so consumers can tell what it is.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import Counter, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

# The brokers' EXTERNAL listeners, as published to the host by infra/docker-compose.yml.
BOOTSTRAP = "localhost:9092,localhost:9094,localhost:9096"


class Topics:
    STATION_STATUS = "velograph.gbfs.station-status.v1"
    WEATHER = "velograph.weather.current.v1"
    TRIPS_REPLAY = "velograph.trips.replay.v1"
    TRIP_METRICS = "velograph.trip-metrics.v1"
    PREDICTIONS = "velograph.predictions.v1"
    ALERTS = "velograph.alerts.v1"
    DLQ = "velograph.dlq.v1"

    ALL: tuple[str, ...] = (STATION_STATUS, WEATHER, TRIPS_REPLAY, TRIP_METRICS, PREDICTIONS, ALERTS, DLQ)


# ARCHITECTURE §9.3. acks=all waits for every in-sync replica (at least min.insync.replicas = 2 of 3), and
# idempotence lets the broker drop the duplicates a retry would otherwise write, so one broker can fail
# mid-send without losing or doubling a message.
PRODUCER_DEFAULTS: dict[str, Any] = {
    "acks": "all",
    "enable.idempotence": True,
    "compression.type": "lz4",
    "linger.ms": 20,
}

log = logging.getLogger("velograph.kafka")


def bootstrap_servers() -> str:
    return os.environ.get("VELOGRAPH_KAFKA_BOOTSTRAP", BOOTSTRAP)


def now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass
class DeliveryStats:
    """What the brokers confirmed, counted in the producer's delivery callbacks."""

    delivered: int = 0
    failed: int = 0
    bytes: int = 0
    by_topic: Counter[str] = field(default_factory=Counter)
    by_partition: Counter[tuple[str, int]] = field(default_factory=Counter)
    errors: Counter[str] = field(default_factory=Counter)
    _latencies_ms: deque[float] = field(default_factory=lambda: deque(maxlen=5000))

    def callback(self, sent_at: float) -> Callable[[KafkaError | None, Message], None]:
        def on_delivery(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                self.failed += 1
                self.errors[err.name()] += 1
                log.error("delivery to %s failed: %s", msg.topic(), err)
                return
            self.delivered += 1
            self.bytes += len(msg.value() or b"")
            self.by_topic[msg.topic()] += 1
            self.by_partition[(msg.topic(), msg.partition())] += 1
            self._latencies_ms.append((time.monotonic() - sent_at) * 1000)

        return on_delivery

    def latency_ms(self, quantile: float) -> float:
        """A quantile of produce -> acknowledged times over the last 5,000 deliveries."""
        if not self._latencies_ms:
            return 0.0
        ordered = sorted(self._latencies_ms)
        return ordered[min(len(ordered) - 1, int(quantile * len(ordered)))]


class JsonProducer:
    """An idempotent ``acks=all`` JSON producer, with delivery accounting and a dead-letter route."""

    def __init__(self, client_id: str, *, bootstrap: str | None = None, **overrides: Any) -> None:
        self.client_id = client_id
        self.stats = DeliveryStats()
        self._producer = Producer(
            {
                **PRODUCER_DEFAULTS,
                "bootstrap.servers": bootstrap or bootstrap_servers(),
                "client.id": client_id,
                **overrides,
            }
        )

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        payload = json.dumps(value, separators=(",", ":"), allow_nan=False).encode("utf-8")
        self.send_raw(topic, key, payload)

    def send_raw(self, topic: str, key: str, payload: bytes) -> None:
        while True:
            try:
                self._producer.produce(
                    topic,
                    key=key.encode("utf-8"),
                    value=payload,
                    on_delivery=self.stats.callback(time.monotonic()),
                )
                break
            except BufferError:  # the local queue is full: serve delivery reports until there is room
                self._producer.poll(0.5)
        self._producer.poll(0)

    def dead_letter(self, error: str, raw: str | bytes, *, origin: str | None = None) -> None:
        """A message nobody downstream could use: kept with the reason, for a human to look at."""
        text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        self.send(
            Topics.DLQ,
            self.client_id,
            {
                "schema": "velograph.dead-letter",
                "v": 1,
                "source": self.client_id,
                "origin": origin,
                "error": error,
                "raw": text[:4000],
                "at": now_iso(),
            },
        )

    def poll(self, timeout: float = 0) -> None:
        self._producer.poll(timeout)

    def flush(self, timeout: float = 30.0) -> int:
        """Waits for outstanding deliveries; returns how many are still unconfirmed."""
        return self._producer.flush(timeout)
