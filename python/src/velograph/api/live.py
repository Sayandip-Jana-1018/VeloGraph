"""Kafka -> server-sent events: one consumer in the ``velograph-gateway`` group, fanned out to every browser.

The scoring job writes a prediction for every station each micro-batch and an alert whenever a station
starts to run empty or full. The consumer thread turns the predictions into one ``batch`` summary per
micro-batch and passes alerts on as they come. Browsers subscribe through ``/api/live/stream``. Each
gets its own bounded queue, so a slow tab drops its oldest events rather than holding up the others.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from velograph.common.kafka import Topics, bootstrap_servers

log = logging.getLogger("velograph.api.live")

GROUP_ID = "velograph-gateway"
RISKS = ("EMPTY", "EMPTY_RISK", "FULL", "FULL_RISK", "OFFLINE", "OK")


@dataclass
class BatchSummary:
    """The predictions of one micro-batch, counted by state."""

    batch_id: int
    as_of: str
    source: str
    stations: int = 0
    risks: Counter[str] = field(default_factory=Counter)

    def add(self, prediction: dict[str, Any]) -> None:
        self.stations += 1
        self.risks[str(prediction.get("risk"))] += 1

    def event(self) -> dict[str, Any]:
        return {
            "batchId": self.batch_id,
            "asOf": self.as_of,
            "source": self.source,
            "stations": self.stations,
            **{risk: self.risks.get(risk, 0) for risk in RISKS},
        }


class BatchAssembler:
    """Groups prediction messages by batch: a batch is complete when the next one starts, or after a pause."""

    def __init__(self, quiet_s: float = 2.0) -> None:
        self._current: BatchSummary | None = None
        self._last_message = 0.0
        self._quiet_s = quiet_s

    def add(self, prediction: dict[str, Any], now: float) -> BatchSummary | None:
        batch_id = int(prediction.get("batch_id", -1))
        finished = None
        if self._current is not None and batch_id != self._current.batch_id:
            finished = self._current
            self._current = None
        if self._current is None:
            self._current = BatchSummary(
                batch_id, str(prediction.get("as_of")), str(prediction.get("source"))
            )
        self._current.add(prediction)
        self._last_message = now
        return finished

    def flush_if_quiet(self, now: float) -> BatchSummary | None:
        if self._current is not None and now - self._last_message >= self._quiet_s:
            finished, self._current = self._current, None
            return finished
        return None


class Broadcaster:
    """Hands events from the consumer thread to every subscribed browser's asyncio queue."""

    def __init__(self, history: int = 50, queue_size: int = 200) -> None:
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue_size = queue_size
        self.recent_alerts: deque[dict[str, Any]] = deque(maxlen=history)
        self.last_batch: dict[str, Any] | None = None
        self.last_event_at: float | None = None

    def bind(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(self._queue_size)
        with self._lock:
            self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        with self._lock:
            self._subscribers.discard(queue)

    @property
    def subscribers(self) -> int:
        return len(self._subscribers)

    def publish(self, event: dict[str, Any]) -> None:
        """Called from the consumer thread."""
        self.last_event_at = time.time()
        if event["event"] == "alert":
            self.recent_alerts.append(event["data"])
        elif event["event"] == "batch":
            self.last_batch = event["data"]
        if self._loop is None:
            return
        with self._lock:
            queues = list(self._subscribers)
        for queue in queues:
            self._loop.call_soon_threadsafe(_offer, queue, event)


def _offer(queue: asyncio.Queue[dict[str, Any]], event: dict[str, Any]) -> None:
    if queue.full():  # a slow browser loses its oldest event, never the stream
        queue.get_nowait()
    queue.put_nowait(event)


class LiveConsumer:
    """The gateway's Kafka consumer, on its own thread: alerts pass straight through, predictions become
    batch summaries."""

    def __init__(self, broadcaster: Broadcaster, bootstrap: str | None = None) -> None:
        self._broadcaster = broadcaster
        self._servers = bootstrap or bootstrap_servers()
        self._halt = threading.Event()
        self._thread = threading.Thread(target=self._run, name="velograph-gateway-consumer", daemon=True)
        self.connected = False
        self.consumed = 0
        self.errors: Counter[str] = Counter()

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._halt.set()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout)

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def _run(self) -> None:
        consumer = Consumer(
            {
                "bootstrap.servers": self._servers,
                "group.id": GROUP_ID,
                "client.id": "velograph-gateway",
                # A gateway that starts late has no use for old alerts; it begins where Kafka is now. Its
                # committed offsets let a restart resume where it stopped instead.
                "auto.offset.reset": "latest",
                "enable.auto.commit": True,
            }
        )
        consumer.subscribe([Topics.ALERTS, Topics.PREDICTIONS])
        assembler = BatchAssembler()
        try:
            while not self._halt.is_set():
                msg = consumer.poll(0.5)
                now = time.time()
                if (done := assembler.flush_if_quiet(now)) is not None:
                    self._broadcaster.publish({"event": "batch", "data": done.event()})
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        self.errors[msg.error().name()] += 1
                        log.warning("consumer error: %s", msg.error())
                    continue
                self.connected = True
                self.consumed += 1
                try:
                    value = json.loads(msg.value())
                except (TypeError, ValueError):
                    self.errors["bad_json"] += 1
                    continue
                if msg.topic() == Topics.ALERTS:
                    self._broadcaster.publish({"event": "alert", "data": value})
                elif (done := assembler.add(value, now)) is not None:
                    self._broadcaster.publish({"event": "batch", "data": done.event()})
        except KafkaException:
            log.exception("gateway consumer stopped")
        finally:
            consumer.close()  # commits the final offsets and leaves the group cleanly
