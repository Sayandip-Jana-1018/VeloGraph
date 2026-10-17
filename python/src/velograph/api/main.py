"""The VeloGraph gateway (roadmap P5.5, ARCHITECTURE §10).

=====================  =================================================================================
``/api/health``        liveness, plus whether Kafka, the Spark master and a fresh live snapshot are there
``/api/live/snapshot`` the scoring job's latest ``snapshot.json``
``/api/live/stream``   server-sent events: ``hello``, then ``batch`` summaries and ``alert`` s, from Kafka
``/api/live/{file}``   the other live exports: ``progress``, ``producer``, ``replay``
``/api/cluster``       the Spark master (workers, cores, apps) and Kafka (brokers, partitions, leaders, ISR)
=====================  =================================================================================

The website works without the gateway (it falls back to the static exports), so every endpoint answers
quickly and reports what is missing instead of failing.

    uv run uvicorn velograph.api.main:app --port 8000
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sse_starlette.sse import EventSourceResponse

from velograph.api.cluster import containers, kafka_cluster, spark_master
from velograph.api.live import Broadcaster, LiveConsumer
from velograph.common.kafka import bootstrap_servers, now_iso
from velograph.common.settings import data_paths

LIVE_FILES = {
    "snapshot": "snapshot.json",
    "progress": "stream-progress.json",
    "producer": "producer-stats.json",
    "replay": "replay-analytics.json",
}
STALE_AFTER_S = 120  # four missed micro-batches


def snapshot_status(live_dir: Path) -> dict[str, Any]:
    path = live_dir / LIVE_FILES["snapshot"]
    if not path.exists():
        return {"exists": False}
    age = time.time() - path.stat().st_mtime
    return {"exists": True, "ageS": round(age, 1), "fresh": age < STALE_AFTER_S}


def create_app(*, live_dir: Path | None = None, start_consumer: bool = True) -> FastAPI:
    live = live_dir or data_paths().serving / "live"
    broadcaster = Broadcaster()
    consumer = LiveConsumer(broadcaster) if start_consumer else None
    admin = AdminClient({"bootstrap.servers": bootstrap_servers(), "socket.timeout.ms": 4000})
    http = httpx.Client()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        broadcaster.bind(asyncio.get_running_loop())
        if consumer is not None:
            consumer.start()
        yield
        if consumer is not None:
            consumer.stop()
            await asyncio.to_thread(consumer.join, 5)
        http.close()

    app = FastAPI(title="VeloGraph gateway", version="1", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        kafka, spark = await asyncio.gather(
            asyncio.to_thread(kafka_cluster, admin), asyncio.to_thread(spark_master, http)
        )
        snapshot = snapshot_status(live)
        stream = {
            "consumer": consumer is not None and consumer.is_alive(),
            "connected": consumer is not None and consumer.connected,
            "consumed": consumer.consumed if consumer else 0,
            "subscribers": broadcaster.subscribers,
            "lastEventAgeS": round(time.time() - broadcaster.last_event_at, 1)
            if broadcaster.last_event_at
            else None,
        }
        ok = kafka["ok"] and snapshot.get("fresh", False)
        return {
            "status": "ok" if ok else "degraded",
            "generatedAt": now_iso(),
            "kafka": {
                "ok": kafka["ok"],
                "brokers": len(kafka.get("brokers", [])),
                "error": kafka.get("error"),
            },
            "spark": {
                "ok": spark["ok"],
                "workers": spark.get("aliveWorkers", 0),
                "error": spark.get("error"),
            },
            "snapshot": snapshot,
            "stream": stream,
        }

    @app.get("/api/live/stream")
    async def stream(request: Request) -> EventSourceResponse:
        async def events() -> AsyncIterator[dict[str, str]]:
            queue = broadcaster.subscribe()
            try:
                hello = {"lastBatch": broadcaster.last_batch, "alerts": list(broadcaster.recent_alerts)}
                yield {"event": "hello", "data": json.dumps(hello)}
                while not await request.is_disconnected():
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=10)
                    except TimeoutError:
                        continue
                    yield {"event": event["event"], "data": json.dumps(event["data"])}
            finally:
                broadcaster.unsubscribe(queue)

        return EventSourceResponse(events(), ping=15)

    @app.get("/api/live/{name}")
    async def live_file(name: str) -> FileResponse:
        if name not in LIVE_FILES:
            raise HTTPException(404, f"no live export {name!r}; known: {sorted(LIVE_FILES)}")
        path = live / LIVE_FILES[name]
        if not path.exists():
            raise HTTPException(503, f"{LIVE_FILES[name]} has not been written yet")
        # Files are replaced atomically by their writers, so a read never sees half of one.
        return FileResponse(path, media_type="application/json", headers={"Cache-Control": "no-store"})

    @app.get("/api/cluster")
    async def cluster() -> dict[str, Any]:
        kafka, spark, docker = await asyncio.gather(
            asyncio.to_thread(kafka_cluster, admin),
            asyncio.to_thread(spark_master, http),
            asyncio.to_thread(container_stats),
        )
        return {"generatedAt": now_iso(), "spark": spark, "kafka": kafka, "containers": docker}

    stats: dict[str, Any] = {"at": float("-inf"), "value": None}

    def container_stats() -> dict[str, Any]:
        # `docker stats` takes about two seconds, and the page polls every ten: reuse a reading for 15 s.
        if stats["value"] is None or time.monotonic() - stats["at"] > 15:
            stats["value"], stats["at"] = containers(), time.monotonic()
        return stats["value"]

    return app


app = create_app()
