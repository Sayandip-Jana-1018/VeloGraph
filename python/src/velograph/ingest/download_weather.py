"""Download hourly New York weather from the Open-Meteo archive into ``data/raw/weather/``.

One request covers the whole range at Central Park, the reference point for all NYC weather in
VeloGraph. The raw API response is kept unchanged in ``nyc_hourly.json``. ``manifest.json`` records the
request and how many hours actually have values: the archive (ERA5) trails real time by about five days
and returns nulls for that gap.

Usage (from ``python/``)::

    uv run python -m velograph.ingest.download_weather --start 2026-06-01
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any
from zoneinfo import ZoneInfo

import httpx
import typer

from velograph.common.http import http_client, retrying
from velograph.common.logs import configure_logging
from velograph.common.settings import data_paths

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
CENTRAL_PARK = {"latitude": 40.7812, "longitude": -73.9665}
HOURLY_VARIABLES = (
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "snowfall",
    "wind_speed_10m",
    "wind_gusts_10m",
    "relative_humidity_2m",
    "cloud_cover",
    "weather_code",
)

log = logging.getLogger("velograph.download_weather")


@retrying(attempts=6, max_wait=30)
def fetch_hourly(client: httpx.Client, start: date, end: date) -> dict[str, Any]:
    params: dict[str, str | float] = {
        **CENTRAL_PARK,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": "America/New_York",  # local wall-clock hours, matching the trip timestamps
    }
    response = client.get(ARCHIVE_URL, params=params)
    response.raise_for_status()
    return response.json()


def coverage(payload: dict[str, Any]) -> dict[str, Any]:
    """Hours returned vs hours with a temperature value; the archive leaves its most recent days empty."""
    times = payload["hourly"]["time"]
    temps = payload["hourly"]["temperature_2m"]
    filled = [t for t, v in zip(times, temps, strict=True) if v is not None]
    return {
        "hoursReturned": len(times),
        "hoursWithValues": len(filled),
        "firstHour": filled[0] if filled else None,
        "lastHour": filled[-1] if filled else None,
    }


def _write_json(path: Path, payload: Any) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    tmp.replace(path)


def main(
    start: Annotated[str, typer.Option(help="First day (YYYY-MM-DD).")] = "2026-06-01",
    end: Annotated[str | None, typer.Option(help="Last day (YYYY-MM-DD); default: yesterday in NYC.")] = None,
) -> None:
    paths = data_paths()
    configure_logging("download_weather", log_dir=paths.logs)
    first = date.fromisoformat(start)
    yesterday = datetime.now(ZoneInfo("America/New_York")).date() - timedelta(days=1)
    last = date.fromisoformat(end) if end else yesterday
    if last < first:
        raise typer.BadParameter(f"end {last} is before start {first}")

    with http_client(timeout=60) as client:
        payload = fetch_hourly(client, first, last)

    out_dir = paths.raw / "weather"
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_json(out_dir / "nyc_hourly.json", payload)
    stats = coverage(payload)
    manifest = {
        "schema": "velograph.raw-weather-manifest",
        "version": 1,
        "source": ARCHIVE_URL,
        "location": CENTRAL_PARK,
        "variables": list(HOURLY_VARIABLES),
        "requested": {"start": first.isoformat(), "end": last.isoformat()},
        "fetchedAt": datetime.now(UTC).isoformat(timespec="seconds"),
        **stats,
    }
    _write_json(out_dir / "manifest.json", manifest)
    log.info(
        "weather %s..%s: %d hours returned, %d with values (%s -> %s)",
        first,
        last,
        stats["hoursReturned"],
        stats["hoursWithValues"],
        stats["firstHour"],
        stats["lastHour"],
    )


if __name__ == "__main__":
    typer.run(main)
