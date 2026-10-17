"""Minimal GBFS 2.3 client for Citi Bike (system ``bkn``, operated by Lyft).

Feed URLs are resolved through the official discovery document rather than hard-coded, because the operator
already moved them once (``gbfs.citibikenyc.com`` now points at ``gbfs.lyft.com``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from velograph.common.http import retrying

DISCOVERY_URL = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"

_ONE_DECIMAL = re.compile(r"^\d+\.\d$")


def normalize_station_id(short_name: str) -> str:
    """The lake's station id for a GBFS ``short_name``: ``5980.1`` -> ``5980.10``.

    Mirrors ``velograph.etl.StationIds`` in Scala. Ids carry two decimals, but a few GBFS entries (and about
    1 % of trip records) drop the trailing zero; without the fix the live feed would miss those stations'
    features.
    """
    return short_name + "0" if _ONE_DECIMAL.match(short_name) else short_name


@dataclass(frozen=True)
class StationInfo:
    station_id: str  # the lake's id (normalized short_name), the Kafka key
    name: str
    lat: float
    lon: float
    capacity: int


def station_registry(information: dict[str, Any]) -> dict[str, StationInfo]:
    """GBFS station UUID -> the station as the lake knows it, from a ``station_information`` payload."""
    registry: dict[str, StationInfo] = {}
    for s in information["data"]["stations"]:
        short_name = s.get("short_name")
        if not short_name:
            continue  # without a short_name a station cannot be joined to its trips or features
        registry[s["station_id"]] = StationInfo(
            station_id=normalize_station_id(str(short_name)),
            name=str(s.get("name", "")),
            lat=float(s["lat"]),
            lon=float(s["lon"]),
            capacity=int(s.get("capacity") or 0),
        )
    return registry


# Fields that describe a station's inventory. A station counts as "changed" only when one of these moves;
# `last_reported` alone ticks over constantly and would turn every delta into a full snapshot.
INVENTORY_FIELDS: tuple[str, ...] = (
    "num_bikes_available",
    "num_ebikes_available",
    "num_bikes_disabled",
    "num_docks_available",
    "num_docks_disabled",
    "is_installed",
    "is_renting",
    "is_returning",
)

Station = dict[str, Any]
InventoryKey = tuple[Any, ...]


def inventory_key(station: Station) -> InventoryKey:
    return tuple(station.get(field) for field in INVENTORY_FIELDS)


def select_changes(
    previous: dict[str, InventoryKey], stations: list[Station], *, force_snapshot: bool
) -> tuple[str, list[Station], dict[str, InventoryKey]]:
    """Change data capture over one poll.

    Returns ``(kind, stations_to_write, new_state)``. ``kind`` is ``"snapshot"`` (every station; used on the
    first poll and periodically, so recordings are self-contained) or ``"delta"`` (only stations whose
    inventory changed).
    """
    current = {s["station_id"]: inventory_key(s) for s in stations}
    if force_snapshot or not previous:
        return "snapshot", stations, current
    changed = [s for s in stations if previous.get(s["station_id"]) != current[s["station_id"]]]
    return "delta", changed, current


class GbfsClient:
    def __init__(self, http: httpx.Client, discovery_url: str = DISCOVERY_URL, language: str = "en") -> None:
        self._http = http
        self._discovery_url = discovery_url
        self._language = language
        self._feeds: dict[str, str] = {}

    def feed_url(self, name: str) -> str:
        if name not in self._feeds:
            self._feeds = self._discover()
        try:
            return self._feeds[name]
        except KeyError:
            raise KeyError(f"GBFS feed {name!r} not listed; available: {sorted(self._feeds)}") from None

    def fetch(self, name: str) -> dict[str, Any]:
        return self._get(name).json()

    def fetch_with_clock(self, name: str) -> tuple[dict[str, Any], float | None]:
        """The feed, and the server's clock when it answered, in epoch seconds.

        The feed sits behind a CDN: a cached copy keeps the origin's ``Date`` and says in ``Age`` how many
        seconds it has been cached, so the time now is their sum.
        """
        response = self._get(name)
        date = response.headers.get("date")
        if not date:
            return response.json(), None
        age = response.headers.get("age", "0")
        server = parsedate_to_datetime(date).timestamp() + (int(age) if age.isdigit() else 0)
        return response.json(), server

    @retrying()
    def _get(self, name: str) -> httpx.Response:
        response = self._http.get(self.feed_url(name))
        response.raise_for_status()
        return response

    @retrying()
    def _discover(self) -> dict[str, str]:
        response = self._http.get(self._discovery_url)
        response.raise_for_status()
        feeds = response.json()["data"][self._language]["feeds"]
        return {feed["name"]: feed["url"] for feed in feeds}
