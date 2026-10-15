"""Minimal GBFS 2.3 client for Citi Bike (system ``bkn``, operated by Lyft).

Feed URLs are resolved through the official discovery document rather than hard-coded, because the operator
already moved them once (``gbfs.citibikenyc.com`` now points at ``gbfs.lyft.com``).
"""

from __future__ import annotations

from typing import Any

import httpx

from velograph.common.http import retrying

DISCOVERY_URL = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"

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

    @retrying()
    def fetch(self, name: str) -> dict[str, Any]:
        response = self._http.get(self.feed_url(name))
        response.raise_for_status()
        return response.json()

    @retrying()
    def _discover(self) -> dict[str, str]:
        response = self._http.get(self._discovery_url)
        response.raise_for_status()
        feeds = response.json()["data"][self._language]["feeds"]
        return {feed["name"]: feed["url"] for feed in feeds}
