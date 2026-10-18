"""Evidence pack (roadmap P7.5): screenshots of the website and of the running cluster's own UIs.

Run while ``scripts/demo.ps1`` is up, so the live pages show live data and the Spark application UI exists::

    uv run python -m velograph.ops.evidence_shots

Writes JPEGs to ``docs/assets/evidence/``: the website (home, search, live map, cluster, every portal) and the
Spark master, the scoring job's Spark UI and Kafka UI. Each shot names what it proves in the report.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated

import typer
from playwright.sync_api import Page, sync_playwright

from velograph.common.logs import configure_logging
from velograph.common.settings import REPO_ROOT

log = logging.getLogger("velograph.evidence_shots")

WEB = "http://localhost:3000"
KAFKA_UI = "http://localhost:8089/ui/clusters/velograph"
MAP = "figure.map-surface"
CENTER = "e => e.scrollIntoView({ block: 'center', behavior: 'instant' })"
# The colour preset's localStorage key (web/src/components/site/themes.ts), read before the first paint.
THEME_KEY = "velograph-theme"


@dataclass(frozen=True)
class Shot:
    file: str
    url: str
    wait_ms: int = 2500
    scroll_to: str | None = None  # CSS selector brought to the middle of the screen first
    keys: list[str] = field(default_factory=list)  # pressed in order (a string is typed)
    full_page: bool = False
    theme: str | None = None  # the site's colour preset, so that a page of the report is not all one colour


def plan() -> list[Shot]:
    def portal(n: int, pid: str, theme: str, scroll_to: str | None = None) -> Shot:
        return Shot(
            f"{n:02d}-portal-{pid}.jpg", f"{WEB}/portal/{pid}", wait_ms=4000, scroll_to=scroll_to, theme=theme
        )

    # Neighbouring shots in the report get different presets.
    return [
        Shot("01-home-hero.jpg", f"{WEB}/", wait_ms=6000, theme="nebula"),
        Shot("02-home-pipeline-story.jpg", f"{WEB}/", scroll_to="#stop-graphx", wait_ms=3000, theme="ember"),
        Shot("03-home-portals.jpg", f"{WEB}/", scroll_to="#portals h2", wait_ms=3000, theme="noir"),
        Shot(
            "04-station-search.jpg",
            f"{WEB}/",
            keys=["Control+k", "type:bedford"],
            wait_ms=2000,
            theme="ember",
        ),
        Shot("05-live-map.jpg", f"{WEB}/live", scroll_to=MAP, wait_ms=9000, theme="midnight"),
        Shot(
            "06-live-station-drawer.jpg",
            f"{WEB}/live?station=5308.04",
            scroll_to=MAP,
            wait_ms=9000,
            theme="ember",
        ),
        Shot(
            "07-cluster-topology.jpg",
            f"{WEB}/cluster",
            scroll_to="text=Spark standalone",
            wait_ms=8000,
            theme="noir",
        ),
        Shot(
            "08-cluster-pipeline.jpg",
            f"{WEB}/cluster",
            scroll_to="text=The pipeline, end",
            wait_ms=5000,
            theme="aurora",
        ),
        portal(9, "lake", "aurora"),
        portal(10, "rdd-lab", "rose"),
        portal(11, "sql", "noir"),
        portal(12, "spark-ui", "midnight"),
        portal(13, "pregel", "nebula"),
        portal(14, "network", "aurora"),
        portal(15, "graph-ops", "midnight"),
        portal(16, "models", "rose"),
        portal(17, "models", "ember", scroll_to="text=Will a station drain or fill?"),
        portal(18, "streaming", "rose"),
        portal(19, "streaming", "aurora", scroll_to="text=Pull the plug"),
        Shot("20-syllabus.jpg", f"{WEB}/syllabus", wait_ms=3500, theme="nebula"),
        Shot("21-spark-master.jpg", "http://localhost:8080/", wait_ms=1500),
        Shot("22-spark-app-jobs.jpg", "http://localhost:4040/jobs/", wait_ms=1500),
        Shot("23-spark-structured-streaming.jpg", "http://localhost:4040/StreamingQuery/", wait_ms=1500),
        Shot("24-kafka-ui-brokers.jpg", f"{KAFKA_UI}/brokers", wait_ms=4000),
        Shot("25-kafka-ui-topics.jpg", f"{KAFKA_UI}/all-topics", wait_ms=4000),
    ]


def capture(page: Page, shot: Shot, out_dir: Path) -> Path:
    if shot.theme:
        # Init scripts run before the page's own, in the order they were added, so the latest preset wins.
        page.add_init_script(f"try {{ localStorage.setItem('{THEME_KEY}', '{shot.theme}') }} catch (e) {{}}")
    page.goto(shot.url, wait_until="networkidle", timeout=60_000)
    target = page.locator(shot.scroll_to).first if shot.scroll_to else None
    if target is not None:
        target.wait_for(state="attached", timeout=15_000)
        target.evaluate(CENTER)
    for key in shot.keys:
        if key.startswith("type:"):
            page.keyboard.type(key[5:], delay=60)
        else:
            page.keyboard.press(key)
        page.wait_for_timeout(400)
    page.wait_for_timeout(shot.wait_ms)  # reveals, charts created near the viewport, map tiles, live updates
    if target is not None:
        # Charts and code panels above the target grow as they mount, which pushes the target down: aim again.
        target.evaluate(CENTER)
        page.wait_for_timeout(1200)
    path = out_dir / shot.file
    page.screenshot(path=str(path), type="jpeg", quality=82, full_page=shot.full_page)
    return path


def main(
    out_dir: Annotated[Path, typer.Option(help="Where the JPEGs go.")] = REPO_ROOT / "docs/assets/evidence",
    only: Annotated[str | None, typer.Option(help="Comma-separated file-name prefixes, e.g. 05,07.")] = None,
) -> None:
    configure_logging("evidence_shots")
    out_dir.mkdir(parents=True, exist_ok=True)
    shots = [s for s in plan() if not only or any(s.file.startswith(p) for p in only.split(","))]
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge", headless=True)
        # 1.5x pixels: crisp in a printed report without huge files.
        context = browser.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=1.5)
        page = context.new_page()
        try:
            for shot in shots:
                path = capture(page, shot, out_dir)
                log.info("%-36s <- %s (%.0f KB)", shot.file, shot.url, path.stat().st_size / 1024)
        finally:
            browser.close()


if __name__ == "__main__":
    typer.run(main)
