"""Backup demo video (roadmap P8.4): a captioned walkthrough of the running system, recorded in headless Edge.

Run while ``scripts/demo.ps1`` is up (live data, the scoring job's Spark UI on :4040)::

    uv run python -m velograph.ops.demo_video

It follows the demo script (ROADMAP section 8): the home page, the pipeline story, the station search, the
live map, then the cluster while broker 2 is stopped for real (``velograph.ops.drills broker``), Spark's and
Kafka's own UIs during the outage, and the portals. A caption names each step and a dot shows the pointer,
since a headless recording has neither a narrator nor a cursor. The result is a WebM file.
"""

from __future__ import annotations

import logging
import subprocess
import time
from pathlib import Path
from typing import Annotated

import typer
from playwright.sync_api import Page, sync_playwright

from velograph.common.logs import configure_logging
from velograph.common.settings import REPO_ROOT

log = logging.getLogger("velograph.demo_video")

WEB = "http://localhost:3000"
SIZE = {"width": 1280, "height": 720}

# A caption bar and a pointer dot, added to every page the recording opens.
OVERLAY = """
(() => {
  const add = () => {
    if (document.getElementById('__demo_caption')) return;
    const cap = document.createElement('div');
    cap.id = '__demo_caption';
    cap.style.cssText = 'position:fixed;left:50%;bottom:22px;transform:translateX(-50%);z-index:2147483647;' +
      'max-width:86%;padding:10px 20px;border-radius:14px;background:rgba(10,8,30,.86);color:#fff;' +
      'font:600 17px/1.35 Inter,Segoe UI,sans-serif;box-shadow:0 10px 30px rgba(0,0,0,.45);' +
      'border:1px solid rgba(255,224,102,.55);pointer-events:none;transition:opacity .3s;opacity:0';
    document.body.appendChild(cap);
    const dot = document.createElement('div');
    dot.id = '__demo_cursor';
    dot.style.cssText = 'position:fixed;left:0;top:0;width:18px;height:18px;margin:-9px 0 0 -9px;' +
      'z-index:2147483647;border-radius:50%;background:rgba(255,224,102,.9);' +
      'box-shadow:0 0 0 4px rgba(255,224,102,.3);' +
      'pointer-events:none;transition:transform .08s';
    document.body.appendChild(dot);
    addEventListener('mousemove', (e) => {
      dot.style.left = e.clientX + 'px';
      dot.style.top = e.clientY + 'px';
    }, true);
    addEventListener('mousedown', () => { dot.style.transform = 'scale(.7)'; }, true);
    addEventListener('mouseup', () => { dot.style.transform = ''; }, true);
  };
  window.__caption = (text) => {
    add();
    const cap = document.getElementById('__demo_caption');
    cap.textContent = text;
    cap.style.opacity = text ? '1' : '0';
  };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', add); else add();
})();
"""


class Walk:
    """The recorded browser page, with the small moves a person would make."""

    def __init__(self, page: Page) -> None:
        self.page = page

    def go(self, url: str, caption: str, settle_ms: int = 2500) -> None:
        self.page.goto(url, wait_until="networkidle", timeout=60_000)
        self.caption(caption)
        self.page.mouse.move(SIZE["width"] * 0.62, SIZE["height"] * 0.45)
        self.page.wait_for_timeout(settle_ms)

    def caption(self, text: str) -> None:
        self.page.evaluate("t => window.__caption && window.__caption(t)", text)

    def pause(self, ms: int) -> None:
        self.page.wait_for_timeout(ms)

    def scroll(self, pixels: int, step: int = 9, every_ms: int = 16) -> None:
        """Scrolls smoothly, like a trackpad: small wheel steps on every frame."""
        moved = 0
        while moved < pixels:
            self.page.mouse.wheel(0, step)
            moved += step
            self.page.wait_for_timeout(every_ms)

    def scroll_to(self, selector: str, block: str = "center") -> None:
        self.page.locator(selector).first.evaluate(
            "(e, b) => e.scrollIntoView({ block: b, behavior: 'smooth' })", block
        )
        self.page.wait_for_timeout(1400)

    def point_at(self, selector: str) -> None:
        box = self.page.locator(selector).first.bounding_box()
        if box:
            self.page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=25)


def storyboard(walk: Walk, drill: list[subprocess.Popen[bytes]]) -> None:
    p = walk.page
    walk.go(
        f"{WEB}/", "VeloGraph: 20.5 million real Citi Bike trips through Spark, GraphX, MLlib and Kafka", 4500
    )
    walk.caption("The hero replays one real evening (Thu 17 Sep): every streak is a real trip, at 120x speed")
    walk.pause(5000)
    walk.scroll(520)
    walk.caption("Headline numbers, all read from what the Spark jobs wrote")
    walk.pause(2500)

    walk.scroll_to("#pipeline h2", "start")
    walk.caption("The pipeline as a subway line: each stop is a Spark module with its own evidence portal")
    walk.pause(2000)
    for stop in ("lake", "rdds", "sql", "graphx", "mllib", "streaming", "cluster", "syllabus"):
        walk.scroll_to(f"#stop-{stop}")
        walk.pause(1300)
    walk.scroll_to("#portals h2", "start")
    walk.caption("Eleven evidence portals, one per module")
    walk.pause(3000)

    walk.caption("Ctrl K: search any of the 2,490 stations")
    p.keyboard.press("Control+k")
    walk.pause(700)
    p.keyboard.type("bedford", delay=110)
    walk.pause(1400)
    p.keyboard.press("Enter")
    walk.pause(2600)
    walk.point_at("text=Show on the live map")
    walk.pause(600)
    p.click("text=Show on the live map")
    p.wait_for_url("**/live?station=*", timeout=20_000)
    walk.pause(1500)
    walk.scroll_to("figure.map-surface")
    walk.caption(
        "Live: Kafka -> Spark Structured Streaming on the cluster, every station re-scored every 30 s"
    )
    walk.pause(6000)
    walk.caption(
        "Red: empty or about to be; blue: full or about to be; yellow arcs: suggested rebalancing moves"
    )
    walk.point_at("text=Alerts")
    walk.pause(5000)

    walk.go(
        f"{WEB}/cluster",
        "The real cluster: Spark master + 2 workers, 3 Kafka brokers (KRaft), live every 10 s",
        2000,
    )
    walk.scroll_to("text=Spark standalone")
    walk.pause(4000)
    walk.caption("Resilience drill: stopping Kafka broker 2 now, while the stream runs")
    drill.append(
        subprocess.Popen(
            [
                "uv",
                "run",
                "python",
                "-m",
                "velograph.ops.drills",
                "broker",
                "--broker",
                "2",
                "--down-s",
                "75",
            ],
            cwd=REPO_ROOT / "python",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    )
    walk.pause(
        26_000
    )  # the topology refreshes every 10 s: broker 2 drops out and its partitions change leader
    walk.caption(
        "Broker 2 is down: its partitions moved to brokers 1 and 3; every partition keeps 2 in-sync copies"
    )
    walk.pause(6000)

    walk.go(
        "http://localhost:8089/ui/clusters/velograph/brokers",
        "Kafka UI during the outage: 2 brokers online",
        5000,
    )
    walk.go(
        "http://localhost:4040/StreamingQuery/",
        "Spark's own UI: the streaming query keeps processing micro-batches",
        4000,
    )
    walk.go(
        "http://localhost:8080/", "Spark master: the scoring application running on the two workers", 4000
    )
    walk.go(
        f"{WEB}/live", "The live map keeps updating through the outage (acks=all, replication factor 3)", 3000
    )
    walk.scroll_to("figure.map-surface")
    walk.pause(6000)

    walk.go(
        f"{WEB}/portal/network",
        "GraphX: stations as vertices, trips as edges; PageRank by Pregel, 15 districts",
        6000,
    )
    walk.scroll(900)
    walk.pause(3000)
    walk.go(
        f"{WEB}/portal/models",
        "MLlib: next-hour demand models judged against a seasonal naive baseline",
        4000,
    )
    walk.scroll(1400)
    walk.pause(2500)
    walk.go(f"{WEB}/portal/sql", "Spark SQL: 14 insights, each with the exact query that produced it", 4500)
    walk.go(
        f"{WEB}/portal/pregel", "Pregel, superstep by superstep: PageRank converging on real stations", 2000
    )
    play = p.locator("button:has-text('Play')").first
    if play.count():
        walk.point_at("button:has-text('Play')")
        play.click()
    walk.pause(7000)
    walk.go(
        f"{WEB}/portal/streaming", "Streaming evidence: latency, watermarks, late data and the drills", 4000
    )
    walk.scroll(1200)
    walk.pause(2500)

    walk.go(f"{WEB}/cluster", "Broker 2 is back: replicas resynchronise and leadership is handed back", 2000)
    walk.scroll_to("text=Spark standalone")
    walk.pause(8000)
    walk.go(
        f"{WEB}/syllabus", "Every unit of the INT315 syllabus and CO4-CO6, traced to what was built", 4000
    )
    walk.scroll(800)
    walk.pause(2500)
    walk.go(f"{WEB}/", "VeloGraph - Sayandip Jana - INT315 Cluster Computing", 4000)


def main(
    out_dir: Annotated[Path, typer.Option(help="Where the WebM goes.")] = REPO_ROOT / "submission",
    name: Annotated[str, typer.Option(help="File name of the video.")] = "VeloGraph-demo.webm",
) -> None:
    configure_logging("demo_video")
    out_dir.mkdir(parents=True, exist_ok=True)
    drill: list[subprocess.Popen[bytes]] = []
    started = time.monotonic()
    with sync_playwright() as pw:
        browser = pw.chromium.launch(channel="msedge", headless=True)
        context = browser.new_context(viewport=SIZE, record_video_dir=str(out_dir), record_video_size=SIZE)
        context.add_init_script(OVERLAY)
        page = context.new_page()
        recording = Path(page.video.path()) if page.video else None  # known now, written on close
        try:
            storyboard(Walk(page), drill)
        finally:
            context.close()  # the recording is written when its page closes
            browser.close()
    target = out_dir / name
    if recording:
        recording.replace(target)
    for proc in drill:
        proc.wait(timeout=600)  # the drill restarts the broker and waits until every partition is in sync
    log.info(
        "recorded %s (%.1f MB) in %.0f s", target, target.stat().st_size / 1e6, time.monotonic() - started
    )


if __name__ == "__main__":
    typer.run(main)
