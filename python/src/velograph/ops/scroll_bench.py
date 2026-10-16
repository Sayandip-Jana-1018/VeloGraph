"""Scroll-smoothness benchmark for the web app (the frame budget of ADR 012).

Drives the installed Microsoft Edge headlessly through Playwright. On the home page and in three
portal overlays it scrolls by a fixed step on every animation frame and records the time between
frames. Headless Edge has no vsync, so that interval is what a frame really costs to produce; below
16.7 ms a display keeps 60 fps. A frame slower than 33.4 ms (under 30 fps) counts as a slow frame.

Usage (web dev or production server on :3000; from ``python/``)::

    uv run python -m velograph.ops.scroll_bench
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

import typer
from playwright.sync_api import Page, sync_playwright

from velograph.common.logs import configure_logging

log = logging.getLogger("velograph.scroll_bench")

PORTALS = ("rdd-lab", "sql", "network")
VIEWPORT = {"width": 1440, "height": 900}
# Ask for the GPU path, as a desktop browser would use; without it headless Chromium composites in software.
GPU_ARGS = ["--use-angle=d3d11", "--enable-gpu-rasterization", "--ignore-gpu-blocklist"]

MEASURE = """
async ({ selector, distance, step }) => {
  const scroller = selector ? document.querySelector(selector) : document.scrollingElement;
  document.documentElement.style.scrollBehavior = 'auto';
  scroller.scrollTop = 0;
  await new Promise((r) => setTimeout(r, 400));
  const max = Math.min(distance, scroller.scrollHeight - scroller.clientHeight);
  const deltas = [];
  let last = performance.now();
  let y = 0;
  await new Promise((resolve) => {
    const tick = (now) => {
      deltas.push(now - last);
      last = now;
      y += step;
      scroller.scrollTop = y;
      if (y < max) requestAnimationFrame(tick); else resolve();
    };
    requestAnimationFrame(tick);
  });
  deltas.shift();
  const sorted = [...deltas].sort((a, b) => a - b);
  const blurred = [...document.querySelectorAll('*')].filter((e) => {
    const s = getComputedStyle(e);
    return s.backdropFilter && s.backdropFilter !== 'none';
  });
  return {
    scrolledPx: Math.round(max),
    frames: deltas.length,
    meanMs: deltas.reduce((a, b) => a + b, 0) / deltas.length,
    p95Ms: sorted[Math.floor(sorted.length * 0.95)],
    slowFramesPct: (100 * deltas.filter((d) => d > 33.4).length) / deltas.length,
    backdropFilters: blurred.length,
  };
}
"""


def measure(page: Page, selector: str | None, distance: int, step: int) -> dict[str, Any]:
    return page.evaluate(MEASURE, {"selector": selector, "distance": distance, "step": step})


def main(
    base: Annotated[str, typer.Option(help="Web app URL.")] = "http://localhost:3000",
    step: Annotated[int, typer.Option(help="Pixels scrolled per frame.")] = 14,
) -> None:
    configure_logging("scroll_bench")
    results: dict[str, dict[str, Any]] = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge", headless=True, args=GPU_ARGS)
        try:
            page = browser.new_page(viewport=VIEWPORT)
            page.goto(f"{base}/", wait_until="networkidle")
            page.wait_for_timeout(2500)
            results["home"] = measure(page, None, 5200, step)
            for portal in PORTALS:
                # Open each portal the way a visitor does: as an overlay, from its card on the home page.
                page.goto(f"{base}/", wait_until="networkidle")
                page.wait_for_timeout(1500)
                page.locator(f'a[href="/portal/{portal}"]').first.click()
                page.wait_for_selector("[role=dialog] article h2", timeout=30_000)
                page.wait_for_timeout(2500)
                results[f"portal:{portal}"] = measure(page, "[role=dialog]", 7000, step)
        finally:
            browser.close()
    for name, r in results.items():
        log.info(
            "%-16s %4d frames over %5d px  mean %5.1f ms  p95 %5.1f ms  slow %4.1f %%  backdrop filters %d",
            name,
            r["frames"],
            r["scrolledPx"],
            r["meanMs"],
            r["p95Ms"],
            r["slowFramesPct"],
            r["backdropFilters"],
        )


if __name__ == "__main__":
    typer.run(main)
