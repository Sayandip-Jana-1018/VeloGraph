"""Render the report's SVG diagrams (figures/*.svg, from figures.mjs) to PNGs at twice their size, in headless
Edge.

    node docs/report/figures.mjs; cd python; uv run python ../docs/report/render.py
"""

import logging
from pathlib import Path

from playwright.sync_api import sync_playwright

log = logging.getLogger("velograph.report.render")
HERE = Path(__file__).parent / "figures"

logging.basicConfig(level=logging.INFO, format="%(message)s")
with sync_playwright() as pw:
    browser = pw.chromium.launch(channel="msedge", headless=True)
    page = browser.new_page(device_scale_factor=2)
    for svg in sorted(HERE.glob("*.svg")):
        page.goto(svg.as_uri())
        target = svg.with_suffix(".png")
        page.locator("svg").first.screenshot(path=str(target))
        log.info("%s: %d KB", target.name, target.stat().st_size // 1024)
    browser.close()
