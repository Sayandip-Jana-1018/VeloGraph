"""Fill the report's pages: find the pages that end with empty space because the next figure did not fit, and
narrow that figure until it does.

    cd python; uv run --with pymupdf python ../docs/report/layout.py           # measure, write layout.json
    cd python; uv run --with pymupdf python ../docs/report/layout.py --report  # measure only

Reads submission/VeloGraph-Report.pdf (export.ps1 writes it) and keeps figure widths in layout.json, which
kit.mjs applies on the next build. Front-matter pages (declaration, certificate, contents...) and the last
page are left alone: their space is meant.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

import pymupdf

log = logging.getLogger("velograph.report.layout")

HERE = Path(__file__).resolve().parent
PDF = HERE.parent.parent / "submission" / "VeloGraph-Report.pdf"
LAYOUT = HERE / "layout.json"

TOP, BOTTOM = 72.0, 770.0  # A4 in points with 2.54 cm margins: the body sits between these
GAP_PT = 60.0  # a page ending more than this above the bottom margin counts as a gap
MIN_SCALE = 0.62  # never shrink a figure below this share of its normal width
FULL_PX, PAIR_PX = 602, 292  # kit.mjs: TEXT_PX and the width of each image in a figure pair
CAPTION = re.compile(r"Figure (\d+):")
SKIP = (
    "Declaration",
    "Certificate",
    "Acknowledgement",
    "Abstract",
    "Contents",
    "List of Figures",
    "List of Tables",
)


def body_blocks(page: pymupdf.Page) -> list[dict]:
    blocks = page.get_text("dict")["blocks"]
    return [b for b in blocks if b["bbox"][1] >= TOP - 2 and b["bbox"][3] <= BOTTOM + 4]


def text_of(block: dict) -> str:
    return " ".join(s["text"] for line in block.get("lines", []) for s in line["spans"]).strip()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    report_only = "--report" in sys.argv
    layout: dict[str, int] = json.loads(LAYOUT.read_text()) if LAYOUT.exists() else {}
    doc = pymupdf.open(PDF)
    changes = 0
    for i in range(1, len(doc) - 1):  # not the cover, not the last page
        page, nxt = doc[i], doc[i + 1]
        blocks = body_blocks(page)
        if not blocks or any(text_of(b).startswith(SKIP) for b in blocks[:2]):
            continue
        gap = BOTTOM - max(b["bbox"][3] for b in blocks)
        if gap < GAP_PT:
            continue
        following = sorted(body_blocks(nxt), key=lambda b: b["bbox"][1])
        images = [b for b in following if b["type"] == 1 and b["bbox"][1] < following[0]["bbox"][1] + 30]
        caption = next((CAPTION.search(text_of(b)) for b in following if CAPTION.search(text_of(b))), None)
        verdict = "a gap"
        if images and caption:
            n = caption.group(1)
            height = max(b["bbox"][3] - b["bbox"][1] for b in images)
            pair = len(images) == 2
            normal = PAIR_PX if pair else FULL_PX
            current = layout.get(n, normal)
            # The figure's caption and spacing move up with it: about 40 pt (a pair's own notes add 22 pt).
            scale = (gap - (62 if pair else 40)) / height
            if MIN_SCALE * normal / current <= scale < 1:
                verdict = f"figure {n}: {current} -> {int(current * scale)} px"
                if not report_only:
                    layout[n] = int(current * scale)
                    changes += 1
            else:
                verdict = f"figure {n} would need {scale:.0%} of its size: left as it is"
        log.info("page %2d: %5.0f pt empty at the bottom; %s", i + 1, gap, verdict)
    if not report_only:
        ordered = dict(sorted(layout.items(), key=lambda kv: int(kv[0])))
        LAYOUT.write_text(json.dumps(ordered, indent=1) + "\n")
        log.info("%d figure(s) narrowed; %s holds %d", changes, LAYOUT.name, len(layout))
    return 0


if __name__ == "__main__":
    sys.exit(main())
