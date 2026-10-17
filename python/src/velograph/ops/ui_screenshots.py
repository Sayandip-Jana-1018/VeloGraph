"""Capture Spark UI evidence from the history server (roadmap P2.5, reused in P7.5).

Drives the installed Microsoft Edge headlessly through Playwright (no browser download). Application, job,
stage and SQL-execution ids come from the history server's REST API, so the script works after any rerun.

Usage (history server on :18080; from ``python/``)::

    uv run python -m velograph.ops.ui_screenshots
"""

from __future__ import annotations

import itertools
import json
import logging
import re
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import quote

import typer
from playwright.sync_api import Page, sync_playwright

from velograph.common.logs import configure_logging
from velograph.common.settings import REPO_ROOT

log = logging.getLogger("velograph.ui_screenshots")


@dataclass(frozen=True)
class Shot:
    file: str
    path: str  # relative to the history server root
    expand_dag: bool = False
    wait_for: str | None = None


_APP_ID = re.compile(r"(app|local)-[\w-]+")


class HistoryApi:
    def __init__(self, base: str) -> None:
        self.base = base.rstrip("/")

    def get(self, path: str) -> Any:
        with urllib.request.urlopen(f"{self.base}/api/v1{path}", timeout=30) as response:
            return json.load(response)

    def latest_app(self, name: str) -> str:
        apps = [a for a in self.get("/applications") if a["name"] == name]
        if not apps:
            raise typer.BadParameter(f"no application named {name!r} in the history server")
        app = str(max(apps, key=lambda a: a["attempts"][0]["endTimeEpoch"])["id"])
        # The id goes into further API paths: accept only Spark's own format, URL-encoded.
        if not _APP_ID.fullmatch(app):
            raise ValueError("the history server returned an unexpected application id")
        return quote(app, safe="")

    def job_for_group(self, app: str, group: str) -> dict[str, Any]:
        jobs = [j for j in self.get(f"/applications/{app}/jobs") if j.get("jobGroup") == group]
        return max(jobs, key=lambda j: j["jobId"])

    def shuffle_stage(self, app: str, job: dict[str, Any]) -> int:
        stages = {s["stageId"]: s for s in self.get(f"/applications/{app}/stages")}
        writers = [sid for sid in job["stageIds"] if stages.get(sid, {}).get("shuffleWriteBytes", 0) > 0]
        return max(writers, key=lambda sid: stages[sid]["shuffleWriteBytes"])

    def superstep_page(self, app: str, page_size: int) -> int:
        """The jobs-list page (newest first) that shows the supersteps of the job's first Pregel run.

        Every Pregel superstep is one `isEmpty` Spark job, and the first run is weighted PageRank.
        """
        jobs = self.get(f"/applications/{app}/jobs")
        steps = sorted(j["jobId"] for j in jobs if j["name"].startswith("isEmpty at Pregel"))
        # Consecutive superstep jobs form one run; other jobs separate the runs.
        last_of_first_run = next((a for a, b in itertools.pairwise(steps) if b != a + 1), steps[-1])
        newest = max(j["jobId"] for j in jobs)
        return (newest - last_of_first_run) // page_size + 1

    def richest_sql_execution(self, app: str) -> int:
        """The SQL execution whose plan shows the most shuffles (Exchange nodes)."""
        executions = self.get(f"/applications/{app}/sql?details=true&planDescription=false")
        exchanges = {
            e["id"]: sum(node["nodeName"].startswith("Exchange") for node in e["nodes"]) for e in executions
        }
        return max(exchanges, key=exchanges.__getitem__)


def plan(api: HistoryApi) -> list[Shot]:
    rdd = api.latest_app("velograph-rdd-fundamentals")
    grouped, reduced = api.job_for_group(rdd, "groupByKey"), api.job_for_group(rdd, "reduceByKey")
    sql_app = api.latest_app("velograph-insights")
    execution = api.richest_sql_execution(sql_app)
    graph = api.latest_app("velograph-graph-metrics")
    supersteps = (
        f"/history/{graph}/jobs/?completedJob.pageSize=40&completedJob.page={api.superstep_page(graph, 40)}"
    )
    stage = f"/history/{rdd}/stages/stage/?attempt=0&id="
    return [
        Shot("01-history-applications.png", "/", wait_for="#history-summary"),
        Shot("02-groupbykey-job-dag.png", f"/history/{rdd}/jobs/job/?id={grouped['jobId']}", True),
        Shot("03-reducebykey-job-dag.png", f"/history/{rdd}/jobs/job/?id={reduced['jobId']}", True),
        Shot("04-groupbykey-shuffle-write.png", stage + str(api.shuffle_stage(rdd, grouped))),
        Shot("05-reducebykey-shuffle-write.png", stage + str(api.shuffle_stage(rdd, reduced))),
        Shot("06-storage-cached-rdd.png", f"/history/{rdd}/storage/"),
        Shot(
            "07-sql-plan-with-exchanges.png",
            f"/history/{sql_app}/SQL/execution/?id={execution}",
            wait_for="#plan-viz-graph svg",
        ),
        # P3: Pregel runs one Spark job per superstep, so weighted PageRank shows as a wall of `isEmpty` jobs.
        # (The history server keeps DAGs only for an app's newest 1,000 stages: too few for a superstep DAG.)
        Shot("08-graphx-pregel-supersteps.png", supersteps, wait_for="#completedJob-table"),
    ]


def capture(page: Page, base: str, shot: Shot, out_dir: Path) -> Path:
    page.goto(base + shot.path, wait_until="networkidle")
    if shot.expand_dag:
        # The UI remembers the toggle in localStorage: after the first expansion, later pages open expanded
        # and a click would collapse them again.
        graph = page.locator("#dag-viz-graph svg")
        if not graph.is_visible():
            page.click("span.expand-dag-viz")
        graph.wait_for(state="visible", timeout=15_000)
    if shot.wait_for:
        page.wait_for_selector(shot.wait_for, timeout=15_000)
    page.wait_for_timeout(500)  # let d3 finish laying out the graph
    target = out_dir / shot.file
    page.screenshot(path=str(target), full_page=False)
    return target


def main(
    base: Annotated[str, typer.Option(help="History server URL.")] = "http://localhost:18080",
    out_dir: Annotated[Path, typer.Option(help="Where the PNGs go.")] = REPO_ROOT / "docs/assets/spark-ui",
) -> None:
    configure_logging("ui_screenshots")
    shots = plan(HistoryApi(base))
    out_dir.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge", headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 960})
        try:
            for shot in shots:
                path = capture(page, base, shot, out_dir)
                log.info("%-34s <- %s (%.0f KB)", shot.file, shot.path, path.stat().st_size / 1024)
        finally:
            browser.close()


if __name__ == "__main__":
    typer.run(main)
