"""PySpark smoke test (roadmap P0.4): read, from Python, the Parquet written by the Scala ``SmokeJob``.

Proves the Python side shares the lake with the Scala jobs: PySpark 3.5.8 on the project's Python 3.11, with
JDK 17 and winutils provided by ``scripts/env.ps1``. Also exercises the Arrow path used by ``toPandas()``.

Usage (after ``. .\\scripts\\env.ps1``, from ``python/``)::

    uv run python -m velograph.ops.pyspark_smoke
"""

from __future__ import annotations

import typer

from velograph.common.logs import configure_logging
from velograph.common.settings import data_paths

EXPECTED_ROWS = 1_000_000


def main() -> None:
    from pyspark.sql import SparkSession  # imported lazily: the base install does not include pyspark

    log = configure_logging("pyspark_smoke")
    source = data_paths().lake / "_smoke"
    if not source.exists():
        log.error("%s not found - run the Scala smoke job first (scripts/submit.ps1 smoke)", source)
        raise typer.Exit(code=1)

    spark = (
        SparkSession.builder.master("local[4]")
        .appName("velograph-pyspark-smoke")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        df = spark.read.parquet(source.as_uri())
        rows = df.count()
        hours = df.select("hour").distinct().count()
        sample = df.orderBy("id").limit(3).toPandas()  # Arrow-backed conversion
        log.info("read %s rows over %d distinct hours from %s", f"{rows:,}", hours, source)
        log.info("toPandas via Arrow:\n%s", sample.to_string(index=False))
        if rows != EXPECTED_ROWS or hours != 24:
            log.error("expected %s rows and 24 hours", f"{EXPECTED_ROWS:,}")
            raise typer.Exit(code=1)
        log.info("PySpark %s OK", spark.version)
    finally:
        spark.stop()


if __name__ == "__main__":
    typer.run(main)
