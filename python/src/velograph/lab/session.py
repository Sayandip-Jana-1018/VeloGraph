"""A local Spark session for the lab notebooks."""

from __future__ import annotations

from pyspark.sql import SparkSession

from velograph.common.settings import data_paths


def lab_session(app: str, *, cores: int = 16, memory: str = "8g") -> SparkSession:
    """Local session with the project's settings.

    New York session time (trip times are wall-clock New York), Kryo, a host UI port from 4050
    (4040-4042 belong to the Docker master) and compressed event logs in ``data/spark-events``, so
    every lab run appears in the history server next to the batch jobs.
    """
    events = data_paths().root / "spark-events"
    events.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.appName(f"velograph-lab-{app}")
        .master(f"local[{cores}]")
        .config("spark.driver.memory", memory)
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.port", "4050")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", events.as_uri())
        .config("spark.eventLog.compress", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
