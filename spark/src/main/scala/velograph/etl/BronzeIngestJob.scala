package velograph.etl

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

import velograph.common.{JobContext, SparkJob}
import velograph.common.LakePaths.PathOps

object BronzeTransforms {

  /** Lineage columns: the source file, its month (from the `<yyyymm>` folder) and the ingestion time. */
  def withIngestMetadata(raw: DataFrame): DataFrame =
    raw
      .withColumn("_source_file", regexp_extract(input_file_name(), "([^/]+)$", 1))
      .withColumn("ym", regexp_extract(input_file_name(), "/(\\d{6})/[^/]+$", 1))
      .withColumn("_ingested_at", current_timestamp())
}

/**
 * Roadmap P1.5: raw trip CSVs -> `lake/bronze/trips`, partitioned by month.
 *
 * Bronze is a faithful, typed-as-text copy of the source plus lineage columns. Re-ingesting a month replaces
 * only that month's partition (dynamic partition overwrite). The job then reconciles its row counts with the
 * counts the downloader recorded and fails on any difference, so no row can be lost or duplicated silently.
 *
 * Usage: `scripts/submit.ps1 bronze-trips [--months 202609,202608]`.
 */
object BronzeIngestJob extends SparkJob {

  val jobName = "bronze-trips"

  private final case class Options(months: Seq[String] = Seq.empty)

  private val parser = new scopt.OptionParser[Options](jobName) {
    opt[Seq[String]]("months")
      .valueName("yyyymm,...")
      .text("months to ingest (default: every month in data/raw/citibike/manifest.json)")
      .action((months, options) => options.copy(months = months))
  }

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark   = ctx.spark
    val options =
      parser.parse(args, Options()).getOrElse(throw new IllegalArgumentException("invalid arguments"))

    val rawRoot  = ctx.paths.raw.resolve("citibike")
    val expected = RawManifest.rowsByMonth(rawRoot.resolve("manifest.json"))
    val months   = (if (options.months.isEmpty) expected.keys.toSeq else options.months).sorted
    val unknown  = months.filterNot(expected.contains)
    require(unknown.isEmpty, s"months not downloaded yet: ${unknown.mkString(", ")}")
    log.info(s"ingesting ${months.mkString(", ")}")

    val raw = spark.read
      .schema(TripSchemas.RawTrip)
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", TripSchemas.CorruptRecord)
      .option("pathGlobFilter", "*.csv")
      .csv(months.map(ym => rawRoot.resolve(ym).uri): _*)

    val lake = new LakeTables(spark, ctx.paths)
    BronzeTransforms
      .withIngestMetadata(raw)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("ym")
      .parquet(lake.uri(LakeTable.BronzeTrips))

    // Reconciliation with the downloader's counts: at most one row per month comes back to the driver.
    val counted = lake
      .read(LakeTable.BronzeTrips)
      .where(col("ym").isin(months: _*))
      .groupBy("ym")
      .agg(count(lit(1)).as("rows"), count(col(TripSchemas.CorruptRecord)).as("malformed"))
      .collect()
      .map(row => row.getString(0) -> ((row.getLong(1), row.getLong(2))))
      .toMap
    val mismatches = months.filterNot(ym => counted.get(ym).map(_._1).contains(expected(ym)))
    require(
      mismatches.isEmpty,
      mismatches
        .map(ym => s"$ym: bronze ${counted.get(ym).map(_._1).getOrElse(0L)} vs manifest ${expected(ym)}")
        .mkString("row counts differ from manifest.json - ", "; ", "")
    )

    Map(
      "months"                 -> months,
      "rows"                   -> counted.values.map(_._1).sum,
      "malformedRows"          -> counted.values.map(_._2).sum,
      "rowsByMonth"            -> months.map(ym => ym -> counted(ym)._1).toMap,
      "reconciledWithManifest" -> true
    )
  }
}
