package velograph.etl

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

import velograph.common.{JobContext, ServingJson, SourceRange, SparkJob}
import velograph.common.LakePaths.PathOps

/**
 * Roadmap P1.6: `lake/bronze/trips` -> `lake/silver/trips` (valid trips, partitioned by start date) and
 * `lake/silver/trips_quarantine` (rejected rows with their reason, partitioned by month and reason), plus
 * `data/serving/dq-report.json`.
 *
 * Silver is rebuilt from all of bronze in one pass. Trips near midnight on a month's last day can sit in the
 * next month's file, so overwriting date partitions one month at a time would silently drop them.
 */
object SilverTripsJob extends SparkJob {

  val jobName = "silver-trips"

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val lake   = new LakeTables(ctx.spark, ctx.paths)
    val rules  = DqRules.fromConfig(ctx.cfg.raw.getConfig("velograph.dq"))
    val bronze = lake.read(LakeTable.BronzeTrips)

    // Three actions reuse this result (silver write, quarantine write, DQ report). Persisting it means the bronze
    // scan, the parsing and the duplicate-detection shuffle run once instead of three times.
    val classified = SilverTripsTransforms.classify(bronze, rules).persist(StorageLevel.MEMORY_AND_DISK_SER)

    classified
      .where(col("reject_reason").isNull)
      .select(SilverTripsTransforms.SilverColumns.map(col): _*)
      .repartition(col("start_date")) // one task, hence one file, per day
      .write
      .mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "static") // full rebuild: replace the whole table
      .partitionBy("start_date")
      .parquet(lake.uri(LakeTable.SilverTrips))

    classified
      .where(col("reject_reason").isNotNull)
      .drop("_dup_rank")
      .repartition(col("ym"), col("reject_reason"))
      .write
      .mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "static")
      .partitionBy("ym", "reject_reason")
      .parquet(ctx.paths.silver.resolve("trips_quarantine").uri)

    val report = DataQuality.report(classified, rules)
    classified.unpersist()

    ServingJson.write(
      ctx.paths.serving.resolve("dq-report.json"),
      schema = "dq-report",
      job = jobName,
      data = report,
      rows = Some(report.inputRows),
      source = Some(SourceRange(report.firstStart.take(10), report.lastStart.take(10)))
    )
    log.info(
      f"silver trips: ${report.validRows}%,d valid of ${report.inputRows}%,d " +
        f"(${report.quarantinedPct}%.3f%% quarantined)"
    )

    Map(
      "inputRows"       -> report.inputRows,
      "validRows"       -> report.validRows,
      "quarantinedRows" -> report.quarantinedRows,
      "quarantinedPct"  -> report.quarantinedPct,
      "topReasons"      -> report.byReason.take(5).map(r => r.reason -> r.rows).toMap
    )
  }
}
