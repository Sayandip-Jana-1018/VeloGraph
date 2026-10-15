package velograph.etl

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

final case class ReasonCount(reason: String, rows: Long, pct: Double)
final case class MonthCount(ym: String, bronzeRows: Long, validRows: Long, quarantinedRows: Long)
final case class Quantiles(p50: Double, p95: Double, p99: Double, max: Double)

/** Contents of `data/serving/dq-report.json`. */
final case class DqReport(
    inputRows: Long,
    validRows: Long,
    quarantinedRows: Long,
    quarantinedPct: Double,
    byReason: Seq[ReasonCount],
    byMonth: Seq[MonthCount],
    validTripShares: Map[String, Double],
    durationSeconds: Quantiles,
    distanceMeters: Quantiles,
    firstStart: String,
    lastStart: String,
    rules: DqRules
)

/**
 * Data-quality report over classified trips. Counts come from exact aggregations. Accumulators would be the
 * RDD-era alternative, but Spark only guarantees their values inside actions, so they belong in the RDD demo
 * (P2.3).
 */
object DataQuality {

  private def pct(part: Long, whole: Long): Double =
    if (whole == 0) 0.0 else math.round(part * 100000.0 / whole) / 1000.0

  def report(classified: DataFrame, rules: DqRules): DqReport = {
    val valid = classified.where(col("reject_reason").isNull)

    // Every collect below is bounded: at most 14 reasons, 12 months or 1 summary row.
    val reasonRows = classified
      .groupBy("reject_reason")
      .count()
      .collect()
      .map(row => Option(row.getString(0)) -> row.getLong(1))
    val inputRows = reasonRows.map(_._2).sum
    val validRows = reasonRows.collectFirst { case (None, rows) => rows }.getOrElse(0L)
    val byReason  = reasonRows
      .collect { case (Some(reason), rows) => ReasonCount(reason, rows, pct(rows, inputRows)) }
      .sortBy(-_.rows)
      .toSeq

    val byMonth = classified
      .groupBy("ym")
      .agg(count(lit(1)).as("rows"), count(when(col("reject_reason").isNull, 1)).as("valid"))
      .orderBy("ym")
      .collect()
      .map(row =>
        MonthCount(row.getString(0), row.getLong(1), row.getLong(2), row.getLong(1) - row.getLong(2))
      )
      .toSeq

    def share(condition: Column): Column = avg(when(condition, 100.0).otherwise(0.0))
    val summary                          = valid
      .agg(
        share(col("start_station_id").isNull).as("missingStartStationPct"),
        share(col("end_station_id").isNull).as("missingEndStationPct"),
        share(col("is_round_trip")).as("roundTripPct"),
        share(col("rideable_type") === "electric_bike").as("ebikePct"),
        share(col("member_casual") === "member").as("memberPct"),
        percentile_approx(col("duration_s"), lit(Array(0.5, 0.95, 0.99)), lit(10000)).as("durationQ"),
        max("duration_s").as("durationMax"),
        percentile_approx(col("distance_m"), lit(Array(0.5, 0.95, 0.99)), lit(10000)).as("distanceQ"),
        max("distance_m").as("distanceMax"),
        // Formatted in the session zone (New York); see CLAUDE.md on timestamps.
        date_format(min("started_at"), "yyyy-MM-dd HH:mm:ss").as("firstStart"),
        date_format(max("started_at"), "yyyy-MM-dd HH:mm:ss").as("lastStart")
      )
      .head()

    val shareNames =
      Seq("missingStartStationPct", "missingEndStationPct", "roundTripPct", "ebikePct", "memberPct")
    def quantiles(field: String, maxField: String): Quantiles = {
      val q = summary.getAs[Seq[Number]](field).map(_.doubleValue())
      Quantiles(q(0), q(1), q(2), summary.getAs[Number](maxField).doubleValue())
    }

    DqReport(
      inputRows = inputRows,
      validRows = validRows,
      quarantinedRows = inputRows - validRows,
      quarantinedPct = pct(inputRows - validRows, inputRows),
      byReason = byReason,
      byMonth = byMonth,
      validTripShares =
        shareNames.map(name => name -> math.round(summary.getAs[Double](name) * 1000) / 1000.0).toMap,
      durationSeconds = quantiles("durationQ", "durationMax"),
      distanceMeters = quantiles("distanceQ", "distanceMax"),
      firstStart = summary.getAs[String]("firstStart"),
      lastStart = summary.getAs[String]("lastStart"),
      rules = rules
    )
  }
}
