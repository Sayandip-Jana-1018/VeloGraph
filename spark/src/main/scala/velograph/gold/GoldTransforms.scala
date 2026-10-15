package velograph.gold

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import velograph.etl.LakeSchemas

/** Inclusive date range (`yyyy-MM-dd`, New York) covered by the gold tables. */
final case class PanelSpan(from: String, to: String)

/** Working-day commute windows, as [start, end) hours. */
final case class PeakHours(amStart: Int, amEnd: Int, pmStart: Int, pmEnd: Int)

object GoldConfig {
  def span(cfg: Config): PanelSpan =
    PanelSpan(cfg.getString("velograph.gold.panel.from"), cfg.getString("velograph.gold.panel.to"))

  def peaks(cfg: Config): PeakHours = {
    val p = cfg.getConfig("velograph.gold.peaks")
    PeakHours(p.getInt("am-start"), p.getInt("am-end"), p.getInt("pm-start"), p.getInt("pm-end"))
  }
}

/** Silver trips -> the station x hour panel (`gold/station_hour`). */
object StationHourTransforms {

  val CountColumns: Seq[String] =
    Seq("departures", "dep_member", "dep_casual", "dep_ebike", "arrivals", "arr_ebike")

  /** Every hour start of the span in New York time (sequence follows the session zone, so DST is handled). */
  def hours(spark: SparkSession, span: PanelSpan): DataFrame =
    spark
      .range(1)
      .select(
        explode(
          sequence(
            to_timestamp(lit(s"${span.from} 00:00:00")),
            to_timestamp(lit(s"${span.to} 23:00:00")),
            expr("INTERVAL 1 HOUR")
          )
        ).as("ts_hour")
      )

  def departures(trips: DataFrame): DataFrame =
    trips
      .groupBy(col("start_station_id").as("station_id"), date_trunc("hour", col("started_at")).as("ts_hour"))
      .agg(
        count(lit(1)).as("departures"),
        count(when(col("member_casual") === "member", 1)).as("dep_member"),
        count(when(col("member_casual") === "casual", 1)).as("dep_casual"),
        count(when(col("rideable_type") === "electric_bike", 1)).as("dep_ebike"),
        avg("duration_s").as("avg_dep_duration_s")
      )

  def arrivals(trips: DataFrame): DataFrame =
    trips
      .where(col("end_station_id").isNotNull)
      .groupBy(col("end_station_id").as("station_id"), date_trunc("hour", col("ended_at")).as("ts_hour"))
      .agg(
        count(lit(1)).as("arrivals"),
        count(when(col("rideable_type") === "electric_bike", 1)).as("arr_ebike")
      )

  /**
   * Every hour of every station's active span, from its first to its last day with a trip. Zero-filling
   * outside that span would invent "no demand" for stations that did not exist yet, or had closed.
   */
  def grid(stations: DataFrame, hours: DataFrame): DataFrame =
    stations
      .where(col("in_trips"))
      .select(
        col("station_id"),
        to_date(col("first_seen")).as("first_day"),
        to_date(col("last_seen")).as("last_day")
      )
      .crossJoin(broadcast(hours))
      .where(to_date(col("ts_hour")).between(col("first_day"), col("last_day")))
      .select("station_id", "ts_hour")

  /** The panel: grid ⟕ departures ⟕ arrivals, zero-filled, with calendar flags and the hour's weather. */
  def panel(
      trips: DataFrame,
      stations: DataFrame,
      weather: DataFrame,
      calendar: DataFrame,
      span: PanelSpan
  ): DataFrame =
    grid(stations, hours(trips.sparkSession, span))
      .join(departures(trips), Seq("station_id", "ts_hour"), "left")
      .join(arrivals(trips), Seq("station_id", "ts_hour"), "left")
      .na
      .fill(0L, CountColumns)
      .withColumn("net_flow", col("arrivals") - col("departures"))
      .withColumn("date", to_date(col("ts_hour")))
      .withColumn("hour", hour(col("ts_hour")))
      .join(broadcast(calendar.select("date", "dow", "is_weekend", "is_holiday")), Seq("date"), "left")
      .join(
        broadcast(
          weather.select(
            "ts_hour",
            "temp_c",
            "apparent_temp_c",
            "precip_mm",
            "wind_kmh",
            "humidity_pct",
            "cloud_pct",
            "weather_code",
            "is_raining"
          )
        ),
        Seq("ts_hour"),
        "left"
      )
      .select(LakeSchemas.GoldStationHour.fieldNames.map(col): _*)
}

/** Silver trips -> directed route edges (`gold/route_edges`), the raw material of the GraphX graph. */
object RouteTransforms {

  def edges(trips: DataFrame, calendar: DataFrame, peaks: PeakHours): DataFrame = {
    val workingDay                      = !col("is_weekend") && !col("is_holiday")
    def startsIn(from: Int, until: Int) = col("start_hour") >= from && col("start_hour") < until

    trips
      .where(col("end_station_id").isNotNull)
      .join(broadcast(calendar.select("date", "is_weekend", "is_holiday")), col("start_date") === col("date"))
      .groupBy(col("start_station_id").as("src_id"), col("end_station_id").as("dst_id"))
      .agg(
        count(lit(1)).as("trips"),
        count(when(workingDay && startsIn(peaks.amStart, peaks.amEnd), 1)).as("trips_am_peak"),
        count(when(workingDay && startsIn(peaks.pmStart, peaks.pmEnd), 1)).as("trips_pm_peak"),
        count(when(col("is_weekend"), 1)).as("trips_weekend"),
        percentile_approx(col("duration_s"), lit(0.5), lit(1000)).as("median_duration_s"),
        round(avg(when(col("member_casual") === "member", 1.0).otherwise(0.0)), 4).as("member_share"),
        round(avg(when(col("rideable_type") === "electric_bike", 1.0).otherwise(0.0)), 4).as("ebike_share")
      )
      .withColumn("is_self_loop", col("src_id") === col("dst_id"))
      .select(LakeSchemas.GoldRouteEdge.fieldNames.map(col): _*)
  }
}
