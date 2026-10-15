package velograph.gold

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import velograph.common.{JobContext, SparkJob}
import velograph.etl.{LakeTable, LakeTables}

/**
 * Roadmap P2.1: silver trips, stations, weather and calendar -> `gold/station_hour`, partitioned by date.
 *
 * The panel is complete: every station has a row for every hour of its active span, with zero departures and
 * arrivals where nothing happened. That is what demand models need, and what a plain `groupBy` would miss (it
 * only returns hours with at least one trip). The job then reconciles the panel's totals with the trip counts
 * and fails on any difference.
 */
object GoldStationHourJob extends SparkJob {

  val jobName = "gold-station-hour"

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val lake = new LakeTables(ctx.spark, ctx.paths)
    val span = GoldConfig.span(ctx.cfg.raw)

    val trips = lake.read(LakeTable.SilverTrips)
    val panel = StationHourTransforms.panel(
      trips,
      lake.read(LakeTable.Stations),
      lake.read(LakeTable.WeatherHourly),
      lake.read(LakeTable.Calendar),
      span
    )
    panel
      .repartition(col("date")) // one file per day
      .write
      .mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "static")
      .partitionBy("date")
      .parquet(lake.uri(LakeTable.StationHour))

    // Second pass over the written Parquet (columnar, cheap) instead of caching seven million joined rows.
    val written = lake.read(LakeTable.StationHour)
    val s       = written
      .agg(
        count(lit(1)).as("rows"),
        countDistinct("station_id").as("stations"),
        countDistinct("ts_hour").as("hours"),
        sum("departures").as("departures"),
        sum("arrivals").as("arrivals"),
        avg(when(col("departures") === 0, 100.0).otherwise(0.0)).as("zeroDeparturePct"),
        avg("departures").as("meanDepartures"),
        count(when(col("temp_c").isNull, 1)).as("rowsWithoutWeather")
      )
      .head() // bounded: 1 row

    val inSpan          = col("start_date").between(span.from, span.to)
    val expectedDepart  = trips.where(inSpan).count()
    val expectedArrival = trips
      .where(col("end_station_id").isNotNull && to_date(col("ended_at")).between(span.from, span.to))
      .count()
    require(
      s.getAs[Long]("departures") == expectedDepart && s.getAs[Long]("arrivals") == expectedArrival,
      s"panel totals (${s.getAs[Long]("departures")} departures, ${s.getAs[Long]("arrivals")} arrivals) " +
        s"differ from trips ($expectedDepart, $expectedArrival)"
    )

    val busiest = written
      .orderBy(col("departures").desc)
      .limit(1)
      .join(lake.read(LakeTable.Stations).select("station_id", "name"), Seq("station_id"))
      .select(col("name"), date_format(col("ts_hour"), "yyyy-MM-dd HH:mm").as("hour"), col("departures"))
      .head() // bounded: 1 row

    log.info(
      f"panel: ${s.getAs[Long]("rows")}%,d rows (${s.getAs[Long]("stations")} stations x " +
        f"${s.getAs[Long]("hours")} hours), reconciled with $expectedDepart%,d departures"
    )
    Map(
      "span"                  -> s"${span.from}..${span.to}",
      "rows"                  -> s.getAs[Long]("rows"),
      "stations"              -> s.getAs[Long]("stations"),
      "hours"                 -> s.getAs[Long]("hours"),
      "departures"            -> s.getAs[Long]("departures"),
      "arrivals"              -> s.getAs[Long]("arrivals"),
      "reconciledWithTrips"   -> true,
      "zeroDeparturePct"      -> math.round(s.getAs[Double]("zeroDeparturePct") * 100) / 100.0,
      "meanDeparturesPerHour" -> math.round(s.getAs[Double]("meanDepartures") * 1000) / 1000.0,
      "rowsWithoutWeather"    -> s.getAs[Long]("rowsWithoutWeather"),
      "busiestStationHour"    ->
        s"${busiest.getString(0)}, ${busiest.getString(1)}: ${busiest.getLong(2)} departures"
    )
  }
}
