package velograph.testkit

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import velograph.etl.{CalendarTransforms, SilverTripsTransforms, StationTransforms}
import velograph.gold.{PanelSpan, PeakHours, RouteTransforms, StationHourTransforms}

/**
 * A tiny lake for tests: five trips over Saturday 12 - Monday 14 Sep 2026, put through the real transforms.
 * Each trip exists for a reason, noted below.
 */
object Fixtures {

  final case class Trip(
      id: String,
      rideable: String,
      rider: String,
      start: String,
      end: String,
      from: String,
      to: String
  )

  val Span: PanelSpan  = PanelSpan("2026-09-12", "2026-09-14")
  val Peaks: PeakHours = PeakHours(7, 10, 16, 19)

  val StationNames: Map[String, String] =
    Map("A" -> "W 84 St & Columbus Ave", "B" -> "E 7 St & Avenue A", "C" -> "Central Park S & 6 Ave")
  private val Coordinates: Map[String, (Double, Double)] =
    Map("A" -> ((40.785, -73.976)), "B" -> ((40.724, -73.983)), "C" -> ((40.765, -73.976)))

  val Trips: Seq[Trip] = Seq(
    Trip("t1", "classic_bike", "member", "2026-09-14 08:10:00", "2026-09-14 08:25:00", "A", "B"), // AM peak
    Trip(
      "t2",
      "electric_bike",
      "casual",
      "2026-09-14 17:30:00",
      "2026-09-14 17:50:00",
      "B",
      "A"
    ), // PM peak, rain
    Trip(
      "t3",
      "electric_bike",
      "member",
      "2026-09-12 11:00:00",
      "2026-09-12 11:40:00",
      "A",
      "A"
    ), // weekend round trip
    Trip(
      "t4",
      "classic_bike",
      "member",
      "2026-09-14 23:50:00",
      "2026-09-15 00:10:00",
      "A",
      "B"
    ), // ends after the span
    Trip(
      "t5",
      "classic_bike",
      "casual",
      "2026-09-13 09:00:00",
      "2026-09-13 09:20:00",
      "C",
      "A"
    ) // C is active one day
  )

  /** Silver trips built through Spark, so times are read in the session zone exactly like real data. */
  def silverTrips(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Trips
      .map { t =>
        val ((fromLat, fromLng), (toLat, toLng)) = (Coordinates(t.from), Coordinates(t.to))
        (
          t.id,
          t.rideable,
          t.rider,
          t.start,
          t.end,
          t.from,
          StationNames(t.from),
          t.to,
          StationNames(t.to),
          fromLat,
          fromLng,
          toLat,
          toLng
        )
      }
      .toDF(
        "ride_id",
        "rideable_type",
        "member_casual",
        "started",
        "ended",
        "start_station_id",
        "start_station_name",
        "end_station_id",
        "end_station_name",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng"
      )
      .withColumn("started_at", to_timestamp($"started"))
      .withColumn("ended_at", to_timestamp($"ended"))
      .transform(SilverTripsTransforms.derive)
      .select(SilverTripsTransforms.SilverColumns.map(col): _*)
  }

  private val GbfsColumns = StructType(
    Seq(
      StructField("short_name", StringType),
      StructField("gbfs_station_id", StringType),
      StructField("gbfs_name", StringType),
      StructField("gbfs_lat", DoubleType),
      StructField("gbfs_lon", DoubleType),
      StructField("capacity", IntegerType),
      StructField("region_id", StringType)
    )
  )

  /** The station dimension from the trips alone (an empty GBFS registry). */
  def stations(spark: SparkSession): DataFrame = {
    val noGbfs = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], GbfsColumns)
    StationTransforms.dimension(
      StationTransforms.summarize(StationTransforms.endpoints(silverTrips(spark))),
      noGbfs
    )
  }

  /** Dry hours at 21.5 °C, except 2 mm of rain in every 17:00 hour. */
  def weather(spark: SparkSession): DataFrame = {
    val raining = hour(col("ts_hour")) === 17
    StationHourTransforms
      .hours(spark, Span)
      .withColumn("temp_c", lit(21.5))
      .withColumn("apparent_temp_c", lit(22.0))
      .withColumn("precip_mm", when(raining, 2.0).otherwise(0.0))
      .withColumn("rain_mm", when(raining, 2.0).otherwise(0.0))
      .withColumn("snow_cm", lit(0.0))
      .withColumn("wind_kmh", lit(12.0))
      .withColumn("gust_kmh", lit(25.0))
      .withColumn("humidity_pct", lit(60.0))
      .withColumn("cloud_pct", lit(40.0))
      .withColumn("weather_code", when(raining, 61).otherwise(1))
      .withColumn("date", to_date(col("ts_hour")))
      .withColumn("hour", hour(col("ts_hour")))
      .withColumn("is_raining", raining)
  }

  def calendar(spark: SparkSession): DataFrame =
    CalendarTransforms.build(spark, Span.from, Span.to, CalendarTransforms.holidays(spark))

  def stationHour(spark: SparkSession): DataFrame =
    StationHourTransforms.panel(silverTrips(spark), stations(spark), weather(spark), calendar(spark), Span)

  def routes(spark: SparkSession): DataFrame =
    RouteTransforms.edges(silverTrips(spark), calendar(spark), Peaks)
}
