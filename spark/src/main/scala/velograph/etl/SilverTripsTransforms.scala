package velograph.etl

import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, TimestampType}

/** Thresholds of the silver data-quality rules, from `velograph.dq` in application.conf. */
final case class DqRules(
    minDurationS: Int,
    maxDurationS: Int,
    maxSpeedKmh: Double,
    minLat: Double,
    maxLat: Double,
    minLng: Double,
    maxLng: Double
)

object DqRules {
  def fromConfig(dq: Config): DqRules = DqRules(
    minDurationS = dq.getInt("min-duration-s"),
    maxDurationS = dq.getInt("max-duration-s"),
    maxSpeedKmh = dq.getDouble("max-speed-kmh"),
    minLat = dq.getDouble("bbox.min-lat"),
    maxLat = dq.getDouble("bbox.max-lat"),
    minLng = dq.getDouble("bbox.min-lng"),
    maxLng = dq.getDouble("bbox.max-lng")
  )
}

/**
 * Bronze trips -> typed, enriched and classified trips. Every step is a pure DataFrame => DataFrame function.
 */
object SilverTripsTransforms {

  val RideableTypes: Seq[String] = Seq("classic_bike", "electric_bike")
  val RiderTypes: Seq[String]    = Seq("member", "casual")

  /** Mean Earth radius (IUGG), metres. */
  private val EarthRadiusM = 6371008.8

  /** Columns of `lake/silver/trips`, in order; `start_date` is the partition column. */
  val SilverColumns: Seq[String] = Seq(
    "ride_id",
    "rideable_type",
    "member_casual",
    "started_at",
    "ended_at",
    "start_station_id",
    "start_station_name",
    "end_station_id",
    "end_station_name",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "duration_s",
    "distance_m",
    "speed_kmh",
    "is_round_trip",
    "start_hour",
    "dow",
    "start_date"
  )

  /**
   * Great-circle distance in metres, built from Spark functions rather than a UDF so Catalyst can optimise
   * it.
   */
  def haversineMeters(lat1: Column, lng1: Column, lat2: Column, lng2: Column): Column = {
    val halfDLat = radians(lat2 - lat1) / 2
    val halfDLng = radians(lng2 - lng1) / 2
    val a        = pow(sin(halfDLat), 2) + cos(radians(lat1)) * cos(radians(lat2)) * pow(sin(halfDLng), 2)
    lit(2 * EarthRadiusM) * asin(sqrt(a))
  }

  /** ISO day of week, Monday = 1 ... Sunday = 7 (Spark's `dayofweek` counts Sunday = 1 ... Saturday = 7). */
  def isoDayOfWeek(date: Column): Column = (dayofweek(date) + 5) % 7 + 1

  /** Trimmed text, with empty strings turned into nulls (the CSVs write missing values as ""). */
  private def text(name: String): Column = {
    val trimmed = trim(col(name))
    when(trimmed === "", lit(null).cast(StringType)).otherwise(trimmed)
  }

  /**
   * Bronze strings -> typed columns. Anything unparseable becomes null and is caught by [[rejectReason]];
   * station ids are normalised (see [[StationIds]]).
   */
  def parse(bronze: DataFrame): DataFrame =
    bronze.select(
      text("ride_id").as("ride_id"),
      lower(text("rideable_type")).as("rideable_type"),
      lower(text("member_casual")).as("member_casual"),
      text("started_at").cast(TimestampType).as("started_at"),
      text("ended_at").cast(TimestampType).as("ended_at"),
      StationIds.normalize(text("start_station_id")).as("start_station_id"),
      text("start_station_name").as("start_station_name"),
      StationIds.normalize(text("end_station_id")).as("end_station_id"),
      text("end_station_name").as("end_station_name"),
      text("start_lat").cast(DoubleType).as("start_lat"),
      text("start_lng").cast(DoubleType).as("start_lng"),
      text("end_lat").cast(DoubleType).as("end_lat"),
      text("end_lng").cast(DoubleType).as("end_lng"),
      col(TripSchemas.CorruptRecord),
      col("_source_file"),
      col("ym")
    )

  /** Duration, straight-line distance and speed, round-trip flag, and calendar keys. */
  def derive(parsed: DataFrame): DataFrame = {
    val seconds = col("ended_at").cast(DoubleType) - col("started_at").cast(DoubleType)
    parsed
      .withColumn("duration_s", round(seconds).cast(IntegerType))
      .withColumn(
        "distance_m",
        haversineMeters(col("start_lat"), col("start_lng"), col("end_lat"), col("end_lng"))
      )
      .withColumn("speed_kmh", when(col("duration_s") > 0, col("distance_m") / col("duration_s") * 3.6))
      .withColumn("is_round_trip", coalesce(col("start_station_id") === col("end_station_id"), lit(false)))
      .withColumn("start_date", to_date(col("started_at")))
      .withColumn("start_hour", hour(col("started_at")))
      .withColumn("dow", isoDayOfWeek(col("start_date")))
  }

  /** Position of each row among rows sharing its `ride_id`; anything above 1 is a duplicate. */
  def withDuplicateRank(df: DataFrame): DataFrame =
    df.withColumn(
      "_dup_rank",
      row_number().over(Window.partitionBy("ride_id").orderBy(col("_source_file"), col("started_at")))
    )

  /**
   * Reason code for the first rule a row breaks, or null for a valid trip. Rules are checked in order, so
   * each one can assume the earlier ones passed (e.g. the speed rule only sees parseable times and
   * coordinates).
   */
  def rejectReason(rules: DqRules): Column = {
    def missing(lat: Column, lng: Column): Column     = lat.isNull || lng.isNull
    def outsideBbox(lat: Column, lng: Column): Column =
      lat < rules.minLat || lat > rules.maxLat || lng < rules.minLng || lng > rules.maxLng

    when(col(TripSchemas.CorruptRecord).isNotNull, "MALFORMED_CSV_ROW")
      .when(col("ride_id").isNull, "MISSING_RIDE_ID")
      .when(col("started_at").isNull || col("ended_at").isNull, "UNPARSEABLE_TIMESTAMP")
      .when(col("duration_s") <= 0, "NON_POSITIVE_DURATION")
      .when(col("duration_s") < rules.minDurationS, "DURATION_TOO_SHORT")
      .when(col("duration_s") > rules.maxDurationS, "DURATION_TOO_LONG")
      .when(
        col("rideable_type").isNull || !col("rideable_type").isin(RideableTypes: _*),
        "UNKNOWN_RIDEABLE_TYPE"
      )
      .when(col("member_casual").isNull || !col("member_casual").isin(RiderTypes: _*), "UNKNOWN_RIDER_TYPE")
      .when(missing(col("start_lat"), col("start_lng")), "MISSING_START_COORD") // e-bike without a GPS fix
      .when(outsideBbox(col("start_lat"), col("start_lng")), "START_COORD_OUT_OF_AREA")
      .when(missing(col("end_lat"), col("end_lng")), "MISSING_END_COORD") // bike never docked
      .when(outsideBbox(col("end_lat"), col("end_lng")), "END_COORD_OUT_OF_AREA")
      .when(col("speed_kmh") > rules.maxSpeedKmh, "IMPOSSIBLE_SPEED")
      .when(col("_dup_rank") > 1, "DUPLICATE_RIDE_ID")
  }

  /** The whole silver pipeline for one DataFrame: every row kept, plus its `reject_reason` (null = valid). */
  def classify(bronze: DataFrame, rules: DqRules): DataFrame =
    withDuplicateRank(derive(parse(bronze))).withColumn("reject_reason", rejectReason(rules))
}
