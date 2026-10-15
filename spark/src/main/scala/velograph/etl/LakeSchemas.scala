package velograph.etl

import org.apache.spark.sql.types._

/**
 * Declared schemas of the lake tables (ARCHITECTURE §5). Every read of a lake table uses one: Parquet stores
 * column types, but partition column types would otherwise be guessed from folder names (`ym=202609` reads
 * back as an integer). Each table's writer has a unit test asserting it produces exactly this schema.
 */
object LakeSchemas {

  private def fields(columns: (String, DataType)*): StructType =
    StructType(columns.map { case (name, dataType) => StructField(name, dataType) })

  /** `bronze/trips`, partitioned by `ym`: the raw text columns plus lineage. */
  val BronzeTrip: StructType = TripSchemas.RawTrip
    .add("_source_file", StringType)
    .add("_ingested_at", TimestampType)
    .add("ym", StringType)

  /** `silver/trips`, partitioned by `start_date`; columns in `SilverTripsTransforms.SilverColumns` order. */
  val SilverTrip: StructType = fields(
    "ride_id"            -> StringType,
    "rideable_type"      -> StringType,
    "member_casual"      -> StringType,
    "started_at"         -> TimestampType,
    "ended_at"           -> TimestampType,
    "start_station_id"   -> StringType,
    "start_station_name" -> StringType,
    "end_station_id"     -> StringType,
    "end_station_name"   -> StringType,
    "start_lat"          -> DoubleType,
    "start_lng"          -> DoubleType,
    "end_lat"            -> DoubleType,
    "end_lng"            -> DoubleType,
    "duration_s"         -> IntegerType,
    "distance_m"         -> DoubleType,
    "speed_kmh"          -> DoubleType,
    "is_round_trip"      -> BooleanType,
    "start_hour"         -> IntegerType,
    "dow"                -> IntegerType,
    "start_date"         -> DateType
  )

  /** `silver/stations`: one row per station seen in trips or listed by GBFS. */
  val SilverStation: StructType = fields(
    "station_id"      -> StringType,
    "gbfs_station_id" -> StringType,
    "name"            -> StringType,
    "lat"             -> DoubleType,
    "lon"             -> DoubleType,
    "capacity"        -> IntegerType,
    "region_id"       -> StringType,
    "first_seen"      -> TimestampType,
    "last_seen"       -> TimestampType,
    "trips_out"       -> LongType,
    "trips_in"        -> LongType,
    "in_gbfs"         -> BooleanType,
    "in_trips"        -> BooleanType
  )

  /** `silver/weather_hourly`: one row per New York hour. */
  val SilverWeatherHourly: StructType =
    StructType(
      StructField("ts_hour", TimestampType) +:
        TripSchemas.WeatherColumns.map { case (_, name, dataType) => StructField(name, dataType) }
    ).add("date", DateType).add("hour", IntegerType).add("is_raining", BooleanType)

  /** `silver/calendar`: one row per day. */
  val SilverCalendar: StructType = fields(
    "date"         -> DateType,
    "dow"          -> IntegerType,
    "is_weekend"   -> BooleanType,
    "month"        -> IntegerType,
    "iso_week"     -> IntegerType,
    "holiday_name" -> StringType,
    "holiday_kind" -> StringType,
    "is_holiday"   -> BooleanType
  )

  /**
   * `gold/station_hour`, partitioned by `date`: the zero-filled station x hour panel with weather and
   * calendar.
   */
  val GoldStationHour: StructType = fields(
    "station_id"         -> StringType,
    "ts_hour"            -> TimestampType,
    "hour"               -> IntegerType,
    "dow"                -> IntegerType,
    "is_weekend"         -> BooleanType,
    "is_holiday"         -> BooleanType,
    "departures"         -> LongType,
    "arrivals"           -> LongType,
    "net_flow"           -> LongType,
    "dep_member"         -> LongType,
    "dep_casual"         -> LongType,
    "dep_ebike"          -> LongType,
    "arr_ebike"          -> LongType,
    "avg_dep_duration_s" -> DoubleType,
    "temp_c"             -> DoubleType,
    "apparent_temp_c"    -> DoubleType,
    "precip_mm"          -> DoubleType,
    "wind_kmh"           -> DoubleType,
    "humidity_pct"       -> DoubleType,
    "cloud_pct"          -> DoubleType,
    "weather_code"       -> IntegerType,
    "is_raining"         -> BooleanType,
    "date"               -> DateType
  )

  /** `gold/route_edges`: one row per directed station pair with at least one trip. */
  val GoldRouteEdge: StructType = fields(
    "src_id"            -> StringType,
    "dst_id"            -> StringType,
    "trips"             -> LongType,
    "trips_am_peak"     -> LongType,
    "trips_pm_peak"     -> LongType,
    "trips_weekend"     -> LongType,
    "median_duration_s" -> IntegerType,
    "member_share"      -> DoubleType,
    "ebike_share"       -> DoubleType,
    "is_self_loop"      -> BooleanType
  )
}
