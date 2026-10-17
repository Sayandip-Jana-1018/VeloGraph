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

  /**
   * `gold/station_archetypes`: each station's archetype in each profile window (P4.1). The clusters are
   * learned on profile-a and applied unchanged to profile-b, so an archetype id means the same in both.
   */
  val GoldStationArchetype: StructType = fields(
    "station_id"     -> StringType,
    "window"         -> StringType,
    "trips"          -> LongType,
    "fitted"         -> BooleanType,
    "archetype"      -> IntegerType,
    "archetype_name" -> StringType,
    "pca_x"          -> DoubleType,
    "pca_y"          -> DoubleType
  )

  /**
   * `gold/ml_features`, partitioned by `date` (P4.2): one row per station-hour of the fit and test splits,
   * with features drawn only from each split's profile window (ARCHITECTURE §8.2, §8.3) and the targets.
   */
  val GoldMlFeature: StructType = fields(
    "station_id"      -> StringType,
    "ts_hour"         -> TimestampType,
    "split"           -> StringType,
    "is_validation"   -> BooleanType,
    "hour"            -> IntegerType,
    "hour_sin"        -> DoubleType,
    "hour_cos"        -> DoubleType,
    "dow"             -> IntegerType,
    "is_weekend"      -> BooleanType,
    "is_holiday"      -> BooleanType,
    "temp_c"          -> DoubleType,
    "apparent_temp_c" -> DoubleType,
    "precip_mm"       -> DoubleType,
    "wind_kmh"        -> DoubleType,
    "humidity_pct"    -> DoubleType,
    "cloud_pct"       -> DoubleType,
    "is_raining"      -> BooleanType,
    "capacity"        -> IntegerType,
    "capacity_known"  -> BooleanType,
    "lat"             -> DoubleType,
    "lon"             -> DoubleType,
    "archetype"       -> IntegerType,
    "pagerank_w"      -> DoubleType,
    "pr_am"           -> DoubleType,
    "pr_pm"           -> DoubleType,
    "pr_shift"        -> DoubleType,
    "in_strength"     -> LongType,
    "out_strength"    -> LongType,
    "imbalance_ratio" -> DoubleType,
    "clustering_coef" -> DoubleType,
    "community"       -> IntegerType,
    "prof_dep_mean"   -> DoubleType,
    "prof_arr_mean"   -> DoubleType,
    "prof_dep_std"    -> DoubleType,
    "prof_arr_std"    -> DoubleType,
    "has_profile"     -> BooleanType,
    "departures"      -> LongType,
    "arrivals"        -> LongType,
    "net_flow"        -> LongType,
    "regime"          -> IntegerType,
    "date"            -> DateType
  )

  /**
   * `gold/live_stations` (P5.3): the static features of live scoring, one row per station, from profile-l
   * (the last eight weeks of trips). Community ids follow the fit window's numbering, which the models
   * learned.
   */
  val GoldLiveStation: StructType = fields(
    "station_id"      -> StringType,
    "name"            -> StringType,
    "lat"             -> DoubleType,
    "lon"             -> DoubleType,
    "capacity"        -> IntegerType,
    "capacity_known"  -> BooleanType,
    "archetype"       -> IntegerType,
    "archetype_name"  -> StringType,
    "pagerank_w"      -> DoubleType,
    "pr_am"           -> DoubleType,
    "pr_pm"           -> DoubleType,
    "pr_shift"        -> DoubleType,
    "in_strength"     -> LongType,
    "out_strength"    -> LongType,
    "imbalance_ratio" -> DoubleType,
    "clustering_coef" -> DoubleType,
    "community"       -> IntegerType
  )

  /**
   * `gold/live_profiles` (P5.3): hour-of-week profiles over profile-l, one row per station, weekday and hour.
   */
  val GoldLiveProfile: StructType = fields(
    "station_id"    -> StringType,
    "dow"           -> IntegerType,
    "hour"          -> IntegerType,
    "prof_dep_mean" -> DoubleType,
    "prof_arr_mean" -> DoubleType,
    "prof_dep_std"  -> DoubleType,
    "prof_arr_std"  -> DoubleType
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

  /**
   * `graph/station_metrics`: one row per station with every GraphX metric (P3). Degrees and PageRank ignore
   * round trips; strengths count them. Strong-graph columns are null for stations without a strong tie, hop
   * columns for stations that cannot reach a hub along strong ties.
   */
  val GraphStationMetrics: StructType = fields(
    "station_id"        -> StringType,
    "vertex_id"         -> LongType,
    "name"              -> StringType,
    "lat"               -> DoubleType,
    "lon"               -> DoubleType,
    "capacity"          -> IntegerType,
    "in_degree"         -> IntegerType,
    "out_degree"        -> IntegerType,
    "in_strength"       -> LongType,
    "out_strength"      -> LongType,
    "self_loop_trips"   -> LongType,
    "net_strength"      -> LongType,
    "imbalance_ratio"   -> DoubleType,
    "pagerank_w"        -> DoubleType,
    "pagerank_u"        -> DoubleType,
    "pagerank_rank"     -> IntegerType,
    "pr_am"             -> DoubleType,
    "pr_pm"             -> DoubleType,
    "pr_shift"          -> DoubleType,
    "community_id"      -> IntegerType,
    "community_size"    -> IntegerType,
    "lpa_builtin_label" -> LongType,
    "wcc_id"            -> LongType,
    "scc_id"            -> LongType,
    "scc_size"          -> IntegerType,
    "strong_degree"     -> IntegerType,
    "triangles"         -> IntegerType,
    "clustering_coef"   -> DoubleType,
    "hops_to_hub"       -> IntegerType,
    "nearest_hub_id"    -> StringType
  )
}
