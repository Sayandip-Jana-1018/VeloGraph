package velograph.streaming

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import velograph.etl.LakeSchemas
import velograph.ml.{FeatureBuildJob, FeatureTransforms}

/** Message schemas of the live topics (ARCHITECTURE §9.2), as the Python producer writes them. */
object LiveSchemas {

  private def fields(columns: (String, DataType)*): StructType =
    StructType(columns.map { case (name, dataType) => StructField(name, dataType) })

  val StatusSchema = "velograph.station-status"

  /** `velograph.gbfs.station-status.v1`: one station's inventory, keyed by station id. */
  val StationStatus: StructType = fields(
    "schema"         -> StringType,
    "v"              -> IntegerType,
    "source"         -> StringType,
    "kind"           -> StringType,
    "station_id"     -> StringType,
    "gbfs_id"        -> StringType,
    "name"           -> StringType,
    "lat"            -> DoubleType,
    "lon"            -> DoubleType,
    "capacity"       -> IntegerType,
    "bikes"          -> IntegerType,
    "ebikes"         -> IntegerType,
    "bikes_disabled" -> IntegerType,
    "docks"          -> IntegerType,
    "docks_disabled" -> IntegerType,
    "is_installed"   -> BooleanType,
    "is_renting"     -> BooleanType,
    "is_returning"   -> BooleanType,
    "last_reported"  -> LongType,
    "fetched_at"     -> DoubleType,
    "feed_updated"   -> LongType
  )
}

/** Roadmap P5.3: the DataFrame side of live scoring, as pure functions. */
object LiveTransforms {

  /**
   * Kafka records -> station-status rows. A record that is not valid JSON, or not a station-status message,
   * keeps `valid = false` and its raw text, for the dead-letter queue. Event time is the station's own
   * `last_reported`; `kafka_ts` is the broker's append time.
   */
  def parse(kafka: DataFrame): DataFrame = {
    val required = Seq("station_id", "last_reported", "bikes", "docks", "is_installed", "fetched_at")
    kafka
      .select(
        col("value").cast(StringType).as("raw"),
        col("timestamp").as("kafka_ts"),
        col("partition"),
        col("offset")
      )
      .withColumn("m", from_json(col("raw"), LiveSchemas.StationStatus))
      .select(col("raw"), col("kafka_ts"), col("partition"), col("offset"), col("m.*"))
      .withColumn(
        "error",
        when(col("schema").isNull, lit("not a JSON object"))
          .when(col("schema") =!= LiveSchemas.StatusSchema, concat(lit("unexpected schema "), col("schema")))
          .when(required.map(c => col(c).isNull).reduce(_ || _), lit("missing required fields"))
      )
      .withColumn("valid", col("error").isNull)
      .withColumn("event_time", timestamp_seconds(col("last_reported")))
  }

  /** The newest report of every station: by `last_reported`, then by fetch time. */
  def newest(statuses: DataFrame): DataFrame =
    statuses
      .groupBy("station_id")
      .agg(max_by(
        struct(statuses.columns.map(col): _*),
        struct(col("last_reported"), col("fetched_at"))
      ).as("s"))
      .select("s.*")

  /** Columns as [[StationStatus]] reads them. */
  val StatusColumns: Seq[Column] = Seq(
    col("station_id").as("stationId"),
    coalesce(col("name"), lit("")).as("name"),
    coalesce(col("lat"), lit(0.0)).as("lat"),
    coalesce(col("lon"), lit(0.0)).as("lon"),
    coalesce(col("capacity"), lit(0)).as("capacity"),
    col("bikes"),
    coalesce(col("ebikes"), lit(0)).as("ebikes"),
    col("docks"),
    col("is_installed").as("installed"),
    coalesce(col("is_renting"), lit(false)).as("renting"),
    coalesce(col("is_returning"), lit(false)).as("returning"),
    col("last_reported").as("lastReported"),
    col("fetched_at").as("fetchedAt"),
    unix_millis(col("kafka_ts")).as("kafkaMillis")
  )

  /**
   * Model input for every station in the current and the next hour (`slot` 0 and 1), with the columns and
   * types the champions were trained on (`gold/ml_features`, ARCHITECTURE §8.3):
   *   - calendar and weather from `hours`, one row per slot;
   *   - station, graph and archetype features from `stations` (gold/live_stations);
   *   - hour-of-week profiles from `profiles` (gold/live_profiles), with the network-wide profile standing in
   *     for a station that has none, as in training.
   * A station the lake has never seen gets the defaults FeatureBuildJob gives an unknown station: archetype
   * -1, no community, zero graph features.
   */
  def featureRows(
      statuses: DataFrame,
      hours: DataFrame,
      stations: DataFrame,
      profiles: DataFrame,
      medianCapacity: Int
  ): DataFrame = {
    val (hourSin, hourCos) = FeatureTransforms.hourOnCircle(col("hour"))
    val rows               = statuses
      .select(
        col("stationId").as("station_id"),
        col("capacity").as("live_capacity"),
        col("lat").as("live_lat"),
        col("lon").as("live_lon")
      )
      .crossJoin(broadcast(hours))
      .join(broadcast(stations.drop("name")), Seq("station_id"), "left")
    val modelTypes = LakeSchemas.GoldMlFeature.fields.map(f => f.name -> f.dataType).toMap
    FeatureTransforms
      .withProfiles(rows, broadcast(profiles))
      .withColumn(
        "capacity",
        when(
          col("live_capacity") > 0,
          col("live_capacity")
        ).otherwise(coalesce(col("capacity"), lit(medianCapacity)))
      )
      .withColumn("lat", coalesce(col("live_lat"), col("lat")))
      .withColumn("lon", coalesce(col("live_lon"), col("lon")))
      .withColumn("archetype", coalesce(col("archetype"), lit(-1)))
      .withColumn("archetype_name", coalesce(col("archetype_name"), lit("No profile")))
      .na
      .fill(0.0, Seq("pagerank_w", "pr_am", "pr_pm", "pr_shift", "imbalance_ratio", "clustering_coef"))
      .na
      .fill(0L, Seq("in_strength", "out_strength"))
      .na
      .fill(0, Seq("community"))
      .withColumn("hour_sin", hourSin)
      .withColumn("hour_cos", hourCos)
      .select(
        Seq(col("station_id"), col("slot"), col("archetype_name")) ++
          FeatureBuildJob.Dictionary.map(f => col(f.name).cast(modelTypes(f.name))): _*
      )
  }
}
