package velograph.etl

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import velograph.common.{JobContext, SparkJob}
import velograph.common.LakePaths.PathOps

object StationTransforms {

  /** One row per station visit, a trip start ("out") or a trip end ("in"), with what the trip recorded. */
  def endpoints(trips: DataFrame): DataFrame = {
    val starts = trips.select(
      col("start_station_id").as("station_id"),
      col("start_station_name").as("name"),
      col("start_lat").as("lat"),
      col("start_lng").as("lng"),
      col("started_at").as("ts"),
      lit("out").as("direction")
    )
    val ends = trips.select(
      col("end_station_id").as("station_id"),
      col("end_station_name").as("name"),
      col("end_lat").as("lat"),
      col("end_lng").as("lng"),
      col("ended_at").as("ts"),
      lit("in").as("direction")
    )
    starts.unionByName(ends).where(col("station_id").isNotNull)
  }

  /**
   * Per station: the most frequent name, median coordinates (robust to e-bike GPS noise), the span it was
   * active and its trip counts.
   */
  def summarize(endpoints: DataFrame): DataFrame = {
    val names = endpoints
      .where(col("name").isNotNull)
      .groupBy("station_id", "name")
      .count()
      .withColumn(
        "rank",
        row_number().over(Window.partitionBy("station_id").orderBy(col("count").desc, col("name")))
      )
      .where(col("rank") === 1)
      .select(col("station_id"), col("name").as("trip_name"))

    endpoints
      .groupBy("station_id")
      .agg(
        percentile_approx(col("lat"), lit(0.5), lit(10000)).as("trip_lat"),
        percentile_approx(col("lng"), lit(0.5), lit(10000)).as("trip_lng"),
        min("ts").as("first_seen"),
        max("ts").as("last_seen"),
        count(when(col("direction") === "out", 1)).as("trips_out"),
        count(when(col("direction") === "in", 1)).as("trips_in")
      )
      .join(names, Seq("station_id"), "left")
  }

  /**
   * GBFS station registry, renamed so its columns cannot clash with the trip-derived ones. The registry drops
   * trailing zeros too (`6131.1`), so its ids go through the same normalisation as the trips'.
   */
  def gbfsStations(stationInformation: DataFrame): DataFrame =
    stationInformation
      .select(explode(col("data.stations")).as("s"))
      .select(
        SilverTripsTransforms.normalizeStationId(col("s.short_name")).as("short_name"),
        col("s.station_id").as("gbfs_station_id"),
        col("s.name").as("gbfs_name"),
        col("s.lat").as("gbfs_lat"),
        col("s.lon").as("gbfs_lon"),
        // A capacity of 0 marks virtual or temporarily removed stations: unknown, not "zero docks".
        when(col("s.capacity") > 0, col("s.capacity")).as("capacity"),
        col("s.region_id").as("region_id")
      )

  /**
   * The station dimension: trip history full-outer-joined with the live registry on `short_name`. GBFS is
   * authoritative for the name and position today; trip data fills in stations that have since closed.
   */
  def dimension(tripStations: DataFrame, gbfs: DataFrame): DataFrame =
    tripStations
      .join(gbfs, tripStations("station_id") === gbfs("short_name"), "full_outer")
      .select(
        coalesce(col("station_id"), col("short_name")).as("station_id"),
        col("gbfs_station_id"),
        coalesce(col("gbfs_name"), col("trip_name")).as("name"),
        coalesce(col("gbfs_lat"), col("trip_lat")).as("lat"),
        coalesce(col("gbfs_lon"), col("trip_lng")).as("lon"),
        col("capacity"),
        col("region_id"),
        col("first_seen"),
        col("last_seen"),
        coalesce(col("trips_out"), lit(0L)).as("trips_out"),
        coalesce(col("trips_in"), lit(0L)).as("trips_in"),
        col("gbfs_station_id").isNotNull.as("in_gbfs"),
        col("first_seen").isNotNull.as("in_trips")
      )
}

/**
 * Roadmap P1.7: `lake/silver/trips` + the latest recorded GBFS `station_information` ->
 * `lake/silver/stations`.
 */
object SilverStationsJob extends SparkJob {

  val jobName = "silver-stations"

  /** Newest `station_information.json` captured by the GBFS recorder (one folder per New York day). */
  private def latestStationInformation(recordings: Path): Path = {
    val snapshots =
      if (!Files.isDirectory(recordings)) Seq.empty
      else {
        val days = Files.list(recordings)
        try days.iterator().asScala.map(_.resolve("station_information.json")).filter(Files.isRegularFile(
            _
          )).toList
        finally days.close()
      }
    snapshots.sortBy(_.getParent.getFileName.toString).lastOption.getOrElse {
      throw new IllegalStateException(
        s"no station_information.json under $recordings - start the GBFS recorder"
      )
    }
  }

  private def readGbfs(spark: SparkSession, path: Path): DataFrame =
    spark.read.schema(TripSchemas.GbfsStationInformation).option("multiLine", "true").json(path.uri)

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark    = ctx.spark
    val snapshot = latestStationInformation(ctx.paths.raw.resolve("gbfs_recordings"))
    log.info(s"GBFS registry: $snapshot")

    val trips = spark.read.schema(TripSchemas.SilverTrip).parquet(ctx.paths.silver.resolve("trips").uri)
    val tripStations = StationTransforms.summarize(StationTransforms.endpoints(trips))
    val gbfs         = StationTransforms.gbfsStations(readGbfs(spark, snapshot))
    val stations     = StationTransforms.dimension(tripStations, gbfs).cache() // two writes + metrics below

    stations
      .coalesce(1) // ~2.6k rows: one small file
      .write
      .mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "static")
      .parquet(ctx.paths.silver.resolve("stations").uri)

    // Match quality, weighted by trips: what share of trip endpoints the live registry knows (bounded: 1 row).
    val m = stations
      .agg(
        count(when(col("in_trips"), 1)).as("tripStations"),
        count(when(col("in_trips") && col("in_gbfs"), 1)).as("matchedStations"),
        count(when(col("in_gbfs") && !col("in_trips"), 1)).as("gbfsOnlyStations"),
        sum(col("trips_out") + col("trips_in")).as("endpoints"),
        sum(when(col("in_gbfs"), col("trips_out") + col("trips_in")).otherwise(0L)).as("matchedEndpoints")
      )
      .head()
    val unmatched = stations
      .where(col("in_trips") && !col("in_gbfs"))
      .orderBy((col("trips_out") + col("trips_in")).desc)
      .select("station_id", "name", "trips_out", "trips_in")
      .limit(10)
      .collect() // bounded: 10 rows
      .map(r => s"${r.getString(0)} ${r.getString(1)} (${r.getLong(2) + r.getLong(3)} endpoints)")
      .toSeq
    stations.unpersist()

    val matchPct = math.round(m.getAs[Long]("matchedEndpoints") * 100000.0 / m.getAs[Long]("endpoints")) /
      1000.0
    log.info(
      f"stations: ${m.getAs[Long]("tripStations")}%,d in trips, endpoint match with GBFS $matchPct%.3f%%"
    )
    Map(
      "gbfsSnapshot"             -> snapshot.getParent.getFileName.toString,
      "tripStations"             -> m.getAs[Long]("tripStations"),
      "matchedStations"          -> m.getAs[Long]("matchedStations"),
      "gbfsOnlyStations"         -> m.getAs[Long]("gbfsOnlyStations"),
      "endpointMatchPct"         -> matchPct,
      "topUnmatchedTripStations" -> unmatched
    )
  }
}
