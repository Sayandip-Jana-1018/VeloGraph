package velograph.gold

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import velograph.common.{JobContext, SparkJob}
import velograph.etl.{LakeTable, LakeTables}

/**
 * Roadmap P2.2: silver trips -> `gold/route_edges`, one row per directed station pair, with working-day peak
 * and weekend splits. GraphX (P3) builds its graph from this table.
 */
object GoldRoutesJob extends SparkJob {

  val jobName = "gold-routes"

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val lake  = new LakeTables(ctx.spark, ctx.paths)
    val span  = GoldConfig.span(ctx.cfg.raw)
    val trips = lake.read(LakeTable.SilverTrips).where(col("start_date").between(span.from, span.to))

    RouteTransforms
      .edges(trips, lake.read(LakeTable.Calendar), GoldConfig.peaks(ctx.cfg.raw))
      .coalesce(4) // ~600k small rows
      .write
      .mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "static")
      .parquet(lake.uri(LakeTable.RouteEdges))

    val edges = lake.read(LakeTable.RouteEdges)
    val s     = edges
      .agg(
        count(lit(1)).as("edges"),
        count(when(col("is_self_loop"), 1)).as("selfLoops"),
        sum("trips").as("trips"),
        sum("trips_am_peak").as("amPeakTrips"),
        sum("trips_pm_peak").as("pmPeakTrips"),
        sum("trips_weekend").as("weekendTrips"),
        count(when(col("trips") >= 20 && !col("is_self_loop"), 1)).as("strongEdges"),
        percentile_approx(col("trips"), lit(0.99), lit(10000)).as("p99Trips")
      )
      .head() // bounded: 1 row

    val expected = trips.where(col("end_station_id").isNotNull).count()
    require(s.getAs[Long]("trips") == expected, s"edge trips ${s.getAs[Long]("trips")} differ from $expected")

    // Concentration: how much traffic the busiest 1 % of routes carry.
    val topShare = edges
      .where(col("trips") >= s.getAs[Long]("p99Trips"))
      .agg(sum("trips"))
      .head()
      .getLong(0) * 100.0 / expected

    val names = lake.read(LakeTable.Stations).select("station_id", "name")
    val top   = edges
      .where(!col("is_self_loop"))
      .orderBy(col("trips").desc)
      .limit(5)
      .join(names.withColumnRenamed("station_id", "src_id").withColumnRenamed("name", "from"), Seq("src_id"))
      .join(names.withColumnRenamed("station_id", "dst_id").withColumnRenamed("name", "to"), Seq("dst_id"))
      .orderBy(col("trips").desc)
      .collect() // bounded: 5 rows
      .map(r => s"${r.getAs[String]("from")} -> ${r.getAs[String]("to")}: ${r.getAs[Long]("trips")}")
      .toSeq

    log.info(f"route edges: ${s.getAs[Long]("edges")}%,d edges carrying $expected%,d trips")
    Map(
      "edges"                  -> s.getAs[Long]("edges"),
      "selfLoops"              -> s.getAs[Long]("selfLoops"),
      "trips"                  -> s.getAs[Long]("trips"),
      "reconciledWithTrips"    -> true,
      "amPeakTrips"            -> s.getAs[Long]("amPeakTrips"),
      "pmPeakTrips"            -> s.getAs[Long]("pmPeakTrips"),
      "weekendTrips"           -> s.getAs[Long]("weekendTrips"),
      "strongEdges"            -> s.getAs[Long]("strongEdges"),
      "top1PctRoutesTripShare" -> math.round(topShare * 10) / 10.0,
      "topRoutes"              -> top
    )
  }
}
