package velograph.graph

import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/** A station as a GraphX vertex. `capacity` is None for the few trip stations GBFS no longer lists. */
final case class StationAttr(stationId: String, name: String, lat: Double, lon: Double, capacity: Option[Int])

/** A directed route as a GraphX edge: one row of `gold/route_edges`. Peak counts are working days only. */
final case class RouteAttr(
    trips: Long,
    amTrips: Long,
    pmTrips: Long,
    weekendTrips: Long,
    medianDurationS: Int
)

/**
 * Which routes count as strong ties (ARCHITECTURE §7): enough trips, and among the source's top-k
 * destinations.
 */
final case class StrongEdgeRule(minTrips: Long, topK: Int)

/**
 * Roadmap P3.1: the station property graph. Stations become vertices and routes become edges; lookups and
 * filters run as DataFrames, where Catalyst optimizes them, and the graph is built once at the end.
 */
object StationGraph {

  /**
   * GraphX identifies vertices by Long. xxhash64 of the station id gives every job the same id without a
   * lookup table; `check` proves that no two stations collide.
   */
  def vertexId(stationId: Column): Column = xxhash64(stationId)

  /** Stations seen in trips, keyed by vertex id. */
  def vertexTable(stations: DataFrame): DataFrame =
    stations
      .where(col("in_trips"))
      .select(
        vertexId(col("station_id")).as("vid"),
        col("station_id"),
        col("name"),
        col("lat"),
        col("lon"),
        col("capacity")
      )

  /** Routes with both endpoints as vertex ids. */
  def edgeTable(routes: DataFrame): DataFrame =
    routes.select(vertexId(col("src_id")).as("src"), vertexId(col("dst_id")).as("dst"), col("*"))

  /**
   * Fails unless vertex ids are unique and every route endpoint is a known station. GraphX would otherwise
   * invent a vertex with a null attribute for an unknown endpoint, silently.
   */
  def check(vertices: DataFrame, edges: DataFrame): Unit = {
    val ids = vertices.agg(count(lit(1)), countDistinct("vid")).head() // bounded: 1 row
    require(
      ids.getLong(0) == ids.getLong(1),
      s"xxhash64 collision: ${ids.getLong(1)} ids for ${ids.getLong(0)} stations"
    )
    val endpoints = edges.select(col("src").as("vid")).union(edges.select(col("dst").as("vid"))).distinct()
    val unknown   = endpoints.join(vertices, Seq("vid"), "left_anti").count()
    require(unknown == 0, s"$unknown route endpoints are not stations")
  }

  def vertexRdd(vertices: DataFrame): RDD[(VertexId, StationAttr)] =
    vertices.rdd.map { r =>
      val capacity = if (r.isNullAt(r.fieldIndex("capacity"))) None else Some(r.getAs[Int]("capacity"))
      r.getAs[Long]("vid") -> StationAttr(
        r.getAs[String]("station_id"),
        r.getAs[String]("name"),
        r.getAs[Double]("lat"),
        r.getAs[Double]("lon"),
        capacity
      )
    }

  /**
   * The full route graph, self-loops (round trips) included. EdgePartition2D places each edge by (source
   * block, destination block) on a √P × √P grid, so a vertex is copied to at most 2·√P − 1 edge partitions.
   */
  def build(vertices: DataFrame, edges: DataFrame, partitions: Int): Graph[StationAttr, RouteAttr] = {
    val edgeRdd: RDD[Edge[RouteAttr]] = edges.rdd.map { r =>
      Edge(
        r.getAs[Long]("src"),
        r.getAs[Long]("dst"),
        RouteAttr(
          r.getAs[Long]("trips"),
          r.getAs[Long]("trips_am_peak"),
          r.getAs[Long]("trips_pm_peak"),
          r.getAs[Long]("trips_weekend"),
          r.getAs[Int]("median_duration_s")
        )
      )
    }
    // Explicit type arguments: with named arguments scalac would type the default vertex attribute as Nothing.
    val raw = Graph[StationAttr, RouteAttr](
      vertexRdd(vertices),
      edgeRdd,
      edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel = StorageLevel.MEMORY_AND_DISK
    )
    val graph = raw.partitionBy(PartitionStrategy.EdgePartition2D, partitions).cache()
    graph.edges.count() // materialize the partitioned copy before releasing the unpartitioned one
    graph.vertices.count()
    raw.unpersist(blocking = false)
    graph
  }

  /** Directed strong edges (src, dst, trips): enough trips and among the source's top-k destinations. */
  def strongDirected(edges: DataFrame, rule: StrongEdgeRule): DataFrame = {
    val bySource =
      Window.partitionBy("src").orderBy(col("trips").desc, col("dst_id")) // ties broken by station id
    edges
      .where(!col("is_self_loop"))
      .withColumn("destination_rank", row_number().over(bySource))
      .where(col("trips") >= rule.minTrips && col("destination_rank") <= rule.topK)
      .select("src", "dst", "trips")
  }

  /**
   * The undirected strong graph (communities, triangles): one row (a, b) with a < b for each pair joined by a
   * strong edge in either direction, weighted by all trips between them in both directions.
   */
  def strongUndirected(edges: DataFrame, strong: DataFrame): DataFrame = {
    def pairs(df: DataFrame): DataFrame =
      df.select(least(col("src"), col("dst")).as("a"), greatest(col("src"), col("dst")).as("b"), col("trips"))
    val weights = pairs(edges.where(!col("is_self_loop"))).groupBy("a", "b").agg(sum("trips").as("weight"))
    pairs(strong).select("a", "b").distinct().join(weights, Seq("a", "b"))
  }

  /** A graph with Double edge weights over the given vertices, from rows of (src, dst, weight). */
  def weighted(
      vertices: RDD[(VertexId, StationAttr)],
      edges: DataFrame,
      partitions: Int
  ): Graph[StationAttr, Double] = {
    val edgeRdd =
      edges.rdd.map(r => Edge(r.getLong(0), r.getLong(1), r.get(2).asInstanceOf[Number].doubleValue))
    Graph[StationAttr, Double](vertices, edgeRdd).partitionBy(PartitionStrategy.EdgePartition2D, partitions)
  }
}
