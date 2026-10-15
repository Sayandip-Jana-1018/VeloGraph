package velograph.graph

import scala.reflect.ClassTag

import org.apache.spark.graphx._

/** Trips through a station: arriving from, and leaving to, other stations, plus round trips. */
final case class Strength(inTrips: Long, outTrips: Long, selfLoopTrips: Long) {
  def +(other: Strength): Strength =
    Strength(inTrips + other.inTrips, outTrips + other.outTrips, selfLoopTrips + other.selfLoopTrips)

  /** Arrivals minus departures; round trips cancel out. */
  def net: Long = inTrips - outTrips

  /** Net flow as a share of all flow through the station, in [-1, 1]: +1 only arrives, -1 only departs. */
  def imbalance: Double = if (inTrips + outTrips == 0) 0.0 else net.toDouble / (inTrips + outTrips)
}

object Strength {
  val Zero: Strength = Strength(0L, 0L, 0L)
}

/** A station together with the trips through it, after `outerJoinVertices`. */
final case class StationFlow(station: StationAttr, strength: Strength)

/** Vertex-cut quality of one partitioning strategy (P3.1). */
final case class PartitioningRun(
    strategy: String,
    partitions: Int,
    replicationFactor: Double,
    maxEdgesPerPartition: Long,
    minEdgesPerPartition: Long,
    edgeImbalance: Double
)

/** Structural metrics (roadmap P3.1, P3.4, P3.6) built from GraphX's neighbourhood operators. */
object GraphStructure {

  /**
   * In-, out- and round-trip strength per station with one `aggregateMessages` pass. Only the edge attribute
   * is read (TripletFields.EdgeOnly), so GraphX does not ship vertex attributes to the edge partitions at
   * all.
   */
  def strength(graph: Graph[StationAttr, RouteAttr]): VertexRDD[Strength] =
    graph.aggregateMessages[Strength](
      ctx =>
        if (ctx.srcId == ctx.dstId) ctx.sendToSrc(Strength(0L, 0L, ctx.attr.trips))
        else {
          ctx.sendToSrc(Strength(0L, ctx.attr.trips, 0L))
          ctx.sendToDst(Strength(ctx.attr.trips, 0L, 0L))
        },
      _ + _,
      TripletFields.EdgeOnly
    )

  /** Attaches the strengths to the stations; stations without trips (none, in practice) get zeros. */
  def withFlows(graph: Graph[StationAttr, RouteAttr]): Graph[StationFlow, RouteAttr] =
    graph.outerJoinVertices(strength(graph))((_, station, flow) =>
      StationFlow(station, flow.getOrElse(Strength.Zero))
    )

  /**
   * Local clustering coefficient on the undirected strong graph: of the pairs of a station's strong
   * neighbours, the share that are strong neighbours of each other, 2·T / (d·(d − 1)) for T triangles and
   * degree d. Returns (degree, triangles, coefficient) for every vertex with at least one edge.
   */
  def clustering[VD: ClassTag](undirected: Graph[VD, Double]): VertexRDD[(Int, Int, Double)] = {
    val triangles = undirected.triangleCount().vertices
    undirected.degrees.innerJoin(triangles) { (_, degree, t) =>
      (degree, t, if (degree < 2) 0.0 else 2.0 * t / (degree.toLong * (degree - 1)))
    }
  }

  /**
   * GraphX partitions edges and copies vertices to every partition that holds one of their edges (a vertex
   * cut). The replication factor, copies per vertex on average, is what a strategy costs in memory and in
   * shipping vertex attributes to edges in each superstep.
   */
  def partitioning[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED],
      strategy: PartitionStrategy,
      partitions: Int
  ): PartitioningRun = {
    val partitioned  = graph.partitionBy(strategy, partitions)
    val perPartition = partitioned.edges
      .mapPartitions { edges =>
        // GraphX reuses one Edge object while iterating, so only the ids are kept.
        val (vertices, count) = edges.foldLeft((Set.empty[VertexId], 0L)) { case ((seen, n), e) =>
          (seen + e.srcId + e.dstId, n + 1)
        }
        Iterator((vertices.size.toLong, count))
      }
      .collect() // bounded: one pair per partition
    partitioned.unpersist(blocking = false)

    val counts    = perPartition.map(_._2)
    val vertices  = graph.vertices.count()
    val meanEdges = counts.sum.toDouble / counts.length
    PartitioningRun(
      strategy = strategy.toString,
      partitions = partitions,
      replicationFactor = perPartition.map(_._1).sum.toDouble / vertices,
      maxEdgesPerPartition = counts.max,
      minEdgesPerPartition = counts.min,
      edgeImbalance = counts.max / meanEdges
    )
  }
}
