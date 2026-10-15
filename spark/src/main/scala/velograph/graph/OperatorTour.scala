package velograph.graph

import java.util.Locale

import org.apache.spark.graphx._

/**
 * Roadmap G10: the GraphX operators, each applied once to the real graph with a checked, measurable result,
 * for Unit III "GraphX features and operations". Every claim in a result string is asserted before it is
 * reported.
 */
object OperatorTour {

  private def n(value: Long): String = String.format(Locale.US, "%,d", Long.box(value))

  def run(
      graph: Graph[StationAttr, RouteAttr],
      nonLoop: Graph[StationAttr, RouteAttr],
      strong: Graph[StationAttr, Double],
      minStrongTrips: Long,
      partitions: Int
  ): Seq[OperatorDemo] = {
    val stations = graph.numVertices

    // triplets: an edge seen together with both of its endpoint attributes.
    val (from, to, busiest) =
      nonLoop.triplets.map(t => (t.srcAttr.name, t.dstAttr.name, t.attr.trips)).top(1)(Ordering.by(_._3)).head

    // degrees: distinct routes in plus out; the reduce keeps the largest (smallest id on ties).
    val (hubId, hubDegree) = nonLoop.degrees.reduce { (a, b) =>
      if (a._2 > b._2 || (a._2 == b._2 && a._1 < b._1)) a else b
    }
    val hubName = nonLoop.vertices.lookup(hubId).head.name

    // collectNeighborIds: the hub's distinct destinations.
    val destinations = nonLoop.collectNeighborIds(EdgeDirection.Out).lookup(hubId).head.length

    // reverse: every edge flipped, so in-degrees of the reversed graph are the original out-degrees.
    val reversedIn = nonLoop.reverse.inDegrees
    val mismatches = reversedIn.join(nonLoop.outDegrees).filter { case (_, (a, b)) => a != b }.count()
    require(mismatches == 0 && reversedIn.count() == nonLoop.outDegrees.count(), "reverse changed degrees")

    // subgraph: an edge predicate keeps busy routes; a vertex predicate keeps docked stations and their edges.
    val busy   = nonLoop.subgraph(epred = _.attr.trips >= minStrongTrips)
    val docked = graph.subgraph(vpred = (_, s) => s.capacity.isDefined)

    // mask: the route graph restricted to the strong ties, keeping the full route attributes. Both graphs use the
    // same EdgePartition2D layout, which mask relies on to match edges partition by partition.
    val masked = nonLoop.mask(strong)
    require(masked.numEdges == strong.numEdges, "mask lost strong edges")

    // mapVertices: station -> docks; mapEdges: route -> trips as a weight; mapTriplets: weight -> probability.
    val docks    = graph.mapVertices((_, s) => s.capacity.getOrElse(0).toLong).vertices.map(_._2).sum().toLong
    val weighted = nonLoop.mapEdges(_.attr.trips.toDouble)
    val totalW   = weighted.edges.map(_.attr).sum().toLong
    val probs    = WeightedPageRank.transitionProbabilities(weighted)
    val maxDev   = probs
      .aggregateMessages[Double](ctx => ctx.sendToSrc(ctx.attr), _ + _, TripletFields.EdgeOnly)
      .map { case (_, total) => math.abs(total - 1.0) }
      .max()
    require(maxDev < 1e-9, s"probabilities out of a station sum to 1 +- $maxDev")

    // groupEdges: split every route into AM-peak, PM-peak and other trips (a multigraph with parallel edges),
    // then merge them back. partitionBy must come first: groupEdges only merges edges within one partition.
    val split = nonLoop.edges.flatMap { e =>
      val other = e.attr.trips - e.attr.amTrips - e.attr.pmTrips
      Seq(e.attr.amTrips, e.attr.pmTrips, other).filter(_ > 0).map(trips => Edge(e.srcId, e.dstId, trips))
    }
    val multigraph  = Graph[StationAttr, Long](nonLoop.vertices, split)
    val parallel    = multigraph.numEdges
    val merged      = multigraph.partitionBy(PartitionStrategy.EdgePartition2D, partitions).groupEdges(_ + _)
    val mergedEdges = merged.numEdges
    require(
      mergedEdges == nonLoop.numEdges,
      s"groupEdges left $mergedEdges edges, expected ${nonLoop.numEdges}"
    )
    require(merged.edges.map(_.attr).sum().toLong == totalW, "groupEdges changed the trip total")
    merged.unpersist(blocking = false)
    multigraph.unpersist(blocking = false)

    Seq(
      OperatorDemo(
        "triplets",
        "an edge together with both endpoint attributes",
        s"busiest route: $from -> $to, ${n(busiest)} trips",
        busiest.toDouble
      ),
      OperatorDemo(
        "degrees",
        "distinct routes into plus out of each station",
        s"$hubName has the most routes: ${n(hubDegree.toLong)}",
        hubDegree.toDouble
      ),
      OperatorDemo(
        "collectNeighborIds",
        "the neighbour ids of every vertex",
        s"$hubName sends riders to ${n(destinations.toLong)} different stations",
        destinations.toDouble
      ),
      OperatorDemo(
        "reverse",
        "flips every edge",
        s"in-degrees of the reversed graph equal the out-degrees for all ${n(reversedIn.count())} stations with departures",
        reversedIn.count().toDouble
      ),
      OperatorDemo(
        "subgraph (edge predicate)",
        "keeps the edges that pass a test",
        s"${n(busy.numEdges)} of ${n(nonLoop.numEdges)} routes have at least $minStrongTrips trips",
        busy.numEdges.toDouble
      ),
      OperatorDemo(
        "subgraph (vertex predicate)",
        "keeps the vertices that pass a test, and the edges between them",
        s"${n(docked.numVertices)} of ${n(stations)} stations have a known capacity; ${n(docked.numEdges)} routes join them",
        docked.numVertices.toDouble
      ),
      OperatorDemo(
        "mask",
        "keeps only what also exists in another graph",
        s"the route graph masked by the strong-tie graph keeps ${n(masked.numEdges)} routes with full attributes",
        masked.numEdges.toDouble
      ),
      OperatorDemo(
        "mapVertices",
        "transforms every vertex attribute",
        s"station -> docks: ${n(docks)} docks across the network",
        docks.toDouble
      ),
      OperatorDemo(
        "mapEdges",
        "transforms every edge attribute",
        s"route -> trips as a Double weight: ${n(totalW)} trips between different stations",
        totalW.toDouble
      ),
      OperatorDemo(
        "mapTriplets",
        "transforms edges using their endpoints",
        f"weight / out-strength of the source = route probability; per station they sum to 1 (max error $maxDev%.1e)",
        maxDev
      ),
      OperatorDemo(
        "groupEdges",
        "merges parallel edges (after partitionBy)",
        s"${n(parallel)} AM/PM/other parallel edges merged back into ${n(mergedEdges)} routes, trips unchanged",
        mergedEdges.toDouble
      ),
      OperatorDemo(
        "aggregateMessages + outerJoinVertices",
        "per-vertex sums of edge messages, joined back onto the vertices",
        "in-, out- and round-trip strength of every station (see strength)",
        stations.toDouble
      ),
      OperatorDemo(
        "Pregel",
        "iterative vertex programs: vprog, sendMsg, mergeMsg per superstep",
        "weighted PageRank, AM/PM PageRank, personalized PageRank, label propagation and travel times",
        5.0
      )
    )
  }
}
