package velograph.graph

import scala.collection.immutable.BitSet

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite

import velograph.testkit.SharedSparkSession

/** The GraphX algorithms on toy graphs whose answers are known or computed independently on the driver. */
class GraphAlgorithmsSpec extends AnyFunSuite with SharedSparkSession {

  private val Alpha = 0.15

  private def graphOf(edges: Seq[(Long, Long, Double)]): Graph[Int, Double] =
    Graph.fromEdges(
      spark.sparkContext.parallelize(edges.map { case (s, d, w) => Edge(s, d, w) }, 2),
      defaultValue = 0
    )

  /**
   * Plain power iteration of PR = α·t + (1 − α)·Pᵀ·PR with p(u→v) = w(u,v) / out-strength(u), normalized to
   * sum 1: an independent driver-side reference for the Pregel programs.
   */
  private def powerIteration(
      edges: Seq[(Long, Long, Double)],
      teleport: Long => Double
  ): Map[Long, Double] = {
    val nodes = edges.flatMap(e => Seq(e._1, e._2)).distinct
    val out   = edges.groupBy(_._1).map { case (src, es) => src -> es.map(_._3).sum }
    val fixed = (1 to 2000).foldLeft(nodes.map(_ -> 0.0).toMap) { (pr, _) =>
      nodes.map { v =>
        v ->
          (Alpha * teleport(v) + (1 - Alpha) * edges.collect { case (s, `v`, w) => pr(s) * w / out(s) }.sum)
      }.toMap
    }
    val total = fixed.values.sum
    fixed.map { case (v, r) => v -> r / total }
  }

  // A small weighted network with uneven weights and no dangling vertex.
  private val Weighted = Seq[(Long, Long, Double)](
    (1, 2, 3),
    (1, 3, 1),
    (2, 3, 2),
    (3, 1, 5),
    (3, 4, 1),
    (4, 1, 1),
    (4, 5, 2),
    (5, 1, 1)
  )

  test("weighted Pregel PageRank reaches the power-iteration fixed point") {
    val result   = WeightedPageRank.run(graphOf(Weighted), Alpha, tol = 1e-10, maxSupersteps = 500)
    val ranks    = result.ranks.collect().toMap
    val expected = powerIteration(Weighted, _ => 1.0)
    expected.foreach { case (v, r) => assert(math.abs(ranks(v) - r) < 1e-7, s"vertex $v: ${ranks(v)} vs $r") }
    assert(math.abs(ranks.values.sum - 1.0) < 1e-12)
    assert(result.converged && result.supersteps > 10)
    assert(result.activeVertices.head == 5, "superstep 0 updates every vertex")
  }

  test("with equal weights it reproduces GraphX's built-in pageRank") {
    val unit = Weighted.map { case (s, d, _) => (s, d, 1.0) }
    val ours =
      WeightedPageRank.run(graphOf(unit), Alpha, tol = 1e-6, maxSupersteps = 500).ranks.collect().toMap
    val builtin = graphOf(unit).pageRank(1e-6, Alpha).vertices.collect().toMap
    val total   = builtin.values.sum
    builtin.foreach { case (v, r) => assert(math.abs(ours(v) - r / total) < 1e-12, s"vertex $v") }
  }

  test("weights change the ranking where route counts alone cannot") {
    // From 1, nine riders go to 2 and one goes to 3; both ride straight back.
    val edges = Seq[(Long, Long, Double)]((1, 2, 9), (1, 3, 1), (2, 1, 1), (3, 1, 1))
    val ours  =
      WeightedPageRank.run(graphOf(edges), Alpha, tol = 1e-8, maxSupersteps = 500).ranks.collect().toMap
    val builtin = graphOf(edges).pageRank(1e-8, Alpha).vertices.collect().toMap
    assert(ours(2) > 2 * ours(3))
    assert(math.abs(builtin(2) - builtin(3)) < 1e-9)
  }

  test("personalized PageRank from two sources at once matches each source's reference") {
    val sources = Array[VertexId](1, 4)
    val ranks   =
      WeightedPageRank.personalized(
        graphOf(Weighted),
        sources,
        Alpha,
        tol = 1e-10,
        maxSupersteps = 500
      ).collect().toMap
    sources.zipWithIndex.foreach { case (source, i) =>
      val expected = powerIteration(Weighted, v => if (v == source) 1.0 else 0.0)
      expected.foreach { case (v, r) =>
        assert(math.abs(ranks(v)(i) - r) < 1e-7, s"source $source, vertex $v")
      }
      assert(math.abs(ranks.values.map(_(i)).sum - 1.0) < 1e-12)
      assert(ranks(source)(i) >= Alpha, "a rider teleports back to the start with probability alpha")
    }
  }

  test("weighted label propagation separates two cliques joined by a weak bridge") {
    val clique        = (ids: Seq[Long]) => for (a <- ids; b <- ids if a < b) yield (a, b, 10.0)
    val edges         = clique(Seq(1L, 2, 3, 4)) ++ clique(Seq(5L, 6, 7, 8)) :+ ((4L, 5L, 1.0))
    val labels        = Communities.propagate(graphOf(edges), supersteps = 10).vertices.collect().toMap
    val (left, right) =
      (Seq(1L, 2, 3, 4).map(labels(_).id).distinct, Seq(5L, 6, 7, 8).map(labels(_).id).distinct)
    assert(left.size == 1 && right.size == 1 && left != right)
    assert(labels.values.flatMap(_.changes.lastOption).max <= 3, "settles within three supersteps")
  }

  test("label votes: the heaviest label wins, ties keep the current label, else the smallest") {
    assert(Communities.winner(9, Map(3L -> 2.0, 5L -> 7.0)) == 5)
    assert(Communities.winner(5, Map(3L -> 7.0, 5L -> 7.0)) == 5)
    assert(Communities.winner(9, Map(4L -> 7.0, 3L -> 7.0)) == 3)
    assert(Communities.mayUpdate(4, 2) && !Communities.mayUpdate(4, 3) && Communities.mayUpdate(7, 1))
  }

  test("modularity of two triangles joined by a bridge, split at the bridge, is 5/14") {
    import spark.implicits._
    val edges =
      Seq((1L, 2L, 1L), (1L, 3L, 1L), (2L, 3L, 1L), (4L, 5L, 1L), (4L, 6L, 1L), (5L, 6L, 1L), (3L, 4L, 1L))
        .toDF("a", "b", "weight")
    val split  = Seq(1L -> 0L, 2L -> 0L, 3L -> 0L, 4L -> 1L, 5L -> 1L, 6L -> 1L).toDF("vid", "label")
    val single = Seq(1L, 2L, 3L, 4L, 5L, 6L).map(_ -> 0L).toDF("vid", "label")
    assert(math.abs(Communities.modularity(edges, split) - 5.0 / 14) < 1e-12)
    assert(math.abs(Communities.modularity(edges, single)) < 1e-12, "one community scores 0")
  }

  test("greedy modularity merging joins singletons into the two triangles, and stops there") {
    // Two triangles {1,2,3} and {4,5,6} joined by the bridge 3-4; every vertex starts as its own community.
    val edges  = Seq((1L, 2L), (1L, 3L), (2L, 3L), (4L, 5L), (4L, 6L), (5L, 6L), (3L, 4L))
    val degree = edges.flatMap { case (a, b) => Seq(a, b) }.groupBy(identity).map { case (v, vs) =>
      v -> vs.size.toDouble
    }
    val merged = Communities.greedyMerge(edges.map(_ -> 1.0).toMap, degree, m = edges.size.toDouble)
    val label  = (v: Long) => merged.getOrElse(v, v)
    assert(Seq(1L, 2L, 3L).map(label).distinct == Seq(1L) && Seq(4L, 5L, 6L).map(label).distinct == Seq(4L))
  }

  test("clustering coefficient: 1 inside a complete graph, diluted by a pendant") {
    val k4    = for (a <- 1L to 4L; b <- 1L to 4L if a < b) yield (a, b, 1.0)
    val stats = GraphStructure.clustering(graphOf(k4 :+ ((1L, 5L, 1.0)))).collect().toMap
    assert(stats(2) == ((3, 3, 1.0)))
    assert(stats(1) == ((4, 3, 0.5)))
    assert(stats(5) == ((1, 0, 0.0)))
  }

  test("multi-source travel times follow the fastest chain of rides") {
    val edges = Seq[(Long, Long, Double)]((1, 2, 5), (2, 3, 5), (1, 3, 15), (3, 4, 1), (5, 1, 2))
    val times = TravelTimes.run(graphOf(edges), Array[VertexId](1, 3), maxSupersteps = 20).collect().toMap
    assert(times(3) == Map(0 -> 10.0, 1 -> 0.0))
    assert(times(4) == Map(0 -> 11.0, 1 -> 1.0))
    assert(times(5).isEmpty, "nothing reaches station 5")
  }

  test("per-vertex superstep sets count the active vertices per superstep") {
    val activity =
      spark.sparkContext.parallelize(Seq(BitSet(0, 1, 2), BitSet(0, 1), BitSet(0), BitSet(0, 70)))
    assert(Supersteps.counts(activity, upTo = Some(4)) == Seq(4L, 2L, 1L, 0L, 0L))
    assert(Supersteps.counts(activity).size == 71, "no cap on the number of supersteps")
  }

  test("strong ties: enough trips and among the source's top-k destinations, weighted both ways") {
    import spark.implicits._
    val routes = Seq(
      ("A", "B", 50L),
      ("A", "C", 30L),
      ("A", "D", 25L),
      ("A", "E", 10L),
      ("A", "A", 100L),
      ("B", "A", 5L)
    ).toDF("src_id", "dst_id", "trips")
      .selectExpr(
        "*",
        "0L AS trips_am_peak",
        "0L AS trips_pm_peak",
        "0L AS trips_weekend",
        "600 AS median_duration_s",
        "0.5 AS member_share",
        "0.5 AS ebike_share",
        "src_id = dst_id AS is_self_loop"
      )
    val edges  = StationGraph.edgeTable(routes)
    val strong = StationGraph.strongDirected(edges, StrongEdgeRule(minTrips = 20, topK = 2))
    val names  = edges.select(
      "src",
      "src_id"
    ).union(edges.select("dst", "dst_id")).distinct().as[(Long, String)].collect().toMap
    val pairs = strong.select("src", "dst").as[(Long, Long)].collect().map { case (s, d) =>
      (names(s), names(d))
    }
    assert(pairs.toSet == Set(("A", "B"), ("A", "C")))
    val weights = StationGraph.strongUndirected(edges, strong).collect().map {
      case Row(a: Long, b: Long, w: Long) =>
        Set(names(a), names(b)) -> w
    }
    assert(weights.toMap == Map(Set("A", "B") -> 55L, Set("A", "C") -> 30L))
  }

  test("the vertex check rejects duplicate stations and unknown route endpoints") {
    import spark.implicits._
    val vertices = Seq("A", "B").toDF("station_id").selectExpr("xxhash64(station_id) AS vid")
    val edges    = Seq(("A", "Z")).toDF("a", "b").selectExpr("xxhash64(a) AS src", "xxhash64(b) AS dst")
    assert(intercept[IllegalArgumentException](StationGraph.check(
      vertices,
      edges
    )).getMessage.contains("1 route endpoints"))
    assert(intercept[IllegalArgumentException](StationGraph.check(vertices.union(vertices), edges.limit(0)))
      .getMessage.contains("collision"))
  }
}
