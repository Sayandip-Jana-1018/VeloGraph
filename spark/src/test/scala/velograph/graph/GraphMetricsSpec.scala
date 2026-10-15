package velograph.graph

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.funsuite.AnyFunSuite

import velograph.etl.LakeSchemas
import velograph.testkit.SharedSparkSession

/**
 * The whole GraphX suite on a synthetic city: two neighbourhoods of four stations, busy inside and barely
 * connected, where commuters ride to the "office" station O1 in the morning and home to H1 in the evening.
 */
class GraphMetricsSpec extends AnyFunSuite with SharedSparkSession {

  private val Office = Seq("O1", "O2", "O3", "O4") // around 40.755, -73.985 (inside the test district)
  private val Home   = Seq("H1", "H2", "H3", "H4") // around 40.680, -73.950

  private val Config = GraphConfig(
    partitions = 4,
    resetProb = 0.15,
    tol = 1e-4,
    maxSupersteps = 100,
    strong = StrongEdgeRule(minTrips = 20, topK = 10),
    lpaSupersteps = 8,
    minCommunitySize = 3,
    hubs = 2,
    landmarks = 2,
    landmarkSeparationKm = 3.0,
    travelMinTrips = 1,
    pprTol = 1e-6,
    explainerStations = 4,
    officeDistricts = Seq(District("Test Midtown", 40.745, 40.765, -74.0, -73.97)),
    shiftSample = 3,
    exports = ExportSizes(flows = 10, peakFlows = 5, pprDestinations = 3, topStations = 3)
  )

  private def stations: DataFrame = {
    val rows = Office.zipWithIndex.map { case (id, i) =>
      Row(id, id, s"Office $id", 40.755 + i * 0.002, -73.985, 30, "71", null, null, 0L, 0L, true, true)
    } ++
      Home.zipWithIndex.map { case (id, i) =>
        Row(id, id, s"Home $id", 40.680 + i * 0.002, -73.950, 20, "71", null, null, 0L, 0L, true, true)
      }
    spark.createDataFrame(spark.sparkContext.parallelize(rows), LakeSchemas.SilverStation)
  }

  private def routes: DataFrame = {
    def within(group: Seq[String], trips: Long) =
      for (a <- group; b <- group if a != b) yield (a, b, trips, 0L, 0L)
    // The commute stays below the strong-tie threshold, so the neighbourhoods are the only strong structure.
    val commute = Home.map(h => (h, "O1", 15L, 12L, 0L)) ++ Office.map(o => (o, "H1", 15L, 0L, 12L))
    val rows    = (within(Office, 40) ++ within(Home, 40)).filterNot(r => r._2 == "O1" || r._2 == "H1") ++
      commute ++
      Office.filterNot(_ == "O1").map(o => (o, "O1", 40L, 0L, 0L)) ++
      Home.filterNot(_ == "H1").map(h => (h, "H1", 40L, 0L, 0L)) :+
      (("O2", "O2", 5L, 0L, 0L))
    val distinct = rows.groupBy(r => (r._1, r._2)).map { case ((a, b), rs) =>
      (a, b, rs.map(_._3).sum, rs.map(_._4).sum, rs.map(_._5).sum)
    }
    spark.createDataFrame(
      spark.sparkContext.parallelize(distinct.toSeq.map { case (a, b, trips, am, pm) =>
        Row(a, b, trips, am, pm, 0L, 600, 0.8, 0.7, a == b)
      }),
      LakeSchemas.GoldRouteEdge
    )
  }

  private lazy val run = GraphMetrics.compute(spark, stations, routes, Config)

  test("station_metrics has one row per station in the declared schema") {
    assert(run.stationMetrics.schema.map(f => f.name -> f.dataType) ==
      LakeSchemas.GraphStationMetrics.map(f => f.name -> f.dataType))
    assert(run.stationMetrics.count() == 8 && run.nodes.size == 8)
    assert(math.abs(run.nodes.map(_.pagerank).sum - 1.0) < 1e-6)
    assert(run.nodes.head.pagerankRank == 1)
  }

  test("the commute shows up as morning and evening magnets") {
    val byId = run.nodes.map(n => n.id -> n).toMap
    assert(byId("O1").prShift > 0.5, "the office hub is where morning riders end up")
    assert(byId("H1").prShift < -0.5, "the home hub is where evening riders end up")
    val district = run.summary.shift.districts.head
    assert(district.morningShare > district.eveningShare)
  }

  test("label propagation finds the two neighbourhoods and modularity rewards them") {
    val community = run.nodes.map(n => n.id -> n.community).toMap
    assert(Office.map(community).distinct.size == 1 && Home.map(community).distinct.size == 1)
    assert(community("O1") != community("H1") && community("O1") > 0)
    assert(run.communities.communities == 2 && run.communities.modularity > 0.2)
    assert(run.communities.modularity >= run.communities.lpaModularity)
  }

  test("strength counts round trips, and the exports are complete") {
    val o2 = run.nodes.find(_.id == "O2").get
    assert(o2.selfLoopTrips == 5)
    assert(run.summary.graph.selfLoops == 1 && run.summary.graph.vertices == 8)
    assert(run.summary.components.weak.count == 1)
    assert(run.explainer.stations.size == 4 && run.explainer.steps.size == run.explainer.convergedAfter + 1)
    assert(math.abs(run.explainer.finalRanks.sum - 1.0) < 1e-9)
    assert(run.paths.landmarks.size == 2 && run.paths.landmarks.forall(_.travelSeconds.size == 8))
    assert(run.summary.operators.map(_.operator).contains("groupEdges"))
    assert(run.flows.nonEmpty && run.flows.size <= 20)
  }
}
