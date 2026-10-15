package velograph.graph

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.{LabelPropagation, ShortestPaths}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import velograph.common.Timing.timed
import velograph.etl.LakeSchemas

/** Everything one run of the GraphX suite produces; `stationMetrics` is persisted. */
final case class GraphRun(
    stationMetrics: DataFrame,
    nodes: Seq[NodeRow],
    summary: NetworkSummary,
    flows: Seq[Flow],
    communities: CommunityReport,
    paths: PathsReport,
    explainer: PregelExplainerReport
)

/**
 * Roadmap P3: the GraphX suite over the station graph (ARCHITECTURE §7, G1-G10). Algorithms run on graphs;
 * their per-station results come back as small DataFrames joined into one row per station, and the exports
 * are built on the driver from that table (bounded: one row per station, ~2.5k).
 */
object GraphMetrics {

  private val Seed = 42L

  def compute(spark: SparkSession, stations: DataFrame, routes: DataFrame, cfg: GraphConfig): GraphRun = {
    import spark.implicits._
    val timings                              = mutable.LinkedHashMap.empty[String, Long]
    def step[A](name: String)(body: => A): A = {
      val (result, millis) = timed(body)
      timings(name) = millis
      result
    }
    def keep(df: DataFrame)
        : DataFrame = { // materialize a small per-vertex result before its graph is released
      val kept = df.persist(StorageLevel.MEMORY_AND_DISK)
      kept.count()
      kept
    }

    // ---- P3.1 Construction: the full route graph, the graph without round trips, and the strong-tie graphs.
    val vertexDf = StationGraph.vertexTable(stations).persist(StorageLevel.MEMORY_AND_DISK)
    val edgeDf   = StationGraph.edgeTable(routes).persist(StorageLevel.MEMORY_AND_DISK)
    val graph    = step("build") {
      StationGraph.check(vertexDf, edgeDf)
      StationGraph.build(vertexDf, edgeDf, cfg.partitions)
    }
    val nonLoop    = graph.subgraph(epred = t => t.srcId != t.dstId).cache()
    val tripsGraph = nonLoop.mapEdges(_.attr.trips.toDouble).cache()

    val strongDirectedDf =
      StationGraph.strongDirected(edgeDf, cfg.strong).persist(StorageLevel.MEMORY_AND_DISK)
    val strongUndirectedDf =
      StationGraph.strongUndirected(edgeDf, strongDirectedDf).persist(StorageLevel.MEMORY_AND_DISK)
    val strongDirected   = StationGraph.weighted(graph.vertices, strongDirectedDf, cfg.partitions).cache()
    val strongUndirected =
      StationGraph.weighted(
        graph.vertices,
        strongUndirectedDf.select("a", "b", "weight"),
        cfg.partitions
      ).cache()

    val partitioning = step("partitioning") {
      Seq(
        PartitionStrategy.RandomVertexCut,
        PartitionStrategy.CanonicalRandomVertexCut,
        PartitionStrategy.EdgePartition1D,
        PartitionStrategy.EdgePartition2D
      ).map(GraphStructure.partitioning(nonLoop, _, cfg.partitions))
    }

    val inDegreeDf  = keep(nonLoop.inDegrees.toDF("vid", "in_degree"))
    val outDegreeDf = keep(nonLoop.outDegrees.toDF("vid", "out_degree"))

    // ---- P3.2 PageRank: ours (weighted, Pregel) against GraphX's built-in (unweighted).
    val weightedPr =
      step("pagerankWeighted")(WeightedPageRank.run(tripsGraph, cfg.resetProb, cfg.tol, cfg.maxSupersteps))
    val builtinPr = step("pagerankBuiltin") {
      val ranked = nonLoop.pageRank(cfg.tol, cfg.resetProb)
      val total  = ranked.vertices.map(_._2).sum()
      val ranks  = keep(ranked.vertices.mapValues(_ / total).toDF("vid", "pagerank_u"))
      ranked.unpersist(blocking = false)
      ranks
    }
    val prWeightedDf = keep(weightedPr.ranks.toDF("vid", "pagerank_w"))
    val paired       = prWeightedDf.join(builtinPr, "vid").select("pagerank_w", "pagerank_u").rdd.map(r =>
      (r.getDouble(0), r.getDouble(1))
    ).cache()
    val spearman = Statistics.corr(paired.map(_._1), paired.map(_._2), "spearman")
    val pearson  = Statistics.corr(paired.map(_._1), paired.map(_._2), "pearson")
    paired.unpersist(blocking = false)

    // ---- P3.3 Morning vs evening: the same PageRank on working-day peak trips only.
    def peakRank(name: String, trips: RouteAttr => Long) = step(name) {
      WeightedPageRank.run(
        nonLoop.subgraph(epred = t => trips(t.attr) > 0).mapEdges(e => trips(e.attr).toDouble),
        cfg.resetProb,
        cfg.tol,
        cfg.maxSupersteps
      )
    }
    val amPr   = peakRank("pagerankAm", _.amTrips)
    val pmPr   = peakRank("pagerankPm", _.pmTrips)
    val peakDf = keep(amPr.ranks.toDF("vid", "pr_am").join(pmPr.ranks.toDF("vid", "pr_pm"), "vid"))

    // ---- P3.4 Strength and imbalance: aggregateMessages + outerJoinVertices.
    val strengthDf = step("strength") {
      keep(
        GraphStructure
          .withFlows(graph)
          .vertices
          .map { case (vid, f) => (vid, f.strength.inTrips, f.strength.outTrips, f.strength.selfLoopTrips) }
          .toDF("vid", "in_strength", "out_strength", "self_loop_trips")
      )
    }

    // ---- P3.5 Communities: weighted label propagation (ours) and GraphX's unweighted LabelPropagation.
    val (labelDf, labelChanges) = step("labelPropagation") {
      val labelled = Communities.propagate(strongUndirected, cfg.lpaSupersteps)
      val labels   = keep(labelled.vertices.map { case (vid, l) => (vid, l.id) }.toDF("vid", "label"))
      val changes  =
        Supersteps.counts(labelled.vertices.map(_._2.changes), upTo = Some(cfg.lpaSupersteps)).drop(1)
      labelled.unpersist(blocking = false)
      (labels, changes)
    }
    val builtinLabelDf = step("labelPropagationBuiltin") {
      val labelled = LabelPropagation.run(strongUndirected, cfg.lpaSupersteps)
      val labels   = keep(labelled.vertices.toDF("vid", "label"))
      labelled.unpersist(blocking = false)
      labels
    }
    val strongPairs   = strongUndirectedDf.select("a", "b", "weight")
    val lpaModularity = step("modularity")(Communities.modularity(strongPairs, labelDf))
    val lpaGroups     = labelDf.groupBy("label").count().where(col("count") >= cfg.minCommunitySize).count()
    val merges        = step("modularityMerge")(Communities.mergeByModularity(strongPairs, labelDf))
    val mergedDf      = keep(
      labelDf
        .join(broadcast(merges.toSeq.toDF("label", "merged")), Seq("label"), "left")
        .select(col("vid"), coalesce(col("merged"), col("label")).as("label"))
    )
    val modularity = Communities.modularity(strongPairs, mergedDf)
    require(
      modularity >= lpaModularity - 1e-9,
      s"modularity merging lowered Q: $lpaModularity -> $modularity"
    )
    val builtinModularity = Communities.modularity(strongPairs, builtinLabelDf)
    val labelSizes        = mergedDf.groupBy("label").agg(count(lit(1)).as("size"))
    val communityDf       = keep(
      mergedDf
        .join(
          labelSizes.withColumn("order", row_number().over(Window.orderBy(col("size").desc, col("label")))),
          "label"
        )
        .select(
          col("vid"),
          when(col("size") >= cfg.minCommunitySize, col("order")).otherwise(0).as("community_id"),
          when(col("size") >= cfg.minCommunitySize, col("size")).otherwise(0).as("community_size")
        )
    )

    // ---- P3.6 Structure: connected and strongly connected components; triangles and clustering coefficient.
    val wccDf =
      step("connectedComponents")(keep(nonLoop.connectedComponents().vertices.toDF("vid", "wcc_id")))
    val sccDf = step("stronglyConnectedComponents") {
      keep(nonLoop.stronglyConnectedComponents(numIter = 10).vertices.toDF("vid", "scc_id"))
    }
    val strongCcDf   = keep(strongUndirected.connectedComponents().vertices.toDF("vid", "cc_id"))
    val clusteringDf = step("triangles") {
      keep(
        GraphStructure
          .clustering(strongUndirected)
          .map { case (vid, (degree, triangles, coef)) => (vid, degree, triangles, coef) }
          .toDF("vid", "strong_degree", "triangles", "clustering_coef")
      )
    }

    // ---- P3.7 Paths: hops to the hubs, travel times from landmarks, personalized PageRank.
    final case class Ranked(
        vid: Long,
        stationId: String,
        name: String,
        lat: Double,
        lon: Double,
        pagerank: Double
    )
    val byRank = prWeightedDf
      .join(vertexDf, "vid")
      .orderBy(col("pagerank_w").desc, col("station_id"))
      .collect() // bounded: one row per station
      .map(r =>
        Ranked(
          r.getAs[Long]("vid"),
          r.getAs[String]("station_id"),
          r.getAs[String]("name"),
          r.getAs[Double]("lat"),
          r.getAs[Double]("lon"),
          r.getAs[Double]("pagerank_w")
        )
      )
      .toSeq
    val hubs        = byRank.take(cfg.hubs)
    val hubOrder    = hubs.map(_.vid).zipWithIndex.toMap
    val landmarks   = Geo.spread(byRank, cfg.landmarks, cfg.landmarkSeparationKm)(r => (r.lat, r.lon))
    val landmarkIds = landmarks.map(_.vid).toArray

    val (hopsDf, hopsMs) = timed {
      val reached = ShortestPaths.run(strongDirected, hubs.map(_.vid)).vertices.map { case (vid, hops) =>
        val nearest =
          if (hops.isEmpty) None else Some(hops.toSeq.minBy { case (hub, d) => (d, hubOrder(hub)) })
        (vid, nearest.map(_._2), nearest.map(_._1))
      }
      keep(
        reached
          .toDF("vid", "hops_to_hub", "nearest_hub_vid")
          .join(
            vertexDf.select(col("vid").as("nearest_hub_vid"), col("station_id").as("nearest_hub_id")),
            Seq("nearest_hub_vid"),
            "left"
          )
          .drop("nearest_hub_vid")
      )
    }
    val travelGraph =
      nonLoop.subgraph(epred =
        _.attr.trips >= cfg.travelMinTrips
      ).mapEdges(_.attr.medianDurationS.toDouble).cache()
    val travelEdges        = travelGraph.numEdges
    val (travel, travelMs) = timed(TravelTimes.run(
      travelGraph,
      landmarkIds,
      cfg.maxSupersteps
    ).collect().toMap) // bounded: one map per station
    val (personal, pprMs) = timed {
      WeightedPageRank.personalized(
        tripsGraph,
        landmarkIds,
        cfg.resetProb,
        cfg.pprTol,
        cfg.maxSupersteps
      ).collect().toMap // bounded: 20 ranks per station
    }
    travelGraph.unpersist(blocking = false)

    // ---- G10 operator tour and the step-by-step Pregel demo on the top hub's neighbourhood.
    val operators = step("operatorTour")(OperatorTour.run(
      graph,
      nonLoop,
      strongDirected,
      cfg.strong.minTrips,
      cfg.partitions
    ))
    val explainer = step("pregelExplainer") {
      val center   = hubs.head.vid
      val partners = strongPairs
        .where(col("a") === center || col("b") === center)
        .select(when(col("a") === center, col("b")).otherwise(col("a")).as("vid"), col("weight"))
        .orderBy(col("weight").desc, col("vid"))
        .limit(cfg.explainerStations - 1)
        .collect() // bounded: explainer stations
        .map(_.getLong(0))
      val members = center +: partners.toSeq
      val attrs   = graph.vertices.filter(v => members.contains(v._1)).collect().sortBy(v =>
        members.indexOf(v._1)
      ).toSeq // bounded
      val routes = edgeDf
        .where(!col("is_self_loop") && col("src").isin(members: _*) && col("dst").isin(members: _*))
        .select("src", "dst", "trips")
        .collect() // bounded: explainer stations squared
        .map(r => (r.getLong(0), r.getLong(1), r.getLong(2)))
        .toSeq
      PregelExplainer.trace(spark.sparkContext, attrs, routes, cfg.resetProb, cfg.tol, cfg.maxSupersteps)
    }

    // ---- P3.8 One row per station with every metric.
    val sccSizeDf = sccDf.groupBy("scc_id").agg(count(lit(1)).as("scc_size"))
    val metrics   = vertexDf
      .join(inDegreeDf, Seq("vid"), "left")
      .join(outDegreeDf, Seq("vid"), "left")
      .join(strengthDf, Seq("vid"), "left")
      .join(prWeightedDf, Seq("vid"), "left")
      .join(builtinPr, Seq("vid"), "left")
      .join(peakDf, Seq("vid"), "left")
      .join(communityDf, Seq("vid"), "left")
      .join(builtinLabelDf.withColumnRenamed("label", "lpa_builtin_label"), Seq("vid"), "left")
      .join(wccDf, Seq("vid"), "left")
      .join(sccDf.join(sccSizeDf, "scc_id"), Seq("vid"), "left")
      .join(clusteringDf, Seq("vid"), "left")
      .join(hopsDf, Seq("vid"), "left")
      .withColumn("in_degree", coalesce(col("in_degree"), lit(0)))
      .withColumn("out_degree", coalesce(col("out_degree"), lit(0)))
      .withColumn("net_strength", col("in_strength") - col("out_strength"))
      .withColumn(
        "imbalance_ratio",
        when(col("in_strength") + col("out_strength") === 0, lit(0.0))
          .otherwise(col("net_strength") / (col("in_strength") + col("out_strength")))
      )
      .withColumn(
        "pagerank_rank",
        row_number().over(Window.orderBy(col("pagerank_w").desc, col("station_id")))
      )
      .withColumn("pr_shift", (col("pr_am") - col("pr_pm")) / (col("pr_am") + col("pr_pm")))
      .withColumnRenamed("vid", "vertex_id")
      .select(LakeSchemas.GraphStationMetrics.fields.map(f => col(f.name).cast(f.dataType)): _*)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val nodes = metrics
      .orderBy("pagerank_rank")
      .select(
        col("station_id").as("id"),
        col("name"),
        col("lat"),
        col("lon"),
        col("capacity"),
        col("in_degree").as("inDegree"),
        col("out_degree").as("outDegree"),
        col("in_strength").as("inStrength"),
        col("out_strength").as("outStrength"),
        col("self_loop_trips").as("selfLoopTrips"),
        col("net_strength").as("netStrength"),
        round(col("imbalance_ratio"), 4).as("imbalance"),
        round(col("pagerank_w"), 7).as("pagerank"),
        round(col("pagerank_u"), 7).as("pagerankUnweighted"),
        col("pagerank_rank").as("pagerankRank"),
        round(col("pr_am"), 7).as("prAm"),
        round(col("pr_pm"), 7).as("prPm"),
        round(col("pr_shift"), 4).as("prShift"),
        col("community_id").as("community"),
        col("community_size").as("communitySize"),
        col("scc_size").as("sccSize"),
        col("strong_degree").as("strongDegree"),
        col("triangles"),
        round(col("clustering_coef"), 4).as("clustering"),
        col("hops_to_hub").as("hopsToHub"),
        col("nearest_hub_id").as("nearestHub")
      )
      .as[NodeRow]
      .collect() // bounded: one row per station
      .toSeq

    // ---- Driver-side summaries over the bounded station table.
    val byId                         = nodes.map(n => n.id -> n).toMap
    val stationOf                    = byRank.map(r => r.vid -> r.stationId).toMap
    val top                          = cfg.exports.topStations
    def value(n: NodeRow, v: Double) = StationValue(n.id, n.name, v)

    val edgeTotals = edgeDf
      .agg(
        sum(when(!col("is_self_loop"), col("trips"))).as("trips"),
        sum(when(col("is_self_loop"), col("trips"))).as("loop_trips"),
        sum(when(!col("is_self_loop"), col("trips_am_peak"))).as("am"),
        sum(when(!col("is_self_loop"), col("trips_pm_peak"))).as("pm"),
        count(when(col("is_self_loop"), 1)).as("loops")
      )
      .head() // bounded: 1 row
    val edgeCount   = nonLoop.numEdges
    val vertexCount = nodes.size.toLong
    val reciprocal  = edgeDf
      .where(!col("is_self_loop"))
      .select("src", "dst")
      .join(edgeDf.select(col("dst").as("src"), col("src").as("dst")), Seq("src", "dst"))
      .count()
    val strongUndirectedEdges = strongUndirected.numEdges
    val size                  = GraphSize(
      vertices = vertexCount,
      edges = edgeCount,
      selfLoops = edgeTotals.getAs[Long]("loops"),
      trips = edgeTotals.getAs[Long]("trips"),
      selfLoopTrips = edgeTotals.getAs[Long]("loop_trips"),
      density = edgeCount.toDouble / (vertexCount * (vertexCount - 1)),
      avgOutDegree = edgeCount.toDouble / vertexCount,
      maxOutDegree = nodes.map(_.outDegree).max,
      reciprocity = reciprocal.toDouble / edgeCount,
      danglingVertices = nodes.count(_.outDegree == 0).toLong,
      strongDirectedEdges = strongDirected.numEdges,
      strongUndirectedEdges = strongUndirectedEdges
    )

    def linearBins(values: Seq[Int], width: Int): Seq[HistogramBin] =
      values.groupBy(_ / width).toSeq.sortBy(_._1).map { case (bin, members) =>
        HistogramBin(bin * width.toDouble, (bin + 1) * width.toDouble, members.size.toLong)
      }
    val strengthBins = nodes
      .map(n => n.inStrength + n.outStrength + n.selfLoopTrips)
      .filter(_ > 0)
      .groupBy(total => math.floor(math.log10(total.toDouble) * 4).toInt)
      .toSeq
      .sortBy(_._1)
      .map { case (bin, members) =>
        HistogramBin(math.pow(10, bin / 4.0), math.pow(10, (bin + 1) / 4.0), members.size.toLong)
      }

    def pregelRun(r: WeightedPageRank.Result, name: String) =
      PregelRunReport(r.supersteps, r.converged, r.activeVertices, timings(name))

    val unweightedRank = nodes.sortBy(n => (-n.pagerankUnweighted, n.id)).map(_.id).zipWithIndex.map {
      case (id, i) => id -> (i + 1)
    }.toMap
    val topWeighted      = nodes.take(top)
    val topUnweighted    = nodes.sortBy(n => (-n.pagerankUnweighted, n.id)).take(top)
    def move(n: NodeRow) = RankMove(n.id, n.name, n.pagerankRank, unweightedRank(n.id))
    val pagerank         = PageRankReport(
      weighted = pregelRun(weightedPr, "pagerankWeighted"),
      builtinMillis = timings("pagerankBuiltin"),
      spearman = spearman,
      pearson = pearson,
      topOverlap = topWeighted.map(_.id).intersect(topUnweighted.map(_.id)).size,
      topWeighted = topWeighted.map(n => value(n, n.pagerank)),
      topUnweighted = topUnweighted.map(n => value(n, n.pagerankUnweighted)),
      risers = nodes.filter(_.pagerankRank <= 100).sortBy(n =>
        (n.pagerankRank - unweightedRank(n.id), n.id)
      ).take(5).map(move),
      fallers = nodes.filter(n => unweightedRank(n.id) <= 100).sortBy(n =>
        (unweightedRank(n.id) - n.pagerankRank, n.id)
      ).take(5).map(move)
    )

    // Peak sinks: of the busiest stations at peak times, where do mornings end vs evenings end?
    val sample      = cfg.shiftSample
    val busiestPeak = nodes.sortBy(n => (-(n.prAm + n.prPm), n.id)).take(200)
    val topMorning  = nodes.sortBy(n => (-n.prAm, n.id)).take(sample)
    val topEvening  = nodes.sortBy(n => (-n.prPm, n.id)).take(sample)
    def share(group: Seq[NodeRow], inside: NodeRow => Boolean) = group.count(inside).toDouble / group.size
    val districtShares                                         = cfg.officeDistricts.map { d =>
      DistrictShare(
        d.name,
        share(topMorning, n => d.contains(n.lat, n.lon)),
        share(topEvening, n => d.contains(n.lat, n.lon))
      )
    } :+ {
      val anyOffice = (n: NodeRow) => cfg.officeDistricts.exists(_.contains(n.lat, n.lon))
      DistrictShare(
        cfg.officeDistricts.map(_.name).mkString(" or "),
        share(topMorning, anyOffice),
        share(topEvening, anyOffice)
      )
    }
    val shift = ShiftReport(
      am = pregelRun(amPr, "pagerankAm"),
      pm = pregelRun(pmPr, "pagerankPm"),
      amTrips = edgeTotals.getAs[Long]("am"),
      pmTrips = edgeTotals.getAs[Long]("pm"),
      morningMagnets = busiestPeak.sortBy(n => (-n.prShift, n.id)).take(15).map(n => value(n, n.prShift)),
      eveningMagnets = busiestPeak.sortBy(n => (n.prShift, n.id)).take(15).map(n => value(n, n.prShift)),
      sample = sample,
      districts = districtShares
    )

    val strength = StrengthReport(
      sinks = nodes.sortBy(n => (-n.netStrength, n.id)).take(10).map(n => value(n, n.netStrength.toDouble)),
      sources = nodes.sortBy(n => (n.netStrength, n.id)).take(10).map(n => value(n, n.netStrength.toDouble)),
      busiest = nodes
        .sortBy(n => (-(n.inStrength + n.outStrength + n.selfLoopTrips), n.id))
        .take(10)
        .map(n => value(n, (n.inStrength + n.outStrength + n.selfLoopTrips).toDouble))
    )

    // Communities: compactness against a random relabelling with the same sizes, and how much tie weight stays inside.
    val members                                      = nodes.filter(_.community > 0)
    def centroid(ms: Seq[NodeRow]): (Double, Double) =
      (ms.map(_.lat).sum / ms.size, ms.map(_.lon).sum / ms.size)
    val centres                 = members.groupBy(_.community).map { case (c, ms) => c -> centroid(ms) }
    def median(xs: Seq[Double]) = if (xs.isEmpty) 0.0 else xs.sorted.apply(xs.size / 2)
    def distancesToCentre(labelOf: NodeRow => Int, of: Seq[NodeRow]) = {
      val centre = of.groupBy(labelOf).map { case (c, ms) => c -> centroid(ms) }
      of.map { n =>
        val (lat, lon) = centre(labelOf(n))
        Geo.haversineKm(lat, lon, n.lat, n.lon)
      }
    }
    val shuffled = members.map(_.id).zip(new Random(Seed).shuffle(members.map(_.community))).toMap
    val ties     = strongPairs
      .join(communityDf.select(col("vid").as("a"), col("community_id").as("ca")), "a")
      .join(communityDf.select(col("vid").as("b"), col("community_id").as("cb")), "b")
    val inside = ties.where(
      col("ca") === col("cb") && col("ca") > 0
    ).groupBy(col("ca").as("community")).agg(sum("weight").as("inside"))
    val incident = ties
      .select(col("ca").as("community"), col("weight"))
      .union(ties.select(col("cb").as("community"), col("weight")))
      .groupBy("community")
      .agg(sum("weight").as("incident"))
    val tieShare = inside
      .join(incident, "community")
      .collect() // bounded: one row per community
      .map(r => r.getAs[Int]("community") -> 2.0 * r.getAs[Long]("inside") / r.getAs[Long]("incident"))
      .toMap
    val communityList = members.groupBy(_.community).toSeq.sortBy(_._1).map { case (c, ms) =>
      val (lat, lon) = centres(c)
      CommunitySummary(
        id = c,
        size = ms.size,
        lat = lat,
        lon = lon,
        radiusKm = median(ms.map(n => Geo.haversineKm(lat, lon, n.lat, n.lon))),
        internalShare = tieShare.getOrElse(c, 0.0),
        meanShift = ms.map(_.prShift).sum / ms.size,
        netStrength = ms.map(_.netStrength).sum,
        topStations = ms.sortBy(_.pagerankRank).take(3).map(_.name)
      )
    }
    val builtinCommunities =
      builtinLabelDf.groupBy("label").count().where(col("count") >= cfg.minCommunitySize).count()
    val communities = CommunityReport(
      supersteps = cfg.lpaSupersteps,
      labelChanges = labelChanges,
      lpaModularity = lpaModularity,
      lpaCommunities = lpaGroups,
      merges = merges.size,
      modularity = modularity,
      builtinModularity = builtinModularity,
      labels = labelDf.select("label").distinct().count(),
      communities = communityList.size,
      coverage = members.size.toDouble / nodes.size,
      builtinCommunities = builtinCommunities,
      medianRadiusKm = median(distancesToCentre(_.community, members)),
      randomMedianRadiusKm = median(distancesToCentre(n => shuffled(n.id), members)),
      communityList = communityList
    )

    def componentStats(df: DataFrame, idColumn: String) = {
      val sizes = df.groupBy(
        idColumn
      ).count().collect().map(_.getLong(1)).sorted(Ordering[Long].reverse) // bounded: <= stations
      ComponentStats(sizes.length.toLong, sizes.headOption.getOrElse(0L), sizes.take(10).toSeq)
    }
    val sccStats   = componentStats(sccDf, "scc_id")
    val components = ComponentReport(
      weak = componentStats(wccDf, "wcc_id"),
      strong = sccStats,
      strongTies = componentStats(strongCcDf, "cc_id"),
      outsideGiantScc = nodes
        .filter(_.sccSize < sccStats.largest)
        .sortBy(n => (-(n.inStrength + n.outStrength), n.id))
        .take(100)
        .map(n => value(n, (n.inStrength + n.outStrength).toDouble))
    )

    val withTies    = nodes.filter(_.strongDegree.isDefined)
    val triangleSum = withTies.flatMap(_.triangles).map(_.toLong).sum
    val triples     = withTies.flatMap(_.strongDegree).map(d => d.toLong * (d - 1) / 2).sum
    val clusterable = withTies.filter(_.strongDegree.exists(_ >= 2))
    val triangles   = TriangleReport(
      triangles = triangleSum / 3,
      transitivity = if (triples == 0) 0.0 else triangleSum.toDouble / triples,
      averageClustering = clusterable.flatMap(_.clustering).sum / math.max(clusterable.size, 1),
      randomGraphClustering = 2.0 * strongUndirectedEdges / (vertexCount * (vertexCount - 1)),
      mostClustered = withTies
        .filter(_.strongDegree.exists(_ >= 5))
        .sortBy(n => (-n.clustering.getOrElse(0.0), n.id))
        .take(10)
        .map(n => value(n, n.clustering.getOrElse(0.0)))
    )

    val stationIds    = nodes.map(_.id)
    val landmarkPaths = landmarks.zipWithIndex.map { case (l, i) =>
      val times = byRank.map(r =>
        r.stationId -> travel.get(r.vid).flatMap(_.get(i)).map(s => math.round(s).toInt)
      ).toMap
      val aligned = stationIds.map(times)
      val ranks   =
        personal.toSeq.map { case (station, values) => (stationOf(station), values(i)) }.filter(_._1 !=
          l.stationId)
      LandmarkPaths(
        id = l.stationId,
        name = l.name,
        lat = l.lat,
        lon = l.lon,
        reachable = aligned.count(_.isDefined).toLong,
        within15Min = aligned.count(_.exists(_ <= 15 * 60)).toLong,
        within30Min = aligned.count(_.exists(_ <= 30 * 60)).toLong,
        travelSeconds = aligned,
        destinations = ranks
          .sortBy { case (id, p) => (-p, id) }
          .take(cfg.exports.pprDestinations)
          .map { case (id, p) =>
            StationValue(
              id,
              byId(id).name,
              BigDecimal(p).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
            )
          }
      )
    }
    val hubValues    = hubs.map(h => StationValue(h.stationId, h.name, h.pagerank))
    val paths        = PathsReport(stationIds, hubValues, nodes.map(_.hopsToHub), landmarkPaths)
    val pathsSummary = PathsSummary(
      hubs = hubValues,
      hopHistogram = nodes.flatMap(_.hopsToHub).groupBy(identity).toSeq.sortBy(_._1).map { case (h, ns) =>
        HopBin(h, ns.size.toLong)
      },
      unreachable = nodes.count(_.hopsToHub.isEmpty).toLong,
      landmarks = landmarks.map(l => StationValue(l.stationId, l.name, l.pagerank)),
      travelEdges = travelEdges,
      medianReachableWithin15Min = median(landmarkPaths.map(_.within15Min.toDouble)),
      shortestPathsMillis = hopsMs,
      travelTimesMillis = travelMs,
      personalizedMillis = pprMs
    )

    val strongSet                     = strongDirectedDf.select("src", "dst").withColumn("strong", lit(true))
    val nonLoopEdges                  = edgeDf.where(!col("is_self_loop"))
    def topBy(column: String, n: Int) =
      nonLoopEdges.orderBy(col(column).desc, col("src_id"), col("dst_id")).limit(n)
    val flows = topBy("trips", cfg.exports.flows)
      .union(topBy("trips_am_peak", cfg.exports.peakFlows))
      .union(topBy("trips_pm_peak", cfg.exports.peakFlows))
      .dropDuplicates("src", "dst")
      .join(strongSet, Seq("src", "dst"), "left")
      .orderBy(col("trips").desc, col("src_id"), col("dst_id"))
      .collect() // bounded: at most flows + 2 * peak-flows rows
      .map { r =>
        Flow(
          from = r.getAs[String]("src_id"),
          to = r.getAs[String]("dst_id"),
          trips = r.getAs[Long]("trips"),
          amTrips = r.getAs[Long]("trips_am_peak"),
          pmTrips = r.getAs[Long]("trips_pm_peak"),
          weekendTrips = r.getAs[Long]("trips_weekend"),
          medianMinutes = math.round(r.getAs[Int]("median_duration_s") / 6.0) / 10.0,
          strong = !r.isNullAt(r.fieldIndex("strong"))
        )
      }
      .toSeq

    val summary = NetworkSummary(
      graph = size,
      partitioning = partitioning,
      outDegreeHistogram = linearBins(nodes.map(_.outDegree), 100),
      inDegreeHistogram = linearBins(nodes.map(_.inDegree), 100),
      strengthHistogram = strengthBins,
      pagerank = pagerank,
      shift = shift,
      strength = strength,
      communities = communities.copy(communityList = communityList.sortBy(c => (-c.size, c.id)).take(10)),
      components = components,
      triangles = triangles,
      paths = pathsSummary,
      operators = operators,
      timingsMs = ListMap(timings.toSeq: _*)
    )

    Seq(tripsGraph, strongDirected, strongUndirected, nonLoop, graph).foreach(_.unpersist(blocking = false))
    GraphRun(metrics, nodes, summary, flows, communities, paths, explainer)
  }
}
