package velograph.graph

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * The graph features of the ML table (ARCHITECTURE §8.3) for one window of trips. They come from the same
 * algorithms as the P3 suite (GraphMetrics), but run on the routes of that window only:
 *   - weighted PageRank, and its working-day morning and evening versions;
 *   - in- and out-strength and their imbalance;
 *   - the clustering coefficient of the strong-tie graph;
 *   - label-propagation communities, merged by modularity.
 * The P3 table covers June to September, test month included, so a model fed from it would see the hours it
 * is asked to predict. This version cannot.
 */
object GraphFeatures {

  val Columns: Seq[String] = Seq(
    "station_id",
    "pagerank_w",
    "pr_am",
    "pr_pm",
    "pr_shift",
    "in_strength",
    "out_strength",
    "imbalance_ratio",
    "clustering_coef",
    "community"
  )

  /** `routes` has the shape of `gold/route_edges`; `strong` is the strong-tie rule scaled to the window. */
  def compute(
      spark: SparkSession,
      stations: DataFrame,
      routes: DataFrame,
      cfg: GraphConfig,
      strong: StrongEdgeRule
  ): DataFrame = {
    import spark.implicits._
    def keep(df: DataFrame)
        : DataFrame = { // materialize a small per-vertex result before its graph is released
      val kept = df.persist(StorageLevel.MEMORY_AND_DISK)
      kept.count()
      kept
    }

    val vertexDf = StationGraph.vertexTable(stations).persist(StorageLevel.MEMORY_AND_DISK)
    val edgeDf   = StationGraph.edgeTable(routes).persist(StorageLevel.MEMORY_AND_DISK)
    StationGraph.check(vertexDf, edgeDf)
    val graph   = StationGraph.build(vertexDf, edgeDf, cfg.partitions)
    val nonLoop = graph.subgraph(epred = t => t.srcId != t.dstId).cache()

    def rank(name: String, trips: RouteAttr => Long): DataFrame = {
      val weighted = nonLoop.subgraph(epred = t => trips(t.attr) > 0).mapEdges(e => trips(e.attr).toDouble)
      val result   = WeightedPageRank.run(weighted, cfg.resetProb, cfg.tol, cfg.maxSupersteps)
      keep(result.ranks.toDF("vid", name))
    }
    val ranks = rank("pagerank_w", _.trips)
      .join(rank("pr_am", _.amTrips), Seq("vid"), "full")
      .join(rank("pr_pm", _.pmTrips), Seq("vid"), "full")

    val strength = keep(
      GraphStructure
        .strength(graph)
        .map { case (vid, s) => (vid, s.inTrips, s.outTrips) }
        .toDF("vid", "in_strength", "out_strength")
    )

    val strongPairs = StationGraph
      .strongUndirected(edgeDf, StationGraph.strongDirected(edgeDf, strong))
      .select("a", "b", "weight")
      .persist(StorageLevel.MEMORY_AND_DISK)
    val strongGraph = StationGraph.weighted(graph.vertices, strongPairs, cfg.partitions).cache()
    val clustering  = keep(
      GraphStructure.clustering(strongGraph).map { case (vid, (_, _, coef)) => (vid, coef) }.toDF(
        "vid",
        "clustering_coef"
      )
    )

    val labelled = Communities.propagate(strongGraph, cfg.lpaSupersteps)
    val labels   = keep(labelled.vertices.map { case (vid, l) => (vid, l.id) }.toDF("vid", "label"))
    labelled.unpersist(blocking = false)
    val merges = Communities.mergeByModularity(strongPairs, labels)
    val merged = labels
      .join(broadcast(merges.toSeq.toDF("label", "merged")), Seq("label"), "left")
      .select(col("vid"), coalesce(col("merged"), col("label")).as("label"))
    val sizes       = merged.groupBy("label").agg(count(lit(1)).as("size"))
    val communities = keep(
      merged
        .join(
          sizes.withColumn("order", row_number().over(Window.orderBy(col("size").desc, col("label")))),
          "label"
        )
        .select(
          col("vid"),
          when(col("size") >= cfg.minCommunitySize, col("order")).otherwise(0).as("community")
        )
    )

    // Stations without trips in the window are vertices without edges: zero strength, no community.
    val features = vertexDf
      .select("vid", "station_id")
      .join(ranks, Seq("vid"), "left")
      .join(strength, Seq("vid"), "left")
      .join(clustering, Seq("vid"), "left")
      .join(communities, Seq("vid"), "left")
      .na
      .fill(0.0, Seq("pagerank_w", "pr_am", "pr_pm", "clustering_coef"))
      .na
      .fill(0L, Seq("in_strength", "out_strength"))
      .na
      .fill(0, Seq("community"))
      .withColumn(
        "imbalance_ratio",
        when(col("in_strength") + col("out_strength") === 0, lit(0.0))
          .otherwise((col("in_strength") - col("out_strength")) / (col("in_strength") + col("out_strength")))
      )
      .withColumn(
        "pr_shift",
        when(col("pr_am") + col("pr_pm") === 0, lit(0.0))
          .otherwise((col("pr_am") - col("pr_pm")) / (col("pr_am") + col("pr_pm")))
      )
      .select(Columns.map(col): _*)
    val result = keep(features)
    Seq(graph, nonLoop, strongGraph).foreach(_.unpersist(blocking = false))
    result
  }
}
