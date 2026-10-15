package velograph.graph

import org.apache.spark.sql.SaveMode

import velograph.common.{JobContext, ServingJson, SourceRange, SparkJob}
import velograph.etl.{LakeTable, LakeTables}
import velograph.gold.GoldConfig

/**
 * Roadmap P3: gold/route_edges + silver/stations -> `graph/station_metrics` and the network exports for the
 * website (`network-summary.json`, `-nodes`, `-flows`, `-communities`, `-paths`, `-pregel`).
 */
object GraphMetricsJob extends SparkJob {

  val jobName = "graph-metrics"

  // Pregel caches a few RDDs per superstep; recording every block update made a 1.1 GB event log that the history
  // server could not replay. The Storage tab is demonstrated by the RDD job instead.
  override val sparkConf: Map[String, String] = Map("spark.eventLog.logBlockUpdates.enabled" -> "false")

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val lake   = new LakeTables(ctx.spark, ctx.paths)
    val cfg    = GraphConfig(ctx.cfg.raw)
    val span   = GoldConfig.span(ctx.cfg.raw)
    val source = Some(SourceRange(span.from, span.to))
    val result =
      GraphMetrics.compute(ctx.spark, lake.read(LakeTable.Stations), lake.read(LakeTable.RouteEdges), cfg)

    result.stationMetrics
      .coalesce(1) // ~2.5k rows
      .write
      .mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "static")
      .parquet(lake.uri(LakeTable.StationMetrics))
    val written = lake.read(LakeTable.StationMetrics).count()
    require(written == result.nodes.size, s"station_metrics has $written rows, expected ${result.nodes.size}")

    val serving                                            = ctx.paths.serving
    def publish(name: String, data: Any, rows: Long): Unit =
      ServingJson.write(serving.resolve(s"$name.json"), name, jobName, data, Some(rows), source)
    publish("network-summary", result.summary, 1)
    publish("network-nodes", result.nodes, result.nodes.size.toLong)
    publish("network-flows", result.flows, result.flows.size.toLong)
    publish("network-communities", result.communities, result.communities.communityList.size.toLong)
    publish("network-paths", result.paths, result.paths.landmarks.size.toLong)
    publish("network-pregel", result.explainer, result.explainer.steps.size.toLong)

    val s = result.summary
    log.info(
      f"graph: ${s.graph.vertices}%,d stations, ${s.graph.edges}%,d routes; PageRank ${s.pagerank.weighted.supersteps} " +
        f"supersteps, Spearman ${s.pagerank.spearman}%.3f; ${s.communities.communities} communities, " +
        f"Q = ${s.communities.modularity}%.3f (built-in ${s.communities.builtinModularity}%.3f)"
    )
    Map(
      "stations"                    -> s.graph.vertices,
      "routes"                      -> s.graph.edges,
      "strongTies"                  -> s.graph.strongUndirectedEdges,
      "pagerankSupersteps"          -> s.pagerank.weighted.supersteps,
      "pagerankConverged"           -> s.pagerank.weighted.converged,
      "spearmanWeightedVsBuiltin"   -> s.pagerank.spearman,
      "communities"                 -> s.communities.communities,
      "modularity"                  -> s.communities.modularity,
      "builtinModularity"           -> s.communities.builtinModularity,
      "triangles"                   -> s.triangles.triangles,
      "stronglyConnectedComponents" -> s.components.strong.count,
      "timingsMs"                   -> s.timingsMs
    )
  }
}
