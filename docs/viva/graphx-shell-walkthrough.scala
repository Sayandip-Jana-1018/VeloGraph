// VeloGraph GraphX walkthrough (Unit III, "GraphX features and operations"). From the repository root:
//   . .\scripts\env.ps1
//   spark-shell --master "local[8]" --driver-memory 4g --conf spark.ui.port=4050
// then paste one numbered block at a time; :quit leaves the shell. Expected output and explanations:
// docs/viva/graphx-shell-walkthrough.md.

import org.apache.spark.graphx._
import org.apache.spark.sql.functions.{col, xxhash64}

// 1. A property graph from two lake tables: stations are vertices (attribute: name), busy routes are edges
//    (attribute: trips). xxhash64 turns each station id into the Long that GraphX needs as a VertexId.
val vertices = spark.read.parquet("data/lake/silver/stations").where("in_trips")
  .select(xxhash64(col("station_id")), col("name")).rdd.map(r => (r.getLong(0), r.getString(1)))
val edges = spark.read.parquet("data/lake/gold/route_edges").where("NOT is_self_loop AND trips >= 20")
  .select(xxhash64(col("src_id")), xxhash64(col("dst_id")), col("trips")).rdd
  .map(r => Edge(r.getLong(0), r.getLong(1), r.getLong(2)))
val graph = Graph(vertices, edges).partitionBy(PartitionStrategy.EdgePartition2D, 16).cache()
println(s"stations: ${graph.numVertices}, routes with at least 20 trips: ${graph.numEdges}")

// 2. Triplets: an edge seen together with both endpoint attributes.
graph.triplets.sortBy(-_.attr).take(3).foreach(t => println(s"${t.srcAttr} -> ${t.dstAttr}: ${t.attr} trips"))

// 3. Degrees: the station with the most busy routes in and out.
val (hub, degree) = graph.degrees.reduce((a, b) => if (a._2 >= b._2) a else b)
println(s"${graph.vertices.lookup(hub).head}: $degree busy routes")

// 4. aggregateMessages: every edge sends its trips to its destination; the sums are arrivals per station.
val arrivals = graph.aggregateMessages[Long](ctx => ctx.sendToDst(ctx.attr), _ + _)
arrivals.join(graph.vertices).sortBy(-_._2._1).take(3).foreach { case (_, (n, name)) => println(s"$name: $n arrivals") }

// 5. Built-in PageRank (unweighted): where a random rider ends up most often.
val ranks = graph.pageRank(0.001).vertices
ranks.join(graph.vertices).sortBy(-_._2._1).take(5).foreach { case (_, (r, name)) => println(f"$r%6.2f  $name") }

// 6. Connected components and triangles.
val components = graph.connectedComponents().vertices.map(_._2).countByValue()
println(s"components: ${components.size}, largest: ${components.values.max} stations")
println(s"triangles: ${graph.triangleCount().vertices.map(_._2.toLong).reduce(_ + _) / 3}") // each is seen from 3 corners

// 7. The full GraphX suite's output: one row per station (weighted PageRank, AM/PM shift, community, ...).
spark.read.parquet("data/lake/graph/station_metrics").orderBy("pagerank_rank")
  .select("name", "pagerank_w", "pr_shift", "community_id", "clustering_coef").show(5, truncate = false)
