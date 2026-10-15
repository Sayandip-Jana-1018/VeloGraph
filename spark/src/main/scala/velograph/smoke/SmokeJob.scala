package velograph.smoke

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, count, countDistinct, xxhash64}

import velograph.common.{JobContext, SparkJob}
import velograph.common.LakePaths.PathOps
import velograph.common.Timing.timed

/**
 * Roadmap P0.3 / P0.6: proves that Spark SQL with Parquet IO, GraphX (with Kryo) and MLlib all work where the
 * job runs - JDK 17 plus winutils on the Windows host, the shared data mount on the Docker cluster.
 */
object SmokeJob extends SparkJob {

  val jobName = "smoke"

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark = ctx.spark
    import spark.implicits._

    // 1. Spark SQL + Parquet round trip. xxhash64 is the planned GraphX vertex id, so prove it does not collide.
    val rows         = 1000000L
    val target       = ctx.paths.lake.resolve("_smoke").uri
    val (_, writeMs) = timed {
      spark
        .range(rows)
        .select(
          col("id"),
          (col("id") % 24).cast("int").as("hour"),
          xxhash64(col("id").cast("string")).as("vertex_id")
        )
        .write
        .mode(SaveMode.Overwrite)
        .parquet(target)
    }
    val ((total, distinctIds), readMs) = timed {
      spark.read.parquet(target).agg(count("*"), countDistinct("vertex_id")).as[(Long, Long)].head()
    }
    require(total == rows, s"Parquet round trip lost rows: $total of $rows")
    require(distinctIds == rows, s"xxhash64 collided: $distinctIds distinct ids for $rows rows")

    // 2. GraphX: every other vertex links to vertex 1, so PageRank must rank it first.
    // Two partitions: PageRank runs one Spark job per iteration, so a toy graph must not pay for 20 partitions.
    val edges = spark.sparkContext.parallelize(
      Seq(Edge(2L, 1L, 1), Edge(3L, 1L, 1), Edge(4L, 1L, 1), Edge(1L, 2L, 1)),
      numSlices = 2
    )
    val (leaderId, leaderRank) = Graph
      .fromEdges(edges, defaultValue = 0)
      .pageRank(tol = 1e-6)
      .vertices
      .collect() // bounded: 4 vertices
      .maxBy(_._2)
    require(leaderId == 1L, s"unexpected PageRank leader: $leaderId")

    // 3. MLlib: recover y = 3x + 2 exactly (the normal-equation solver is exact on noise-free data).
    val points   = (1 to 200).map(i => (i.toDouble, 3.0 * i + 2.0)).toDF("x", "y")
    val features = new VectorAssembler().setInputCols(Array("x")).setOutputCol("features").transform(points)
    val model    = new LinearRegression().setLabelCol("y").fit(features)
    require(math.abs(model.coefficients(0) - 3.0) < 1e-6, s"unexpected slope ${model.coefficients(0)}")

    Map(
      "parquetRows"         -> total,
      "parquetWriteMs"      -> writeMs,
      "parquetReadMs"       -> readMs,
      "pagerankLeader"      -> leaderId,
      "pagerankLeaderRank"  -> leaderRank,
      "regressionSlope"     -> model.coefficients(0),
      "regressionIntercept" -> model.intercept,
      "sparkVersion"        -> spark.version,
      "javaVersion"         -> sys.props("java.version"),
      "sessionTimeZone"     -> spark.conf.get("spark.sql.session.timeZone"),
      "defaultParallelism"  -> spark.sparkContext.defaultParallelism
    )
  }
}
