package velograph.ml

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{PCA, StandardScaler}
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import velograph.common.{JobContext, ServingJson, SourceRange, SparkJob}
import velograph.etl.{LakeSchemas, LakeTable, LakeTables}
import velograph.gold.GoldConfig

final case class KChoice(k: Int, silhouette: Double, smallestShare: Double)
final case class ArchetypeCurves(
    workDep: Seq[Double],
    workArr: Seq[Double],
    offDep: Seq[Double],
    offArr: Seq[Double]
)
final case class ArchetypeSummary(
    id: Int,
    name: String,
    stations: Long,
    fitted: Long,
    offToWork: Double,
    amDepartures: Double,
    amArrivals: Double,
    pmDepartures: Double,
    pmArrivals: Double,
    curves: ArchetypeCurves,
    examples: Seq[String]
)
final case class ArchetypePoint(
    station: String,
    name: String,
    x: Double,
    y: Double,
    archetype: Int,
    trips: Long
)
final case class ArchetypeStability(stations: Long, changed: Long)
final case class ArchetypeReport(
    window: SourceRange,
    k: Int,
    silhouette: Double,
    selection: Seq[KChoice],
    minTrips: Long,
    explainedVariance: Seq[Double],
    archetypes: Seq[ArchetypeSummary],
    points: Seq[ArchetypePoint],
    stability: ArchetypeStability
)

/**
 * Roadmap P4.1: station archetypes. Every station's hourly share profile (ArchetypeTransforms.profiles) is
 * clustered with KMeans for k in [k-min, k-max]; the k with the best silhouette wins. Clusters, names and the
 * 2-D PCA view are learned on profile-a only, then applied unchanged to profile-b and to profile-l (the live
 * features). That keeps archetype ids consistent across the fit, test and live rows, and keeps the test month
 * out of everything the models learn from (ARCHITECTURE §8.2). Writes `gold/station_archetypes` and
 * `serving/archetypes.json`.
 */
object StationArchetypesJob extends SparkJob {

  val jobName = "station-archetypes"

  private def round(x: Double, digits: Int): Double = math.rint(x * math.pow(10, digits)) /
    math.pow(10, digits)

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark = ctx.spark
    import spark.implicits._
    val lake  = new LakeTables(spark, ctx.paths)
    val ml    = MlConfig(ctx.cfg.raw)
    val c     = ml.archetypes
    val peaks = GoldConfig.peaks(ctx.cfg.raw)
    val panel = lake.read(LakeTable.StationHour)

    val learnOn  = ml.fit.profile // profile-a
    val windows  = (ml.splits :+ ml.live).map(_.profile)
    val profiles =
      windows.map(w =>
        w -> ArchetypeTransforms.profiles(panel, w, c.binHours).persist(StorageLevel.MEMORY_AND_DISK)
      ).toMap
    // ~2.3k short vectors: a few partitions keep each KMeans iteration a small job rather than 96 tiny tasks.
    val train =
      profiles(learnOn).where(col("trips") >= c.minTrips).coalesce(4).persist(StorageLevel.MEMORY_AND_DISK)
    val fitted = train.count()

    // ---- For each k: the best of `restarts` KMeans runs by within-cluster cost (one seed can land in a poor
    // local optimum), scored by silhouette: how much closer a station is to its own cluster than to the next.
    val evaluator  = new ClusteringEvaluator().setFeaturesCol("shares").setPredictionCol("cluster")
    val candidates = (c.kMin to c.kMax).map { k =>
      val model = (0 until c.restarts)
        .map { r =>
          new KMeans()
            .setK(k)
            .setMaxIter(c.maxIter)
            .setSeed(ml.seed + r)
            .setFeaturesCol("shares")
            .setPredictionCol("cluster")
            .fit(train)
        }
        .minBy(_.summary.trainingCost)
      val score    = evaluator.evaluate(model.transform(train))
      val smallest = model.summary.clusterSizes.min.toDouble / fitted
      log.info(f"k = $k%2d: silhouette $score%.4f, smallest cluster ${smallest * 100}%.1f %%")
      (k, score, smallest, model)
    }
    // The best silhouette among the k whose clusters are all real groups, not a sliver of outliers.
    val eligible                  = candidates.filter(_._3 >= c.minClusterShare)
    val (k, silhouette, _, model) = (if (eligible.nonEmpty) eligible else candidates).maxBy(_._2)

    // ---- Archetype 0 is the most common cluster. Each is named by its members' mean hourly curves (the centre
    // in hourly resolution, which the binned centre is the sum of).
    val sizes = model.summary.clusterSizes
    require(sizes.forall(_ > 0), s"KMeans left an empty cluster at k = $k")
    val order  = sizes.indices.sortBy(i => (-sizes(i), i))
    val hourly = model
      .transform(train)
      .select(col("cluster"), posexplode(col("hourly")).as(Seq("slot", "share")))
      .groupBy("cluster", "slot")
      .agg(avg("share").as("share"))
      .as[(Int, Int, Double)]
      .collect() // bounded: k x 96 rows
      .groupBy(_._1)
      .map { case (cluster, rows) => cluster -> rows.sortBy(_._2).map(_._3) }
    val shapes = order.map(i => DayShape(hourly(i)))
    val names  = ArchetypeTransforms.distinct(shapes.map(ArchetypeTransforms.name(_, peaks, c)))
    val legend = order.zipWithIndex.map { case (cluster, id) => (cluster, id, names(id)) }
      .toDF("cluster", "archetype", "archetype_name")

    // ---- The 2-D view. Spark's PCA projects the vectors as given, so they are centred first.
    val centring = new StandardScaler()
      .setInputCol("shares")
      .setOutputCol("centred")
      .setWithMean(true)
      .setWithStd(false)
      .fit(train)
    val pca = new PCA().setInputCol("centred").setOutputCol("pc").setK(2).fit(centring.transform(train))

    val assigned = windows
      .map { w =>
        pca
          .transform(centring.transform(model.transform(profiles(w))))
          .join(broadcast(legend), "cluster")
          .withColumn("xy", vector_to_array(col("pc")))
          .select(
            col("station_id"),
            lit(w.name).as("window"),
            col("trips"),
            (lit(w == learnOn) && col("trips") >= c.minTrips).as("fitted"),
            col("archetype"),
            col("archetype_name"),
            col("xy")(0).as("pca_x"),
            col("xy")(1).as("pca_y")
          )
      }
      .reduce(_ unionByName _)
      .select(LakeSchemas.GoldStationArchetype.fieldNames.map(col): _*)
    assigned.coalesce(1).write.mode(SaveMode.Overwrite).parquet(lake.uri(LakeTable.Archetypes))

    // ---- Exports, from the written table (bounded: one row per station and window, ~7.5k).
    val written  = lake.read(LakeTable.Archetypes).persist(StorageLevel.MEMORY_AND_DISK)
    val inWindow = written.where(col("window") === learnOn.name)
    val counts   = inWindow
      .groupBy("archetype")
      .agg(count(lit(1)).as("stations"), count(when(col("fitted"), 1)).as("fitted"))
      .as[(Int, Long, Long)]
      .collect() // bounded: k rows
      .map { case (id, n, f) => (id, (n, f)) }
      .toMap
    val examples = inWindow
      .join(lake.read(LakeTable.Stations).select("station_id", "name"), "station_id")
      .withColumn(
        "rank",
        row_number().over(Window.partitionBy("archetype").orderBy(col("trips").desc, col("station_id")))
      )
      .where(col("rank") <= 5)
      .orderBy("archetype", "rank")
      .select("archetype", "name")
      .as[(Int, String)]
      .collect() // bounded: 5 per archetype
      .groupBy(_._1)
      .map { case (id, rows) => id -> rows.map(_._2).toSeq }
    def percent(curve: Vector[Double]) = curve.map(v => round(v * 100, 3))
    val summaries                      = shapes.zipWithIndex.map { case (s, id) =>
      val (n, f) = counts.getOrElse(id, (0L, 0L))
      ArchetypeSummary(
        id = id,
        name = names(id),
        stations = n,
        fitted = f,
        offToWork = round(s.off / s.work, 3),
        amDepartures = round(s.departuresIn(peaks.amStart, peaks.amEnd) * 100, 2),
        amArrivals = round(s.arrivalsIn(peaks.amStart, peaks.amEnd) * 100, 2),
        pmDepartures = round(s.departuresIn(peaks.pmStart, peaks.pmEnd) * 100, 2),
        pmArrivals = round(s.arrivalsIn(peaks.pmStart, peaks.pmEnd) * 100, 2),
        curves =
          ArchetypeCurves(percent(s.workDep), percent(s.workArr), percent(s.offDep), percent(s.offArr)),
        examples = examples.getOrElse(id, Seq.empty)
      )
    }
    val points = inWindow
      .join(lake.read(LakeTable.Stations).select("station_id", "name"), "station_id")
      .orderBy("station_id")
      .select("station_id", "name", "pca_x", "pca_y", "archetype", "trips")
      .as[(String, String, Double, Double, Int, Long)]
      .collect() // bounded: one row per station
      .map { case (id, n, x, y, a, t) => ArchetypePoint(id, n, round(x, 5), round(y, 5), a, t) }
      .toSeq
    val pairs = written
      .where(col("window") === learnOn.name)
      .select(col("station_id"), col("archetype").as("a"))
      .join(
        written.where(col("window") === ml.test.profile.name).select(
          col("station_id"),
          col("archetype").as("b")
        ),
        "station_id"
      )
      .agg(count(lit(1)), count(when(col("a") =!= col("b"), 1)))
      .head() // bounded: 1 row
    val stability = ArchetypeStability(pairs.getLong(0), pairs.getLong(1))

    val report = ArchetypeReport(
      window = SourceRange(learnOn.from.toString, learnOn.to.toString),
      k = k,
      silhouette = round(silhouette, 4),
      selection = candidates.map { case (kk, s, smallest, _) =>
        KChoice(kk, round(s, 4), round(smallest, 4))
      },
      minTrips = c.minTrips,
      explainedVariance = pca.explainedVariance.toArray.map(round(_, 4)).toSeq,
      archetypes = summaries,
      points = points,
      stability = stability
    )
    ServingJson.write(
      ctx.paths.serving.resolve("archetypes.json"),
      "archetypes",
      jobName,
      report,
      rows = Some(points.size.toLong),
      source = Some(SourceRange(learnOn.from.toString, learnOn.to.toString))
    )

    log.info(
      f"k = $k (silhouette $silhouette%.3f) over $fitted stations; " +
        summaries.map(s => s"${s.id} ${s.name} (${s.stations})").mkString(", ")
    )
    Map(
      "window"            -> learnOn.toString,
      "k"                 -> k,
      "silhouette"        -> round(silhouette, 4),
      "selection"         -> candidates.map { case (kk, s, _, _) => s"$kk:${round(s, 4)}" }.mkString(" "),
      "stationsFitted"    -> fitted,
      "stationsAssigned"  -> points.size,
      "archetypes"        -> summaries.map(s => s"${s.name} (${s.stations})").mkString(", "),
      "explainedVariance" -> report.explainedVariance.mkString(" + "),
      "changedAtoB"       -> s"${stability.changed} of ${stability.stations}"
    )
  }
}
