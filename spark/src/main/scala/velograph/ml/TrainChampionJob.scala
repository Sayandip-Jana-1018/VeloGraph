package velograph.ml

import java.nio.file.Path

import scala.collection.JavaConverters._
import scala.sys.process.Process
import scala.util.Try

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.Config
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import velograph.common.{JobContext, ServingJson, SourceRange, SparkJob}
import velograph.common.LakePaths.PathOps
import velograph.common.Timing.timed
import velograph.etl.{LakeTable, LakeTables}

/** A model the lab chose, as declared in `velograph.ml.champions`. */
final case class Champion(name: String, algorithm: String, label: String, params: Config, weighted: Boolean)

final case class ModelCard(
    name: String,
    target: String,
    algorithm: String,
    params: Map[String, String],
    features: Seq[String],
    fit: SourceRange,
    test: SourceRange,
    trainingRows: Long,
    trainingMillis: Long,
    metric: String,
    testMetric: Double,
    labMetric: Double,
    deltaPct: Double,
    regression: Option[RegressionMetrics],
    classification: Option[ClassificationMetrics],
    tau: Option[Double],
    modelPath: String,
    sparkVersion: String,
    gitSha: String
)

/**
 * Roadmap P4.7: retrains the champions the PySpark lab chose (ARCHITECTURE §8.5). The algorithm and
 * parameters come from config; the feature preparation is FeaturePipeline, the same stages as the lab's. Each
 * model is fit on the fit split (August), scored on the test split (September), and must reproduce the lab's
 * September metric within `champion-tolerance`, or the job fails: two implementations agreeing is the
 * evidence that the models the streaming job will load are the ones the lab measured. Saves
 * `data/models/<name>` (PipelineModel) and `serving/model-cards.json`.
 */
object TrainChampionJob extends SparkJob {

  val jobName = "train-champion"

  /**
   * Forests and boosted trees sample rows per input partition, and file splits follow the core count. The
   * lab's sessions run local[16], so the scan is cut into the same splits here, and the same seeds draw the
   * same samples.
   */
  override val sparkConf: Map[String, String] = Map("spark.sql.files.minPartitionNum" -> "16")

  private val mapper = new ObjectMapper()

  def champions(raw: Config): Seq[Champion] = {
    val c = raw.getConfig("velograph.ml.champions")
    c.root.keySet.asScala.toSeq.sorted.map { name =>
      val m = c.getConfig(name)
      Champion(
        name,
        m.getString("algorithm"),
        m.getString("label"),
        m.getConfig("params"),
        Try(m.getBoolean("weighted")).getOrElse(false)
      )
    }
  }

  /** The lab's September metric for an algorithm: MAE for a count target, macro-F1 for the regime. */
  private def labMetric(serving: Path, champion: Champion): Double =
    if (champion.label == "regime") {
      val board =
        mapper.readTree(serving.resolve("ml-classification.json").toFile).path("data").path("leaderboard")
      entry(board, champion.algorithm).path("test").path("macroF1").asDouble()
    } else {
      val board = mapper
        .readTree(serving.resolve("ml-regression.json").toFile)
        .path("data")
        .path("targets")
        .path(champion.label)
        .path("leaderboard")
      entry(board, champion.algorithm).path("test").path("mae").asDouble()
    }

  private def entry(board: JsonNode, algorithm: String): JsonNode =
    board.elements.asScala
      .find(_.path("model").asText() == algorithm)
      .getOrElse(throw new IllegalStateException(
        s"the lab's leaderboard has no $algorithm: re-run the lab first"
      ))

  private def gitSha(): String =
    Try(Process(Seq("git", "rev-parse", "--short", "HEAD")).!!.trim).getOrElse("unknown")

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark     = ctx.spark
    val lake      = new LakeTables(spark, ctx.paths)
    val ml        = MlConfig(ctx.cfg.raw)
    val tolerance = ctx.cfg.raw.getDouble("velograph.ml.champion-tolerance")
    val features  = lake.read(LakeTable.MlFeatures)
    val fit       = features.where(col("split") === ml.fit.split).persist(StorageLevel.MEMORY_AND_DISK)
    val test      = features.where(col("split") === ml.test.split).persist(StorageLevel.MEMORY_AND_DISK)
    val tau       = mapper.readTree(
      ctx.paths.serving.resolve("ml-features.json").toFile
    ).path("data").path("tau").asDouble()

    val cards = champions(ctx.cfg.raw).map { c =>
      val linear = FeaturePipeline.Linear(c.algorithm)
      val weight = if (c.weighted) Some("weight") else None
      val train  = if (c.weighted) ModelMetrics.withClassWeights(fit, c.label) else fit
      val stages = FeaturePipeline.stages(FeatureBuildJob.Dictionary, linear) :+
        FeaturePipeline.estimator(c.algorithm, c.label, c.params, weight)
      val pipeline        = new Pipeline().setStages(stages.toArray[PipelineStage])
      val (model, millis) = timed(pipeline.fit(train))
      val scored          = model.transform(test).persist(StorageLevel.MEMORY_AND_DISK)

      val (metricName, value, regression, classification) =
        if (c.label == "regime") {
          val m =
            ModelMetrics.classification(ModelMetrics.confusion(scored, "regime", Seq(0, 1, 2)), Seq(0, 1, 2))
          ("macroF1", m.macroF1, None, Some(m))
        } else {
          val m = ModelMetrics.regression(scored, c.label)
          ("mae", m.mae, Some(m), None)
        }
      scored.unpersist()
      val lab   = labMetric(ctx.paths.serving, c)
      val delta = (value - lab) / lab * 100
      log.info(
        f"${c.name}: ${c.algorithm} $metricName $value%.5f against the lab's $lab%.5f ($delta%+.2f %%) in $millis ms"
      )
      require(
        math.abs(delta) <= tolerance * 100,
        f"${c.name}: $metricName $value%.5f differs from the lab's $lab%.5f by $delta%+.2f %% (allowed ±${tolerance *
            100}%.1f %%)"
      )

      val target = ctx.paths.models.resolve(c.name)
      model.write.overwrite().save(target.uri)
      ModelCard(
        name = c.name,
        target = c.label,
        algorithm = c.algorithm,
        params = c.params.entrySet.asScala.map(e => e.getKey -> e.getValue.unwrapped.toString).toMap,
        features = FeatureBuildJob.Dictionary.map(_.name),
        fit = SourceRange(ml.fit.rows.from.toString, ml.fit.rows.to.toString),
        test = SourceRange(ml.test.rows.from.toString, ml.test.rows.to.toString),
        trainingRows = train.count(),
        trainingMillis = millis,
        metric = metricName,
        testMetric = value,
        labMetric = lab,
        deltaPct = math.rint(delta * 1000) / 1000,
        regression = regression,
        classification = classification,
        tau = if (c.label == "regime") Some(tau) else None,
        modelPath = target.toString,
        sparkVersion = spark.version,
        gitSha = gitSha()
      )
    }

    ServingJson.write(
      ctx.paths.serving.resolve("model-cards.json"),
      "model-cards",
      jobName,
      cards,
      rows = Some(cards.size.toLong),
      source = Some(SourceRange(ml.fit.profile.from.toString, ml.test.rows.to.toString))
    )
    cards.map(c =>
      c.name ->
        f"${c.algorithm}: ${c.metric} ${c.testMetric}%.4f (lab ${c.labMetric}%.4f, ${c.deltaPct}%+.2f %%)"
    ).toMap
  }
}
