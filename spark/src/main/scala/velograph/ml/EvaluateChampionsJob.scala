package velograph.ml

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import velograph.common.{JobContext, ServingJson, SourceRange, SparkJob}
import velograph.common.LakePaths.PathOps
import velograph.etl.{LakeTable, LakeTables}

/** A saved demand model on the test month, next to the seasonal naive forecast on the same rows. */
final case class DemandEvaluation(
    name: String,
    algorithm: String,
    model: RegressionMetrics,
    naive: RegressionMetrics
) {
  def betterByPct: Double = (naive.mae - model.mae) / naive.mae * 100
}

/**
 * The saved regime classifier on the test month, next to always predicting the fit month's majority class.
 */
final case class RegimeEvaluation(
    name: String,
    algorithm: String,
    model: ClassificationMetrics,
    majority: ClassificationMetrics
)

/**
 * The champion models as the live job uses them: loads every PipelineModel that TrainChampionJob saved under
 * `data/models/`, scores the test split (September, never used for tuning) and prints the results next to
 * their baselines, so anyone can check the models with one command. Writes `serving/model-evaluation.json`.
 */
object EvaluateChampionsJob extends SparkJob {

  val jobName = "evaluate-champions"

  /** The regime labels of FeatureTransforms, in label order. */
  val Regimes: Seq[(Int, String)] =
    Seq(
      FeatureTransforms.Draining -> "draining",
      FeatureTransforms.Stable   -> "stable",
      FeatureTransforms.Filling  -> "filling"
    )

  /**
   * The lab's seasonal naive forecast: the station's mean for that weekday and hour over the profile window.
   */
  val Naive: Map[String, String] = Map("departures" -> "prof_dep_mean", "arrivals" -> "prof_arr_mean")

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val lake     = new LakeTables(ctx.spark, ctx.paths)
    val ml       = MlConfig(ctx.cfg.raw)
    val features = lake.read(LakeTable.MlFeatures)
    val test     = features.where(col("split") === ml.test.split).persist(StorageLevel.MEMORY_AND_DISK)
    val loaded   = TrainChampionJob.champions(ctx.cfg.raw).map { c =>
      c -> PipelineModel.load(ctx.paths.models.resolve(c.name).uri)
    }

    val demand = loaded.collect {
      case (c, model) if c.label != "regime" =>
        val scored = model.transform(test)
        DemandEvaluation(
          c.name,
          c.algorithm,
          ModelMetrics.regression(scored, c.label),
          ModelMetrics.regression(test, c.label, prediction = Naive(c.label))
        )
    }
    val regime = loaded.collect {
      case (c, model) if c.label == "regime" =>
        val classes  = Regimes.map(_._1)
        val majority = features
          .where(col("split") === ml.fit.split)
          .groupBy("regime")
          .count()
          .orderBy(desc("count"))
          .head() // bounded: 1 row
          .getInt(0)
        RegimeEvaluation(
          c.name,
          c.algorithm,
          ModelMetrics.classification(
            ModelMetrics.confusion(model.transform(test), "regime", classes),
            classes
          ),
          ModelMetrics.classification(
            ModelMetrics.confusion(test.withColumn("prediction", lit(majority)), "regime", classes),
            classes
          )
        )
    }
    val rows = test.count()
    test.unpersist()

    println(report(ml.test.rows.from.toString, ml.test.rows.to.toString, rows, demand, regime))
    ServingJson.write(
      ctx.paths.serving.resolve("model-evaluation.json"),
      "model-evaluation",
      jobName,
      Map("testRows" -> rows, "demand" -> demand, "regime" -> regime),
      rows = Some(rows),
      source = Some(SourceRange(ml.test.rows.from.toString, ml.test.rows.to.toString))
    )
    (demand.map(d => d.name -> f"${d.algorithm}: MAE ${d.model.mae}%.4f, naive ${d.naive.mae}%.4f") ++
      regime.map(r =>
        r.name -> f"${r.algorithm}: macro-F1 ${r.model.macroF1}%.4f, accuracy ${r.model.accuracy}%.4f"
      )).toMap
  }

  /** The console report: plain text, so it reads the same in any terminal and in the report's screenshot. */
  def report(
      from: String,
      to: String,
      rows: Long,
      demand: Seq[DemandEvaluation],
      regime: Seq[RegimeEvaluation]
  ): String = {
    val out                        = new StringBuilder
    def line(s: String = ""): Unit = out ++= s ++= "\n"
    line()
    line(f"VeloGraph champion models on the test month, $from to $to: $rows%,d station-hours")
    line()
    // ASCII only: a Windows console would garble "²".
    line(
      f"${"Next-hour target"}%-17s${"Model"}%-16s${"MAE"}%8s${"RMSE"}%8s${"R2"}%8s   ${"Seasonal naive MAE"}%-19s${"Better by"}%9s"
    )
    demand.foreach { d =>
      line(
        f"${d.name}%-17s${d.algorithm}%-16s${d.model.mae}%8.3f${d.model.rmse}%8.3f${d.model.r2}%8.3f   ${d.naive.mae}%-19.3f${d.betterByPct}%8.1f %%"
      )
    }
    regime.foreach { r =>
      line()
      line(f"Regime over the next hour (draining, stable, filling): ${r.algorithm}")
      line(
        f"  accuracy ${r.model.accuracy}%.3f   macro-F1 ${r.model.macroF1}%.3f      " +
          f"always the majority class: accuracy ${r.majority.accuracy}%.3f, macro-F1 ${r.majority.macroF1}%.3f"
      )
      line()
      line(f"  ${"Class"}%-10s${"Precision"}%11s${"Recall"}%9s${"F1"}%8s${"Support"}%11s")
      r.model.perClass.zip(Regimes).foreach { case (k, (_, name)) =>
        line(f"  $name%-10s${k.precision}%11.3f${k.recall}%9.3f${k.f1}%8.3f${k.support}%,11d")
      }
      line()
      line(f"  ${"Actual \\ predicted"}%-20s" + Regimes.map { case (_, name) => f"$name%11s" }.mkString)
      r.model.confusion.zip(Regimes).foreach { case (counts, (_, name)) =>
        line(f"  $name%-20s" + counts.map(n => f"$n%,11d").mkString)
      }
    }
    out.result()
  }
}
