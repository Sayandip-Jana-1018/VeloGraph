package velograph.ml

import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.scalatest.funsuite.AnyFunSuite

import velograph.testkit.SharedSparkSession

class ModelPipelineSpec extends AnyFunSuite with SharedSparkSession {

  test("regression metrics: MAE, RMSE and R² = 1 - MSE / Var(label)") {
    import spark.implicits._
    val scored = Seq((1L, 1.0), (2L, 3.0), (3L, 5.0)).toDF("departures", "prediction")
    val m      = ModelMetrics.regression(scored, "departures")
    assert(m.mae == 1.0 && math.abs(m.rmse - math.sqrt(5.0 / 3)) < 1e-12)
    assert(math.abs(m.r2 - (1 - (5.0 / 3) / (2.0 / 3))) < 1e-12 && m.rows == 3L)
  }

  test("a confusion matrix gives accuracy, per-class precision and recall, and macro-F1") {
    import spark.implicits._
    // actual 0 twice (one right), actual 1 three times (all right), actual 2 once (taken for 1)
    val scored = Seq((0, 0.0), (0, 1.0), (1, 1.0), (1, 1.0), (1, 1.0), (2, 1.0)).toDF("regime", "prediction")
    val matrix = ModelMetrics.confusion(scored, "regime", Seq(0, 1, 2))
    assert(matrix == Seq(Seq(1L, 1L, 0L), Seq(0L, 3L, 0L), Seq(0L, 1L, 0L)))
    val m = ModelMetrics.classification(matrix, Seq(0, 1, 2))
    assert(m.accuracy == 4.0 / 6)
    val f1Stable = 2 * (3.0 / 5) * 1.0 / (3.0 / 5 + 1.0)
    assert(math.abs(m.macroF1 - (2 * 1.0 * 0.5 / 1.5 + f1Stable + 0.0) / 3) < 1e-12)
  }

  test("class weights give every class the same total weight") {
    import spark.implicits._
    val weighted = ModelMetrics.withClassWeights(Seq(0, 1, 1, 1).toDF("regime"), "regime")
    val totals   = weighted.groupBy("regime").sum("weight").as[(Int, Double)].collect().toMap
    assert(math.abs(totals(0) - 2.0) < 1e-12 && math.abs(totals(1) - 2.0) < 1e-12)
  }

  test(
    "the feature stages follow the lab: numeric and flags first, then indices (trees) or one-hot (linear)"
  ) {
    val dictionary = FeatureBuildJob.Dictionary
    val plain      = dictionary.filter(f => f.kind != "categorical").map(_.name)
    val categories = dictionary.filter(_.kind == "categorical").map(_.name)

    val trees = FeaturePipeline.stages(dictionary, linear = false)
    assert(trees.map(_.getClass) == Seq(classOf[StringIndexer], classOf[VectorAssembler]))
    assert(trees.last.asInstanceOf[VectorAssembler].getInputCols.toSeq == plain ++ categories.map(_ + "_idx"))

    val linear = FeaturePipeline.stages(dictionary, linear = true)
    assert(linear.map(_.getClass) ==
      Seq(classOf[StringIndexer], classOf[OneHotEncoder], classOf[VectorAssembler]))
    assert(linear.last.asInstanceOf[VectorAssembler].getInputCols.toSeq ==
      plain ++ categories.map(_ + "_ohe"))

    val withoutGraph = FeaturePipeline.stages(dictionary, linear = false, dropGroups = Set("graph"))
    assert(!withoutGraph.last.asInstanceOf[VectorAssembler].getInputCols.exists(_.startsWith("pagerank")))
  }

  test("estimators take the lab's parameter names from config, typed by each Param") {
    val params = ConfigFactory.parseString(
      "maxIter = 60, maxDepth = 7, stepSize = 0.1, subsamplingRate = 0.8, seed = 315"
    )
    val gbt =
      FeaturePipeline.estimator("gbt", "departures", params, weightCol = None).asInstanceOf[GBTRegressor]
    assert(gbt.getMaxIter == 60 && gbt.getMaxDepth == 7 && gbt.getStepSize == 0.1 && gbt.getSeed == 315L)
    assert(gbt.getLabelCol == "departures")

    val forest = FeaturePipeline
      .estimator(
        "random-forest",
        "regime",
        ConfigFactory.parseString("numTrees = 40"),
        weightCol = Some("weight")
      )
      .asInstanceOf[RandomForestClassifier]
    assert(forest.getNumTrees == 40 && forest.getWeightCol == "weight" && forest.getLabelCol == "regime")
  }
}
