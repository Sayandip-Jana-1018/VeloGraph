package velograph.ml

import scala.collection.JavaConverters._

import com.typesafe.config.Config
import org.apache.spark.ml.{Estimator, PipelineStage}
import org.apache.spark.ml.classification.{
  DecisionTreeClassifier,
  LinearSVC,
  LogisticRegression,
  NaiveBayes,
  OneVsRest,
  RandomForestClassifier
}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.param._
import org.apache.spark.ml.regression.{
  DecisionTreeRegressor,
  GBTRegressor,
  LinearRegression,
  RandomForestRegressor
}

/**
 * The feature preparation and the estimators of the PySpark lab (python/src/velograph/lab), rebuilt in Scala
 * stage for stage, so TrainChampionJob trains exactly what the lab measured:
 *   - categorical columns go through StringIndexer (unseen values kept);
 *   - trees read the indices, linear models and naive Bayes one-hot vectors;
 *   - VectorAssembler takes the numeric and flag columns first, then the categorical ones, each in dictionary
 *     order.
 */
object FeaturePipeline {

  /** Algorithms that read one-hot categories instead of indices. */
  val Linear: Set[String] = Set("linear-regression", "logistic-regression", "naive-bayes", "linear-svc-ovr")

  def stages(
      dictionary: Seq[FeatureSpec],
      linear: Boolean,
      dropGroups: Set[String] = Set.empty
  ): Seq[PipelineStage] = {
    val kept        = dictionary.filterNot(f => dropGroups(f.group))
    val plain       = kept.filter(f => f.kind == "numeric" || f.kind == "flag").map(_.name)
    val categorical = kept.filter(_.kind == "categorical").map(_.name)
    val indexed     = categorical.map(c => s"${c}_idx")
    val encoded     = categorical.map(c => s"${c}_ohe")
    val indexer     = new StringIndexer().setInputCols(
      categorical.toArray
    ).setOutputCols(indexed.toArray).setHandleInvalid("keep")
    val encoder = new OneHotEncoder().setInputCols(
      indexed.toArray
    ).setOutputCols(encoded.toArray).setHandleInvalid("keep")
    val assembler = new VectorAssembler()
      .setInputCols((plain ++ (if (linear) encoded else indexed)).toArray)
      .setOutputCol("features")
    if (categorical.isEmpty) Seq(new VectorAssembler().setInputCols(plain.toArray).setOutputCol("features"))
    else if (linear) Seq(indexer, encoder, assembler)
    else Seq(indexer, assembler)
  }

  /** The estimator for a lab algorithm name, with the lab's parameter names (typed from each Param). */
  def estimator(algorithm: String, label: String, params: Config, weightCol: Option[String]): Estimator[_] = {
    def configured[E <: Params](e: E): E = {
      val extra = ParamMap()
      params.entrySet.asScala.foreach { entry =>
        val key        = entry.getKey
        val param      = e.getParam(key)
        val value: Any = (param: Param[_]) match { // getParam erases the type; the Param subclass carries it

          case _: IntParam     => params.getInt(key)
          case _: LongParam    => params.getLong(key)
          case _: DoubleParam  => params.getDouble(key)
          case _: BooleanParam => params.getBoolean(key)
          case _               => params.getString(key)
        }
        extra.put(param.asInstanceOf[Param[Any]], value)
      }
      e.copy(extra).asInstanceOf[E]
    }
    def weighted[E <: Params](e: E): E = weightCol.fold(e)(w =>
      e.copy(ParamMap(e.getParam("weightCol").asInstanceOf[Param[Any]] -> w)).asInstanceOf[E]
    )
    algorithm match {
      case "linear-regression"                  => configured(new LinearRegression().setLabelCol(label))
      case "decision-tree" if label == "regime" =>
        weighted(configured(new DecisionTreeClassifier().setLabelCol(label)))
      case "decision-tree"                      => configured(new DecisionTreeRegressor().setLabelCol(label))
      case "random-forest" if label == "regime" =>
        weighted(configured(new RandomForestClassifier().setLabelCol(label)))
      case "random-forest"       => configured(new RandomForestRegressor().setLabelCol(label))
      case "gbt"                 => configured(new GBTRegressor().setLabelCol(label))
      case "logistic-regression" => weighted(configured(new LogisticRegression().setLabelCol(label)))
      case "naive-bayes"         => weighted(configured(new NaiveBayes().setLabelCol(label)))
      case "linear-svc-ovr"      =>
        weighted(new OneVsRest().setClassifier(configured(new LinearSVC())).setLabelCol(label))
      case other => throw new IllegalArgumentException(s"unknown algorithm $other")
    }
  }
}
