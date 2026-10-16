package velograph.ml

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

final case class RegressionMetrics(mae: Double, rmse: Double, r2: Double, rows: Long)
final case class ClassMetrics(label: Int, precision: Double, recall: Double, f1: Double, support: Long)
final case class ClassificationMetrics(
    accuracy: Double,
    macroF1: Double,
    perClass: Seq[ClassMetrics],
    confusion: Seq[Seq[Long]],
    rows: Long
)

/**
 * The lab's metrics (python/src/velograph/lab/metrics.py), with the same definitions, so the two can be
 * compared.
 */
object ModelMetrics {

  /** MAE, RMSE and R² = 1 - MSE / Var(label), in one pass. */
  def regression(scored: DataFrame, label: String, prediction: String = "prediction"): RegressionMetrics = {
    val err = col(prediction) - col(label)
    val r   = scored
      .agg(avg(abs(err)), avg(err * err), var_pop(col(label).cast("double")), count(lit(1)))
      .head() // bounded: 1 row
    val (mse, variance) = (r.getDouble(1), r.getDouble(2))
    RegressionMetrics(
      r.getDouble(0),
      math.sqrt(mse),
      if (variance > 0) 1.0 - mse / variance else 0.0,
      r.getLong(3)
    )
  }

  /** confusion(actual)(predicted), in the order of `classes`. */
  def confusion(
      scored: DataFrame,
      label: String,
      classes: Seq[Int],
      prediction: String = "prediction"
  ): Seq[Seq[Long]] = {
    val counts = scored
      .groupBy(col(label).cast("int"), col(prediction).cast("int"))
      .count()
      .collect() // bounded: classes x classes
      .map(r => (r.getInt(0), r.getInt(1)) -> r.getLong(2))
      .toMap
    classes.map(a => classes.map(p => counts.getOrElse((a, p), 0L)))
  }

  def classification(matrix: Seq[Seq[Long]], classes: Seq[Int]): ClassificationMetrics = {
    val total    = matrix.map(_.sum).sum
    val perClass = classes.indices.map { i =>
      val tp        = matrix(i)(i).toDouble
      val predicted = matrix.map(_(i)).sum
      val actual    = matrix(i).sum
      val precision = if (predicted > 0) tp / predicted else 0.0
      val recall    = if (actual > 0) tp / actual else 0.0
      val f1        = if (precision + recall > 0) 2 * precision * recall / (precision + recall) else 0.0
      ClassMetrics(classes(i), precision, recall, f1, actual)
    }
    ClassificationMetrics(
      accuracy = classes.indices.map(i => matrix(i)(i)).sum.toDouble / total,
      macroF1 = perClass.map(_.f1).sum / perClass.size,
      perClass = perClass,
      confusion = matrix,
      rows = total
    )
  }

  /** weight = n / (k * n_class): every class carries the same total weight in training (as in the lab). */
  def withClassWeights(df: DataFrame, label: String): DataFrame = {
    val counts =
      df.groupBy(label).count().collect().map(r => r.getInt(0) -> r.getLong(1)).toMap // bounded: k rows
    val n     = counts.values.sum.toDouble
    val pairs = counts.toSeq.flatMap { case (c, nc) => Seq(lit(c), lit(n / (counts.size * nc))) }
    df.withColumn("weight", map(pairs: _*)(col(label)))
  }
}
