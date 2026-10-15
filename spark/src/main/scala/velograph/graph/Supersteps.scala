package velograph.graph

import scala.collection.immutable.BitSet

import org.apache.spark.rdd.RDD

/**
 * Superstep bookkeeping for our Pregel programs. Each vertex keeps a BitSet in its state holding the
 * supersteps in which it did something (was updated, or changed its label). Being part of the vertex state,
 * it is recomputed exactly like the rest of the state if Spark retries a task, unlike an accumulator.
 * Afterwards it tells how many vertices were active in each superstep, which the Network page charts.
 */
object Supersteps {

  /**
   * Vertices active in superstep s, for s = 0 .. `upTo` (default: the latest superstep any vertex recorded).
   */
  def counts(activity: RDD[BitSet], upTo: Option[Int] = None): Seq[Long] = {
    val perStep = activity.flatMap(identity).countByValue() // bounded: one key per superstep
    val until   = upTo.getOrElse(if (perStep.isEmpty) -1 else perStep.keys.max)
    (0 to until).map(s => perStep.getOrElse(s, 0L))
  }
}
