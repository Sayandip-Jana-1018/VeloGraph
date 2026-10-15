package velograph.graph

import scala.annotation.tailrec
import scala.collection.immutable.BitSet
import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Rebalancing zones (roadmap P3.5): weighted label propagation written with Pregel, refined by greedy
 * modularity merging and scored with Newman's modularity.
 *
 * Every station starts with its own label. In each superstep it collects its neighbours' labels, each
 * weighted by the trips on the connecting edge (sendMsg, mergeMsg), and adopts the heaviest one (vprog). Ties
 * keep the current label, otherwise the smallest label wins, so every run gives the same answer. Fully
 * synchronous propagation can make two neighbours swap labels back and forth forever, so stations take turns:
 * a station may change its label only in supersteps whose parity matches its vertex id's, which damps that
 * oscillation.
 */
object Communities {

  /** Current label, the supersteps in which it changed, and the supersteps this vertex has seen. */
  final case class Label(id: VertexId, changes: BitSet, superstep: Int)

  /** Total edge weight behind each neighbouring label. */
  type Votes = Map[VertexId, Double]

  def propagate[VD: ClassTag](graph: Graph[VD, Double], supersteps: Int): Graph[Label, Double] = {
    val start = graph.mapVertices((id, _) => Label(id, BitSet.empty, 0))

    val vprog = (id: VertexId, label: Label, votes: Votes) =>
      if (votes.isEmpty) label // superstep 0: everyone keeps its own label
      else {
        val step = label.superstep + 1
        val next = if (mayUpdate(id, step)) winner(label.id, votes) else label.id
        Label(next, if (next != label.id) label.changes + step else label.changes, step)
      }
    val sendMsg = (t: EdgeTriplet[Label, Double]) =>
      Iterator(t.srcId -> Map(t.dstAttr.id -> t.attr), t.dstId -> Map(t.srcAttr.id -> t.attr))
    val mergeMsg = (a: Votes, b: Votes) =>
      b.foldLeft(a) { case (votes, (label, weight)) =>
        votes.updated(label, votes.getOrElse(label, 0.0) + weight)
      }

    // Every vertex with an edge hears from its neighbours in every superstep, so this runs exactly `supersteps`.
    Pregel(start, Map.empty[VertexId, Double], supersteps)(vprog, sendMsg, mergeMsg)
  }

  /**
   * Stations take turns: a vertex may change its label only when its id and the superstep have the same
   * parity.
   */
  def mayUpdate(id: VertexId, superstep: Int): Boolean = ((id ^ superstep.toLong) & 1L) == 0L

  /**
   * The heaviest label; on a tie the current label stays if it is among the heaviest, else the smallest wins.
   */
  def winner(current: VertexId, votes: Votes): VertexId = {
    val best = votes.values.max // weights are whole trip counts, so equal sums compare exactly
    if (votes.get(current).contains(best)) current
    else votes.collect { case (label, weight) if weight == best => label }.min
  }

  /**
   * Newman's modularity of a labelling of a weighted undirected graph,
   *
   * Q = Σ_c [ L_c / m − (D_c / 2m)² ],
   *
   * with m the total edge weight, L_c the weight inside community c and D_c the summed weighted degree of its
   * vertices. Q is 0 for a random split and grows towards 1 as ties concentrate inside communities.
   *
   * @param edges
   *   columns `a`, `b`, `weight` (one row per undirected edge)
   * @param labels
   *   columns `vid`, `label`
   */
  def modularity(edges: DataFrame, labels: DataFrame): Double = {
    val weight = col("weight").cast("double")
    val m      = edges.agg(coalesce(sum(weight), lit(0.0))).head().getDouble(0) // bounded: 1 row
    if (m == 0.0) return 0.0 // no edges: every labelling is as good as random
    val inside = edges
      .join(labels.select(col("vid").as("a"), col("label").as("label_a")), "a")
      .join(labels.select(col("vid").as("b"), col("label").as("label_b")), "b")
      .where(col("label_a") === col("label_b"))
      .agg(coalesce(sum(weight), lit(0.0)))
      .head()
      .getDouble(0)
    val degrees = edges
      .select(col("a").as("vid"), weight.as("w"))
      .union(edges.select(col("b").as("vid"), weight.as("w")))
      .groupBy("vid")
      .agg(sum("w").as("k"))
    val degreeSquares = degrees
      .join(labels, "vid")
      .groupBy("label")
      .agg(sum("k").as("d"))
      .agg(sum(col("d") * col("d")))
      .head()
      .getDouble(0)
    inside / m - degreeSquares / (4 * m * m)
  }

  /**
   * Greedy modularity merging (Clauset, Newman and Moore) on top of label propagation. Each community becomes
   * one node, and the two linked communities whose union raises Q the most are merged, again and again, until
   * no merge raises it. Merging a and b changes Q by ΔQ = w_ab / m − D_a·D_b / (2m²), with w_ab the weight
   * between them and D the summed weighted degree, so Q only goes up. Label propagation finds tight
   * neighbourhoods but stops at a local optimum; this step joins the neighbourhoods that are better off
   * together.
   *
   * @param edges
   *   columns `a`, `b`, `weight` (one row per undirected edge)
   * @param labels
   *   columns `vid`, `label`
   * @return
   *   the merged label of every label that was merged into another
   */
  def mergeByModularity(edges: DataFrame, labels: DataFrame): Map[VertexId, VertexId] = {
    val weight   = col("weight").cast("double")
    val labelled = edges
      .join(labels.select(col("vid").as("a"), col("label").as("label_a")), "a")
      .join(labels.select(col("vid").as("b"), col("label").as("label_b")), "b")
    val between = labelled
      .where(col("label_a") =!= col("label_b"))
      .groupBy(least(col("label_a"), col("label_b")), greatest(col("label_a"), col("label_b")))
      .agg(sum(weight))
      .collect() // bounded: pairs of linked communities, a few thousand
      .map(r => (r.getLong(0), r.getLong(1)) -> r.getDouble(2))
      .toMap
    val degree = labelled
      .select(col("label_a").as("label"), weight.as("w"))
      .union(labelled.select(col("label_b").as("label"), weight.as("w")))
      .groupBy("label")
      .agg(sum("w"))
      .collect() // bounded: one row per label
      .map(r => r.getLong(0) -> r.getDouble(1))
      .toMap
    val m = edges.agg(coalesce(sum(weight), lit(0.0))).head().getDouble(0) // bounded: 1 row
    if (m == 0.0) Map.empty else greedyMerge(between, degree, m)
  }

  /**
   * The merging loop itself, on plain collections: `between` holds the weight between each pair of linked
   * labels (smaller label first), `degree` the summed weighted degree of each label, `m` the total edge
   * weight.
   */
  def greedyMerge(
      between: Map[(VertexId, VertexId), Double],
      degree: Map[VertexId, Double],
      m: Double
  ): Map[VertexId, VertexId] = {
    @tailrec
    def loop(
        links: Map[(VertexId, VertexId), Double],
        degree: Map[VertexId, Double],
        parent: Map[VertexId, VertexId]
    ): Map[VertexId, VertexId] =
      if (links.isEmpty) parent
      else {
        val gains = links.map { case (pair @ (x, y), w) =>
          pair -> (w / m - degree(x) * degree(y) / (2 * m * m))
        }
        val ((a, b), gain) = gains.maxBy { case ((x, y), g) => (g, -x, -y) } // ties: smallest labels first
        if (gain <= 0) parent
        else { // b joins a: its links now belong to a
          val relinked = links.toSeq
            .collect {
              case ((x, y), w) if (x, y) != ((a, b)) =>
                val (x2, y2) = (if (x == b) a else x, if (y == b) a else y)
                (math.min(x2, y2), math.max(x2, y2)) -> w
            }
            .groupBy(_._1)
            .map { case (pair, ws) => pair -> ws.map(_._2).sum }
          loop(relinked, degree.updated(a, degree(a) + degree(b)) - b, parent + (b -> a))
        }
      }

    val parent                          = loop(between, degree, Map.empty)
    def root(label: VertexId): VertexId = parent.get(label).map(root).getOrElse(label)
    parent.keys.map(label => label -> root(label)).toMap
  }
}
