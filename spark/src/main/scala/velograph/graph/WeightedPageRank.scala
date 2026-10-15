package velograph.graph

import scala.collection.immutable.BitSet
import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

/**
 * Weighted PageRank written with GraphX's Pregel API (roadmap P3.2, P3.3, P3.7).
 *
 * A random rider at station u either teleports to a random station (probability α, the reset probability) or
 * takes one of u's routes, picked in proportion to its trips: p(u→v) = trips(u→v) / out-strength(u). A
 * station's rank is the share of time such a rider spends there:
 *
 * PR(v) = α + (1 − α) · Σ_{u→v} PR(u) · p(u→v), normalized at the end so that the ranks sum to 1.
 *
 * Pregel computes it in the delta form that GraphX's own dynamic PageRank uses. In superstep 0 every vertex
 * gets rank α. From then on a vertex forwards only the change of its rank along its out-edges (sendMsg), the
 * receiver adds up the incoming changes (mergeMsg) and adds (1 − α) of the sum to its rank (vprog). A vertex
 * whose change falls below `tol` stops sending, it "votes to halt", and Pregel ends when no messages are
 * left.
 *
 * Stations without outgoing routes (dangling vertices) pass nothing on; as in GraphX, the rank they would
 * have spread is restored by the final normalization.
 */
object WeightedPageRank {

  /**
   * Rank so far, its latest change, the superstep of that change, and every superstep that updated this
   * vertex.
   */
  final case class State(rank: Double, delta: Double, superstep: Int, updates: BitSet)

  /** Rank mass for the receiver, tagged with the superstep that delivers it. */
  final case class Message(mass: Double, superstep: Int)

  /** Normalized ranks plus how the run went: supersteps used and vertices updated in each superstep. */
  final case class Result(
      ranks: VertexRDD[Double],
      supersteps: Int,
      activeVertices: Seq[Long],
      converged: Boolean
  )

  /** Replaces each edge weight by the probability of taking that route from its source station. */
  def transitionProbabilities[VD: ClassTag](graph: Graph[VD, Double]): Graph[Double, Double] = {
    val outStrength =
      graph.aggregateMessages[Double](ctx => ctx.sendToSrc(ctx.attr), _ + _, TripletFields.EdgeOnly)
    graph
      .outerJoinVertices(outStrength)((_, _, strength) => strength.getOrElse(0.0))
      .mapTriplets(t => t.attr / t.srcAttr, TripletFields.Src)
  }

  /**
   * The Pregel program itself. It is a plain serializable value, so GraphX can ship it to the executors and
   * the step-through replay (`PregelExplainer`) can run exactly the same functions on the driver.
   */
  final case class Program(resetProb: Double, tol: Double) {

    /** Superstep 0 delivers α / (1 − α) to every vertex, so that every rank starts at exactly α. */
    val initial: Message = Message(resetProb / (1 - resetProb), 0)

    /** vprog: add (1 − α) of the incoming rank change to the rank, and remember the superstep. */
    def vprog(id: VertexId, state: State, msg: Message): State = {
      val rank = state.rank + (1 - resetProb) * msg.mass
      State(rank, rank - state.rank, msg.superstep, state.updates + msg.superstep)
    }

    /**
     * sendMsg: pass the source's latest rank change on, scaled by the route's probability, unless it is tiny.
     */
    def sendMsg(t: EdgeTriplet[State, Double]): Iterator[(VertexId, Message)] =
      if (t.srcAttr.delta > tol)
        Iterator(t.dstId -> Message(t.srcAttr.delta * t.attr, t.srcAttr.superstep + 1))
      else Iterator.empty

    /** mergeMsg: rank changes arriving at the same vertex add up. */
    def mergeMsg(a: Message, b: Message): Message =
      Message(a.mass + b.mass, math.max(a.superstep, b.superstep))
  }

  /** Every vertex before superstep 0, on the graph of route probabilities. */
  def start[VD: ClassTag](graph: Graph[VD, Double]): Graph[State, Double] =
    transitionProbabilities(graph).mapVertices((_, _) => State(0.0, 0.0, 0, BitSet.empty))

  /** The raw Pregel run: un-normalized states after at most `maxSupersteps` supersteps. */
  def pregel[VD: ClassTag](
      graph: Graph[VD, Double],
      resetProb: Double,
      tol: Double,
      maxSupersteps: Int
  ): Graph[State, Double] = {
    val program = Program(resetProb, tol)
    Pregel(start(graph), program.initial, maxSupersteps, EdgeDirection.Out)(
      program.vprog,
      program.sendMsg,
      program.mergeMsg
    )
  }

  def run[VD: ClassTag](
      graph: Graph[VD, Double],
      resetProb: Double,
      tol: Double,
      maxSupersteps: Int
  ): Result = {
    val ranked = pregel(graph, resetProb, tol, maxSupersteps)
    val states = ranked.vertices // Pregel leaves its result cached
    val total  = states.map(_._2.rank).sum()
    val ranks  = states.mapValues(_.rank / total).persist(StorageLevel.MEMORY_AND_DISK)
    ranks.count()
    val active = Supersteps.counts(states.map(_._2.updates))
    ranked.unpersist(blocking = false)
    Result(
      ranks,
      supersteps = active.size - 1,
      activeVertices = active,
      converged = active.size - 1 < maxSupersteps
    )
  }

  /** Rank vectors for personalized PageRank: entry i belongs to source i. */
  final case class VectorState(rank: Array[Double], delta: Array[Double])

  /**
   * Personalized PageRank from several sources in one Pregel run (P3.7): the same delta program, but every
   * vertex carries one rank per source, and teleports always return to that source. Entry i of a vertex then
   * answers "of the riders who start at source i, what share ends up here?". Each source's ranks are
   * normalized to sum to 1.
   */
  def personalized[VD: ClassTag](
      graph: Graph[VD, Double],
      sources: Array[VertexId],
      resetProb: Double,
      tol: Double,
      maxSupersteps: Int
  ): VertexRDD[Array[Double]] = {
    val alpha = resetProb
    val empty = Array.emptyDoubleArray
    val start = transitionProbabilities(graph).mapVertices((_, _) => VectorState(empty, empty))

    val vprog = (id: VertexId, state: VectorState, msg: Array[Double]) =>
      if (msg.isEmpty) { // superstep 0: all mass sits at the sources
        val teleport = sources.map(source => if (source == id) alpha else 0.0)
        VectorState(teleport, teleport)
      } else {
        val delta = msg.map(_ * (1 - alpha))
        VectorState(state.rank.zip(delta).map { case (r, d) => r + d }, delta)
      }
    val sendMsg = (t: EdgeTriplet[VectorState, Double]) =>
      if (t.srcAttr.delta.exists(_ > tol)) Iterator(t.dstId -> t.srcAttr.delta.map(_ * t.attr))
      else Iterator.empty
    val mergeMsg = (a: Array[Double], b: Array[Double]) => a.zip(b).map { case (x, y) => x + y }

    val ranked = Pregel(start, empty, maxSupersteps, EdgeDirection.Out)(vprog, sendMsg, mergeMsg)
    val totals = ranked.vertices.map(_._2.rank).reduce((a, b) => a.zip(b).map { case (x, y) => x + y })
    val result = ranked.vertices
      .mapValues(state => state.rank.zip(totals).map { case (r, total) => r / total })
      .persist(StorageLevel.MEMORY_AND_DISK)
    result.count()
    ranked.unpersist(blocking = false)
    result
  }
}
