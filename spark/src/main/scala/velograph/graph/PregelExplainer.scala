package velograph.graph

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}

import velograph.graph.WeightedPageRank.{Message, Program, State}

final case class ExplainerStation(index: Int, id: String, name: String, lat: Double, lon: Double)
final case class ExplainerEdge(from: Int, to: Int, trips: Long, probability: Double)
final case class ExplainerMessage(from: Int, to: Int, mass: Double)

/**
 * Vertex states after a superstep (un-normalized ranks, as Pregel holds them) and the messages that the
 * vertices updated in it send during the next superstep.
 */
final case class ExplainerStep(
    superstep: Int,
    ranks: Seq[Double],
    deltas: Seq[Double],
    updated: Seq[Boolean],
    messages: Seq[ExplainerMessage]
)

/** Contents of `network-pregel.json`. */
final case class PregelExplainerReport(
    resetProb: Double,
    tol: Double,
    stations: Seq[ExplainerStation],
    edges: Seq[ExplainerEdge],
    steps: Seq[ExplainerStep],
    convergedAfter: Int,
    finalRanks: Seq[Double]
)

/**
 * Weighted PageRank superstep by superstep on a small real neighbourhood, for the website's step-through.
 *
 * GraphX's Pregel offers no hook between supersteps, so the run is done twice: once by GraphX, and once
 * replayed on the driver with the very same `Program` (vprog, sendMsg, mergeMsg) and Pregel's rules:
 * superstep 0 applies vprog to the initial message everywhere; afterwards only vertices that received a
 * message are updated, and only vertices updated in the previous superstep send. The replay records every
 * superstep and must end in exactly the state GraphX computed; anything else fails the job.
 */
object PregelExplainer {

  private final case class Sent(src: VertexId, dst: VertexId, message: Message)

  def trace(
      sc: SparkContext,
      stations: Seq[(VertexId, StationAttr)],
      routes: Seq[(VertexId, VertexId, Long)],
      resetProb: Double,
      tol: Double,
      maxSupersteps: Int
  ): PregelExplainerReport = {
    val index   = stations.map(_._1).zipWithIndex.toMap
    val program = Program(resetProb, tol)
    val graph   = Graph[StationAttr, Double](
      sc.parallelize(stations, numSlices = 1),
      sc.parallelize(routes.map { case (src, dst, trips) => Edge(src, dst, trips.toDouble) }, numSlices = 1)
    ).cache()

    val byGraphX =
      WeightedPageRank.pregel(graph, resetProb, tol, maxSupersteps).vertices.collect().toMap // bounded
    val start   = WeightedPageRank.start(graph)
    val initial = start.vertices.collect().toMap // bounded: the explainer's few stations
    val edges = start.triplets.map(t => (t.srcId, t.dstId, t.attr)).collect().toSeq // bounded: its routes
    graph.unpersist(blocking = false)

    val recorded         = replay(program, initial, edges, maxSupersteps)
    val (finalStates, _) = recorded.last
    stations.foreach { case (id, s) =>
      val (ours, theirs) = (finalStates(id), byGraphX(id))
      require(
        math.abs(ours.rank - theirs.rank) < 1e-9 && ours.superstep == theirs.superstep,
        s"replay diverged from GraphX at ${s.name}: $ours vs $theirs"
      )
    }

    def ordered(states: Map[VertexId, State]) = stations.map { case (id, _) => states(id) }
    val total                                 = finalStates.values.map(_.rank).sum
    PregelExplainerReport(
      resetProb = resetProb,
      tol = tol,
      stations = stations.map { case (id, s) =>
        ExplainerStation(index(id), s.stationId, s.name, s.lat, s.lon)
      },
      edges = edges
        .map { case (src, dst, probability) =>
          val trips = routes.collectFirst { case (s, d, n) if s == src && d == dst => n }.getOrElse(0L)
          ExplainerEdge(index(src), index(dst), trips, probability)
        }
        .sortBy(e => (e.from, e.to)),
      steps = recorded.zipWithIndex.map { case ((states, sent), s) =>
        ExplainerStep(
          superstep = s,
          ranks = ordered(states).map(_.rank),
          deltas = ordered(states).map(_.delta),
          updated = ordered(states).map(_.updates.contains(s)),
          messages = sent.map(m => ExplainerMessage(index(m.src), index(m.dst), m.message.mass))
        )
      },
      convergedAfter = finalStates.values.map(_.superstep).max,
      finalRanks = ordered(finalStates).map(_.rank / total)
    )
  }

  /** Pregel's loop on the driver: the states after each superstep, with the messages sent in the next one. */
  private def replay(
      program: Program,
      initial: Map[VertexId, State],
      edges: Seq[(VertexId, VertexId, Double)],
      maxSupersteps: Int
  ): Vector[(Map[VertexId, State], Seq[Sent])] = {
    def send(states: Map[VertexId, State], active: Set[VertexId]): Seq[Sent] =
      edges.filter { case (src, _, _) => active(src) }.flatMap { case (src, dst, probability) =>
        val t = new EdgeTriplet[State, Double]
        t.srcId = src
        t.dstId = dst
        t.attr = probability
        t.srcAttr = states(src)
        t.dstAttr = states(dst)
        program.sendMsg(t).map { case (to, message) => Sent(src, to, message) }
      }

    @scala.annotation.tailrec
    def loop(
        states: Map[VertexId, State],
        active: Set[VertexId],
        done: Vector[(Map[VertexId, State], Seq[Sent])]
    ): Vector[(Map[VertexId, State], Seq[Sent])] = {
      val sent     = send(states, active)
      val recorded = done :+ (states -> sent)
      if (sent.isEmpty || done.size == maxSupersteps) recorded
      else {
        val inbox = sent.groupBy(_.dst).map { case (dst, in) =>
          dst -> in.map(_.message).reduce(program.mergeMsg)
        }
        loop(
          states ++ inbox.map { case (id, m) => id -> program.vprog(id, states(id), m) },
          inbox.keySet,
          recorded
        )
      }
    }

    val superstep0 = initial.map { case (id, s) => id -> program.vprog(id, s, program.initial) }
    loop(superstep0, superstep0.keySet, Vector.empty)
  }
}
