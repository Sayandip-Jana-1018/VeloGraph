package velograph.graph

import scala.reflect.ClassTag

import org.apache.spark.graphx._

/**
 * Single-source shortest paths from many sources at once (roadmap P3.7), written with Pregel: the fastest
 * chain of rides from each landmark to every station, with each route weighted by its median ride time. The
 * Network page turns the result into travel-time isochrones.
 *
 * Every vertex keeps its best known time from each landmark. A vertex that improved in the last superstep
 * offers "my time + this ride" to each out-neighbour (sendMsg), only where that beats what the neighbour
 * already knows; offers to the same vertex keep the minimum per landmark (mergeMsg) and the vertex keeps the
 * better of old and offered times (vprog). It is Bellman-Ford with one round per superstep; Pregel stops when
 * nothing improves.
 */
object TravelTimes {

  /** Best known seconds from each landmark, keyed by the landmark's index. */
  type Times = Map[Int, Double]

  def run[VD: ClassTag](
      graph: Graph[VD, Double],
      landmarks: Array[VertexId],
      maxSupersteps: Int
  ): VertexRDD[Times] = {
    val index = landmarks.zipWithIndex.toMap
    val start =
      graph.mapVertices((id, _) => index.get(id).map(i => Map(i -> 0.0)).getOrElse(Map.empty[Int, Double]))

    val vprog   = (_: VertexId, times: Times, offer: Times) => fastest(times, offer)
    val sendMsg = (t: EdgeTriplet[Times, Double]) => {
      val better = t.srcAttr.collect {
        case (landmark, seconds) if t.dstAttr.get(landmark).forall(_ > seconds + t.attr) =>
          landmark -> (seconds + t.attr)
      }
      if (better.isEmpty) Iterator.empty else Iterator(t.dstId -> better)
    }

    Pregel(start, Map.empty[Int, Double], maxSupersteps, EdgeDirection.Out)(vprog, sendMsg, fastest).vertices
  }

  /** Per landmark, the smaller of two times. */
  def fastest(a: Times, b: Times): Times =
    b.foldLeft(a) { case (best, (landmark, seconds)) =>
      if (best.get(landmark).forall(_ > seconds)) best.updated(landmark, seconds) else best
    }
}
