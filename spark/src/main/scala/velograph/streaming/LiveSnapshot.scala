package velograph.streaming

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.streaming.StreamingQueryListener

import velograph.common.ServingJson

/** The weather the forecast used for the current hour. */
final case class SnapshotWeather(
    hour: String,
    tempC: Double,
    precipMm: Double,
    windKmh: Double,
    raining: Boolean,
    observed: String,
    fallback: Boolean
)

final case class SnapshotKpis(
    stations: Int,
    reporting: Int,
    offline: Int,
    empty: Int,
    emptyRisk: Int,
    full: Int,
    fullRisk: Int,
    bikes: Long,
    ebikes: Long,
    docks: Long,
    moves: Int,
    bikesToMove: Int
)

/** End to end: the oldest message of the batch, from the broker's append to this snapshot's write. */
final case class SnapshotLatency(
    brokerToSnapshotS: Double,
    processingMs: Long,
    batchRows: Long,
    freshnessS: Double
)

/** One station on the live map, with short names: the website polls about 2.5k of these every 30 s. */
final case class SnapshotStation(
    id: String,
    name: String,
    lat: Double,
    lon: Double,
    capacity: Int,
    bikes: Int,
    ebikes: Int,
    docks: Int,
    projected: Double,
    projectedDocks: Double,
    dep60: Double,
    arr60: Double,
    net60: Double,
    dep0: Double,
    arr0: Double,
    dep1: Double,
    arr1: Double,
    risk: String,
    regime: Int,
    archetype: String,
    community: Int,
    pagerank: Double,
    reported: Long
)

final case class SnapshotAlert(
    at: String,
    stationId: String,
    name: String,
    risk: String,
    previous: String,
    projected: Double,
    capacity: Int,
    lat: Double,
    lon: Double
)

/** `serving/live/snapshot.json`: what the live page shows, replaced atomically after every micro-batch. */
final case class LiveSnapshot(
    asOf: String,
    asOfEpoch: Double,
    source: String,
    batchId: Long,
    hour: String,
    fraction: Double,
    weather: SnapshotWeather,
    kpis: SnapshotKpis,
    latency: SnapshotLatency,
    stations: Seq[SnapshotStation],
    moves: Seq[Move],
    alerts: Seq[SnapshotAlert]
)

/** What the scorer measured in one micro-batch, merged into the progress export. */
final case class BatchMetrics(
    stations: Int,
    empty: Int,
    emptyRisk: Int,
    full: Int,
    fullRisk: Int,
    alerts: Int,
    moves: Int,
    latencyS: Double,
    processingMs: Long
)

final case class ProgressEntry(
    query: String,
    batchId: Long,
    timestamp: String,
    inputRows: Long,
    inputRowsPerSecond: Double,
    processedRowsPerSecond: Double,
    durationMs: Map[String, Long],
    watermark: Option[String],
    maxEventTime: Option[String],
    stateRows: Long,
    droppedByWatermark: Long,
    stateMemoryBytes: Long,
    scoring: Option[BatchMetrics]
)

final case class StreamProgress(queries: Seq[String], batches: Seq[ProgressEntry])

/**
 * Keeps `serving/live/stream-progress.json` current: Spark's own progress report of every micro-batch (input
 * rate, durations, watermark, state rows, rows dropped as late), joined with what the scorer measured in it.
 * Listener events arrive on Spark's listener thread after a batch commits.
 */
final class ProgressExport(path: Path, job: String, keep: Int) extends StreamingQueryListener {

  val scoring         = new ConcurrentHashMap[Long, BatchMetrics]()
  private val recent  = mutable.Queue.empty[ProgressEntry]
  private val queries = mutable.LinkedHashSet.empty[String]

  /** NaN is not JSON: a rate over a zero-length interval reads as 0. */
  private def finite(x: Double): Double = if (x.isNaN || x.isInfinite) 0.0 else math.rint(x * 10) / 10

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
    synchronized(queries += Option(event.name).getOrElse(event.id.toString))

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val p     = event.progress
    val state = p.stateOperators.toSeq
    val entry = ProgressEntry(
      query = Option(p.name).getOrElse(p.id.toString),
      batchId = p.batchId,
      timestamp = p.timestamp,
      inputRows = p.numInputRows,
      inputRowsPerSecond = finite(p.inputRowsPerSecond),
      processedRowsPerSecond = finite(p.processedRowsPerSecond),
      durationMs = p.durationMs.asScala.map { case (k, v) => k -> v.longValue }.toMap,
      watermark = Option(p.eventTime.get("watermark")),
      maxEventTime = Option(p.eventTime.get("max")),
      stateRows = state.map(_.numRowsTotal).sum,
      droppedByWatermark = state.map(_.numRowsDroppedByWatermark).sum,
      stateMemoryBytes = state.map(_.memoryUsedBytes).sum,
      scoring = if (p.name == job) Option(scoring.remove(p.batchId)) else None
    )
    val snapshot = synchronized {
      recent.enqueue(entry)
      while (recent.size > keep) recent.dequeue()
      StreamProgress(queries.toList, recent.toList)
    }
    ServingJson.write(path, "stream-progress", job, snapshot, rows = Some(snapshot.batches.size.toLong))
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
}
