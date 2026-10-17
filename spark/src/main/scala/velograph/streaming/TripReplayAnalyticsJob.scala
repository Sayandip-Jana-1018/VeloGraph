package velograph.streaming

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

import scala.collection.mutable

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.types._

import velograph.common.{JobContext, ServingJson, SparkJob}
import velograph.common.LakePaths.PathOps
import velograph.etl.{LakeTable, LakeTables}

final case class SystemWindow(
    start: String,
    end: String,
    trips: Long,
    member: Long,
    casual: Long,
    ebike: Long,
    classic: Long,
    late: Long,
    updates: Int
)
final case class StationCount(station: String, name: String, departures: Long, ebike: Long, member: Long)
final case class StationWindow(
    start: String,
    end: String,
    stations: Long,
    departures: Long,
    late: Long,
    top: Seq[StationCount]
)
final case class ReplayBatch(
    query: String,
    batchId: Long,
    timestamp: String,
    inputRows: Long,
    watermark: Option[String],
    stateRows: Long,
    droppedByWatermark: Long,
    durationMs: Long
)
final case class ReplayQuery(
    name: String,
    outputMode: String,
    window: String,
    sink: String,
    batches: Long,
    droppedByWatermark: Long
)

/**
 * Late trips: `sent` by the replayer (counted with `observe()`), `accepted` into the 15-minute windows that
 * have closed, and Spark's own count of rows the watermark dropped. Spark counts after the partial
 * aggregation before the shuffle, so two late trips of one station and window can count once.
 */
final case class LateData(sent: Long, accepted: Long, droppedByWatermark: Long, rows: Long)
final case class ReplayAnalytics(
    watermark: String,
    trigger: String,
    queries: Seq[ReplayQuery],
    late: LateData,
    maxEventTime: Option[String],
    system: Seq[SystemWindow],
    stations: Seq[StationWindow],
    batches: Seq[ReplayBatch]
)

/**
 * Roadmap P5.4, ARCHITECTURE §9.4: windowed analytics over replayed trips (`trip_replayer`). Event time is a
 * trip's start, with a 15-minute watermark. Two queries read the same stream, one per output mode:
 *   - `replay-station-15m`: departures per station in tumbling 15-minute windows, in **append** mode. A
 *     window is written once, when the watermark passes its end and no late trip can change it any more, to
 *     the trip-metrics topic;
 *   - `replay-system-60m`: system totals (members, casual riders, e-bikes) in sliding 60-minute windows every
 *     15 minutes, in **update** mode. Every trigger rewrites the windows that changed, and prints them to the
 *     console.
 * Trips the replayer held back arrive late. Within the watermark they still count, beyond it the state store
 * drops them; `observe()` counts the late ones sent, and the progress reports count the ones dropped. All of
 * it goes to `serving/live/replay-analytics.json` for the streaming portal.
 */
object TripReplayAnalyticsJob extends SparkJob {

  val jobName = "trip-replay-analytics"

  /** Two of the cluster's four cores, beside the live scoring job. */
  override val sparkConf: Map[String, String] =
    Map("spark.sql.shuffle.partitions" -> "6", "spark.cores.max" -> "2")

  val TripSchema: StructType = StructType(
    Seq(
      "schema"           -> StringType,
      "v"                -> IntegerType,
      "source"           -> StringType,
      "ride_id"          -> StringType,
      "rideable_type"    -> StringType,
      "member_casual"    -> StringType,
      "started_at"       -> DoubleType,
      "ended_at"         -> DoubleType,
      "start_station_id" -> StringType,
      "end_station_id"   -> StringType,
      "duration_s"       -> IntegerType,
      "late"             -> BooleanType,
      "late_by_s"        -> DoubleType
    ).map { case (n, t) => StructField(n, t) }
  )

  private final case class Options(durationMin: Option[Int] = None, fresh: Boolean = false)

  private val parser = new scopt.OptionParser[Options](jobName) {
    opt[Int]("duration-min").text("stop after this many minutes").action((m, o) =>
      o.copy(durationMin = Some(m))
    )
    opt[Unit]("fresh").text("delete the checkpoint first: start from the latest offsets").action((_, o) =>
      o.copy(fresh = true)
    )
  }

  val StationQuery = "replay-station-15m"
  val SystemQuery  = "replay-system-60m"

  /** Kafka records -> trips with their event time; anything else is left out. */
  def parse(kafka: DataFrame): DataFrame =
    kafka
      .select(from_json(col("value").cast(StringType), TripSchema).as("t"))
      .select("t.*")
      .where(col("schema") === "velograph.trip" && col("started_at").isNotNull &&
        col("start_station_id").isNotNull)
      .withColumn("event_time", timestamp_seconds(col("started_at")))
      .withColumn("late", coalesce(col("late"), lit(false)))

  private def flag(c: Column): Column = sum(when(c, 1).otherwise(0))

  /** Departures per station in tumbling windows. */
  def perStation(trips: DataFrame, length: String): DataFrame =
    trips
      .groupBy(window(col("event_time"), length), col("start_station_id"))
      .agg(
        count(lit(1)).as("departures"),
        flag(col("rideable_type") === "electric_bike").as("ebike"),
        flag(col("member_casual") === "member").as("member"),
        flag(col("late")).as("late")
      )

  /** System totals in sliding windows. */
  def system(trips: DataFrame, length: String, slide: String): DataFrame =
    trips
      .groupBy(window(col("event_time"), length, slide))
      .agg(
        count(lit(1)).as("trips"),
        flag(col("member_casual") === "member").as("member"),
        flag(col("member_casual") === "casual").as("casual"),
        flag(col("rideable_type") === "electric_bike").as("ebike"),
        flag(col("rideable_type") =!= "electric_bike").as("classic"),
        flag(col("late")).as("late")
      )

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark   = ctx.spark
    val options =
      parser.parse(args, Options()).getOrElse(throw new IllegalArgumentException("invalid arguments"))
    val kafka      = KafkaSettings(ctx.cfg.raw)
    val cfg        = ReplayConfig(ctx.cfg.raw)
    val checkpoint = ctx.paths.checkpoints.resolve(jobName)
    if (options.fresh) StreamRunner.deleteTree(checkpoint)

    val names = new LakeTables(spark, ctx.paths)
      .read(LakeTable.Stations)
      .select("station_id", "name")
      .collect() // bounded: one row per station
      .map(r => r.getString(0) -> r.getString(1))
      .toMap
    val export = new ReplayExport(ctx.paths.serving.resolve("live/replay-analytics.json"), cfg, names)
    spark.streams.addListener(export.listener)

    val trips = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka.bootstrap)
      .option("subscribe", kafka.topics.tripsReplay)
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", cfg.maxOffsetsPerTrigger)
      .option("failOnDataLoss", "false")
      .load()
      .transform(parse)
      .observe(
        "replay",
        count(lit(1)).as("rows"),
        coalesce(flag(col("late")), lit(0L)).as("lateSent"), // a sum over no rows is null
        max(col("started_at")).as("maxStart")
      )
      .withWatermark("event_time", cfg.watermark)

    val toKafka: (DataFrame, Long) => Unit = (batch, batchId) => {
      val closed = batch.persist()
      closed
        .select(
          concat_ws(
            "|",
            date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm"),
            col("start_station_id")
          ).as("key"),
          to_json(
            struct(
              lit("velograph.trip-metric").as("schema"),
              lit(1).as("v"),
              lit("station-15m").as("kind"),
              date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ssXXX").as("window_start"),
              date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ssXXX").as("window_end"),
              col("start_station_id").as("station_id"),
              col("departures"),
              col("ebike"),
              col("member")
            )
          ).as("value")
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap)
        .option("topic", kafka.topics.tripMetrics)
        .save()
      export.closedStationWindows(closed, batchId)
      closed.unpersist()
    }
    val stationQuery = perStation(trips, cfg.tumbling).writeStream
      .queryName(StationQuery)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(cfg.trigger))
      .option("checkpointLocation", checkpoint.resolve("station-15m").uri)
      .foreachBatch(toKafka)
      .start()

    val toConsole: (DataFrame, Long) => Unit = (batch, batchId) => export.updatedSystemWindows(batch, batchId)
    val systemQuery                          = system(trips, cfg.slidingLength, cfg.slidingSlide).writeStream
      .queryName(SystemQuery)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(cfg.trigger))
      .option("checkpointLocation", checkpoint.resolve("system-60m").uri)
      .foreachBatch(toConsole)
      .start()

    val stopFile = ctx.paths.root.resolve("run").resolve(s"$jobName.stop")
    val why      = StreamRunner.awaitStop(Seq(stationQuery, systemQuery), stopFile, options.durationMin)
    export.write()
    export.summary + ("stopped" -> why)
  }
}

/**
 * The replay job's export: closed station windows and updated system windows from the two sinks, plus the
 * progress of both queries (watermark, state rows, late rows dropped) and the `observe()` counts.
 */
final class ReplayExport(path: java.nio.file.Path, cfg: ReplayConfig, names: Map[String, String]) {

  private val zone     = ZoneId.of("America/New_York")
  private val clock    = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm")
  private val system   = mutable.TreeMap.empty[String, SystemWindow]
  private val stations = mutable.TreeMap.empty[String, StationWindow]
  private val batches  = mutable.Queue.empty[ReplayBatch]
  private val queries  = mutable.Map.empty[String, (Long, Long)] // name -> (batches, rows dropped as late)
  private var observed    = (0L, 0L) // (rows, late rows sent), from the station query's observation
  private var maxStart    = Option.empty[Double]
  private var evictedLate = 0L       // late trips accepted into closed windows no longer kept in `stations`

  /** Append mode: each row is a window the watermark has closed, written exactly once. */
  def closedStationWindows(batch: DataFrame, batchId: Long): Unit = synchronized {
    // Formatted in Spark, in the session's New York zone: a collected Timestamp would print in the JVM's zone.
    val rows = batch
      .select(
        date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm"),
        date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm"),
        col("start_station_id"),
        col("departures"),
        col("ebike"),
        col("member"),
        col("late")
      )
      .collect() // bounded: closed windows x stations, a few thousand rows
    rows.groupBy(_.getString(0)).foreach { case (start, members) =>
      val counts = members.map(r =>
        StationCount(
          r.getString(2),
          names.getOrElse(r.getString(2), r.getString(2)),
          r.getLong(3),
          r.getLong(4),
          r.getLong(5)
        )
      )
      stations(start) = StationWindow(
        start,
        members.head.getString(1),
        counts.length.toLong,
        counts.map(_.departures).sum,
        members.map(_.getLong(6)).sum,
        counts.sortBy(c => (-c.departures, c.station)).take(10).toSeq
      )
    }
    while (stations.size > 24) evictedLate += stations.remove(stations.firstKey).map(_.late).getOrElse(0L)
  }

  /** Update mode: every window whose totals changed in this trigger, printed to the console as well. */
  def updatedSystemWindows(batch: DataFrame, batchId: Long): Unit = synchronized {
    val rows = batch
      .select(
        date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm"),
        date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm"),
        col("trips"),
        col("member"),
        col("casual"),
        col("ebike"),
        col("classic"),
        col("late")
      )
      .orderBy(col("window.start"))
      .collect() // bounded: a handful of open windows
    rows.foreach { r =>
      val start   = r.getString(0)
      val updates = system.get(start).map(_.updates).getOrElse(0) + 1
      system(start) =
        SystemWindow(
          start,
          r.getString(1),
          r.getLong(2),
          r.getLong(3),
          r.getLong(4),
          r.getLong(5),
          r.getLong(6),
          r.getLong(7),
          updates
        )
    }
    while (system.size > 32) system.remove(system.firstKey)
    if (rows.nonEmpty) {
      println(
        s"-------- ${TripReplayAnalyticsJob.SystemQuery} batch $batchId: ${rows.length} windows updated --------"
      )
      rows.foreach(r =>
        println(
          f"  ${r.getString(0)} - ${r.getString(1)}  ${r.getLong(2)}%6d trips  ${r.getLong(5)}%6d e-bike  ${r.getLong(7)}%4d late"
        )
      )
    }
  }

  val listener: StreamingQueryListener = new StreamingQueryListener {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit       = ()
    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
    override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit     = {
      val p       = event.progress
      val name    = Option(p.name).getOrElse(p.id.toString)
      val dropped = p.stateOperators.map(_.numRowsDroppedByWatermark).sum
      ReplayExport.this.synchronized {
        val (n, d) = queries.getOrElse(name, (0L, 0L))
        queries(name) = (n + 1, d + dropped)
        batches.enqueue(
          ReplayBatch(
            name,
            p.batchId,
            p.timestamp,
            p.numInputRows,
            Option(p.eventTime.get("watermark")),
            p.stateOperators.map(_.numRowsTotal).sum,
            dropped,
            Option(p.durationMs.get("triggerExecution")).map(_.longValue).getOrElse(0L)
          )
        )
        while (batches.size > 80) batches.dequeue()
        // Both queries observe the same rows; one of them is enough to count what came in.
        if (name == TripReplayAnalyticsJob.StationQuery)
          Option(p.observedMetrics.get("replay")).foreach { r: Row =>
            observed = (observed._1 + r.getLong(0), observed._2 + r.getLong(1))
            Option(r.get(2)).foreach(v =>
              maxStart = Some(math.max(maxStart.getOrElse(0.0), v.asInstanceOf[Double]))
            )
          }
      }
      write()
    }
  }

  def report: ReplayAnalytics = synchronized {
    val stationDropped = queries.get(TripReplayAnalyticsJob.StationQuery).map(_._2).getOrElse(0L)
    ReplayAnalytics(
      watermark = cfg.watermark,
      trigger = cfg.trigger,
      queries = Seq(
        ReplayQuery(
          TripReplayAnalyticsJob.StationQuery,
          "append",
          s"tumbling ${cfg.tumbling}",
          "Kafka trip-metrics",
          queries.get(TripReplayAnalyticsJob.StationQuery).map(_._1).getOrElse(0L),
          stationDropped
        ),
        ReplayQuery(
          TripReplayAnalyticsJob.SystemQuery,
          "update",
          s"sliding ${cfg.slidingLength} every ${cfg.slidingSlide}",
          "console",
          queries.get(TripReplayAnalyticsJob.SystemQuery).map(_._1).getOrElse(0L),
          queries.get(TripReplayAnalyticsJob.SystemQuery).map(_._2).getOrElse(0L)
        )
      ),
      late =
        LateData(observed._2, stations.values.map(_.late).sum + evictedLate, stationDropped, observed._1),
      maxEventTime = maxStart.map(s => Instant.ofEpochMilli((s * 1000).toLong).atZone(zone).format(clock)),
      system = system.values.toList,
      stations = stations.values.toList,
      batches = batches.toList
    )
  }

  def write(): Unit = {
    val r = report
    ServingJson.write(path, "replay-analytics", TripReplayAnalyticsJob.jobName, r, rows = Some(r.late.rows))
  }

  def summary: Map[String, Any] = {
    val r = report
    Map(
      "rows"              -> r.late.rows,
      "lateSent"          -> r.late.sent,
      "lateAccepted"      -> r.late.accepted,
      "lateDropped"       -> r.late.droppedByWatermark,
      "systemWindows"     -> r.system.size,
      "closedStationWins" -> r.stations.size,
      "maxEventTime"      -> r.maxEventTime.getOrElse("")
    )
  }
}
