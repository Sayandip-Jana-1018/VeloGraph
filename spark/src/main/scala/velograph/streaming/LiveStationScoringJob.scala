package velograph.streaming

import java.time.{Instant, LocalDate, ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.storage.StorageLevel

import velograph.common.{JobContext, ServingJson, SparkJob}
import velograph.common.LakePaths.PathOps
import velograph.etl.{LakeTable, LakeTables}
import velograph.ml.{FeatureBuildJob, TrainChampionJob}

/**
 * Roadmap P5.3, ARCHITECTURE §9.4: live station scoring with Structured Streaming.
 *
 *   1. `readStream` on the station-status topic, parsed with an explicit schema. Records that are not station
 *      messages go to the dead-letter topic through a second query.
 *   2. Event time is the station's `last_reported`: a 10-minute watermark, and
 *      `dropDuplicatesWithinWatermark` on (station, last_reported), since a restarted producer re-sends what
 *      it already sent.
 *   3. Every 30 s, `foreachBatch` hands the new rows to [[LiveScorer]]. It forecasts the current and next
 *      hour with the three champions and projects every station's next 60 minutes. Then it derives risk,
 *      alerts and rebalancing moves, and writes them to Kafka, Parquet and an atomic snapshot file.
 *
 * Offsets live in the checkpoint (`checkpoints/live-station-scoring-<source>`), so a restarted job resumes
 * where it stopped. Live and replayed messages share the topic but never a run: `--source` picks one, and
 * each has its own checkpoint, since a replay's event times are hours behind the live watermark. The job runs
 * until `data/run/live-station-scoring.stop` appears, or for `--duration-min` minutes.
 */
object LiveStationScoringJob extends SparkJob {

  val jobName = "live-station-scoring"

  /**
   * One shuffle partition per station-status partition: the batches are a few thousand rows. On the cluster,
   * two of the four cores, so the trip replay job can run beside it.
   */
  override val sparkConf: Map[String, String] =
    Map("spark.sql.shuffle.partitions" -> "6", "spark.cores.max" -> "2")

  private final case class Options(
      source: String = "live",
      durationMin: Option[Int] = None,
      fresh: Boolean = false
  )

  private val parser = new scopt.OptionParser[Options](jobName) {
    opt[String]("source")
      .text("live (default) or replay: which producer's messages to score")
      .validate(s => if (Set("live", "replay")(s)) success else failure("source is live or replay"))
      .action((s, o) => o.copy(source = s))
    opt[Int]("duration-min").text("stop after this many minutes").action((m, o) =>
      o.copy(durationMin = Some(m))
    )
    opt[Unit]("fresh").text("delete the checkpoint first: start from the latest offsets").action((_, o) =>
      o.copy(fresh = true)
    )
  }

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark   = ctx.spark
    val options =
      parser.parse(args, Options()).getOrElse(throw new IllegalArgumentException("invalid arguments"))
    val kafka      = KafkaSettings(ctx.cfg.raw)
    val live       = LiveConfig(ctx.cfg.raw)
    val checkpoint = ctx.paths.checkpoints.resolve(s"$jobName-${options.source}")
    val stopFile   = StreamRunner.stopFile(ctx.paths.root, jobName)
    StreamRunner.clearStop(stopFile)
    if (options.fresh) StreamRunner.deleteTree(checkpoint)

    val progress = new ProgressExport(
      ctx.paths.serving.resolve("live/stream-progress.json"),
      jobName,
      live.progressHistory
    )
    spark.streams.addListener(progress)
    val scorer = new LiveScorer(ctx, kafka, live, options.source, progress)
    scorer.bootstrap()

    val messages = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka.bootstrap)
      .option("subscribe", kafka.topics.stationStatus)
      .option("startingOffsets", "latest") // first run only: afterwards the checkpoint's offsets win
      .option("maxOffsetsPerTrigger", live.maxOffsetsPerTrigger)
      .option("failOnDataLoss", "false") // after a long stop, retention may have deleted unread offsets
      .option("groupIdPrefix", s"velograph-$jobName")
      .load()
      .transform(LiveTransforms.parse)

    val score: (DataFrame, Long) => Unit = (batch, batchId) => scorer.process(batch, batchId)
    val scoring                          = messages
      .where(col("valid") && col("source") === options.source)
      .withWatermark("event_time", live.watermark)
      .dropDuplicatesWithinWatermark("station_id", "last_reported")
      .writeStream
      .queryName(jobName)
      .trigger(Trigger.ProcessingTime(live.trigger))
      .option("checkpointLocation", checkpoint.resolve("scoring").uri)
      .foreachBatch(score)
      .start()

    val deadLetters = messages
      .where(!col("valid"))
      .select(
        lit(jobName).as("key"),
        to_json(
          struct(
            lit("velograph.dead-letter").as("schema"),
            lit(1).as("v"),
            lit(jobName).as("source"),
            lit(kafka.topics.stationStatus).as("origin"),
            col("error"),
            col("raw"),
            date_format(col("kafka_ts"), "yyyy-MM-dd'T'HH:mm:ssXXX").as("at")
          )
        ).as("value")
      )
      .writeStream
      .queryName("live-dead-letters")
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka.bootstrap)
      .option("topic", kafka.topics.dlq)
      .trigger(Trigger.ProcessingTime(live.trigger))
      .option("checkpointLocation", checkpoint.resolve("dead-letters").uri)
      .start()

    val stopReason = StreamRunner.awaitStop(Seq(scoring, deadLetters), stopFile, options.durationMin)
    scorer.summary + ("stopped" -> stopReason) + ("source" -> options.source)
  }
}

/**
 * The driver side of live scoring. It holds the newest status of every station (at most ~2.5k rows, a bounded
 * collect) and the previous risk of each, and runs once per micro-batch.
 */
final class LiveScorer(
    ctx: JobContext,
    kafka: KafkaSettings,
    live: LiveConfig,
    source: String,
    progress: ProgressExport
) {

  private val spark = ctx.spark
  import spark.implicits._

  private val log      = org.slf4j.LoggerFactory.getLogger(classOf[LiveScorer])
  private val lake     = new LakeTables(spark, ctx.paths)
  private val zone     = ZoneId.of("America/New_York")
  private val hourKey  = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:00")
  private val isoZoned = DateTimeFormatter.ISO_OFFSET_DATE_TIME
  private val mapper   = new ObjectMapper()

  /** The champions, by target: departures, arrivals, regime (TrainChampionJob's PipelineModels). */
  private val models: Seq[(String, PipelineModel)] = TrainChampionJob
    .champions(ctx.cfg.raw)
    .map(c => c.label -> PipelineModel.load(ctx.paths.models.resolve(c.name).uri))

  private val stations       = lake.read(LakeTable.LiveStations).persist(StorageLevel.MEMORY_AND_DISK)
  private val profiles       = lake.read(LakeTable.LiveProfiles).persist(StorageLevel.MEMORY_AND_DISK)
  private val medianCapacity = FeatureBuildJob.medianCapacity(lake.read(LakeTable.Stations))

  /** ISO weekday, weekend and holiday flags per date; bounded: one row per calendar day (~200). */
  private val calendar: Map[LocalDate, (Int, Boolean, Boolean)] = lake
    .read(LakeTable.Calendar)
    .select(date_format(col("date"), "yyyy-MM-dd"), col("dow"), col("is_weekend"), col("is_holiday"))
    .as[(String, Int, Boolean, Boolean)]
    .collect()
    .map { case (d, dow, weekend, holiday) => LocalDate.parse(d) -> ((dow, weekend, holiday)) }
    .toMap

  private val state                             = mutable.Map.empty[String, StationStatus]
  private var previousRisk                      = Map.empty[String, String]
  private val recentAlerts                      = mutable.Queue.empty[SnapshotAlert]
  private var weather: Option[(Long, JsonNode)] = None // (when read, latest message)

  private val latencies = mutable.ArrayBuffer.empty[Double]
  private var batches   = 0L
  private var rowsIn    = 0L
  private var alertsOut = 0L
  private var lastKpis  = Option.empty[SnapshotKpis]

  private val snapshotPath = ctx.paths.serving.resolve("live").resolve("snapshot.json")

  private def newestStatuses(kafkaRows: DataFrame): Array[StationStatus] =
    LiveTransforms.newest(kafkaRows).select(LiveTransforms.StatusColumns: _*).as[StationStatus].collect()

  private def merge(rows: Seq[StationStatus]): Unit = rows.foreach { s =>
    if (state.get(s.stationId).forall(s.supersedes)) state(s.stationId) = s
  }

  /**
   * Rebuilds every station's state from the last `bootstrap-minutes` of the topic (a batch read, by broker
   * timestamp), so a (re)started job need not wait for the producer's next full snapshot. The risks of the
   * last snapshot come back too, so a restart does not repeat alerts that were already sent.
   */
  def bootstrap(): Unit = {
    val since  = System.currentTimeMillis() - live.bootstrapMinutes * 60000L
    val recent = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka.bootstrap)
      .option("subscribe", kafka.topics.stationStatus)
      .option("startingTimestamp", since.toString)
      .option("startingOffsetsByTimestampStrategy", "latest") // a partition with nothing that recent: none
      .option("endingOffsets", "latest")
      .load()
      .transform(LiveTransforms.parse)
      .where(col("valid") && col("source") === source)
    merge(newestStatuses(recent).toSeq)
    previousRisk = Try {
      val data = mapper.readTree(snapshotPath.toFile).path("data")
      if (data.path("source").asText() != source) Map.empty[String, String]
      else data.path("stations").elements().asScala.map(s =>
        s.path("id").asText() -> s.path("risk").asText()
      ).toMap
    }.getOrElse(Map.empty)
    log.info(
      s"bootstrap: ${state.size} stations from the last ${live.bootstrapMinutes} min, ${previousRisk.size} previous risks"
    )
  }

  /** The newest weather message for this source, read again at most once a minute (a compacted topic). */
  private def latestWeather(): Option[JsonNode] = {
    val now = System.currentTimeMillis()
    if (weather.forall { case (at, _) => now - at > 60000 }) {
      val key   = if (source == "live") "nyc" else "nyc-replay"
      val found = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap)
        .option("subscribe", kafka.topics.weather)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .where(col("key").cast("string") === key)
        .orderBy(col("offset").desc)
        .select(col("value").cast("string"))
        .as[String]
        .take(1) // bounded: one message
        .headOption
        .map(mapper.readTree)
      weather = found.map(now -> _).orElse(weather.map { case (_, w) => now -> w })
    }
    weather.map(_._2)
  }

  private def number(node: JsonNode, field: String, default: Double): Double = {
    val v = node.path(field)
    if (v.isNumber) v.asDouble() else default
  }

  /**
   * Calendar and weather of one hour, as the model's columns; the configured mild hour when no message has
   * come yet.
   */
  private def hourRow(slot: Int, hour: ZonedDateTime, message: Option[JsonNode]) = {
    val date                    = hour.toLocalDate
    val (dow, weekend, holiday) =
      calendar.getOrElse(date, (date.getDayOfWeek.getValue, date.getDayOfWeek.getValue >= 6, false))
    val key      = hour.format(hourKey)
    val forecast =
      message.flatMap(m => m.path("hours").elements().asScala.find(_.path("hour").asText() == key))
    val values                   = forecast.orElse(message.map(_.path("current")))
    val d                        = live.defaultWeather
    def w(field: String): Double =
      values.fold(d(field).toString.toDouble)(v => number(v, field, d(field).toString.toDouble))
    val raining = values.fold(d("is_raining").toString.toBoolean)(v =>
      if (v.path("is_raining").isBoolean) v.path("is_raining").asBoolean() else w("precip_mm") >= 0.1
    )
    val row = (
      slot,
      hour.getHour,
      dow,
      weekend,
      holiday,
      w("temp_c"),
      w("apparent_temp_c"),
      w("precip_mm"),
      w("wind_kmh"),
      w("humidity_pct"),
      w("cloud_pct"),
      raining
    )
    val shown = SnapshotWeather(
      key,
      w("temp_c"),
      w("precip_mm"),
      w("wind_kmh"),
      raining,
      message.map(_.path("observed").asText()).getOrElse(""),
      fallback = forecast.isEmpty
    )
    (row, shown)
  }

  private def round(x: Double, digits: Int = 2): Double = {
    val scale = math.pow(10, digits)
    math.rint(x * scale) / scale
  }

  /** The three champions on every station's current and next hour, as one forecast per station. */
  private def forecast(hours: Seq[(
      Int,
      Int,
      Int,
      Boolean,
      Boolean,
      Double,
      Double,
      Double,
      Double,
      Double,
      Double,
      Boolean
  )]) = {
    val hourFrame = hours.toDF(
      "slot",
      "hour",
      "dow",
      "is_weekend",
      "is_holiday",
      "temp_c",
      "apparent_temp_c",
      "precip_mm",
      "wind_kmh",
      "humidity_pct",
      "cloud_pct",
      "is_raining"
    )
    val weekHours = hours.map { case (_, hour, dow, _, _, _, _, _, _, _, _, _) => (dow, hour) }
    val wanted    =
      weekHours.map { case (dow, hour) => col("dow") === dow && col("hour") === hour }.reduce(_ || _)
    val rows = LiveTransforms
      .featureRows(state.values.toSeq.toDF(), hourFrame, stations, profiles.where(wanted), medianCapacity)
      .persist(StorageLevel.MEMORY_AND_DISK)
    val predicted = models
      .map { case (target, model) =>
        model.transform(rows).select(col("station_id"), col("slot"), col("prediction").as(target))
      }
      .reduce(_.join(_, Seq("station_id", "slot")))
    def at(slot: Int, target: String) = max(when(col("slot") === slot, col(target)))
    val wide                          = predicted
      .groupBy("station_id")
      .agg(at(0, "departures"), at(0, "arrivals"), at(1, "departures"), at(1, "arrivals"), at(0, "regime"))
      .toDF("station_id", "d0", "a0", "d1", "a1", "r0")
      .join(
        rows.where(col("slot") === 0).select("station_id", "archetype_name", "community", "pagerank_w"),
        "station_id"
      )
      .as[(String, Double, Double, Double, Double, Double, String, Int, Double)]
      .collect() // bounded: one row per station
      .map { case (id, d0, a0, d1, a1, r0, archetype, community, pr) =>
        id -> Forecast(id, d0, a0, d1, a1, r0.toInt, archetype, community, pr)
      }
      .toMap
    rows.unpersist()
    wide
  }

  def process(batch: DataFrame, batchId: Long): Unit = {
    val started = System.currentTimeMillis()
    batch.persist(StorageLevel.MEMORY_AND_DISK)
    val stats = batch
      .agg(count(lit(1)), min(unix_millis(col("kafka_ts"))), max(col("last_reported")))
      .as[(Long, Option[Long], Option[Long])]
      .head() // bounded: 1 row
    val (rows, oldestKafka, newestReport) = stats
    if (rows > 0) merge(newestStatuses(batch).toSeq)
    batch.unpersist()
    if (state.nonEmpty) publish(batchId, started, rows, oldestKafka, newestReport)
  }

  /** Forecast, project, alert and plan moves for every station in the state, then write all the sinks. */
  private def publish(
      batchId: Long,
      started: Long,
      rows: Long,
      oldestKafka: Option[Long],
      newestReport: Option[Long]
  ): Unit = {
    // ---- Where in the hour we are: event time of the newest poll, in New York.
    val asOf                 = state.values.map(_.fetchedAt).max
    val now                  = Instant.ofEpochMilli((asOf * 1000).toLong).atZone(zone)
    val h0                   = now.truncatedTo(ChronoUnit.HOURS)
    val fraction             = ChronoUnit.SECONDS.between(h0, now) / 3600.0
    val message              = latestWeather()
    val (row0, shownWeather) = hourRow(0, h0, message)
    val (row1, _)            = hourRow(1, h0.plusHours(1), message)

    val forecasts   = forecast(Seq(row0, row1))
    val predictions = state.values.toSeq.flatMap(s =>
      forecasts.get(s.stationId).map(LiveRules.predict(s, _, fraction, live.risk))
    )
    val alerts   = LiveRules.alerts(previousRisk, predictions)
    val moves    = Rebalancer.plan(predictions, live.rebalance)
    val asOfText = now.format(isoZoned)

    // ---- Sinks: Kafka (predictions, alerts), Parquet, then the snapshot the website reads.
    val predictionFrame                                                = predictions.toDF()
    def toKafka(frame: DataFrame, topic: String, schema: String): Unit =
      frame
        .select(
          col("stationId").as("key"),
          to_json(
            struct(
              Seq(
                lit(schema).as("schema"),
                lit(1).as("v"),
                lit(source).as("source"),
                lit(asOfText).as("as_of"),
                lit(batchId).as("batch_id")
              ) ++ frame.columns.map(col): _*
            )
          ).as("value")
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap)
        .option("kafka.acks", "all")
        .option("topic", topic)
        .save()
    toKafka(predictionFrame, kafka.topics.predictions, "velograph.prediction")
    if (alerts.nonEmpty) {
      val alertFrame = alerts
        .map { case (p, before) =>
          (
            p.stationId,
            p.name,
            p.lat,
            p.lon,
            p.risk,
            before,
            round(p.projectedBikes),
            round(p.projectedDocks),
            p.capacity,
            p.bikes,
            p.docks
          )
        }
        .toDF(
          "stationId",
          "name",
          "lat",
          "lon",
          "risk",
          "previous",
          "projectedBikes",
          "projectedDocks",
          "capacity",
          "bikes",
          "docks"
        )
      toKafka(alertFrame, kafka.topics.alerts, "velograph.alert")
    }
    predictionFrame
      .withColumn("as_of", to_timestamp(lit(asOfText)))
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite) // a re-run of this batch id replaces its own file: the sink stays idempotent
      .parquet(ctx.paths.stream.resolve(
        "predictions"
      ).resolve(s"source=$source").resolve(s"date=${now.toLocalDate}")
        .resolve(f"batch=$batchId%08d").uri)

    alerts.foreach { case (p, before) =>
      recentAlerts.enqueue(SnapshotAlert(
        asOfText,
        p.stationId,
        p.name,
        p.risk,
        before,
        round(p.projectedBikes),
        p.capacity,
        p.lat,
        p.lon
      ))
    }
    while (recentAlerts.size > 50) recentAlerts.dequeue()

    val reporting = predictions.filterNot(_.risk == LiveRules.Offline)
    val kpis      = SnapshotKpis(
      stations = predictions.size,
      reporting = reporting.size,
      offline = predictions.size - reporting.size,
      empty = predictions.count(_.risk == LiveRules.Empty),
      emptyRisk = predictions.count(_.risk == LiveRules.EmptyRisk),
      full = predictions.count(_.risk == LiveRules.Full),
      fullRisk = predictions.count(_.risk == LiveRules.FullRisk),
      bikes = reporting.map(_.bikes.toLong).sum,
      ebikes = reporting.map(_.ebikes.toLong).sum,
      docks = reporting.map(_.docks.toLong).sum,
      moves = moves.size,
      bikesToMove = moves.map(_.bikes).sum
    )
    val written    = System.currentTimeMillis()
    val latencyS   = oldestKafka.fold(0.0)(k => round((written - k) / 1000.0, 1))
    val freshnessS = newestReport.fold(0.0)(r => round(written / 1000.0 - r, 1))
    val snapshot   = LiveSnapshot(
      asOf = asOfText,
      asOfEpoch = asOf,
      source = source,
      batchId = batchId,
      hour = h0.format(hourKey),
      fraction = round(fraction, 3),
      weather = shownWeather,
      kpis = kpis,
      latency = SnapshotLatency(latencyS, written - started, rows, freshnessS),
      stations = predictions.sortBy(_.stationId).map(p =>
        SnapshotStation(
          p.stationId,
          p.name,
          round(p.lat, 6),
          round(p.lon, 6),
          p.capacity,
          p.bikes,
          p.ebikes,
          p.docks,
          round(p.projectedBikes, 1),
          round(p.projectedDocks, 1),
          round(p.departures60),
          round(p.arrivals60),
          round(p.netFlow60),
          round(p.departures0),
          round(p.arrivals0),
          round(p.departures1),
          round(p.arrivals1),
          p.risk,
          p.regime,
          p.archetype,
          p.community,
          round(p.pagerank, 6),
          p.lastReported
        )
      ),
      moves = moves,
      alerts = recentAlerts.reverse.toList
    )
    ServingJson.write(
      snapshotPath,
      "live-snapshot",
      LiveStationScoringJob.jobName,
      snapshot,
      rows = Some(predictions.size.toLong),
      pretty = false
    )

    previousRisk = predictions.map(p => p.stationId -> p.risk).toMap
    batches += 1
    rowsIn += rows
    alertsOut += alerts.size
    lastKpis = Some(kpis)
    if (rows > 0) latencies += latencyS
    progress.scoring.put(
      batchId,
      BatchMetrics(
        kpis.stations,
        kpis.empty,
        kpis.emptyRisk,
        kpis.full,
        kpis.fullRisk,
        alerts.size,
        moves.size,
        latencyS,
        written - started
      )
    )
    log.info(
      f"batch $batchId: $rows rows, ${kpis.stations} stations; empty ${kpis.empty} now + ${kpis.emptyRisk} " +
        f"forecast, full ${kpis.full} now + ${kpis.fullRisk} forecast; " +
        f"${alerts.size} alerts, ${moves.size} moves; ${written - started} ms, broker -> snapshot $latencyS%.1f s"
    )
  }

  private def quantile(xs: Seq[Double], q: Double): Double =
    if (xs.isEmpty) 0.0 else xs.sorted.apply(math.min(xs.size - 1, (q * xs.size).toInt))

  def summary: Map[String, Any] = Map(
    "batches"     -> batches,
    "rowsIn"      -> rowsIn,
    "alerts"      -> alertsOut,
    "latencyP50S" -> quantile(latencies.toSeq, 0.5),
    "latencyP95S" -> quantile(latencies.toSeq, 0.95),
    "latencyMaxS" -> latencies.foldLeft(0.0)(math.max),
    "stations"    -> state.size,
    "lastKpis"    -> lastKpis.map(_.toString).getOrElse("")
  )
}
