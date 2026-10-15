package velograph.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

import velograph.common.{JobContext, ServingJson, SparkJob}
import velograph.common.LakePaths.PathOps
import velograph.common.Timing.timed
import velograph.etl.{LakeTable, LakeTables}

/**
 * `rejectedLines` are data lines the parser drops: malformed ones, or trips without a start station (e-bikes
 * picked up away from any dock). September 2026 has no malformed lines at all.
 */
final case class AccumulatorDemo(
    inAction: Long,
    inTransformationAfterTwoActions: Long,
    headerLines: Long,
    rejectedLines: Long
)
final case class PersistenceRun(
    level: String,
    firstCountMs: Long,
    secondCountMs: Long,
    memoryBytes: Long,
    diskBytes: Long
)
final case class ShuffleRun(operation: String, millis: Long, shuffleBytes: Long, result: Long)
final case class StationCount(stationId: String, name: String, departures: Long)
final case class HourCount(hour: Int, trips: Long)

/** Contents of `data/serving/rdd-fundamentals.json`. */
final case class RddReport(
    month: String,
    inputPartitions: Int,
    lines: Long,
    trips: Long,
    accumulators: AccumulatorDemo,
    persistence: Seq[PersistenceRun],
    shuffle: Seq[ShuffleRun],
    tripsByHour: Seq[HourCount],
    topStations: Seq[StationCount],
    members: Long,
    stationVisits: Long,
    byRideableType: Map[String, Long],
    sample: Seq[TripLite],
    lineage: Seq[String],
    rddToDataFrame: Map[String, Long]
)

/**
 * Roadmap P2.3: Unit III on real data. One raw month is read as plain text lines and taken through the RDD
 * API — creation, transformations and actions, both kinds of shared variable, persistence levels, pair-RDD
 * shuffles, lineage and three ways to become a DataFrame — with every claim backed by a measured number in
 * `rdd-fundamentals.json`.
 */
object RddFundamentalsJob extends SparkJob {

  val jobName = "rdd-fundamentals"

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark = ctx.spark
    val sc    = spark.sparkContext
    import spark.implicits._

    val meter = new ShuffleMeter(sc)
    val month = ctx.cfg.raw.getString("velograph.rdd-demo.month")

    // 1. Creating RDDs: from storage, from a DataFrame, and from a collection on the driver.
    val lines: RDD[String] = sc.textFile(ctx.paths.raw.resolve("citibike").resolve(month).uri)
    val stationNames: RDD[(String, String)] = new LakeTables(spark, ctx.paths)
      .read(LakeTable.Stations)
      .select("station_id", "name")
      .rdd
      .map(row => (row.getString(0), row.getString(1)))
    val hoursOfDay: RDD[Int] = sc.parallelize(0 until 24, numSlices = 1)

    // 2. Shared variable #1, accumulators. Spark applies an action's updates exactly once per element; updates made
    // inside a transformation repeat whenever Spark recomputes it, here because two actions share an uncached RDD.
    val seenInAction = sc.longAccumulator("lines seen in foreach (an action)")
    lines.foreach(_ => seenInAction.add(1))
    val seenInMap = sc.longAccumulator("lines seen in map (a transformation)")
    val tagged    = lines.map { line => seenInMap.add(1); line }
    tagged.count()
    tagged.count()
    val headers  = sc.longAccumulator("header lines")
    val rejected = sc.longAccumulator("data lines the parser rejects")
    lines.foreach { line =>
      if (line.startsWith("\"ride_id\"") || line.startsWith("ride_id")) headers.add(1)
      else if (CsvLine.parseTrip(line).isEmpty) rejected.add(1)
    }
    val accumulators = AccumulatorDemo(seenInAction.sum, seenInMap.sum, headers.sum, rejected.sum)

    // 3. Persistence. Each level gets a fresh lineage, so no run can reuse another's cached blocks; the second count
    // shows what reuse costs. MEMORY_ONLY_SER stays cached: it serves the rest of the job and the Storage tab.
    def parsed(): RDD[TripLite] =
      lines.flatMap(CsvLine.parseTrip) // flatMap over Option drops the headers and rejected lines
    val levels = Seq(
      "NONE"            -> StorageLevel.NONE,
      "MEMORY_ONLY"     -> StorageLevel.MEMORY_ONLY,
      "MEMORY_ONLY_SER" -> StorageLevel.MEMORY_ONLY_SER,
      "DISK_ONLY"       -> StorageLevel.DISK_ONLY
    )
    val runs = levels.map { case (name, level) =>
      val rdd = parsed().setName(s"trips $month ($name)")
      if (level != StorageLevel.NONE) rdd.persist(level)
      val (_, firstMs)  = timed(rdd.count())
      val (_, secondMs) = timed(rdd.count())
      val storage       = sc.getRDDStorageInfo.find(_.id == rdd.id)
      (
        name,
        rdd,
        PersistenceRun(
          name,
          firstMs,
          secondMs,
          storage.map(_.memSize).getOrElse(0L),
          storage.map(_.diskSize).getOrElse(0L)
        )
      )
    }
    runs.foreach { case (name, rdd, _) => if (name != "MEMORY_ONLY_SER") rdd.unpersist(blocking = true) }
    val trips: RDD[TripLite] = runs.collectFirst { case ("MEMORY_ONLY_SER", rdd, _) => rdd }.get
    val tripCount            = trips.count()

    // 4. Pair RDDs: groupByKey ships every (station, 1) record across the shuffle; reduceByKey adds them up inside
    // each partition first. Same answer, very different traffic (bounded collects: ~2.5k stations).
    val pairs   = trips.map(t => (t.startStationId, 1L))
    val grouped = meter.measure("groupByKey")(pairs.groupByKey().mapValues(_.size.toLong).collect().toMap)
    val reduced = meter.measure("reduceByKey")(pairs.reduceByKey(_ + _).collect().toMap)
    require(grouped.value == reduced.value, "groupByKey and reduceByKey disagree")

    // 5. Shared variable #2, a broadcast: enrich every trip with its station name without moving the trips.
    val names   = sc.broadcast(stationNames.collectAsMap().toMap) // bounded: ~2.6k stations
    val viaJoin =
      meter.measure("join")(trips.map(t => (t.startStationId, t.rideId)).join(stationNames).count())
    val viaBroadcast =
      meter.measure("broadcast")(trips.map(t =>
        (names.value.getOrElse(t.startStationId, "?"), t.rideId)
      ).count())

    // 6. More transformations and actions on the cached RDD.
    val departures = trips.map(t => (t.startStationId, 1L)).reduceByKey(_ + _)
    val top        = departures
      .takeOrdered(10)(Ordering.by[(String, Long), Long](-_._2))
      .map { case (id, n) => StationCount(id, names.value.getOrElse(id, id), n) }
    val byHour    = trips.map(t => (t.startHour, 1L)).reduceByKey(_ + _)
    val everyHour = hoursOfDay
      .map(hour => (hour, ()))
      .leftOuterJoin(byHour) // zero-fill hours without trips, like the gold panel
      .map { case (hour, (_, n)) => HourCount(hour, n.getOrElse(0L)) }
      .sortBy(_.hour)
      .collect() // bounded: 24 rows
    val stationVisits = trips.flatMap(t => t.startStationId +: t.endStationId.toSeq).count()
    val members       = trips.filter(_.member).count()
    val byType        = trips.map(_.rideableType).countByValue().toMap // bounded: 2 bike types
    require(trips.map(_ => 1L).reduce(_ + _) == tripCount, "reduce and count disagree")

    // 7. Lineage: the recipe Spark replays to rebuild a lost partition.
    val lineage = departures.toDebugString.split("\n").map(_.replaceAll("\\s+$", "")).toSeq

    // 8. RDD -> DataFrame three ways, then Spark SQL on top of the result.
    val viaReflection  = trips.toDF() // case-class fields become columns
    val explicitSchema = StructType(
      Seq(
        StructField("ride_id", StringType),
        StructField("station_id", StringType),
        StructField("hour", IntegerType),
        StructField("member", BooleanType)
      )
    )
    val viaSchema = spark.createDataFrame(
      trips.map(t => Row(t.rideId, t.startStationId, t.startHour, t.member)),
      explicitSchema
    )
    val viaTuples = trips.map(t => (t.startStationId, t.startHour)).toDF("station_id", "hour")
    viaReflection.createOrReplaceTempView("rdd_trips")
    val sqlMembers = spark.sql("SELECT COUNT(*) FROM rdd_trips WHERE member").head().getLong(0)
    require(sqlMembers == members, s"SQL counted $sqlMembers members, the RDD $members")

    val report = RddReport(
      month = month,
      inputPartitions = lines.getNumPartitions,
      lines = accumulators.inAction,
      trips = tripCount,
      accumulators = accumulators,
      persistence = runs.map(_._3),
      shuffle = Seq(
        ShuffleRun("groupByKey", grouped.millis, grouped.shuffleBytes, grouped.value.size.toLong),
        ShuffleRun("reduceByKey", reduced.millis, reduced.shuffleBytes, reduced.value.size.toLong),
        ShuffleRun("join", viaJoin.millis, viaJoin.shuffleBytes, viaJoin.value),
        ShuffleRun("broadcast", viaBroadcast.millis, viaBroadcast.shuffleBytes, viaBroadcast.value)
      ),
      tripsByHour = everyHour.toSeq,
      topStations = top.toSeq,
      members = members,
      stationVisits = stationVisits,
      byRideableType = byType,
      sample = trips.take(3).toSeq,
      lineage = lineage,
      rddToDataFrame = Map(
        "reflection"     -> viaReflection.count(),
        "explicitSchema" -> viaSchema.count(),
        "tuples"         -> viaTuples.count(),
        "sqlMembers"     -> sqlMembers
      )
    )
    ServingJson.write(
      ctx.paths.serving.resolve("rdd-fundamentals.json"),
      "rdd-fundamentals",
      jobName,
      report,
      rows = Some(tripCount)
    )

    log.info(
      f"rdd: $tripCount%,d trips; groupByKey shuffled ${grouped.shuffleBytes}%,d B vs reduceByKey " +
        f"${reduced.shuffleBytes}%,d B; accumulator in map counted ${accumulators.inTransformationAfterTwoActions}%,d"
    )
    Map(
      "trips"               -> tripCount,
      "groupByKeyBytes"     -> grouped.shuffleBytes,
      "reduceByKeyBytes"    -> reduced.shuffleBytes,
      "joinBytes"           -> viaJoin.shuffleBytes,
      "broadcastBytes"      -> viaBroadcast.shuffleBytes,
      "accumulatorInAction" -> accumulators.inAction,
      "accumulatorInMap"    -> accumulators.inTransformationAfterTwoActions,
      "cachedRecountMs"     -> runs.map(r => r._1 -> r._3.secondCountMs).toMap
    )
  }
}
