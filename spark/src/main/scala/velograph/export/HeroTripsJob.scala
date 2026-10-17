package velograph.export

import java.time.{LocalDate, LocalTime}

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

import velograph.common.{JobContext, ServingJson, SourceRange, SparkJob}
import velograph.etl.{LakeTable, LakeTables}

final case class HeroWindow(date: String, from: String, until: String, sampleEvery: Int)
final case class HeroMinute(minute: Int, underWay: Long)
final case class HeroTrips(
    window: HeroWindow,
    totalTrips: Long,
    shownTrips: Int,
    stations: Seq[Seq[Double]],
    trips: Seq[Seq[Int]],
    underWay: Seq[HeroMinute]
)

/**
 * Roadmap P6.4: the trips of one real evening for the home page's replay, `serving/hero-trips.json`.
 *
 *   - `trips`: one trip in `sample-every` (chosen by a hash of the ride id, so a re-run picks the same ones),
 *     as [start station, end station, start second, duration in seconds], seconds counted from the window's
 *     start.
 *   - `stations`: [lon, lat] of every station those trips use, indexed by the trips.
 *   - `underWay`: for every minute, how many rides of the evening, all of them and not just the sample, were
 *     under way. That is the counter the hero shows.
 *
 * Round trips are left out: they start and end at one dock, so they would not move on the map.
 */
object HeroTripsJob extends SparkJob {

  val jobName = "hero-trips"

  /** Seconds from the window's start; the session time zone is New York's, like the trip times. */
  private def secondsFrom(start: Column, t: Column): Column = unix_timestamp(t) - unix_timestamp(start)

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark = ctx.spark
    import spark.implicits._
    val c      = ctx.cfg.raw.getConfig("velograph.hero")
    val window =
      HeroWindow(c.getString("date"), c.getString("from"), c.getString("until"), c.getInt("sample-every"))
    val day      = LocalDate.parse(window.date)
    val (t0, t1) = (LocalTime.parse(window.from), LocalTime.parse(window.until))
    require(t0.isBefore(t1), s"the hero window ${window.from}-${window.until} is empty")
    val start = to_timestamp(lit(s"$day ${t0}:00"))
    val end   = to_timestamp(lit(s"$day ${t1}:00"))
    val lake  = new LakeTables(spark, ctx.paths)

    val evening: DataFrame = lake
      .read(LakeTable.SilverTrips)
      .where(col("start_date") === lit(day.toString))
      .where(col("started_at") >= start && col("started_at") < end)
      .where(col("start_station_id").isNotNull && col("end_station_id").isNotNull && !col("is_round_trip"))
      .select("ride_id", "started_at", "ended_at", "start_station_id", "end_station_id")
      .cache()
    val totalTrips = evening.count()

    // Rides under way in each minute of the window, from every trip of the evening.
    val minutes  = (t1.toSecondOfDay - t0.toSecondOfDay) / 60
    val underWay = evening
      .select(
        explode(
          sequence(
            greatest(lit(0), floor(secondsFrom(start, col("started_at")) / 60)).cast("int"),
            least(lit(minutes - 1), floor((secondsFrom(start, col("ended_at")) - 1) / 60)).cast("int")
          )
        ).as("minute")
      )
      .groupBy("minute")
      .count()
      .orderBy("minute")
      .as[(Int, Long)]
      .collect() // bounded: one row per minute of the window (180)
      .map { case (m, n) => HeroMinute(m, n) }
      .toSeq

    val sample = evening
      .where(pmod(xxhash64(col("ride_id")), lit(window.sampleEvery.toLong)) === 0)
      .select(
        col("start_station_id"),
        col("end_station_id"),
        secondsFrom(start, col("started_at")).cast("int").as("t"),
        greatest(
          lit(60L),
          unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))
        ).cast("int").as("d")
      )
      .orderBy("t", "start_station_id")
      .as[(String, String, Int, Int)]
      .collect() // bounded: one trip in sample-every of one evening, about 10k rows
      .toSeq

    val used   = (sample.map(_._1) ++ sample.map(_._2)).distinct.sorted
    val coords = lake
      .read(LakeTable.Stations)
      .where(col("station_id").isin(used: _*))
      .select("station_id", "lon", "lat")
      .as[(String, Double, Double)]
      .collect() // bounded: the stations of the sample, at most one per station (~2.5k)
      .map { case (id, lon, lat) => id -> Seq(math.rint(lon * 1e5) / 1e5, math.rint(lat * 1e5) / 1e5) }
      .toMap
    val known = used.filter(coords.contains)
    val index = known.zipWithIndex.toMap
    val trips = sample.collect {
      case (a, b, t, d) if index.contains(a) && index.contains(b) => Seq(index(a), index(b), t, d)
    }

    val report = HeroTrips(window, totalTrips, trips.size, known.map(coords), trips, underWay)
    ServingJson.write(
      ctx.paths.serving.resolve("hero-trips.json"),
      "hero-trips",
      jobName,
      report,
      rows = Some(trips.size.toLong),
      source = Some(SourceRange(window.date, window.date)),
      pretty = false
    )
    val peak = underWay.maxBy(_.underWay)
    log.info(
      f"${window.date} ${window.from}-${window.until}: $totalTrips%,d trips, ${trips.size}%,d shown over " +
        s"${known.size} stations; peak ${peak.underWay} rides under way at minute ${peak.minute}"
    )
    Map(
      "window"       -> s"${window.date} ${window.from}-${window.until}",
      "totalTrips"   -> totalTrips,
      "shownTrips"   -> trips.size,
      "stations"     -> known.size,
      "peakUnderWay" -> peak.underWay,
      "peakMinute"   -> peak.minute
    )
  }
}
