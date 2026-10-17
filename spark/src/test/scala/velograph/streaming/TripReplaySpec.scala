package velograph.streaming

import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

import velograph.testkit.SharedSparkSession

class TripReplaySpec extends AnyFunSuite with SharedSparkSession {

  private def trip(ride: String, started: Long, station: String, late: Boolean = false) =
    s"""{"schema":"velograph.trip","v":1,"source":"replay","ride_id":"$ride","rideable_type":"electric_bike",""" +
      s""""member_casual":"member","started_at":$started.0,"ended_at":${started + 600}.0,""" +
      s""""start_station_id":"$station","end_station_id":"x","duration_s":600,"late":$late,"late_by_s":0}"""

  private def trips(values: String*) = {
    import spark.implicits._
    TripReplayAnalyticsJob.parse(
      values.map(v => (v.getBytes("UTF-8"), new Timestamp(0L))).toDF("value", "timestamp")
    )
  }

  // 2026-09-17 16:00 New York = 20:00 UTC
  private val four = 1789675200L

  test("trips parse with their start as event time, and other messages are left out") {
    val parsed = trips(trip("r1", four, "a"), "{}", """{"schema":"velograph.trip"}""")
    assert(parsed.select(date_format(col("event_time"), "HH:mm")).as[String](
      org.apache.spark.sql.Encoders.STRING
    ).collect().toSeq == Seq("16:00"))
  }

  test("tumbling windows count departures per station; sliding windows hold each trip four times") {
    val parsed = trips(
      trip("r1", four, "a"),
      trip("r2", four + 300, "a", late = true),
      trip("r3", four + 960, "a"), // 16:16, the next 15-minute window
      trip("r4", four + 60, "b")
    )
    val tumbling = TripReplayAnalyticsJob
      .perStation(parsed, "15 minutes")
      .select(
        date_format(col("window.start"), "HH:mm"),
        col("start_station_id"),
        col("departures"),
        col("late")
      )
      .collect()
      .map(r => (r.getString(0), r.getString(1), r.getLong(2), r.getLong(3)))
      .toSet
    assert(tumbling == Set(("16:00", "a", 2L, 1L), ("16:15", "a", 1L, 0L), ("16:00", "b", 1L, 0L)))

    val sliding = TripReplayAnalyticsJob.system(parsed, "60 minutes", "15 minutes")
    assert(sliding.agg(sum("trips")).head().getLong(0) == 4 * 4) // every trip falls in 60 / 15 = 4 windows
  }
}
