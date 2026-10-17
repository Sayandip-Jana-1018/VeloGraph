package velograph.streaming

import java.sql.Timestamp

import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

import velograph.etl.LakeSchemas
import velograph.ml.FeatureBuildJob
import velograph.testkit.SharedSparkSession

class LiveTransformsSpec extends AnyFunSuite with SharedSparkSession {

  private def message(id: String, bikes: Int, lastReported: Long, fetchedAt: Double = 1.0e9): String =
    s"""{"schema":"velograph.station-status","v":1,"source":"live","kind":"delta","station_id":"$id",""" +
      s""""gbfs_id":"u-$id","name":"Station $id","lat":40.75,"lon":-73.99,"capacity":20,"bikes":$bikes,""" +
      s""""ebikes":0,"bikes_disabled":0,"docks":${20 - bikes},"docks_disabled":0,"is_installed":true,""" +
      s""""is_renting":true,"is_returning":true,"last_reported":$lastReported,"fetched_at":$fetchedAt}"""

  private def kafkaRecords(values: String*) = {
    import spark.implicits._
    values.zipWithIndex
      .map { case (v, i) => (v.getBytes("UTF-8"), new Timestamp(1000L * i), 0, i.toLong) }
      .toDF("value", "timestamp", "partition", "offset")
  }

  test("station messages parse with their event time; anything else is kept for the dead-letter queue") {
    val parsed = LiveTransforms
      .parse(kafkaRecords(
        message("5980.10", 3, 1792000000L),
        "not json",
        """{"schema":"velograph.weather"}""",
        """{"schema":"velograph.station-status","station_id":"x"}"""
      ))
      .select(col("station_id"), col("valid"), col("error"), unix_timestamp(col("event_time")))
      .collect()
      .map(r => (Option(r.getString(0)), r.getBoolean(1), Option(r.getString(2)), Option(r.get(3))))
      .toSeq
    assert(parsed == Seq(
      (Some("5980.10"), true, None, Some(1792000000L)),
      (None, false, Some("not a JSON object"), None),
      (None, false, Some("unexpected schema velograph.weather"), None),
      (Some("x"), false, Some("missing required fields"), None)
    ))
  }

  test("the newest report of a station wins, by last_reported and then by fetch time") {
    import spark.implicits._
    val rows = LiveTransforms.parse(kafkaRecords(
      message("a", 3, lastReported = 200),
      message("a", 7, lastReported = 100),
      message("b", 1, lastReported = 50, fetchedAt = 10),
      message("b", 2, lastReported = 50, fetchedAt = 20)
    ))
    val newest =
      LiveTransforms.newest(rows).select(LiveTransforms.StatusColumns: _*).as[StationStatus].collect()
    assert(newest.map(s => s.stationId -> s.bikes).toMap == Map("a" -> 3, "b" -> 2))
  }

  test("model rows carry exactly the training columns, with an unknown station given the defaults") {
    import spark.implicits._
    val statuses = Seq(
      StationStatus(
        "known",
        "K",
        40.75,
        -73.99,
        0,
        5,
        1,
        15,
        true,
        true,
        true,
        1L,
        1.0,
        0L
      ), // GBFS lists no capacity
      StationStatus("new", "N", 40.70, -74.00, 15, 5, 1, 10, true, true, true, 1L, 1.0, 0L)
    ).toDF()
    val hours = Seq(
      (0, 8, 2, false, false, 20.0, 21.0, 0.0, 9.0, 60.0, 40.0, false),
      (1, 9, 2, false, false, 21.0, 22.0, 0.2, 9.0, 60.0, 40.0, true)
    )
      .toDF(
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
    val stations = Seq((
      "known",
      "K",
      40.75,
      -73.99,
      27,
      true,
      1,
      "Morning origin",
      0.002,
      0.003,
      0.001,
      0.5,
      900L,
      800L,
      0.06,
      0.3,
      4
    ))
      .toDF(LakeSchemas.GoldLiveStation.fieldNames: _*)
    val profiles = Seq(("known", 2, 8, 6.0, 2.0, 1.5, 1.0), ("known", 2, 9, 4.0, 3.0, 1.0, 1.0))
      .toDF(LakeSchemas.GoldLiveProfile.fieldNames: _*)

    val rows  = LiveTransforms.featureRows(statuses, hours, stations, profiles, medianCapacity = 19)
    val types = LakeSchemas.GoldMlFeature.fields.map(f => f.name -> f.dataType).toMap
    assert(rows.schema.fields.drop(3).map(f => f.name -> f.dataType).toSeq ==
      FeatureBuildJob.Dictionary.map(f => f.name -> types(f.name)))

    val byStation = rows.where(col("slot") === 0).collect().map(r => r.getAs[String]("station_id") -> r).toMap
    val known     = byStation("known")
    assert(known.getAs[Int]("capacity") == 27) // the static capacity where GBFS lists none
    assert((known.getAs[Int]("archetype"), known.getAs[Int]("community")) == ((1, 4)))
    assert((known.getAs[Double]("prof_dep_mean"), known.getAs[Boolean]("has_profile")) == ((6.0, true)))
    val unknown = byStation("new")
    assert((
      unknown.getAs[Int]("capacity"),
      unknown.getAs[Int]("archetype"),
      unknown.getAs[Int]("community")
    ) == ((15, -1, 0)))
    // No profile of its own: the network-wide profile of that weekday-hour stands in, flagged.
    assert((unknown.getAs[Double]("prof_dep_mean"), unknown.getAs[Boolean]("has_profile")) == ((6.0, false)))
    assert(unknown.getAs[Double]("pagerank_w") == 0.0)
  }
}
