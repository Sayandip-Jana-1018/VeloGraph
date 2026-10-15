package velograph.etl

import org.apache.spark.sql.functions.{col, lit, to_date}
import org.scalatest.funsuite.AnyFunSuite

import velograph.testkit.SharedSparkSession

/** Same shape as a bronze row: every source field is still text. */
final case class BronzeRow(
    ride_id: String,
    rideable_type: String,
    started_at: String,
    ended_at: String,
    start_station_name: String,
    start_station_id: String,
    end_station_name: String,
    end_station_id: String,
    start_lat: String,
    start_lng: String,
    end_lat: String,
    end_lng: String,
    member_casual: String,
    _corrupt_record: String,
    _source_file: String,
    ym: String
)

class SilverTripsTransformsSpec extends AnyFunSuite with SharedSparkSession {

  import SilverTripsTransforms._

  private val rules = DqRules(60, 14400, 45.0, 40.45, 41.0, -74.30, -73.65)

  /** A valid ten-minute member ride from Times Square to Grand Central; each test breaks one thing. */
  private def trip(
      id: String,
      start: String = "2026-09-01 08:00:00.000",
      end: String = "2026-09-01 08:10:00.000",
      rideable: String = "classic_bike",
      rider: String = "member",
      startLat: String = "40.7580",
      startLng: String = "-73.9855",
      endLat: String = "40.7527",
      endLng: String = "-73.9772",
      corrupt: String = null,
      file: String = "202609-citibike-tripdata_1.csv"
  ): BronzeRow =
    BronzeRow(
      id,
      rideable,
      start,
      end,
      "Times Sq",
      "6726.01",
      "Grand Central",
      "6432.11",
      startLat,
      startLng,
      endLat,
      endLng,
      rider,
      corrupt,
      file,
      "202609"
    )

  private def haversine(lat1: Double, lng1: Double, lat2: Double, lng2: Double): Double = {
    val (dLat, dLng) = (math.toRadians(lat2 - lat1), math.toRadians(lng2 - lng1))
    val a            = math.pow(math.sin(dLat / 2), 2) +
      math.cos(math.toRadians(lat1)) * math.cos(math.toRadians(lat2)) * math.pow(math.sin(dLng / 2), 2)
    2 * 6371008.8 * math.asin(math.sqrt(a))
  }

  test("haversine agrees with a plain-Scala implementation (Times Square to Grand Central, ~0.9 km)") {
    import spark.implicits._
    val actual = spark
      .range(1)
      .select(haversineMeters(lit(40.7580), lit(-73.9855), lit(40.7527), lit(-73.9772)))
      .as[Double]
      .head()
    assert(math.abs(actual - haversine(40.7580, -73.9855, 40.7527, -73.9772)) < 1e-6)
    assert(actual > 850 && actual < 1000)
  }

  test("station ids that lost their trailing zero get it back; everything else is untouched") {
    import spark.implicits._
    val ids = Seq("5980.1", "5980.10", "6432.11", "JC102", "HB101", "123")
      .toDF("id")
      .select(normalizeStationId($"id"))
      .as[String]
      .collect()
    assert(ids.toSeq == Seq("5980.10", "5980.10", "6432.11", "JC102", "HB101", "123"))
  }

  test("ISO day of week counts Monday as 1 and Sunday as 7") {
    import spark.implicits._
    val days = Seq("2026-10-12", "2026-10-17", "2026-10-18")
      .toDF("d")
      .select(isoDayOfWeek(to_date($"d")))
      .as[Int]
      .collect()
    assert(days.toSeq == Seq(1, 6, 7))
  }

  test("every data-quality rule rejects its own fault, and a valid trip passes") {
    import spark.implicits._
    val cases = Seq(
      trip("ok")                                            -> None,
      trip("malformed", corrupt = "a,b,c")                  -> Some("MALFORMED_CSV_ROW"),
      trip(null)                                            -> Some("MISSING_RIDE_ID"),
      trip("bad-time", start = "yesterday")                 -> Some("UNPARSEABLE_TIMESTAMP"),
      trip("backwards", end = "2026-09-01 07:50:00.000")    -> Some("NON_POSITIVE_DURATION"),
      trip("too-short", end = "2026-09-01 08:00:30.000")    -> Some("DURATION_TOO_SHORT"),
      trip("too-long", end = "2026-09-01 13:00:00.000")     -> Some("DURATION_TOO_LONG"),
      trip("scooter", rideable = "scooter")                 -> Some("UNKNOWN_RIDEABLE_TYPE"),
      trip("guest", rider = "guest")                        -> Some("UNKNOWN_RIDER_TYPE"),
      trip("no-gps", startLat = "", startLng = "")          -> Some("MISSING_START_COORD"),
      trip("null-island", startLat = "0", startLng = "0")   -> Some("START_COORD_OUT_OF_AREA"),
      trip("lost-bike", endLat = "", endLng = "")           -> Some("MISSING_END_COORD"),
      trip("far-away", endLat = "42.36", endLng = "-71.06") -> Some("END_COORD_OUT_OF_AREA"),
      trip("teleport", end = "2026-09-01 08:01:00.000", endLat = "40.85", endLng = "-73.90") ->
        Some("IMPOSSIBLE_SPEED"),
      trip("dup", file = "a.csv") -> None,
      trip("dup", file = "b.csv") -> Some("DUPLICATE_RIDE_ID")
    )
    val classified = classify(cases.map(_._1).toDF(), rules)
      .select($"ride_id", $"_source_file", $"reject_reason")
      .as[(Option[String], String, Option[String])]
      .collect()
      .map { case (id, file, reason) => (id.getOrElse("<null>"), file) -> reason }
      .toMap

    cases.foreach { case (row, expected) =>
      val key = (Option(row.ride_id).getOrElse("<null>"), row._source_file)
      assert(classified(key) == expected, s"trip ${key._1} from ${key._2}")
    }
  }

  test("derived columns of a valid trip: duration, distance, round-trip flag, calendar keys") {
    import spark.implicits._
    val row = classify(Seq(trip("ok")).toDF(), rules).head()
    assert(row.getAs[Int]("duration_s") == 600)
    assert(math.abs(row.getAs[Double]("distance_m") - haversine(40.7580, -73.9855, 40.7527, -73.9772)) < 1e-6)
    assert(!row.getAs[Boolean]("is_round_trip"))
    assert(row.getAs[Int]("start_hour") == 8)
    assert(row.getAs[Int]("dow") == 2) // Tuesday 1 Sep 2026
  }

  test("what the silver job writes matches TripSchemas.SilverTrip, the schema every reader uses") {
    import spark.implicits._
    val written = classify(Seq(trip("ok")).toDF(), rules).select(SilverColumns.map(col): _*).schema
    assert(written.map(f => f.name -> f.dataType) == TripSchemas.SilverTrip.map(f => f.name -> f.dataType))
  }
}
