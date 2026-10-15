package velograph.etl

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.scalatest.funsuite.AnyFunSuite

import velograph.common.LakePaths.PathOps
import velograph.testkit.SharedSparkSession

class ReferenceTablesSpec extends AnyFunSuite with SharedSparkSession {

  // Instants built and compared in this JVM; anything shown as local time is formatted by Spark instead.
  private def ts(s: String) = Timestamp.valueOf(s)

  test("bronze lineage: month from the folder name, file name, malformed lines kept whole") {
    val month = Files.createDirectories(Files.createTempDirectory("velograph-raw").resolve("202609"))
    val csv   = TripSchemas.TripColumns.mkString(",") + "\n" +
      "R1,classic_bike,2026-09-01 08:00:00.000,2026-09-01 08:10:00.000,A,1,B,2,40.7,-73.9,40.71,-73.91,member\n" +
      "only,three,fields\n"
    Files.write(month.resolve("trips_1.csv"), csv.getBytes(StandardCharsets.UTF_8))

    val raw = spark.read
      .schema(TripSchemas.RawTrip)
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", TripSchemas.CorruptRecord)
      .csv(month.uri)
    val rows = BronzeTransforms.withIngestMetadata(raw).orderBy("ride_id").collect()

    assert(rows.map(_.getAs[String]("ym")).distinct.toSeq == Seq("202609"))
    assert(rows.map(_.getAs[String]("_source_file")).distinct.toSeq == Seq("trips_1.csv"))
    val malformed = rows.find(_.getAs[String]("ride_id") == "only").get
    assert(malformed.getAs[String](TripSchemas.CorruptRecord) == "only,three,fields")
    assert(rows.find(_.getAs[String]("ride_id") == "R1").get.getAs[String](TripSchemas.CorruptRecord) == null)
  }

  test("stations: most frequent name, median coordinates, trip counts per direction") {
    import spark.implicits._
    val endpoints = Seq(
      ("A", "Main St", 40.0, -73.0, ts("2026-06-01 08:00:00"), "out"),
      ("A", "Main St", 40.1, -73.1, ts("2026-07-01 09:00:00"), "in"),
      ("A", "Main Street", 50.0, -60.0, ts("2026-08-01 10:00:00"), "out"), // rare spelling, GPS outlier
      ("B", "Park Ave", 41.0, -74.0, ts("2026-09-01 11:00:00"), "in")
    ).toDF("station_id", "name", "lat", "lng", "ts", "direction")

    val a = StationTransforms.summarize(endpoints).where($"station_id" === "A").head()
    assert(a.getAs[String]("trip_name") == "Main St")
    assert(a.getAs[Double]("trip_lat") == 40.1)
    assert(a.getAs[Long]("trips_out") == 2 && a.getAs[Long]("trips_in") == 1)
    assert(a.getAs[Timestamp]("first_seen") == ts("2026-06-01 08:00:00"))
  }

  test("station dimension keeps trip-only and GBFS-only stations and prefers GBFS names") {
    import spark.implicits._
    val trips = Seq(("A", "Old Name", 40.0, -73.0, 5L, 7L), ("C", "Closed St", 40.2, -73.2, 1L, 0L))
      .toDF("station_id", "trip_name", "trip_lat", "trip_lng", "trips_out", "trips_in")
      .withColumn("first_seen", current_timestamp())
      .withColumn("last_seen", current_timestamp())
    val gbfs = Seq(
      ("A", "uuid-a", "New Name", 40.01, -73.01, 19, "71"),
      ("N", "uuid-n", "Brand New", 40.3, -73.3, 0, "71")
    )
      .toDF("short_name", "gbfs_station_id", "gbfs_name", "gbfs_lat", "gbfs_lon", "capacity", "region_id")

    val byId =
      StationTransforms.dimension(trips, gbfs).collect().map(r => r.getAs[String]("station_id") -> r).toMap
    assert(byId.keySet == Set("A", "C", "N"))
    assert(byId("A").getAs[String]("name") == "New Name" && byId("A").getAs[Boolean]("in_gbfs"))
    assert(!byId("C").getAs[Boolean]("in_gbfs") && byId("C").getAs[Boolean]("in_trips"))
    assert(byId("N").getAs[Long]("trips_out") == 0L && !byId("N").getAs[Boolean]("in_trips"))
  }

  test("weather: parallel arrays become hourly rows; hours without values are dropped") {
    import spark.implicits._
    val json =
      """{"timezone":"America/New_York","hourly":{"time":["2026-09-01T00:00","2026-09-01T01:00","2026-09-01T02:00"],
        |"temperature_2m":[20.5,19.8,null],"apparent_temperature":[21,20,null],"precipitation":[0.0,0.4,null],
        |"rain":[0,0.4,null],"snowfall":[0,0,null],"wind_speed_10m":[10,12,null],"wind_gusts_10m":[20,25,null],
        |"relative_humidity_2m":[70,75,null],"cloud_cover":[10,90,null],"weather_code":[0,61,null]}}""".stripMargin
        .replace("\n", "")
    val raw  = spark.read.schema(TripSchemas.OpenMeteoHourly).json(Seq(json).toDS())
    val rows = WeatherTransforms
      .hourly(raw)
      .withColumn("local", date_format($"ts_hour", "yyyy-MM-dd HH:mm")) // session zone, not the JVM's
      .orderBy("ts_hour")
      .collect()

    assert(rows.length == 2)
    assert(rows(0).getAs[String]("local") == "2026-09-01 00:00")
    assert(rows(1).getAs[Double]("precip_mm") == 0.4 && rows(1).getAs[Boolean]("is_raining"))
    assert(rows(1).getAs[Int]("weather_code") == 61 && rows(1).getAs[Int]("hour") == 1)
  }

  test("calendar flags weekends and both the observed and the actual Independence Day") {
    val days = CalendarTransforms
      .build(spark, "2026-07-01", "2026-07-07", CalendarTransforms.holidays(spark))
      .collect()
      .map(r => r.getAs[Date]("date").toString -> r)
      .toMap

    assert(days.size == 7)
    assert(days("2026-07-03").getAs[Boolean]("is_holiday") &&
      !days("2026-07-03").getAs[Boolean]("is_weekend"))
    assert(days("2026-07-04").getAs[Boolean]("is_holiday") && days("2026-07-04").getAs[Boolean]("is_weekend"))
    assert(days("2026-07-06").getAs[Int]("dow") == 1 && !days("2026-07-06").getAs[Boolean]("is_holiday"))
  }
}
