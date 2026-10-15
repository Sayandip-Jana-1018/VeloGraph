package velograph.gold

import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

import velograph.etl.LakeSchemas
import velograph.testkit.{Fixtures, SharedSparkSession}

class GoldTransformsSpec extends AnyFunSuite with SharedSparkSession {

  private def namesAndTypes(fields: Seq[org.apache.spark.sql.types.StructField]) =
    fields.map(f => f.name -> f.dataType)

  private lazy val panel = Fixtures.stationHour(spark).cache()

  private def at(station: String, localHour: String) =
    panel.where(col("station_id") === station &&
      date_format(col("ts_hour"), "yyyy-MM-dd HH:mm") === localHour).head()

  test("the panel has a row for every hour of each station's active span, and no others") {
    // A: 12-14 Sep (72 h); B: first seen 14 Sep (24 h); C: only on 13 Sep (24 h).
    assert(panel.count() == 120)
    val perStation =
      panel.groupBy("station_id").count().collect().map(r => r.getString(0) -> r.getLong(1)).toMap
    assert(perStation == Map("A" -> 72L, "B" -> 24L, "C" -> 24L))
  }

  test("totals reconcile with the trips; an arrival after the span is not counted") {
    val totals = panel.agg(sum("departures"), sum("arrivals")).head()
    assert(totals.getLong(0) == 5L) // every trip starts inside the span
    assert(totals.getLong(1) == 4L) // t4 arrives on 15 Sep, outside it
  }

  test(
    "counts land in the right station-hour, quiet hours are zero-filled, net flow = arrivals - departures"
  ) {
    val amPeak = at("A", "2026-09-14 08:00")
    assert(amPeak.getAs[Long]("departures") == 1L && amPeak.getAs[Long]("dep_member") == 1L)
    val roundTrip = at("A", "2026-09-12 11:00")
    assert(roundTrip.getAs[Long]("departures") == 1L && roundTrip.getAs[Long]("arrivals") == 1L)
    assert(roundTrip.getAs[Long]("net_flow") == 0L && roundTrip.getAs[Long]("dep_ebike") == 1L)
    val quiet = at("A", "2026-09-13 03:00")
    assert(quiet.getAs[Long]("departures") == 0L && quiet.getAs[Long]("arrivals") == 0L)
    assert(at("B", "2026-09-14 08:00").getAs[Long]("net_flow") == 1L) // t1 arrives at B
  }

  test("calendar and weather are attached to every row") {
    val saturday = at("A", "2026-09-12 11:00")
    assert(saturday.getAs[Boolean]("is_weekend") && saturday.getAs[Int]("dow") == 6)
    val rainy = at("B", "2026-09-14 17:00")
    assert(rainy.getAs[Boolean]("is_raining") && rainy.getAs[Double]("precip_mm") == 2.0)
    assert(panel.where(col("temp_c").isNull || col("dow").isNull).count() == 0)
  }

  test("the panel writer produces exactly LakeSchemas.GoldStationHour") {
    assert(namesAndTypes(panel.schema) == namesAndTypes(LakeSchemas.GoldStationHour))
  }

  test("route edges split working-day peaks and weekends, and flag round trips") {
    val edges  = Fixtures.routes(spark)
    val byPair = edges.collect().map(r => (r.getAs[String]("src_id"), r.getAs[String]("dst_id")) -> r).toMap
    val ab     = byPair(("A", "B"))
    assert(ab.getAs[Long]("trips") == 2L && ab.getAs[Long]("trips_am_peak") == 1L) // t4 at 23:50 is off-peak
    assert(byPair(("B", "A")).getAs[Long]("trips_pm_peak") == 1L)
    val loop = byPair(("A", "A"))
    assert(loop.getAs[Boolean]("is_self_loop") && loop.getAs[Long]("trips_weekend") == 1L)
    assert(byPair(("C", "A")).getAs[Long]("trips_weekend") == 1L) // Sunday
    assert(namesAndTypes(edges.schema) == namesAndTypes(LakeSchemas.GoldRouteEdge))
  }
}
