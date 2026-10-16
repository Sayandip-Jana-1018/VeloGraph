package velograph.ml

import java.sql.Date
import java.time.LocalDate

import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

import velograph.testkit.SharedSparkSession

class FeatureTransformsSpec extends AnyFunSuite with SharedSparkSession {

  test("communities of another window take the ids of the reference districts they mostly fall in") {
    val reference = Map("a" -> 1, "b" -> 1, "c" -> 2, "d" -> 2, "e" -> 0)
    val other     = Map("a" -> 5, "b" -> 5, "c" -> 7, "d" -> 9, "e" -> 9, "f" -> 9)
    // 5 = {a, b}: all in 1. 7 = {c}: in 2. 9 = {d, e, f}: only d is in a reference district, 1 of 3, so it is new.
    assert(FeatureTransforms.alignCommunities(reference, other) == Map(5 -> 1, 7 -> 2, 9 -> 3, 0 -> 0))
  }

  test("an hour drains or fills a station only beyond tau of its capacity") {
    import spark.implicits._
    val rows    = Seq((-5L, 30), (2L, 30), (3L, 30), (4L, 30), (-3L, 30)).toDF("net_flow", "capacity")
    val classes = rows
      .select(FeatureTransforms.regime(col("net_flow"), col("capacity"), tau = 0.1))
      .as[Int]
      .collect()
      .toSeq
    // -5/30 drains; 2/30 is stable; exactly tau (3/30) is still stable; 4/30 fills.
    assert(classes == Seq(
      FeatureTransforms.Draining,
      FeatureTransforms.Stable,
      FeatureTransforms.Stable,
      FeatureTransforms.Filling,
      FeatureTransforms.Stable
    ))
  }

  test("hour-of-week profiles skip holidays, and a station without one gets the network's") {
    import spark.implicits._
    val panel = Seq(
      ("S", Date.valueOf("2026-08-03"), 1, 8, 4L, 1L, false),
      ("S", Date.valueOf("2026-08-10"), 1, 8, 2L, 1L, false),
      ("S", Date.valueOf("2026-08-17"), 1, 8, 100L, 9L, true) // a holiday Monday: not a typical Monday
    ).toDF("station_id", "date", "dow", "hour", "departures", "arrivals", "is_holiday")
    val window   = DateWindow("p", LocalDate.parse("2026-08-01"), LocalDate.parse("2026-08-31"))
    val profiles = FeatureTransforms.hourOfWeek(panel, window)
    val s        = profiles.head()
    assert(s.getAs[Double]("prof_dep_mean") == 3.0 && s.getAs[Double]("prof_dep_std") == 1.0)

    val rows       = Seq(("S", 1, 8), ("N", 1, 8)).toDF("station_id", "dow", "hour")
    val joined     = FeatureTransforms.withProfiles(rows, profiles).orderBy("station_id").collect()
    val (n, known) = (joined(0), joined(1))
    assert(!n.getAs[Boolean]("has_profile") && n.getAs[Double]("prof_dep_mean") == 3.0) // the network's mean
    assert(known.getAs[Boolean]("has_profile") && known.getAs[Double]("prof_arr_mean") == 1.0)
  }

  test("hours sit on a circle") {
    import spark.implicits._
    val (s, c) = FeatureTransforms.hourOnCircle(col("hour"))
    val points = Seq(0, 6, 23).toDF("hour").select(s, c).as[(Double, Double)].collect()
    assert(math.abs(points(0)._1) < 1e-12 && math.abs(points(0)._2 - 1) < 1e-12)
    assert(math.abs(points(1)._1 - 1) < 1e-12 && math.abs(points(1)._2) < 1e-12)
    val gap = math.hypot(points(2)._1 - points(0)._1, points(2)._2 - points(0)._2)
    assert(gap < 0.27) // 23:00 is as close to midnight as 01:00 is
  }
}
