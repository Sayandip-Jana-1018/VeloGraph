package velograph.ml

import java.sql.Date
import java.time.LocalDate

import org.apache.spark.ml.linalg.Vector
import org.scalatest.funsuite.AnyFunSuite

import velograph.common.AppConfig
import velograph.gold.PeakHours
import velograph.testkit.SharedSparkSession

class ArchetypeTransformsSpec extends AnyFunSuite with SharedSparkSession {

  private val config = MlConfig(AppConfig.load(env = Map("VELOGRAPH_CONF_DIR" -> "no-such-dir")).raw)
  private val peaks  = PeakHours(7, 10, 16, 19)

  test("the configured windows are leakage-safe, and a profile window that overlaps its rows is refused") {
    for (split <- config.splits) assert(split.profile.to.isBefore(split.rows.from), split.toString)
    assert(config.validation.to == config.fit.rows.to)
    val august = DateWindow("fit", LocalDate.parse("2026-08-01"), LocalDate.parse("2026-08-31"))
    val leaky  = DateWindow("profile", LocalDate.parse("2026-07-07"), LocalDate.parse("2026-08-02"))
    val error  = intercept[IllegalArgumentException](FeatureWindow("fit", august, leaky))
    assert(error.getMessage.contains("leakage"))
  }

  test(
    "a profile is 96 hourly shares summing to 1, working days and days off in their own slots, and its bins"
  ) {
    import spark.implicits._
    val monday   = Date.valueOf("2026-08-03")
    val saturday = Date.valueOf("2026-08-08")
    val panel    = Seq(
      ("S", monday, 8, 3L, 1L, false, false),   // morning: three leave, one arrives
      ("S", monday, 17, 1L, 3L, false, false),  // evening: the reverse
      ("S", saturday, 12, 2L, 2L, true, false), // a day off
      ("Z", monday, 8, 0L, 0L, false, false)    // a station without trips has no profile
    ).toDF("station_id", "date", "hour", "departures", "arrivals", "is_weekend", "is_holiday")
    val window = DateWindow("w", LocalDate.parse("2026-08-01"), LocalDate.parse("2026-08-09"))

    val rows = ArchetypeTransforms.profiles(panel, window, binHours = 3).collect()
    assert(rows.map(_.getAs[String]("station_id")).toSeq == Seq("S"))
    val hourly = rows.head.getAs[Seq[Double]]("hourly")
    assert(hourly.length == 96 && math.abs(hourly.sum - 1.0) < 1e-12)
    val expected = Map(8 -> 3.0, 24 + 8 -> 1.0, 17 -> 1.0, 24 + 17 -> 3.0, 48 + 12 -> 2.0, 72 + 12 -> 2.0)
    hourly.zipWithIndex.foreach { case (share, slot) =>
      assert(math.abs(share - expected.getOrElse(slot, 0.0) / 12.0) < 1e-12, s"slot $slot")
    }
    // 3-hour bins: bin b sums slots 3b..3b+2, so hour 8 of the working-day departures lands in bin 2.
    val bins   = rows.head.getAs[Vector]("shares").toArray
    val binned = expected.toSeq.groupBy { case (slot, _) => slot / 3 }.map { case (b, xs) =>
      b -> xs.map(_._2).sum
    }
    assert(bins.length == 32)
    bins.zipWithIndex.foreach { case (share, b) =>
      assert(math.abs(share - binned.getOrElse(b, 0.0) / 12.0) < 1e-12, s"bin $b")
    }
    assert(rows.head.getAs[Long]("trips") == 12L)
  }

  test("a centre is named by its shape") {
    def shape(amDep: Double, amArr: Double, offScale: Double): DayShape = {
      val work = Vector.tabulate(24)(h => if (h == 8) 1.0 else 0.1)
      DayShape(
        work.updated(8, amDep),
        work.updated(8, amArr),
        work.map(_ * offScale),
        work.map(_ * offScale)
      )
    }
    val c = config.archetypes
    assert(ArchetypeTransforms.name(shape(3.0, 1.0, 0.5), peaks, c) == "Strong morning origin")
    assert(ArchetypeTransforms.name(shape(1.6, 1.0, 0.5), peaks, c) == "Morning origin")
    assert(ArchetypeTransforms.name(shape(1.0, 1.6, 0.5), peaks, c) == "Morning destination")
    assert(ArchetypeTransforms.name(shape(1.0, 1.1, 0.5), peaks, c) == "All-day mix")
    assert(ArchetypeTransforms.name(shape(3.0, 1.0, 1.5), peaks, c) == "Leisure") // a busier day off wins
  }

  test("repeated names are numbered in order") {
    assert(ArchetypeTransforms.distinct(Seq("Leisure", "Mix", "Leisure", "Leisure")) ==
      Seq("Leisure", "Mix", "Leisure 2", "Leisure 3"))
  }
}
