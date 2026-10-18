package velograph.streaming

import org.scalatest.funsuite.AnyFunSuite

import velograph.testkit.SharedSparkSession

class LiveBacktestSpec extends AnyFunSuite with SharedSparkSession {

  private val hour = 3600L

  test("a prediction meets the last reading before its target, and the low point of its hour") {
    import spark.implicits._
    // Station "a": 10 bikes at t=0, down to 1 at t=1800, back to 4 at t=3000; the recorder polls every minute.
    val readings =
      Seq(("a", 0L, 10, 5), ("a", 1800L, 1, 14), ("a", 3000L, 4, 11)).toDF("stationId", "t", "bikes", "docks")
    val polls       = (0L to 3700L by 60L).map(_ / 60).toDF("minute")
    val predictions = Seq(("a", 0L, 10, 5, 3.5, "EMPTY_RISK", 2)).toDF(
      "stationId",
      "asOfS",
      "bikes",
      "docks",
      "projectedBikes",
      "risk",
      "margin"
    )
    val out = LiveBacktestTransforms.outcomes(predictions, readings, polls, hour, coverageMin = 3)
    val row = out.select("actualBikes", "minBikes").as[(Int, Int)].collect().toSeq
    assert(row == Seq((4, 1))) // bikes at t+1h: 4; the hour's low point: 1, under the margin of 2
    val hit = LiveBacktestTransforms.hits(out).select("cameTrue").as[Boolean].collect().toSeq
    assert(hit == Seq(true))
  }

  test("a prediction whose target time the recorder did not see is left out") {
    import spark.implicits._
    val readings    = Seq(("a", 0L, 10, 5)).toDF("stationId", "t", "bikes", "docks")
    val polls       = (0L to 600L by 60L).map(_ / 60).toDF("minute") // the recorder stopped after 10 minutes
    val predictions = Seq(("a", 0L, 10, 5, 9.0, "OK", 2)).toDF(
      "stationId",
      "asOfS",
      "bikes",
      "docks",
      "projectedBikes",
      "risk",
      "margin"
    )
    assert(LiveBacktestTransforms.outcomes(predictions, readings, polls, hour, coverageMin = 3).count() == 0)
  }
}
