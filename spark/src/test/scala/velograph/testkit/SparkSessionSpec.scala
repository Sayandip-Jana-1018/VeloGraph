package velograph.testkit

import org.scalatest.funsuite.AnyFunSuite

class SparkSessionSpec extends AnyFunSuite with SharedSparkSession {

  test("a local session runs a job and reads timestamps in New York time") {
    import spark.implicits._
    val hours =
      Seq("2026-09-01 08:15:00").toDF("ts").selectExpr("hour(to_timestamp(ts)) AS h").as[Int].collect()
    assert(hours.toSeq == Seq(8))
    assert(spark.conf.get("spark.sql.session.timeZone") == "America/New_York")
  }
}
