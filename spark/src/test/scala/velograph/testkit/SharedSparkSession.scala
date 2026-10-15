package velograph.testkit

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/** One small local session per suite: two threads, no UI, few shuffle partitions, New York time zone. */
trait SharedSparkSession extends BeforeAndAfterAll { self: Suite =>

  @transient lazy val spark: SparkSession = SparkSession
    .builder()
    .master("local[2]")
    .appName(suiteName)
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.session.timeZone", "America/New_York")
    .getOrCreate()

  override def afterAll(): Unit =
    try spark.stop()
    finally super.afterAll()
}
