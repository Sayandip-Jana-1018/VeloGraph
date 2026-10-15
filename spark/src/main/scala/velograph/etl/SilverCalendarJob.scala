package velograph.etl

import java.sql.Date

import scala.io.Source

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import velograph.common.{JobContext, SparkJob}
import velograph.common.LakePaths.PathOps

final case class Holiday(date: Date, holiday_name: String, holiday_kind: String)

object CalendarTransforms {

  private val HolidaysResource = "/reference/us_federal_holidays_2026.csv"

  /** US federal holidays bundled in the jar, so the same list is used on the host and on the cluster. */
  def holidays(spark: SparkSession): DataFrame = {
    val source = Source.fromInputStream(getClass.getResourceAsStream(HolidaysResource), "UTF-8")
    val rows   =
      try
        source
          .getLines()
          .drop(1) // header
          .filter(_.trim.nonEmpty)
          .map { line =>
            val Array(date, name, kind) = line.split(",", 3)
            Holiday(Date.valueOf(date), name, kind)
          }
          .toList
      finally source.close()
    spark.createDataFrame(rows)
  }

  /** One row per day from `from` to `to` (inclusive) with ISO day of week, weekend and holiday flags. */
  def build(spark: SparkSession, from: String, to: String, holidays: DataFrame): DataFrame =
    spark
      .range(1)
      .select(explode(sequence(
        lit(from).cast("date"),
        lit(to).cast("date"),
        expr("INTERVAL 1 DAY")
      )).as("date"))
      .withColumn("dow", SilverTripsTransforms.isoDayOfWeek(col("date")))
      .withColumn("is_weekend", col("dow") >= 6)
      .withColumn("month", month(col("date")))
      .withColumn("iso_week", weekofyear(col("date")))
      .join(broadcast(holidays), Seq("date"), "left")
      .withColumn("is_holiday", col("holiday_name").isNotNull)
}

/** Roadmap P1.2: the date dimension `lake/silver/calendar`. */
object SilverCalendarJob extends SparkJob {

  val jobName = "silver-calendar"

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark    = ctx.spark
    val span     = ctx.cfg.raw.getConfig("velograph.calendar")
    val calendar = CalendarTransforms.build(
      spark,
      span.getString("from"),
      span.getString("to"),
      CalendarTransforms.holidays(spark)
    )

    calendar
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "static")
      .parquet(ctx.paths.silver.resolve("calendar").uri)

    val written  = spark.read.parquet(ctx.paths.silver.resolve("calendar").uri)
    val holidays = written.where(col("is_holiday")).orderBy("date").select("date", "holiday_name").collect()
    Map(
      "days"     -> written.count(),
      "from"     -> span.getString("from"),
      "to"       -> span.getString("to"),
      "holidays" -> holidays.map(r => s"${r.getDate(0)} ${r.getString(1)}").toSeq // bounded: a dozen rows
    )
  }
}
