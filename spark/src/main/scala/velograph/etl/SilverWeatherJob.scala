package velograph.etl

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

import velograph.common.{JobContext, SparkJob}
import velograph.common.LakePaths.PathOps

object WeatherTransforms {

  /**
   * Open-Meteo's parallel arrays -> one row per local hour; hours without values (archive lag) are dropped.
   */
  def hourly(raw: DataFrame): DataFrame = {
    val sources = "time" +: TripSchemas.WeatherColumns.map(_._1)
    val zipped  = raw.select(explode(arrays_zip(sources.map(name => col(s"hourly.$name")): _*)).as("h"))
    zipped
      .select(
        to_timestamp(col("h.time"), "yyyy-MM-dd'T'HH:mm").as("ts_hour") +:
          TripSchemas.WeatherColumns.map { case (source, target, _) => col(s"h.$source").as(target) }: _*
      )
      .where(col("temp_c").isNotNull)
      .withColumn("date", to_date(col("ts_hour")))
      .withColumn("hour", hour(col("ts_hour")))
      .withColumn("is_raining", col("precip_mm") >= 0.1)
  }
}

/** Roadmap P1.7: `data/raw/weather/nyc_hourly.json` -> `lake/silver/weather_hourly`. */
object SilverWeatherJob extends SparkJob {

  val jobName = "silver-weather"

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark = ctx.spark
    val raw   = spark.read
      .schema(TripSchemas.OpenMeteoHourly)
      .option("multiLine", "true")
      .json(ctx.paths.raw.resolve("weather").resolve("nyc_hourly.json").uri)

    val weather = WeatherTransforms.hourly(raw).cache() // written, then summarised
    weather
      .coalesce(1) // ~3k rows: one small file
      .write
      .mode(SaveMode.Overwrite)
      .option("partitionOverwriteMode", "static")
      .parquet(ctx.paths.silver.resolve("weather_hourly").uri)

    val s = weather
      .agg(
        count(lit(1)).as("hours"),
        // Formatted by Spark in the session zone (New York): a collected java.sql.Timestamp would print in the
        // JVM's default zone instead.
        date_format(min("ts_hour"), "yyyy-MM-dd HH:mm").as("first"),
        date_format(max("ts_hour"), "yyyy-MM-dd HH:mm").as("last"),
        round(avg("temp_c"), 2).as("meanTempC"),
        round(sum("precip_mm"), 1).as("totalPrecipMm"),
        count(when(col("is_raining"), 1)).as("rainyHours")
      )
      .head() // bounded: 1 row
    weather.unpersist()

    Map(
      "hours"         -> s.getAs[Long]("hours"),
      "firstHour"     -> s.getAs[String]("first"),
      "lastHour"      -> s.getAs[String]("last"),
      "meanTempC"     -> s.getAs[Double]("meanTempC"),
      "totalPrecipMm" -> s.getAs[Double]("totalPrecipMm"),
      "rainyHours"    -> s.getAs[Long]("rainyHours")
    )
  }
}
