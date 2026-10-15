package velograph.etl

import org.apache.spark.sql.types._

/**
 * Schemas of the raw inputs (CSV and JSON). Every read uses one: nothing is ever inferred (CLAUDE.md). The
 * lake's own tables are declared in [[LakeSchemas]].
 */
object TripSchemas {

  /** Columns of the Citi Bike trip CSVs, in file order (format in use since 2021). */
  val TripColumns: Seq[String] = Seq(
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual"
  )

  val CorruptRecord = "_corrupt_record"

  /**
   * Bronze reads every field as text: parsing is silver's job, so one malformed value can never fail
   * ingestion. A line whose shape does not fit is kept whole in `_corrupt_record` (PERMISSIVE mode).
   */
  val RawTrip: StructType =
    StructType(TripColumns.map(StructField(_, StringType)) :+ StructField(CorruptRecord, StringType))

  /** GBFS 2.3 `station_information.json`, as saved by the recorder; only the fields VeloGraph uses. */
  val GbfsStationInformation: StructType = StructType(
    Seq(
      StructField("last_updated", LongType),
      StructField(
        "data",
        StructType(
          Seq(
            StructField(
              "stations",
              ArrayType(
                StructType(
                  Seq(
                    StructField("station_id", StringType),
                    StructField("short_name", StringType),
                    StructField("name", StringType),
                    StructField("lat", DoubleType),
                    StructField("lon", DoubleType),
                    StructField("capacity", IntegerType),
                    StructField("region_id", StringType)
                  )
                )
              )
            )
          )
        )
      )
    )
  )

  /** Open-Meteo hourly variables -> silver column names (units: °C, mm, cm, km/h, %, WMO code). */
  val WeatherColumns: Seq[(String, String, DataType)] = Seq(
    ("temperature_2m", "temp_c", DoubleType),
    ("apparent_temperature", "apparent_temp_c", DoubleType),
    ("precipitation", "precip_mm", DoubleType),
    ("rain", "rain_mm", DoubleType),
    ("snowfall", "snow_cm", DoubleType),
    ("wind_speed_10m", "wind_kmh", DoubleType),
    ("wind_gusts_10m", "gust_kmh", DoubleType),
    ("relative_humidity_2m", "humidity_pct", DoubleType),
    ("cloud_cover", "cloud_pct", DoubleType),
    ("weather_code", "weather_code", IntegerType)
  )

  /** Open-Meteo archive response: parallel arrays under `hourly`, one entry per local hour. */
  val OpenMeteoHourly: StructType = StructType(
    Seq(
      StructField("timezone", StringType),
      StructField(
        "hourly",
        StructType(
          StructField("time", ArrayType(StringType)) +:
            WeatherColumns.map { case (source, _, dataType) => StructField(source, ArrayType(dataType)) }
        )
      )
    )
  )
}
