package velograph.etl

import org.apache.spark.sql.types._

/** Schemas of the raw inputs. Every read uses one of these: nothing is ever inferred (CLAUDE.md). */
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

  /**
   * `lake/bronze/trips`. Read with this schema: it also fixes the type of the partition column, which Spark
   * would otherwise guess from the folder names (`ym=202609` looks like an integer).
   */
  val BronzeTrip: StructType = RawTrip
    .add("_source_file", StringType)
    .add("_ingested_at", TimestampType)
    .add("ym", StringType)

  /**
   * `lake/silver/trips`, partitioned by `start_date`; columns in `SilverTripsTransforms.SilverColumns` order.
   */
  val SilverTrip: StructType = StructType(
    Seq(
      StructField("ride_id", StringType),
      StructField("rideable_type", StringType),
      StructField("member_casual", StringType),
      StructField("started_at", TimestampType),
      StructField("ended_at", TimestampType),
      StructField("start_station_id", StringType),
      StructField("start_station_name", StringType),
      StructField("end_station_id", StringType),
      StructField("end_station_name", StringType),
      StructField("start_lat", DoubleType),
      StructField("start_lng", DoubleType),
      StructField("end_lat", DoubleType),
      StructField("end_lng", DoubleType),
      StructField("duration_s", IntegerType),
      StructField("distance_m", DoubleType),
      StructField("speed_kmh", DoubleType),
      StructField("is_round_trip", BooleanType),
      StructField("start_hour", IntegerType),
      StructField("dow", IntegerType),
      StructField("start_date", DateType)
    )
  )

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
