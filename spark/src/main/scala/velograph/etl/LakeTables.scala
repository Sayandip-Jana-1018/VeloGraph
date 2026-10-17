package velograph.etl

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import velograph.common.LakePaths
import velograph.common.LakePaths.PathOps

/** A lake table: its location under `data/lake` and its declared schema. */
final case class LakeTable(relativePath: String, schema: StructType)

object LakeTable {
  val BronzeTrips: LakeTable    = LakeTable("bronze/trips", LakeSchemas.BronzeTrip)
  val SilverTrips: LakeTable    = LakeTable("silver/trips", LakeSchemas.SilverTrip)
  val Stations: LakeTable       = LakeTable("silver/stations", LakeSchemas.SilverStation)
  val WeatherHourly: LakeTable  = LakeTable("silver/weather_hourly", LakeSchemas.SilverWeatherHourly)
  val Calendar: LakeTable       = LakeTable("silver/calendar", LakeSchemas.SilverCalendar)
  val StationHour: LakeTable    = LakeTable("gold/station_hour", LakeSchemas.GoldStationHour)
  val RouteEdges: LakeTable     = LakeTable("gold/route_edges", LakeSchemas.GoldRouteEdge)
  val Archetypes: LakeTable     = LakeTable("gold/station_archetypes", LakeSchemas.GoldStationArchetype)
  val MlFeatures: LakeTable     = LakeTable("gold/ml_features", LakeSchemas.GoldMlFeature)
  val LiveStations: LakeTable   = LakeTable("gold/live_stations", LakeSchemas.GoldLiveStation)
  val LiveProfiles: LakeTable   = LakeTable("gold/live_profiles", LakeSchemas.GoldLiveProfile)
  val StationMetrics: LakeTable = LakeTable("graph/station_metrics", LakeSchemas.GraphStationMetrics)
}

/** Reads and locates lake tables, always with their declared schema. */
final class LakeTables(spark: SparkSession, paths: LakePaths) {

  def path(table: LakeTable): Path = paths.lake.resolve(table.relativePath)

  def uri(table: LakeTable): String = path(table).uri

  def read(table: LakeTable): DataFrame = spark.read.schema(table.schema).parquet(uri(table))
}
