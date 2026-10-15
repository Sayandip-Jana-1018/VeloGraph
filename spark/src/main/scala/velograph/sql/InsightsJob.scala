package velograph.sql

import scala.collection.immutable.ListMap

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import velograph.common.{JobContext, ServingJson, SourceRange, SparkJob}
import velograph.common.Timing.timed
import velograph.etl.{LakeTable, LakeTables}
import velograph.gold.{GoldConfig, PanelSpan}

final case class ColumnInfo(name: String, dataType: String)

/** Contents of `data/serving/insights/<id>.json`. */
final case class InsightResult(
    id: String,
    title: String,
    question: String,
    chart: Map[String, String],
    sql: String,
    columns: Seq[ColumnInfo],
    rows: Seq[ListMap[String, Any]],
    elapsedMs: Long
)

final case class InsightSummary(
    id: String,
    title: String,
    question: String,
    chart: Map[String, String],
    rows: Int,
    elapsedMs: Long
)

/**
 * Roadmap P2.4: runs the 14 named Spark SQL insights over temp views of the lake and writes one JSON file per
 * insight (rows, columns, the SQL text and a chart hint for the website) plus an index.
 */
object InsightsJob extends SparkJob {

  val jobName = "insights"

  /** Insights are aggregates; a query returning more rows than this is a mistake, not a chart. */
  val MaxRows = 500

  /**
   * The views every insight may use, all restricted to the gold span so every number covers Jun-Sep exactly.
   */
  def registerViews(
      trips: DataFrame,
      stations: DataFrame,
      weather: DataFrame,
      calendar: DataFrame,
      stationHour: DataFrame,
      routes: DataFrame,
      span: PanelSpan
  ): Unit = {
    trips.where(col("start_date").between(span.from, span.to)).createOrReplaceTempView("trips")
    stations.createOrReplaceTempView("stations")
    weather.where(col("date").between(span.from, span.to)).createOrReplaceTempView("weather")
    calendar.where(col("date").between(span.from, span.to)).createOrReplaceTempView("calendar")
    stationHour.createOrReplaceTempView("station_hour")
    routes.createOrReplaceTempView("routes")
  }

  def execute(spark: SparkSession, query: InsightQuery): InsightResult = {
    val frame      = spark.sql(query.sql)
    val (rows, ms) = timed(frame.limit(MaxRows + 1).collect()) // bounded: MaxRows + 1
    require(rows.length <= MaxRows, s"${query.id} returned more than $MaxRows rows; insights must aggregate")
    val names = frame.schema.fieldNames
    InsightResult(
      id = query.id,
      title = query.title,
      question = query.question,
      chart = query.chart,
      sql = query.sql,
      columns = frame.schema.map(f => ColumnInfo(f.name, f.dataType.simpleString)),
      rows =
        rows.map(row => ListMap(names.indices.map(i => names(i) -> jsonValue(query.id, row.get(i))): _*)),
      elapsedMs = ms
    )
  }

  /**
   * JSON-friendly values. Timestamps must arrive as text, formatted in SQL in the session zone (CLAUDE.md).
   */
  private def jsonValue(id: String, value: Any): Any = value match {
    case date: java.sql.Date   => date.toString
    case _: java.sql.Timestamp =>
      throw new IllegalArgumentException(s"$id: format timestamps with date_format")
    case decimal: java.math.BigDecimal => decimal.doubleValue
    case other                         => other
  }

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val lake = new LakeTables(ctx.spark, ctx.paths)
    val span = GoldConfig.span(ctx.cfg.raw)
    registerViews(
      lake.read(LakeTable.SilverTrips),
      lake.read(LakeTable.Stations),
      lake.read(LakeTable.WeatherHourly),
      lake.read(LakeTable.Calendar),
      lake.read(LakeTable.StationHour),
      lake.read(LakeTable.RouteEdges),
      span
    )

    val out     = ctx.paths.serving.resolve("insights")
    val source  = Some(SourceRange(span.from, span.to))
    val results = InsightQueries.all().map { query =>
      val result = execute(ctx.spark, query)
      ServingJson.write(
        out.resolve(s"${result.id}.json"),
        "insight",
        jobName,
        result,
        rows = Some(result.rows.size.toLong),
        source = source
      )
      log.info(f"${result.id}%-28s ${result.rows.size}%3d rows in ${result.elapsedMs}%,6d ms")
      result
    }
    ServingJson.write(
      out.resolve("index.json"),
      "insight-index",
      jobName,
      results.map(r => InsightSummary(r.id, r.title, r.question, r.chart, r.rows.size, r.elapsedMs)),
      rows = Some(results.size.toLong),
      source = source
    )

    Map(
      "queries"       -> results.size,
      "totalMs"       -> results.map(_.elapsedMs).sum,
      "slowestMs"     -> results.map(_.elapsedMs).max,
      "elapsedMsById" -> ListMap(results.map(r => r.id -> r.elapsedMs): _*)
    )
  }
}
