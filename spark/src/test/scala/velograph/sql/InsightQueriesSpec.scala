package velograph.sql

import org.scalatest.funsuite.AnyFunSuite

import velograph.testkit.{Fixtures, SharedSparkSession}

class InsightQueriesSpec extends AnyFunSuite with SharedSparkSession {

  private lazy val queries = InsightQueries.all()

  test("all 14 insights load, with unique ids and complete metadata") {
    assert(queries.size == 14)
    assert(queries.map(_.id).distinct.size == 14)
    assert(queries.map(_.id).head == "hourly-demand")
    queries.foreach { q =>
      assert(q.title.nonEmpty && q.question.endsWith("?"), q.file)
      assert(Set("line", "bar", "barh", "table").contains(q.chart("type")), q.file)
      assert(!q.sql.contains("-- title:"), q.file)
    }
  }

  test("chart specs parse into key/value pairs") {
    assert(
      InsightQueries.parseChart("line x=hour y=avg_trips series=day_type") ==
        Map("type" -> "line", "x" -> "hour", "y" -> "avg_trips", "series" -> "day_type")
    )
  }

  test("every insight runs against the fixture lake and returns JSON-safe rows") {
    InsightsJob.registerViews(
      Fixtures.silverTrips(spark),
      Fixtures.stations(spark),
      Fixtures.weather(spark),
      Fixtures.calendar(spark),
      Fixtures.stationHour(spark),
      Fixtures.routes(spark),
      Fixtures.Span
    )
    queries.foreach { q =>
      val result = InsightsJob.execute(spark, q) // throws on SQL errors and on unformatted timestamps
      assert(result.rows.size <= InsightsJob.MaxRows, q.id)
      assert(result.columns.nonEmpty, q.id)
    }
    val hourly = InsightsJob.execute(spark, queries.head)
    assert(hourly.rows.map(_("day_type")).toSet == Set("working day", "weekend or holiday"))
  }
}
