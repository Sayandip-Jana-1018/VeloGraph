package velograph.sql

import scala.io.Source

/**
 * One named Spark SQL insight, read from `resources/sql/insights/<nn>_<name>.sql`. Each file starts with
 * three metadata comments, then the query:
 * {{{
 * -- title: When does New York ride?
 * -- question: How many trips start in each hour of the day ...?
 * -- chart: line x=hour y=avg_trips series=day_type
 * SELECT ...
 * }}}
 */
final case class InsightQuery(
    file: String,
    id: String,
    title: String,
    question: String,
    chart: Map[String, String],
    sql: String
)

object InsightQueries {

  private val Dir      = "/sql/insights/"
  private val MetaKeys = Seq("title", "question", "chart")

  /** Every query listed in `index.txt`, in display order. */
  def all(): Seq[InsightQuery] =
    resource(Dir + "index.txt").linesIterator
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#"))
      .map(load)
      .toList

  def load(file: String): InsightQuery = {
    val lines                      = resource(Dir + file).linesIterator.toList
    def isMeta(line: String)       = MetaKeys.exists(key => line.startsWith(s"-- $key:"))
    val (meta, body)               = lines.partition(isMeta)
    def field(key: String): String =
      meta
        .collectFirst { case line if line.startsWith(s"-- $key:") => line.stripPrefix(s"-- $key:").trim }
        .getOrElse(throw new IllegalArgumentException(s"$file has no '-- $key:' line"))

    InsightQuery(
      file = file,
      id = file.stripSuffix(".sql").replaceFirst("^\\d+_", "").replace('_', '-'),
      title = field("title"),
      question = field("question"),
      chart = parseChart(field("chart")),
      sql = body.mkString("\n").trim
    )
  }

  /**
   * `"line x=hour y=trips series=day_type"` ->
   * `Map(type -> line, x -> hour, y -> trips, series -> day_type)`.
   */
  def parseChart(spec: String): Map[String, String] =
    spec.trim.split("\\s+").toList match {
      case kind :: options =>
        options.map { option =>
          option.split("=", 2) match {
            case Array(key, value) => key -> value
            case _ => throw new IllegalArgumentException(s"bad chart option '$option' in '$spec'")
          }
        }.toMap + ("type" -> kind)
      case Nil => Map.empty
    }

  private def resource(path: String): String = {
    val stream = getClass.getResourceAsStream(path)
    require(stream != null, s"missing resource $path")
    val source = Source.fromInputStream(stream, "UTF-8")
    try source.mkString
    finally source.close()
  }
}
