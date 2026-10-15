package velograph.rdd

import scala.util.Try

import velograph.etl.StationIds

/**
 * One trip as the RDD demo needs it, in plain Scala types; `endStationId` is optional (bikes left off-dock).
 */
final case class TripLite(
    rideId: String,
    rideableType: String,
    member: Boolean,
    startHour: Int,
    startStationId: String,
    endStationId: Option[String]
)

object CsvLine {

  /**
   * Splits one CSV line, honouring double quotes: `"a,b"` stays one field and `""` inside quotes is a literal
   * quote. A small state machine, because `String.split(",")` cannot see quotes; the mutable state never
   * leaves this function.
   */
  def split(line: String): Vector[String] = {
    val fields   = Vector.newBuilder[String]
    val field    = new StringBuilder
    var inQuotes = false
    var i        = 0
    while (i < line.length) {
      val c = line.charAt(i)
      if (inQuotes) {
        if (c != '"') field += c
        else if (i + 1 < line.length && line.charAt(i + 1) == '"') { field += '"'; i += 1 }
        else inQuotes = false
      } else
        c match {
          case '"'   => inQuotes = true
          case ','   => fields += field.result(); field.clear()
          case other => field += other
        }
      i += 1
    }
    fields += field.result()
    fields.result()
  }

  /** A data line -> [[TripLite]]; `None` for the header and for anything malformed. */
  def parseTrip(line: String): Option[TripLite] = {
    val f = split(line)
    if (f.length != 13 || f.head == "ride_id") None
    else
      Try(
        TripLite(
          rideId = f(0),
          rideableType = f(1),
          member = f(12) == "member",
          startHour = f(2).substring(11, 13).toInt, // "2026-09-08 08:11:36.020" -> 8
          startStationId = StationIds.normalize(f(5)),
          endStationId = Some(f(7)).filter(_.nonEmpty).map(StationIds.normalize)
        )
      ).toOption.filter(_.startStationId.nonEmpty)
  }
}
