package velograph.rdd

import org.scalatest.funsuite.AnyFunSuite

class CsvLineSpec extends AnyFunSuite {

  test("split honours quotes, keeps empty fields and unescapes doubled quotes") {
    assert(CsvLine.split("\"a\",\"b,c\",\"\",d") == Vector("a", "b,c", "", "d"))
    assert(CsvLine.split("\"say \"\"hi\"\"\",x") == Vector("say \"hi\"", "x"))
    assert(CsvLine.split("") == Vector(""))
  }

  private val line =
    "\"EFC244DEC104C0D9\",\"electric_bike\",\"2026-09-08 08:11:36.020\",\"2026-09-08 08:28:50.409\"," +
      "\"Grand St\",\"5114.08\",\"South End Ave, Albany St\",\"6926.1\",40.7151,-74.0376,40.7099,-74.0165,\"member\""

  test("a data line becomes a TripLite with normalised station ids") {
    assert(
      CsvLine.parseTrip(line).contains(
        TripLite(
          "EFC244DEC104C0D9",
          "electric_bike",
          member = true,
          startHour = 8,
          "5114.08",
          Some("6926.10")
        )
      )
    )
  }

  test("header, malformed lines and bad timestamps are rejected; a missing end station is None") {
    assert(
      CsvLine.parseTrip("\"ride_id\",\"rideable_type\",\"started_at\",\"ended_at\",\"start_station_name\"," +
        "\"start_station_id\",\"end_station_name\",\"end_station_id\",\"start_lat\",\"start_lng\",\"end_lat\"," +
        "\"end_lng\",\"member_casual\"").isEmpty
    )
    assert(CsvLine.parseTrip("only,three,fields").isEmpty)
    assert(CsvLine.parseTrip(line.replace("2026-09-08 08:11:36.020", "soon")).isEmpty)
    assert(CsvLine.parseTrip(line.replace("\"6926.1\"", "\"\"")).flatMap(_.endStationId).isEmpty)
  }
}
