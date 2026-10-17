package velograph.streaming

import java.time.Duration

import org.scalatest.funsuite.AnyFunSuite

class LiveRulesSpec extends AnyFunSuite {

  private val risk  = RiskRule(minBikes = 2, capacityShare = 0.10)
  private val moves = RebalanceRule(maxKm = 2.5, maxBikes = 12, targetShare = 0.5, maxMoves = 40)

  private def status(id: String, bikes: Int, docks: Int, capacity: Int = 20, renting: Boolean = true) =
    StationStatus(
      id,
      s"Station $id",
      40.75,
      -73.99,
      capacity,
      bikes,
      0,
      docks,
      true,
      renting,
      true,
      1000L,
      1000.0,
      0L
    )

  private def forecast(id: String, dep: Double, arr: Double) =
    Forecast(id, dep, arr, dep, arr, 1, "Leisure", 3, 0.001)

  private def prediction(
      id: String,
      projected: Double,
      riskState: String,
      lat: Double = 40.75,
      lon: Double = -73.99,
      community: Int = 3,
      capacity: Int = 20,
      demand: Double = 0.0
  ) =
    Prediction(
      id,
      id,
      lat,
      lon,
      capacity,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      demand,
      0,
      0,
      projected,
      capacity - projected,
      2,
      riskState,
      1,
      "Leisure",
      community,
      0.0,
      0L
    )

  test("the next 60 minutes blend the rest of this hour with the start of the next") {
    assert(LiveRules.netFlow60(net0 = -6, net1 = 2, fraction = 0.0) == -6)
    assert(LiveRules.netFlow60(net0 = -6, net1 = 2, fraction = 0.25) == -4) // 0.75 x -6 + 0.25 x 2
    assert(LiveRules.netFlow60(net0 = -6, net1 = 2, fraction = 1.0) == 2)
  }

  test("a station is at empty risk when its projected bikes fall below max(2, 10 % of capacity)") {
    // 5 bikes, 4 more leaving than arriving in each hour: 1 projected, below the margin of 2.
    val p = LiveRules.predict(status("a", bikes = 5, docks = 15), forecast("a", dep = 6, arr = 2), 0.5, risk)
    assert((p.netFlow60, p.projectedBikes, p.margin, p.risk) == ((-4.0, 1.0, 2, LiveRules.EmptyRisk)))
    assert(p.departures60 == 6.0)
  }

  test("a station already below its margin is empty (or full) now, whatever the forecast") {
    val empty =
      LiveRules.predict(status("e", bikes = 1, docks = 19), forecast("e", dep = 0, arr = 5), 0.5, risk)
    assert(empty.risk == LiveRules.Empty)
    val full =
      LiveRules.predict(status("f", bikes = 40, docks = 0, capacity = 40), forecast("f", 9, 0), 0.5, risk)
    assert(full.risk == LiveRules.Full)
  }

  test("a station is at full risk when its projected free docks fall below the margin") {
    val p =
      LiveRules.predict(status("b", bikes = 17, docks = 3, capacity = 20), forecast("b", 0, 2), 0.0, risk)
    assert((p.projectedDocks, p.risk) == ((1.0, LiveRules.FullRisk)))
  }

  test("projections stay within the docks, negative forecasts count as zero, closed stations are offline") {
    val drained = LiveRules.predict(status("c", 1, 19), forecast("c", dep = 30, arr = -0.4), 0.0, risk)
    assert((drained.arrivals0, drained.projectedBikes) == ((0.0, 0.0)))
    val closed = LiveRules.predict(
      status("d", 0, 20, renting = false).copy(returning = false),
      forecast("d", 9, 0),
      0,
      risk
    )
    assert(closed.risk == LiveRules.Offline)
  }

  test("an alert fires once, when a station enters the empty or the full family") {
    val now = Seq(
      prediction("a", 0, LiveRules.EmptyRisk), // was OK: alert
      prediction("b", 9, LiveRules.Ok),
      prediction("c", 19, LiveRules.Full), // was FULL_RISK: the forecast came true, no second alert
      prediction("d", 0, LiveRules.Empty), // was FULL: alert
      prediction("e", 0, LiveRules.Empty)  // new station
    )
    val before =
      Map("a" -> LiveRules.Ok, "b" -> LiveRules.Ok, "c" -> LiveRules.FullRisk, "d" -> LiveRules.Full)
    val fired = LiveRules.alerts(before, now)
    assert(fired.map { case (p, was) => (p.stationId, was) } ==
      Seq("a" -> LiveRules.Ok, "d" -> LiveRules.Full, "e" -> "UNKNOWN"))
    assert(LiveRules.alerts(Map.empty, now).isEmpty) // the first batch only sets the baseline
  }

  test("moves bring an empty station to half full from the best donor of its community within reach") {
    val stations = Seq(
      prediction("empty", projected = 1, LiveRules.EmptyRisk),
      prediction("near", projected = 14, LiveRules.Ok, lat = 40.755),       // 0.56 km, spare 14 - 10 = 4
      prediction("full", projected = 19, LiveRules.FullRisk, lat = 40.765), // 1.67 km, spare 9: preferred
      prediction("far", projected = 20, LiveRules.FullRisk, lat = 40.80),   // 5.6 km: out of reach
      prediction("other", projected = 20, LiveRules.FullRisk, lat = 40.751, community = 4) // another district
    )
    val plan = Rebalancer.plan(stations, moves)
    // The empty station needs 10 - 1 = 9 bikes: all 9 from the full-risk donor, which then sits at its target.
    assert(plan.map(m => (m.from, m.to, m.bikes)) == Seq(("full", "empty", 9)))
    assert(math.abs(plan.head.km - 1.67) < 0.01)
  }

  test("the busiest empty stations are served first, and a plan holds at most max-moves moves") {
    val stations = Seq(
      prediction("quiet", projected = 0, LiveRules.Empty, demand = 1.0),
      prediction("busy", projected = 0, LiveRules.EmptyRisk, demand = 9.0),
      prediction("donor", projected = 20, LiveRules.Full, lat = 40.751) // spare 10 for one of them
    )
    assert(Rebalancer.plan(stations, moves).map(m => (m.to, m.bikes)) == Seq("busy" -> 10))
    val many = (1 to 50).map(i => prediction(s"e$i", 0, LiveRules.Empty)) ++
      (1 to 50).map(i => prediction(s"d$i", 20, LiveRules.Ok, lat = 40.7501))
    assert(Rebalancer.plan(many, moves).size == 40)
  }

  test("each district is served in turn, so the moves spread over the city") {
    val stations = Seq(
      prediction("a1", 0, LiveRules.Empty, community = 1, demand = 9),
      prediction("a2", 0, LiveRules.Empty, community = 1, demand = 8),
      prediction("b1", 0, LiveRules.Empty, community = 2, demand = 3),
      prediction(
        "da",
        20,
        LiveRules.Full,
        community = 1,
        lat = 40.751,
        capacity = 60
      ),                                                                // spare 20 - 30 < 0: none
      prediction("d1", 20, LiveRules.Ok, community = 1, lat = 40.751),  // spare 10
      prediction("d1b", 20, LiveRules.Ok, community = 1, lat = 40.752), // spare 10
      prediction("d2", 20, LiveRules.Ok, community = 2, lat = 40.751)   // spare 10
    )
    // District 1's neediest, then district 2's, then district 1's second.
    assert(Rebalancer.plan(stations, moves).map(_.to) == Seq("a1", "b1", "a2"))
  }

  test("a move carries at most 12 bikes, and a large need takes more than one donor") {
    val stations = Seq(
      prediction("big", projected = 0, LiveRules.EmptyRisk, capacity = 60),        // needs 30
      prediction("d1", projected = 40, LiveRules.Ok, lat = 40.751, capacity = 40), // spare 20
      prediction("d2", projected = 30, LiveRules.Ok, lat = 40.752, capacity = 40)  // spare 10
    )
    val plan = Rebalancer.plan(stations, moves)
    assert(plan.map(m => (m.from, m.bikes)) == Seq("d1" -> 12, "d2" -> 10))
  }

  test("haversine distance") {
    // Times Square to Grand Central: about 0.9 km.
    assert(math.abs(Rebalancer.haversineKm(40.7580, -73.9855, 40.7527, -73.9772) - 0.92) < 0.05)
  }

  test("a restart resumes the last snapshot's risks only while that snapshot is recent") {
    assert(LiveRules.resumable(Duration.ofMinutes(2), windowMinutes = 35))
    assert(LiveRules.resumable(
      Duration.ofSeconds(-92),
      windowMinutes = 35
    )) // a cluster snapshot read on the host's slow clock
    assert(!LiveRules.resumable(Duration.ofHours(8), windowMinutes = 35))
    assert(!LiveRules.resumable(Duration.ofMinutes(35), windowMinutes = 35))
  }
}
