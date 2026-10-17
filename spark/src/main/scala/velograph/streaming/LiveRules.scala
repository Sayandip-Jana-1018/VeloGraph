package velograph.streaming

import scala.collection.mutable

/** The newest status of one station, as the producer published it. */
final case class StationStatus(
    stationId: String,
    name: String,
    lat: Double,
    lon: Double,
    capacity: Int,
    bikes: Int,
    ebikes: Int,
    docks: Int,
    installed: Boolean,
    renting: Boolean,
    returning: Boolean,
    lastReported: Long,
    fetchedAt: Double,
    kafkaMillis: Long
) {

  /** A report supersedes another when the station reported later (or the same report was fetched later). */
  def supersedes(other: StationStatus): Boolean =
    lastReported > other.lastReported || (lastReported == other.lastReported && fetchedAt > other.fetchedAt)

  def offline: Boolean = !installed || (!renting && !returning)

  /** GBFS capacity can lag a station's resize; never project past the docks the station shows. */
  def docksInTotal: Int = math.max(capacity, bikes + docks)
}

/** What the champions forecast for a station in the current hour (0) and the next one (1). */
final case class Forecast(
    stationId: String,
    departures0: Double,
    arrivals0: Double,
    departures1: Double,
    arrivals1: Double,
    regime0: Int,
    archetype: String,
    community: Int,
    pagerank: Double
)

/** A station's next 60 minutes: projected bikes and free docks, and the risk they imply. */
final case class Prediction(
    stationId: String,
    name: String,
    lat: Double,
    lon: Double,
    capacity: Int,
    bikes: Int,
    ebikes: Int,
    docks: Int,
    departures0: Double,
    arrivals0: Double,
    departures1: Double,
    arrivals1: Double,
    departures60: Double,
    arrivals60: Double,
    netFlow60: Double,
    projectedBikes: Double,
    projectedDocks: Double,
    margin: Int,
    risk: String,
    regime: Int,
    archetype: String,
    community: Int,
    pagerank: Double,
    lastReported: Long
)

/** A suggested van trip: `bikes` from a station with bikes to spare to one about to run empty. */
final case class Move(
    from: String,
    fromName: String,
    fromLat: Double,
    fromLon: Double,
    to: String,
    toName: String,
    toLat: Double,
    toLon: Double,
    bikes: Int,
    km: Double,
    community: Int
)

/**
 * Roadmap P5.3: projection, risk and alerts (ARCHITECTURE §8.1, §9.4), as plain functions.
 *
 * A station's state, against its margin of max(2, 10 % of its docks):
 *   - EMPTY or FULL: already below the margin in bikes, or in free docks, now;
 *   - EMPTY_RISK or FULL_RISK: fine now, but the forecast takes it below the margin within 60 minutes. This
 *     is what the models add to the live feed;
 *   - OFFLINE: not installed, or neither renting nor returning; OK otherwise.
 */
object LiveRules {

  val Ok        = "OK"
  val Empty     = "EMPTY"
  val EmptyRisk = "EMPTY_RISK"
  val Full      = "FULL"
  val FullRisk  = "FULL_RISK"
  val Offline   = "OFFLINE"

  /** Empty now or soon, full now or soon: an alert is about a station entering one of these. */
  def family(risk: String): String =
    if (risk == Empty || risk == EmptyRisk) Empty else if (risk == Full || risk == FullRisk) Full else risk

  def needsBikes(risk: String): Boolean = family(risk) == Empty

  /** The next 60 minutes are the rest of this hour, (1 - f) of it, and the first f of the next. */
  def netFlow60(net0: Double, net1: Double, fraction: Double): Double =
    (1 - fraction) * net0 + fraction * net1

  private def clamp(x: Double, lo: Double, hi: Double): Double = math.max(lo, math.min(hi, x))

  /** Bikes now plus the expected net flow, kept within the station's docks, and the state that implies. */
  def predict(s: StationStatus, f: Forecast, fraction: Double, rule: RiskRule): Prediction = {
    // Counts cannot be negative; a boosted-tree forecast can dip just below zero.
    val (d0, a0, d1, a1) =
      (
        math.max(0, f.departures0),
        math.max(0, f.arrivals0),
        math.max(0, f.departures1),
        math.max(0, f.arrivals1)
      )
    val departures = (1 - fraction) * d0 + fraction * d1
    val arrivals   = (1 - fraction) * a0 + fraction * a1
    val net        = netFlow60(a0 - d0, a1 - d1, fraction)
    val total      = s.docksInTotal
    val bikes      = clamp(s.bikes + net, 0, total)
    val docks      = clamp(s.docks - net, 0, total)
    val margin     = rule.margin(total)
    val risk       =
      if (s.offline) Offline
      else if (s.bikes < margin) Empty
      else if (s.docks < margin) Full
      else if (bikes < margin) EmptyRisk
      else if (docks < margin) FullRisk
      else Ok
    Prediction(
      s.stationId,
      s.name,
      s.lat,
      s.lon,
      total,
      s.bikes,
      s.ebikes,
      s.docks,
      d0,
      a0,
      d1,
      a1,
      departures,
      arrivals,
      net,
      bikes,
      docks,
      margin,
      risk,
      f.regime0,
      f.archetype,
      f.community,
      f.pagerank,
      s.lastReported
    )
  }

  /**
   * Alerts: stations that entered the empty or the full family since the previous batch, with the state they
   * left. A forecast that comes true (EMPTY_RISK, then EMPTY) does not alert twice. With no previous batch at
   * all there is nothing to compare with, so the first batch only sets the baseline.
   */
  def alerts(previous: Map[String, String], current: Seq[Prediction]): Seq[(Prediction, String)] =
    if (previous.isEmpty) Seq.empty
    else
      current.collect {
        case p
            if (family(p.risk) == Empty || family(p.risk) == Full) &&
              !previous.get(p.stationId).map(family).contains(family(p.risk)) =>
          p -> previous.getOrElse(p.stationId, "UNKNOWN")
      }
}

/**
 * Suggested rebalancing (ARCHITECTURE §9.4): greedy matching of stations about to run empty with stations
 * that will keep more bikes than they need, inside one GraphX community (riders already flow between its
 * stations), within `maxKm` and at most `maxBikes` per move. Driver-side on one row per station: a
 * deliberate, bounded collect of at most ~2.5k rows.
 */
object Rebalancer {

  private val EarthKm = 6371.0088

  def haversineKm(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val (p1, p2) = (math.toRadians(lat1), math.toRadians(lat2))
    val dp       = p2 - p1
    val dl       = math.toRadians(lon2 - lon1)
    val h        = math.pow(math.sin(dp / 2), 2) + math.cos(p1) * math.cos(p2) * math.pow(math.sin(dl / 2), 2)
    2 * EarthKm * math.asin(math.min(1.0, math.sqrt(h)))
  }

  /**
   * Stations that are empty, or about to be, ranked by the riders expected to look for a bike there in the
   * next hour, and taken one district at a time: the neediest of each district, then the second of each, and
   * so on. Each takes bikes from the best donor of its district within reach (a full or nearly full station
   * first, since that move fixes two problems; then the nearest) until it reaches its target or no donor is
   * left. A donor gives only what it holds above its own target. At most `maxMoves` moves: a shift of van
   * crews, not every imbalance in the city.
   */
  def plan(stations: Seq[Prediction], rule: RebalanceRule): Seq[Move] = {
    val ranked = stations
      .filter(p => LiveRules.needsBikes(p.risk))
      .map(p => p -> (rule.target(p.capacity) - math.floor(p.projectedBikes).toInt))
      .filter(_._2 > 0)
      .sortBy { case (p, _) => (-p.departures60, p.stationId) }
    // One crew per district: each district's neediest station in turn, so the plan covers the whole city
    // rather than one busy neighbourhood.
    val districts =
      ranked.groupBy(_._1.community).values.toList.sortBy(d => (-d.head._1.departures60, d.head._1.stationId))
    val needs = Iterator
      .iterate(districts)(_.map(_.tail).filter(_.nonEmpty))
      .takeWhile(_.nonEmpty)
      .flatMap(_.map(_.head))
      .toList
    val donors = stations.filter(p => p.risk == LiveRules.Ok || LiveRules.family(p.risk) == LiveRules.Full)
    val spare  = mutable.Map(donors.map(p =>
      p.stationId ->
        (math.floor(p.projectedBikes).toInt -
          math.max(rule.target(p.capacity), p.margin))
    ): _*)
    val moves = mutable.ArrayBuffer.empty[Move]
    needs.foreach { case (to, need) =>
      val candidates = donors
        .filter(d => d.community == to.community && spare(d.stationId) > 0)
        .map(d => d -> haversineKm(to.lat, to.lon, d.lat, d.lon))
        .filter(_._2 <= rule.maxKm)
        .sortBy { case (d, km) => (LiveRules.family(d.risk) != LiveRules.Full, km, d.stationId) }
      candidates.foldLeft(need) { case (missing, (from, km)) =>
        val bikes = Seq(missing, spare(from.stationId), rule.maxBikes).min
        if (bikes <= 0) missing
        else {
          moves += Move(
            from.stationId,
            from.name,
            from.lat,
            from.lon,
            to.stationId,
            to.name,
            to.lat,
            to.lon,
            bikes,
            math.rint(km * 100) / 100,
            to.community
          )
          spare(from.stationId) -= bikes
          missing - bikes
        }
      }
    }
    moves.take(rule.maxMoves).toList
  }
}
