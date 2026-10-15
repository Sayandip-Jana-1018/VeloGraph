package velograph.graph

import scala.collection.JavaConverters._

import com.typesafe.config.Config

/** A rough lat/lon box around a district, used only to check where morning riders end up. */
final case class District(name: String, minLat: Double, maxLat: Double, minLon: Double, maxLon: Double) {
  def contains(lat: Double, lon: Double): Boolean =
    lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon
}

/** Sizes of the network exports (the website draws these). */
final case class ExportSizes(flows: Int, peakFlows: Int, pprDestinations: Int, topStations: Int)

/** `velograph.graph` in application.conf (ARCHITECTURE §7, §12). */
final case class GraphConfig(
    partitions: Int,
    resetProb: Double,
    tol: Double,
    maxSupersteps: Int,
    strong: StrongEdgeRule,
    lpaSupersteps: Int,
    minCommunitySize: Int,
    hubs: Int,
    landmarks: Int,
    landmarkSeparationKm: Double,
    travelMinTrips: Long,
    pprTol: Double,
    explainerStations: Int,
    officeDistricts: Seq[District],
    shiftSample: Int,
    exports: ExportSizes
)

object GraphConfig {

  def apply(raw: Config): GraphConfig = {
    val c         = raw.getConfig("velograph.graph")
    val districts = c.getConfigList("office-districts").asScala.map { d =>
      District(
        d.getString("name"),
        d.getDouble("min-lat"),
        d.getDouble("max-lat"),
        d.getDouble("min-lon"),
        d.getDouble("max-lon")
      )
    }
    GraphConfig(
      partitions = c.getInt("partitions"),
      resetProb = c.getDouble("reset-prob"),
      tol = c.getDouble("pagerank-tol"),
      maxSupersteps = c.getInt("max-supersteps"),
      strong = StrongEdgeRule(c.getLong("strong.min-trips"), c.getInt("strong.top-k")),
      lpaSupersteps = c.getInt("lpa-supersteps"),
      minCommunitySize = c.getInt("min-community-size"),
      hubs = c.getInt("hubs"),
      landmarks = c.getInt("landmarks.count"),
      landmarkSeparationKm = c.getDouble("landmarks.min-separation-km"),
      travelMinTrips = c.getLong("travel-time-min-trips"),
      pprTol = c.getDouble("ppr-tol"),
      explainerStations = c.getInt("explainer.stations"),
      officeDistricts = districts.toList,
      shiftSample = c.getInt("shift-sample"),
      exports = ExportSizes(
        c.getInt("exports.flows"),
        c.getInt("exports.peak-flows"),
        c.getInt("exports.ppr-destinations"),
        c.getInt("exports.top-stations")
      )
    )
  }
}

/** Great-circle distance for driver-side work on a few thousand stations. */
object Geo {
  private val EarthRadiusKm = 6371.0088

  def haversineKm(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val dLat = math.toRadians(lat2 - lat1)
    val dLon = math.toRadians(lon2 - lon1)
    val a    = math.pow(math.sin(dLat / 2), 2) +
      math.cos(math.toRadians(lat1)) * math.cos(math.toRadians(lat2)) * math.pow(math.sin(dLon / 2), 2)
    2 * EarthRadiusKm * math.asin(math.sqrt(a))
  }

  /**
   * Greedy spatial spread: walk the candidates in rank order and keep each one that lies at least `minKm`
   * from every candidate kept so far, until `count` are kept.
   */
  def spread[A](ranked: Seq[A], count: Int, minKm: Double)(position: A => (Double, Double)): Vector[A] =
    ranked.foldLeft(Vector.empty[A]) { (kept, candidate) =>
      val (lat, lon) = position(candidate)
      val farEnough  = kept.forall { k =>
        val (kLat, kLon) = position(k)
        haversineKm(kLat, kLon, lat, lon) >= minKm
      }
      if (kept.size < count && farEnough) kept :+ candidate else kept
    }
}
