package velograph.graph

/*
 * Contents of the network exports (ARCHITECTURE §10), each wrapped in the serving `meta` envelope:
 * network-summary.json, network-nodes.json, network-flows.json, network-communities.json, network-paths.json and
 * network-pregel.json. Field names are what the website reads.
 */

/**
 * One station with every graph metric: a row of `graph/station_metrics`, as `network-nodes.json` exports it.
 */
final case class NodeRow(
    id: String,
    name: String,
    lat: Double,
    lon: Double,
    capacity: Option[Int],
    inDegree: Int,
    outDegree: Int,
    inStrength: Long,
    outStrength: Long,
    selfLoopTrips: Long,
    netStrength: Long,
    imbalance: Double,
    pagerank: Double,
    pagerankUnweighted: Double,
    pagerankRank: Int,
    prAm: Double,
    prPm: Double,
    prShift: Double,
    community: Int,
    communitySize: Int,
    sccSize: Int,
    strongDegree: Option[Int],
    triangles: Option[Int],
    clustering: Option[Double],
    hopsToHub: Option[Int],
    nearestHub: Option[String]
)

final case class StationValue(id: String, name: String, value: Double)
final case class HistogramBin(from: Double, to: Double, stations: Long)

/** How a Pregel run went: rounds of message passing, vertices updated in each superstep, wall-clock time. */
final case class PregelRunReport(supersteps: Int, converged: Boolean, activeVertices: Seq[Long], millis: Long)

final case class GraphSize(
    vertices: Long,
    edges: Long,
    selfLoops: Long,
    trips: Long,
    selfLoopTrips: Long,
    density: Double,
    avgOutDegree: Double,
    maxOutDegree: Int,
    reciprocity: Double,
    danglingVertices: Long,
    strongDirectedEdges: Long,
    strongUndirectedEdges: Long
)

final case class RankMove(id: String, name: String, weightedRank: Int, unweightedRank: Int)

final case class PageRankReport(
    weighted: PregelRunReport,
    builtinMillis: Long,
    spearman: Double,
    pearson: Double,
    topOverlap: Int,
    topWeighted: Seq[StationValue],
    topUnweighted: Seq[StationValue],
    risers: Seq[RankMove],
    fallers: Seq[RankMove]
)

final case class DistrictShare(district: String, morningShare: Double, eveningShare: Double)

final case class ShiftReport(
    am: PregelRunReport,
    pm: PregelRunReport,
    amTrips: Long,
    pmTrips: Long,
    morningMagnets: Seq[StationValue],
    eveningMagnets: Seq[StationValue],
    sample: Int,
    districts: Seq[DistrictShare]
)

final case class StrengthReport(
    sinks: Seq[StationValue],
    sources: Seq[StationValue],
    busiest: Seq[StationValue]
)

final case class ComponentStats(count: Long, largest: Long, sizes: Seq[Long])

final case class ComponentReport(
    weak: ComponentStats,
    strong: ComponentStats,
    strongTies: ComponentStats,
    outsideGiantScc: Seq[StationValue]
)

final case class TriangleReport(
    triangles: Long,
    transitivity: Double,
    averageClustering: Double,
    randomGraphClustering: Double,
    mostClustered: Seq[StationValue]
)

final case class HopBin(hops: Int, stations: Long)

final case class PathsSummary(
    hubs: Seq[StationValue],
    hopHistogram: Seq[HopBin],
    unreachable: Long,
    landmarks: Seq[StationValue],
    travelEdges: Long,
    medianReachableWithin15Min: Double,
    shortestPathsMillis: Long,
    travelTimesMillis: Long,
    personalizedMillis: Long
)

final case class OperatorDemo(operator: String, purpose: String, result: String, value: Double)

final case class CommunitySummary(
    id: Int,
    size: Int,
    lat: Double,
    lon: Double,
    radiusKm: Double,
    internalShare: Double,
    meanShift: Double,
    netStrength: Long,
    topStations: Seq[String]
)

/** Contents of `network-communities.json`. */
final case class CommunityReport(
    supersteps: Int,
    labelChanges: Seq[Long],
    lpaModularity: Double,
    lpaCommunities: Long,
    merges: Int,
    modularity: Double,
    builtinModularity: Double,
    labels: Long,
    communities: Int,
    coverage: Double,
    builtinCommunities: Long,
    medianRadiusKm: Double,
    randomMedianRadiusKm: Double,
    communityList: Seq[CommunitySummary]
)

/** Contents of `network-summary.json`: the GraphX portal's headline numbers. */
final case class NetworkSummary(
    graph: GraphSize,
    partitioning: Seq[PartitioningRun],
    outDegreeHistogram: Seq[HistogramBin],
    inDegreeHistogram: Seq[HistogramBin],
    strengthHistogram: Seq[HistogramBin],
    pagerank: PageRankReport,
    shift: ShiftReport,
    strength: StrengthReport,
    communities: CommunityReport,
    components: ComponentReport,
    triangles: TriangleReport,
    paths: PathsSummary,
    operators: Seq[OperatorDemo],
    timingsMs: Map[String, Long]
)

/** One directed route for the flow map (`network-flows.json`). */
final case class Flow(
    from: String,
    to: String,
    trips: Long,
    amTrips: Long,
    pmTrips: Long,
    weekendTrips: Long,
    medianMinutes: Double,
    strong: Boolean
)

final case class LandmarkPaths(
    id: String,
    name: String,
    lat: Double,
    lon: Double,
    reachable: Long,
    within15Min: Long,
    within30Min: Long,
    travelSeconds: Seq[Option[Int]],
    destinations: Seq[StationValue]
)

/** Contents of `network-paths.json`; `travelSeconds` and `hopsToHub` are aligned with `stationIds`. */
final case class PathsReport(
    stationIds: Seq[String],
    hubs: Seq[StationValue],
    hopsToHub: Seq[Option[Int]],
    landmarks: Seq[LandmarkPaths]
)
