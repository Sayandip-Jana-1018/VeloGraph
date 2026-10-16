// zod contracts for the serving exports (ARCHITECTURE §10). Every file is parsed through one of these before a
// component sees it, so a change in a Spark job's output fails loudly here instead of drawing wrong numbers.
import { z } from "zod";

export const Meta = z.object({
  schema: z.string(),
  version: z.number(),
  generatedAt: z.string(),
  job: z.string(),
  source: z.object({ from: z.string(), to: z.string() }).optional(),
  rows: z.number().optional(),
});
export type Meta = z.infer<typeof Meta>;

const envelope = <T extends z.ZodType>(data: T) => z.object({ meta: Meta, data });

// ---- P1: lake and data quality -------------------------------------------------------------------------------
export const DqReport = envelope(
  z.object({
    inputRows: z.number(),
    validRows: z.number(),
    quarantinedRows: z.number(),
    quarantinedPct: z.number(),
    byReason: z.array(z.object({ reason: z.string(), rows: z.number(), pct: z.number() })),
    byMonth: z.array(
      z.object({ ym: z.string(), bronzeRows: z.number(), validRows: z.number(), quarantinedRows: z.number() }),
    ),
    validTripShares: z.object({ ebikePct: z.number(), roundTripPct: z.number(), memberPct: z.number() }),
    durationSeconds: z.object({ p50: z.number(), p95: z.number(), p99: z.number(), max: z.number() }),
    distanceMeters: z.object({ p50: z.number(), p95: z.number(), p99: z.number(), max: z.number() }),
    rules: z.object({ minDurationS: z.number(), maxDurationS: z.number(), maxSpeedKmh: z.number() }),
  }),
);
export type DqReport = z.infer<typeof DqReport>;

export const JobReport = envelope(
  z.object({
    job: z.string(),
    status: z.string(),
    appId: z.string(),
    master: z.string(),
    startedAt: z.string(),
    endedAt: z.string(),
    durationMs: z.number(),
    metrics: z.record(z.string(), z.unknown()),
  }),
);
export type JobReport = z.infer<typeof JobReport>;

// ---- P2: RDD fundamentals --------------------------------------------------------------------------------------
export const RddReport = envelope(
  z.object({
    month: z.string(),
    inputPartitions: z.number(),
    lines: z.number(),
    trips: z.number(),
    accumulators: z.object({
      inAction: z.number(),
      inTransformationAfterTwoActions: z.number(),
      headerLines: z.number(),
      rejectedLines: z.number(),
    }),
    persistence: z.array(
      z.object({
        level: z.string(),
        firstCountMs: z.number(),
        secondCountMs: z.number(),
        memoryBytes: z.number(),
        diskBytes: z.number(),
      }),
    ),
    shuffle: z.array(
      z.object({ operation: z.string(), millis: z.number(), shuffleBytes: z.number(), result: z.number() }),
    ),
    tripsByHour: z.array(z.object({ hour: z.number(), trips: z.number() })),
    topStations: z.array(z.object({ stationId: z.string(), name: z.string(), departures: z.number() })),
    members: z.number(),
    byRideableType: z.record(z.string(), z.number()),
    lineage: z.array(z.string()),
    rddToDataFrame: z.record(z.string(), z.number()),
  }),
);
export type RddReport = z.infer<typeof RddReport>;

// ---- P2: Spark SQL insights ------------------------------------------------------------------------------------
export const ChartHint = z.object({
  type: z.enum(["line", "bar", "barh", "table"]),
  x: z.string().optional(),
  y: z.string().optional(),
  series: z.string().optional(),
});
export type ChartHint = z.infer<typeof ChartHint>;

export const InsightIndex = envelope(
  z.array(
    z.object({
      id: z.string(),
      title: z.string(),
      question: z.string(),
      chart: ChartHint,
      rows: z.number(),
      elapsedMs: z.number(),
    }),
  ),
);
export type InsightIndex = z.infer<typeof InsightIndex>;

const Cell = z.union([z.string(), z.number(), z.boolean(), z.null()]);
export const Insight = envelope(
  z.object({
    id: z.string(),
    title: z.string(),
    question: z.string(),
    chart: ChartHint,
    sql: z.string(),
    columns: z.array(z.object({ name: z.string(), dataType: z.string() })),
    rows: z.array(z.record(z.string(), Cell)),
    elapsedMs: z.number(),
  }),
);
export type Insight = z.infer<typeof Insight>;

// ---- P3: GraphX ------------------------------------------------------------------------------------------------
const StationValue = z.object({ id: z.string(), name: z.string(), value: z.number() });
export type StationValue = z.infer<typeof StationValue>;
const PregelRun = z.object({
  supersteps: z.number(),
  converged: z.boolean(),
  activeVertices: z.array(z.number()),
  millis: z.number(),
});
const Bin = z.object({ from: z.number(), to: z.number(), stations: z.number() });
const Components = z.object({ count: z.number(), largest: z.number(), sizes: z.array(z.number()) });

export const CommunitySummary = z.object({
  id: z.number(),
  size: z.number(),
  lat: z.number(),
  lon: z.number(),
  radiusKm: z.number(),
  internalShare: z.number(),
  meanShift: z.number(),
  netStrength: z.number(),
  topStations: z.array(z.string()),
});
export const CommunityReportData = z.object({
  supersteps: z.number(),
  labelChanges: z.array(z.number()),
  lpaModularity: z.number(),
  lpaCommunities: z.number(),
  merges: z.number(),
  modularity: z.number(),
  builtinModularity: z.number(),
  labels: z.number(),
  communities: z.number(),
  coverage: z.number(),
  builtinCommunities: z.number(),
  medianRadiusKm: z.number(),
  randomMedianRadiusKm: z.number(),
  communityList: z.array(CommunitySummary),
});
export const NetworkCommunities = envelope(CommunityReportData);
export type NetworkCommunities = z.infer<typeof NetworkCommunities>;

export const NetworkSummary = envelope(
  z.object({
    graph: z.object({
      vertices: z.number(),
      edges: z.number(),
      selfLoops: z.number(),
      trips: z.number(),
      selfLoopTrips: z.number(),
      density: z.number(),
      avgOutDegree: z.number(),
      maxOutDegree: z.number(),
      reciprocity: z.number(),
      danglingVertices: z.number(),
      strongDirectedEdges: z.number(),
      strongUndirectedEdges: z.number(),
    }),
    partitioning: z.array(
      z.object({
        strategy: z.string(),
        partitions: z.number(),
        replicationFactor: z.number(),
        maxEdgesPerPartition: z.number(),
        minEdgesPerPartition: z.number(),
        edgeImbalance: z.number(),
      }),
    ),
    outDegreeHistogram: z.array(Bin),
    inDegreeHistogram: z.array(Bin),
    strengthHistogram: z.array(Bin),
    pagerank: z.object({
      weighted: PregelRun,
      builtinMillis: z.number(),
      spearman: z.number(),
      pearson: z.number(),
      topOverlap: z.number(),
      topWeighted: z.array(StationValue),
      topUnweighted: z.array(StationValue),
      risers: z.array(
        z.object({ id: z.string(), name: z.string(), weightedRank: z.number(), unweightedRank: z.number() }),
      ),
      fallers: z.array(
        z.object({ id: z.string(), name: z.string(), weightedRank: z.number(), unweightedRank: z.number() }),
      ),
    }),
    shift: z.object({
      am: PregelRun,
      pm: PregelRun,
      amTrips: z.number(),
      pmTrips: z.number(),
      morningMagnets: z.array(StationValue),
      eveningMagnets: z.array(StationValue),
      sample: z.number(),
      districts: z.array(z.object({ district: z.string(), morningShare: z.number(), eveningShare: z.number() })),
    }),
    strength: z.object({
      sinks: z.array(StationValue),
      sources: z.array(StationValue),
      busiest: z.array(StationValue),
    }),
    communities: CommunityReportData,
    components: z.object({
      weak: Components,
      strong: Components,
      strongTies: Components,
      outsideGiantScc: z.array(StationValue),
    }),
    triangles: z.object({
      triangles: z.number(),
      transitivity: z.number(),
      averageClustering: z.number(),
      randomGraphClustering: z.number(),
      mostClustered: z.array(StationValue),
    }),
    paths: z.object({
      hubs: z.array(StationValue),
      hopHistogram: z.array(z.object({ hops: z.number(), stations: z.number() })),
      unreachable: z.number(),
      landmarks: z.array(StationValue),
      travelEdges: z.number(),
      medianReachableWithin15Min: z.number(),
      shortestPathsMillis: z.number(),
      travelTimesMillis: z.number(),
      personalizedMillis: z.number(),
    }),
    operators: z.array(
      z.object({ operator: z.string(), purpose: z.string(), result: z.string(), value: z.number() }),
    ),
    timingsMs: z.record(z.string(), z.number()),
  }),
);
export type NetworkSummary = z.infer<typeof NetworkSummary>;

export const NodeRow = z.object({
  id: z.string(),
  name: z.string(),
  lat: z.number(),
  lon: z.number(),
  capacity: z.number().nullable(),
  inDegree: z.number(),
  outDegree: z.number(),
  inStrength: z.number(),
  outStrength: z.number(),
  selfLoopTrips: z.number(),
  netStrength: z.number(),
  imbalance: z.number(),
  pagerank: z.number(),
  pagerankUnweighted: z.number(),
  pagerankRank: z.number(),
  prAm: z.number(),
  prPm: z.number(),
  prShift: z.number(),
  community: z.number(),
  communitySize: z.number(),
  sccSize: z.number(),
  strongDegree: z.number().nullable(),
  triangles: z.number().nullable(),
  clustering: z.number().nullable(),
  hopsToHub: z.number().nullable(),
  nearestHub: z.string().nullable(),
});
export type NodeRow = z.infer<typeof NodeRow>;
export const NetworkNodes = envelope(z.array(NodeRow));

export const Flow = z.object({
  from: z.string(),
  to: z.string(),
  trips: z.number(),
  amTrips: z.number(),
  pmTrips: z.number(),
  weekendTrips: z.number(),
  medianMinutes: z.number(),
  strong: z.boolean(),
});
export type Flow = z.infer<typeof Flow>;
export const NetworkFlows = envelope(z.array(Flow));

export const NetworkPaths = envelope(
  z.object({
    stationIds: z.array(z.string()),
    hubs: z.array(StationValue),
    hopsToHub: z.array(z.number().nullable()),
    landmarks: z.array(
      z.object({
        id: z.string(),
        name: z.string(),
        lat: z.number(),
        lon: z.number(),
        reachable: z.number(),
        within15Min: z.number(),
        within30Min: z.number(),
        travelSeconds: z.array(z.number().nullable()),
        destinations: z.array(StationValue),
      }),
    ),
  }),
);
export type NetworkPaths = z.infer<typeof NetworkPaths>;

export const NetworkPregel = envelope(
  z.object({
    resetProb: z.number(),
    tol: z.number(),
    stations: z.array(
      z.object({ index: z.number(), id: z.string(), name: z.string(), lat: z.number(), lon: z.number() }),
    ),
    edges: z.array(z.object({ from: z.number(), to: z.number(), trips: z.number(), probability: z.number() })),
    steps: z.array(
      z.object({
        superstep: z.number(),
        ranks: z.array(z.number()),
        deltas: z.array(z.number()),
        updated: z.array(z.boolean()),
        messages: z.array(z.object({ from: z.number(), to: z.number(), mass: z.number() })),
      }),
    ),
    convergedAfter: z.number(),
    finalRanks: z.array(z.number()),
  }),
);
export type NetworkPregel = z.infer<typeof NetworkPregel>;

// ---- Spark UI, redrawn: the history server's REST API, exported by velograph.ops.spark_ui_export ------------
const StageView = z.object({
  stageId: z.number(),
  name: z.string(),
  numTasks: z.number(),
  durationMs: z.number(),
  executorRunTimeMs: z.number(),
  inputBytes: z.number(),
  inputRecords: z.number(),
  shuffleReadBytes: z.number(),
  shuffleReadRecords: z.number(),
  shuffleWriteBytes: z.number(),
  shuffleWriteRecords: z.number(),
  taskRunTimeMs: z.array(z.number()),
});
export type StageView = z.infer<typeof StageView>;
const Executor = z.object({
  id: z.string(),
  cores: z.number(),
  tasks: z.number(),
  taskTimeMs: z.number(),
  gcTimeMs: z.number(),
  inputBytes: z.number(),
  shuffleReadBytes: z.number(),
  shuffleWriteBytes: z.number(),
  maxMemoryBytes: z.number(),
});
export const SparkUi = envelope(
  z.object({
    historyServer: z.string(),
    applications: z.array(
      z.object({
        name: z.string(),
        id: z.string(),
        start: z.number(),
        end: z.number(),
        durationMs: z.number(),
        sparkVersion: z.string(),
      }),
    ),
    rdd: z.object({
      appId: z.string(),
      jobs: z.array(
        z.object({
          jobId: z.number(),
          name: z.string(),
          group: z.string().nullable(),
          startMs: z.number(),
          durationMs: z.number(),
          tasks: z.number(),
          stages: z.number(),
          skippedStages: z.number(),
        }),
      ),
      shuffles: z.array(z.object({ group: z.string(), jobId: z.number(), stages: z.array(StageView) })),
      executors: z.array(Executor),
    }),
    sql: z.object({
      appId: z.string(),
      executions: z.number(),
      executionId: z.number(),
      description: z.string(),
      durationMs: z.number(),
      exchanges: z.number(),
      nodes: z.array(
        z.object({
          id: z.number(),
          name: z.string(),
          codegen: z.number().nullable(),
          metrics: z.array(z.object({ name: z.string(), value: z.string() })),
        }),
      ),
      edges: z.array(z.object({ from: z.number(), to: z.number() })),
    }),
    pregel: z.object({
      appId: z.string(),
      totalJobs: z.number(),
      superstepJobs: z.number(),
      firstRun: z.array(z.object({ jobId: z.number(), durationMs: z.number(), skippedStages: z.number() })),
      executors: z.array(Executor),
    }),
  }),
);
export type SparkUi = z.infer<typeof SparkUi>;

// ---- P4: machine learning (ARCHITECTURE §8) -------------------------------------------------------------------
const Range = z.object({ from: z.string(), to: z.string() });

export const Archetypes = envelope(
  z.object({
    window: Range,
    k: z.number(),
    silhouette: z.number(),
    selection: z.array(z.object({ k: z.number(), silhouette: z.number(), smallestShare: z.number() })),
    minTrips: z.number(),
    explainedVariance: z.array(z.number()),
    archetypes: z.array(
      z.object({
        id: z.number(),
        name: z.string(),
        stations: z.number(),
        fitted: z.number(),
        offToWork: z.number(),
        amDepartures: z.number(),
        amArrivals: z.number(),
        pmDepartures: z.number(),
        pmArrivals: z.number(),
        curves: z.object({
          workDep: z.array(z.number()),
          workArr: z.array(z.number()),
          offDep: z.array(z.number()),
          offArr: z.array(z.number()),
        }),
        examples: z.array(z.string()),
      }),
    ),
    points: z.array(
      z.object({ station: z.string(), name: z.string(), x: z.number(), y: z.number(), archetype: z.number(), trips: z.number() }),
    ),
    stability: z.object({ stations: z.number(), changed: z.number() }),
  }),
);
export type Archetypes = z.infer<typeof Archetypes>;

const FeatureSpec = z.object({ name: z.string(), group: z.string(), kind: z.string(), description: z.string() });
export const MlFeatures = envelope(
  z.object({
    windows: z.array(z.object({ split: z.string(), rows: Range, profile: Range })),
    validation: Range,
    tau: z.number(),
    tauQuantile: z.number(),
    strongMinTrips: z.number(),
    medianCapacity: z.number(),
    communitiesMatched: z.number(),
    communitiesNew: z.number(),
    leakage: z.array(z.object({ split: z.string(), lastProfileDay: z.string(), firstRowDay: z.string(), passed: z.boolean() })),
    splits: z.array(
      z.object({
        split: z.string(),
        rows: z.number(),
        validationRows: z.number(),
        stations: z.number(),
        withoutProfile: z.number(),
        withoutArchetype: z.number(),
        draining: z.number(),
        stable: z.number(),
        filling: z.number(),
        meanDepartures: z.number(),
      }),
    ),
    features: z.array(FeatureSpec),
    targets: z.array(FeatureSpec),
  }),
);
export type MlFeatures = z.infer<typeof MlFeatures>;

export const MlCorrelation = envelope(
  z.object({
    split: z.string(),
    sampleFraction: z.number(),
    sampleRows: z.number(),
    columns: z.array(z.string()),
    constant: z.array(z.string()),
    pearson: z.array(z.array(z.number())),
    spearman: z.array(z.array(z.number())),
    withDepartures: z.array(z.object({ feature: z.string(), pearson: z.number(), spearman: z.number() })),
    demand: z.record(z.string(), z.object({ departures: z.array(z.number()), arrivals: z.array(z.number()) })),
    departuresHistogram: z.array(z.object({ value: z.string(), share: z.number() })),
    dispersion: z.object({ rows: z.number(), mean: z.number(), variance: z.number(), zeroShare: z.number() }),
  }),
);
export type MlCorrelation = z.infer<typeof MlCorrelation>;

const Params = z.record(z.string(), z.union([z.number(), z.string(), z.boolean()]));
const RegMetrics = z.object({ mae: z.number(), rmse: z.number(), r2: z.number(), rows: z.number() });
export type RegMetrics = z.infer<typeof RegMetrics>;
const RegressionTarget = z.object({
  leaderboard: z.array(z.object({ model: z.string(), params: Params, validation: RegMetrics, test: RegMetrics })),
  champion: z.string(),
  improvementVsNaive: z.number(),
  byHour: z.array(z.object({ hour: z.number(), champion: z.number(), naive: z.number() })),
  byArchetype: z.array(
    z.object({ archetype: z.number(), name: z.string(), champion: z.number(), naive: z.number(), rows: z.number() }),
  ),
  residuals: z.array(z.object({ bin: z.number(), share: z.number() })),
  calibration: z.array(z.object({ actual: z.number(), meanPrediction: z.number(), rows: z.number() })),
  importances: z.array(z.object({ feature: z.string(), importance: z.number() })),
  baselines: z.record(z.string(), z.object({ validation: RegMetrics, test: RegMetrics })),
});
export type RegressionTarget = z.infer<typeof RegressionTarget>;
export const MlRegression = envelope(
  z.object({
    protocol: z.object({ train: Range, validation: Range, test: Range, selection: z.string(), tunedOn: z.string() }),
    grid: z.record(z.string(), z.array(Params)),
    tuning: z.record(z.string(), z.array(z.object({ params: Params, validation: RegMetrics }))),
    fixedParams: z.record(z.string(), Params),
    targets: z.record(z.string(), RegressionTarget),
  }),
);
export type MlRegression = z.infer<typeof MlRegression>;

const ClassMetrics = z.object({
  accuracy: z.number(),
  macroF1: z.number(),
  perClass: z.array(
    z.object({ class: z.number(), precision: z.number(), recall: z.number(), f1: z.number(), support: z.number() }),
  ),
  rows: z.number(),
  confusion: z.array(z.array(z.number())),
});
export type ClassMetrics = z.infer<typeof ClassMetrics>;
export const MlClassification = envelope(
  z.object({
    tau: z.number(),
    tauQuantile: z.number(),
    classes: z.array(z.object({ id: z.number(), name: z.string() })),
    balance: z.record(z.string(), z.record(z.string(), z.number())),
    weighting: z.string(),
    selection: z.string(),
    grid: z.record(z.string(), z.array(Params)),
    tuning: z.record(z.string(), z.array(z.object({ params: Params, validation: ClassMetrics }))),
    fixedParams: z.record(z.string(), Params),
    baselines: z.record(z.string(), z.object({ validation: ClassMetrics, test: ClassMetrics })),
    leaderboard: z.array(z.object({ model: z.string(), params: Params, validation: ClassMetrics, test: ClassMetrics })),
    champion: z.string(),
  }),
);
export type MlClassification = z.infer<typeof MlClassification>;

export const MlAblation = envelope(
  z.object({
    target: z.string(),
    model: z.string(),
    params: Params,
    fixedParams: Params,
    naive: RegMetrics,
    variants: z.array(
      z.object({
        name: z.string(),
        dropped: z.array(z.string()),
        features: z.number(),
        test: RegMetrics,
        deltaMae: z.number(),
        deltaMaePct: z.number(),
        deltaR2: z.number(),
      }),
    ),
  }),
);
export type MlAblation = z.infer<typeof MlAblation>;

export const ModelCards = envelope(
  z.array(
    z.object({
      name: z.string(),
      target: z.string(),
      algorithm: z.string(),
      params: z.record(z.string(), z.string()),
      features: z.array(z.string()),
      fit: Range,
      test: Range,
      trainingRows: z.number(),
      trainingMillis: z.number(),
      metric: z.string(),
      testMetric: z.number(),
      labMetric: z.number(),
      deltaPct: z.number(),
      regression: RegMetrics.nullable(),
      classification: z
        .object({
          accuracy: z.number(),
          macroF1: z.number(),
          perClass: z.array(
            z.object({ label: z.number(), precision: z.number(), recall: z.number(), f1: z.number(), support: z.number() }),
          ),
          confusion: z.array(z.array(z.number())),
          rows: z.number(),
        })
        .nullable(),
      tau: z.number().nullable(),
      modelPath: z.string(),
      sparkVersion: z.string(),
      gitSha: z.string(),
    }),
  ),
);
export type ModelCards = z.infer<typeof ModelCards>;
