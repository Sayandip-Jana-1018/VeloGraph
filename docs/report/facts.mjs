// Every number in the report, read from the exports the jobs wrote (data/serving): nothing is typed in by hand.
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

const SERVING = join(import.meta.dirname, "..", "..", "data", "serving");
const load = (file) => JSON.parse(readFileSync(join(SERVING, file), "utf8"));
const data = (file) => load(file).data;

const intFmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
export const int = (n) => intFmt.format(n);
export const fixed = (n, d = 1) =>
  new Intl.NumberFormat("en-US", { minimumFractionDigits: d, maximumFractionDigits: d }).format(n);
export const mb = (bytes) => `${fixed(bytes / 1e6, 1)} MB`;
export const pct = (n, d = 1) => `${fixed(n, d)} %`;
const min = (ms) => fixed(ms / 60000, 1);

/** The RDD lab on the laptop and on the Docker cluster (scripts/benchmark.ps1); null until both runs exist. */
function benchmark() {
  const files = ["local", "cluster", "local-job", "cluster-job"].map((k) => `benchmarks/rdd-fundamentals-${k}.json`);
  if (!files.every((file) => existsSync(join(SERVING, file)))) return null;
  const [local, cluster, localJob, clusterJob] = files.map(data);
  const op = (r, name) => r.shuffle.find((s) => s.operation === name);
  const level = (r, name) => r.persistence.find((p) => p.level === name);
  const time = (ms) => (ms < 1000 ? `${int(ms)} ms` : `${fixed(ms / 1000, 1)} s`);
  const operations = [["groupByKey", "groupByKey"], ["reduceByKey", "reduceByKey"], ["join", "Shuffle join"], ["broadcast", "Broadcast join"]];
  const levels = [["NONE", "not cached"], ["MEMORY_ONLY", "MEMORY_ONLY"], ["MEMORY_ONLY_SER", "MEMORY_ONLY_SER"], ["DISK_ONLY", "DISK_ONLY"]];
  return {
    rows: [
      ["The whole job", time(localJob.durationMs), time(clusterJob.durationMs)],
      ...operations.map(([k, label]) => [label, time(op(local, k).millis), time(op(cluster, k).millis)]),
      ...levels.map(([k, label]) => [`Second count, ${label}`, time(level(local, k).secondCountMs), time(level(cluster, k).secondCountMs)]),
    ],
    same: local.trips === cluster.trips && local.shuffle.every((s) => op(cluster, s.operation)?.shuffleBytes === s.shuffleBytes),
    localMaster: localJob.master,
    clusterMaster: clusterJob.master,
    localS: fixed(localJob.durationMs / 1000, 1),
    clusterS: fixed(clusterJob.durationMs / 1000, 1),
    memLocal: mb(level(local, "MEMORY_ONLY").memoryBytes),
    memCluster: mb(level(cluster, "MEMORY_ONLY").memoryBytes),
    memClusterTime: time(level(cluster, "MEMORY_ONLY").secondCountMs),
    serCluster: mb(level(cluster, "MEMORY_ONLY_SER").memoryBytes),
    serClusterTime: time(level(cluster, "MEMORY_ONLY_SER").secondCountMs),
  };
}

function quantile(xs, q) {
  const s = [...xs].sort((a, b) => a - b);
  return s.length ? s[Math.min(s.length - 1, Math.floor(q * s.length))] : 0;
}

export function facts() {
  const dq = data("dq-report.json");
  const jobs = Object.fromEntries(
    [
      "bronze-trips", "silver-trips", "silver-stations", "silver-weather", "silver-calendar", "gold-station-hour",
      "gold-routes", "rdd-fundamentals", "insights", "graph-metrics", "station-archetypes", "feature-build",
      "train-champion", "live-features", "hero-trips",
    ].map((j) => [j, data(`jobs/${j}.json`)]),
  );
  const batchMs = Object.values(jobs).reduce((s, j) => s + j.durationMs, 0);
  const rdd = data("rdd-fundamentals.json");
  const shuffle = Object.fromEntries(rdd.shuffle.map((s) => [s.operation, s]));
  const persist = Object.fromEntries(rdd.persistence.map((p) => [p.level, p]));
  const net = data("network-summary.json");
  const part = Object.fromEntries(net.partitioning.map((p) => [p.strategy, p]));
  const insights = data("insights/index.json");
  const rain = data("insights/rain-impact.json");
  const worstRain = Math.min(...rain.rows.map((r) => Number(r.vs_dry_pct)));
  const reg = data("ml-regression.json").targets;
  const cls = data("ml-classification.json");
  const ablation = data("ml-ablation.json");
  const arch = data("archetypes.json");
  const features = data("ml-features.json");
  const live = data("live/snapshot.json");
  const progress = data("live/stream-progress.json");
  const scoring = progress.batches.filter((b) => b.query === "live-station-scoring");
  const latencies = scoring.filter((b) => b.scoring && b.inputRows > 0).map((b) => b.scoring.latencyS);
  const producer = data("live/producer-stats.json");
  const drills = data("live/drills.json");
  const replay = data("live/replay-analytics.json");
  const scoringJob = data("jobs/live-station-scoring.json");
  const cluster = data("cluster.json");
  const sparkUi = data("spark-ui.json");
  const hero = data("hero-trips.json");
  const peak = hero.underWay.reduce((a, b) => (b.underWay > a.underWay ? b : a), hero.underWay[0]);
  const champion = (t) => reg[t].leaderboard.find((r) => r.model === reg[t].champion);
  const naive = (t) => reg[t].baselines["seasonal-naive"].test;
  const clsChampion = cls.leaderboard.find((r) => r.model === cls.champion);
  // The models TrainChampionJob saved (Scala), and what evaluate-champions measured when it reloaded them.
  const cards = data("model-cards.json");
  const delta = (name) => cards.find((c) => c.name === name).deltaPct;
  const evaluation = existsSync(join(SERVING, "model-evaluation.json")) ? data("model-evaluation.json") : null;
  const demand = (name) => evaluation?.demand.find((d) => d.name === name);

  return {
    raw: { dq, jobs, rdd, net, reg, cls, ablation, arch, features, live, producer, drills, replay, cluster, hero, insights },
    data: {
      months: dq.byMonth.length,
      inputRows: int(dq.inputRows),
      validRows: int(dq.validRows),
      validRowsM: fixed(dq.validRows / 1e6, 1),
      inputRowsM: fixed(dq.inputRows / 1e6, 1),
      quarantined: int(dq.quarantinedRows),
      quarantinedPct: pct(dq.quarantinedPct, 3),
      reasons: dq.byReason,
      byMonth: dq.byMonth,
      stations: int(net.graph.vertices),
      liveStations: int(live.kpis.stations),
      endpointMatchPct: pct(jobs["silver-stations"].metrics.endpointMatchPct, 1),
    },
    batch: {
      jobs: Object.keys(jobs).length,
      minutes: min(batchMs),
      perJob: Object.entries(jobs).map(([name, j]) => ({ name, minutes: min(j.durationMs), seconds: Math.round(j.durationMs / 1000) })),
    },
    rdd: {
      groupBytes: mb(shuffle.groupByKey.shuffleBytes),
      reduceBytes: mb(shuffle.reduceByKey.shuffleBytes),
      shuffleRatio: fixed(shuffle.groupByKey.shuffleBytes / shuffle.reduceByKey.shuffleBytes, 0),
      joinBytes: mb(shuffle.join.shuffleBytes),
      broadcastBytes: int(shuffle.broadcast.shuffleBytes),
      uncachedMs: int(persist.NONE.secondCountMs),
      memoryMs: int(persist.MEMORY_ONLY.secondCountMs),
      serMs: int(persist.MEMORY_ONLY_SER.secondCountMs),
      diskMs: int(persist.DISK_ONLY.secondCountMs),
      memoryBytes: mb(persist.MEMORY_ONLY.memoryBytes),
      serBytes: mb(persist.MEMORY_ONLY_SER.memoryBytes),
      cacheSpeedup: fixed(persist.NONE.secondCountMs / persist.MEMORY_ONLY.secondCountMs, 0),
      accInAction: int(rdd.accumulators.inAction),
      accInMap: int(rdd.accumulators.inTransformationAfterTwoActions),
      partitions: int(rdd.inputPartitions),
      trips: int(rdd.trips),
    },
    sql: {
      queries: insights.length,
      totalSeconds: fixed(jobs.insights.metrics.totalMs / 1000, 1),
      worstRain: `${fixed(Math.abs(worstRain), 0)} %`,
      titles: insights.map((i) => ({ id: i.id, title: i.title, question: i.question, rows: i.rows })),
    },
    graph: {
      vertices: int(net.graph.vertices),
      edges: int(net.graph.edges),
      trips: int(net.graph.trips),
      density: fixed(net.graph.density, 3),
      reciprocity: fixed(net.graph.reciprocity, 3),
      supersteps: net.pagerank.weighted.supersteps,
      pagerankSeconds: fixed(net.pagerank.weighted.millis / 1000, 1),
      spearman: fixed(net.pagerank.spearman, 3),
      top: net.pagerank.topWeighted.slice(0, 5).map((s) => s.name),
      communities: net.communities.communities,
      modularity: fixed(net.communities.modularity, 3),
      lpaCommunities: net.communities.lpaCommunities,
      lpaModularity: fixed(net.communities.lpaModularity, 3),
      triangles: int(net.triangles.triangles),
      clustering: fixed(net.triangles.averageClustering, 3),
      randomClustering: fixed(net.triangles.randomGraphClustering, 4),
      scc: jobs["graph-metrics"].metrics.stronglyConnectedComponents,
      rf2D: fixed(part.EdgePartition2D.replicationFactor, 1),
      rfRandom: fixed(part.RandomVertexCut.replicationFactor, 1),
      rf1D: fixed(part.EdgePartition1D.replicationFactor, 1),
      partitions: part.EdgePartition2D.partitions,
      jobMinutes: min(jobs["graph-metrics"].durationMs),
    },
    ml: {
      k: arch.k,
      silhouette: fixed(arch.silhouette, 3),
      archetypes: arch.archetypes.map((a) => `${a.name} (${int(a.stations)})`),
      features: jobs["feature-build"].metrics.features,
      fitRows: int(jobs["feature-build"].metrics.fitRows),
      windows: features.windows,
      leakage: features.leakage,
      dep: { model: reg.departures.champion, mae: fixed(champion("departures").test.mae, 3), r2: fixed(champion("departures").test.r2, 3), naiveMae: fixed(naive("departures").mae, 3), improvement: fixed(reg.departures.improvementVsNaive, 1) },
      arr: { model: reg.arrivals.champion, mae: fixed(champion("arrivals").test.mae, 3), r2: fixed(champion("arrivals").test.r2, 3), naiveMae: fixed(naive("arrivals").mae, 3), improvement: fixed(reg.arrivals.improvementVsNaive, 1) },
      leaderboard: (t) => reg[t].leaderboard.map((r) => ({ model: r.model, mae: fixed(r.test.mae, 3), rmse: fixed(r.test.rmse, 3), r2: fixed(r.test.r2, 3) })),
      baselines: (t) => Object.entries(reg[t].baselines).map(([name, b]) => ({ model: name, mae: fixed(b.test.mae, 3), rmse: fixed(b.test.rmse, 3), r2: fixed(b.test.r2, 3) })),
      cls: {
        champion: cls.champion,
        macroF1: fixed(clsChampion.test.macroF1, 3),
        accuracy: fixed(clsChampion.test.accuracy, 3),
        majorityF1: fixed(cls.baselines.majority.test.macroF1, 3),
        naiveF1: fixed(cls.baselines["seasonal-naive"].test.macroF1, 3),
        leaderboard: cls.leaderboard.map((r) => ({ model: r.model, macroF1: fixed(r.test.macroF1, 3), accuracy: fixed(r.test.accuracy, 3) })),
      },
      ablation: ablation.variants.map((v) => ({ name: v.name, features: v.features, mae: fixed(v.test.mae, 3), delta: pct(v.deltaMaePct, 1) })),
      scala: {
        depDelta: fixed(delta("departures"), 2),
        arrDelta: fixed(delta("arrivals"), 2),
        regimeDelta: fixed(delta("regime"), 2),
        maxDelta: fixed(Math.max(...cards.map((c) => Math.abs(c.deltaPct))), 1),
      },
      evaluation: evaluation && {
        rows: int(evaluation.testRows),
        dep: { mae: fixed(demand("departures").model.mae, 3), naive: fixed(demand("departures").naive.mae, 3), better: fixed(((demand("departures").naive.mae - demand("departures").model.mae) / demand("departures").naive.mae) * 100, 1) },
        arr: { mae: fixed(demand("arrivals").model.mae, 3), naive: fixed(demand("arrivals").naive.mae, 3), better: fixed(((demand("arrivals").naive.mae - demand("arrivals").model.mae) / demand("arrivals").naive.mae) * 100, 1) },
        accuracy: fixed(evaluation.regime[0].model.accuracy, 3),
        macroF1: fixed(evaluation.regime[0].model.macroF1, 3),
        majorityAccuracy: fixed(evaluation.regime[0].majority.accuracy, 3),
        majorityF1: fixed(evaluation.regime[0].majority.macroF1, 3),
      },
    },
    stream: {
      batches: scoring.length,
      withRows: latencies.length,
      p50: fixed(quantile(latencies, 0.5), 1),
      p95: fixed(quantile(latencies, 0.95), 1),
      rowsIn: int(scoringJob.metrics.rowsIn),
      alerts: int(scoringJob.metrics.alerts),
      minutes: Math.round(scoringJob.durationMs / 60000),
      delivered: int(producer.delivered),
      failed: int(producer.failed),
      ackP50: int(producer.ackMs.p50),
      ackP95: int(producer.ackMs.p95),
      kpis: live.kpis,
      broker: drills.broker,
      restart: drills.restart,
      replay,
    },
    cluster: {
      workers: cluster.spark.aliveWorkers,
      cores: cluster.spark.cores,
      brokers: cluster.kafka.brokers.length,
      controller: cluster.kafka.controller,
      apps: sparkUi.applications.length,
    },
    hero: { trips: int(hero.totalTrips), shown: int(hero.shownTrips), stations: int(hero.stations.length), peak: int(peak.underWay) },
    bench: benchmark(),
  };
}

if (process.argv[1] && import.meta.filename === process.argv[1]) {
  const f = facts();
  delete f.raw;
  console.log(JSON.stringify(f, (k, v) => (typeof v === "function" ? undefined : v), 1));
}
