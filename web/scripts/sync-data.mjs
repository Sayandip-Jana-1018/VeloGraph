// Brings the batch pipeline's results into the website (ARCHITECTURE §10):
//   data/serving/**/*.json      -> web/public/data/        (what every portal reads)
//   docs/assets/spark-ui/*.png  -> web/public/spark-ui/    (the Spark UI evidence wall)
//   excerpts of spark/src/** and python/lab/** -> web/src/generated/snippets.json  ("How Spark did it" panels)
//   MapLibre's web worker       -> web/public/maplibre/
// Run from web/ with `pnpm sync` (scripts/sync-web-data.ps1 does the same from the repository root).
import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const web = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repo = resolve(web, "..");
const scala = "spark/src/main/scala/velograph";

/**
 * Each excerpt starts at the first line containing `start` and ends at the first later line matching `end`
 * (inclusive), so excerpts follow the code when lines move. A missing marker fails the sync loudly.
 */
const SNIPPETS = [
  { id: "lake-bronze-read", file: `${scala}/etl/BronzeIngestJob.scala`, start: "val raw = spark.read", end: /\.csv\(/ },
  { id: "lake-reject-reason", file: `${scala}/etl/SilverTripsTransforms.scala`, start: "def rejectReason", end: /^  }$/ },
  { id: "rdd-accumulators", file: `${scala}/rdd/RddFundamentalsJob.scala`, start: "// 2. Shared variable #1", end: /tagged\.count\(\)/ },
  { id: "rdd-persistence", file: `${scala}/rdd/RddFundamentalsJob.scala`, start: "val levels = Seq(", end: /secondMs\) = timed/ },
  { id: "rdd-shuffle", file: `${scala}/rdd/RddFundamentalsJob.scala`, start: "// 4. Pair RDDs", end: /require\(grouped\.value/ },
  { id: "rdd-broadcast", file: `${scala}/rdd/RddFundamentalsJob.scala`, start: "// 5. Shared variable #2", end: /^\s*$/ },
  { id: "rdd-lineage", file: `${scala}/rdd/RddFundamentalsJob.scala`, start: "// 7. Lineage", end: /toDebugString/ },
  { id: "sql-views", file: `${scala}/sql/InsightsJob.scala`, start: "def registerViews(", end: /^  }$/ },
  { id: "sparkui-shuffle-meter", file: `${scala}/rdd/ShuffleMeter.scala`, start: "override def onTaskEnd", end: /bytesWritten/ },
  { id: "pregel-program", file: `${scala}/graph/WeightedPageRank.scala`, start: "final case class Program", end: /^  }$/ },
  { id: "network-lpa", file: `${scala}/graph/Communities.scala`, start: "def propagate[", end: /^  }$/ },
  { id: "network-strength", file: `${scala}/graph/GraphStructure.scala`, start: "def strength(", end: /TripletFields\.EdgeOnly/ },
  { id: "graphops-group-edges", file: `${scala}/graph/OperatorTour.scala`, start: "// groupEdges: split", end: /groupEdges\(_ \+ _\)/ },
  { id: "graphops-partitioning", file: `${scala}/graph/GraphStructure.scala`, start: "def partitioning[", end: /partitioned\.unpersist/ },
  { id: "ml-kmeans-selection", file: `${scala}/ml/StationArchetypesJob.scala`, start: "val candidates = (c.kMin to c.kMax)", end: /val \(k, silhouette, _, model\)/ },
  { id: "ml-leakage-check", file: `${scala}/ml/FeatureBuildJob.scala`, start: "val lastProfileDay", end: /^      \)$/ },
  { id: "ml-champion-parity", file: `${scala}/ml/TrainChampionJob.scala`, start: "val lab   = labMetric", end: /^      \)$/ },
  { id: "ml-lab-pipeline", file: "python/lab/02_regression.py", start: "def pipeline(algo, params, target", end: /return Pipeline/ },
  { id: "ml-class-weights", file: "python/lab/03_classification.py", start: "def weighted(df):", end: /return df\.withColumn/ },
  { id: "stream-producer-cdc", file: "python/src/velograph/ingest/gbfs_producer.py", start: "def publish_status(", end: /return kind, sent/ },
  { id: "stream-watermark", file: `${scala}/streaming/LiveStationScoringJob.scala`, start: "val score: (DataFrame, Long) => Unit", end: /\.start\(\)/ },
  { id: "stream-risk", file: `${scala}/streaming/LiveRules.scala`, start: "(1 - fraction) * d0", end: /else Ok/ },
  { id: "stream-rebalance", file: `${scala}/streaming/LiveRules.scala`, start: "def plan(stations", end: /moves\.take\(rule\.maxMoves\)/ },
  { id: "stream-replay-windows", file: `${scala}/streaming/TripReplayAnalyticsJob.scala`, start: "def perStation(", end: /^      \)$/ },
  // Unit II: Scala in this codebase (the syllabus portal).
  { id: "scala-case-class", file: `${scala}/streaming/LiveRules.scala`, start: "final case class StationStatus(", end: /def docksInTotal/ },
  { id: "scala-pattern-match", file: `${scala}/ml/FeaturePipeline.scala`, start: "algorithm match {", end: /case other => throw/ },
  { id: "scala-option-try", file: `${scala}/ml/TrainChampionJob.scala`, start: "def champions(raw: Config)", end: /^  }$/ },
  { id: "scala-higher-order", file: `${scala}/ml/ArchetypeTransforms.scala`, start: "def distinct(names: Seq[String])", end: /^    }$/ },
  { id: "scala-immutable", file: `${scala}/ml/FeatureTransforms.scala`, start: "def alignCommunities(", end: /\.toMap \+ \(0 -> 0\)/ },
  { id: "scala-trait", file: `${scala}/common/SparkJob.scala`, start: "trait SparkJob {", end: /def sparkConf/ },
];

function copyTree(from, to, keep) {
  if (!existsSync(from)) throw new Error(`missing ${from}: run the batch jobs first`);
  rmSync(to, { recursive: true, force: true });
  mkdirSync(to, { recursive: true });
  cpSync(from, to, { recursive: true, filter: (src) => !src.match(/\.[a-z0-9]+$/i) || keep.test(src) });
}

function dedent(lines) {
  const indent = Math.min(...lines.filter((l) => l.trim()).map((l) => l.match(/^ */)[0].length));
  return lines.map((l) => l.slice(indent)).join("\n");
}

function excerpt({ id, file, start, end }) {
  const lines = readFileSync(join(repo, file), "utf8").split(/\r?\n/);
  const from = lines.findIndex((l) => l.includes(start));
  if (from < 0) throw new Error(`${id}: no line contains "${start}" in ${file}`);
  const to = lines.findIndex((l, i) => i > from && end.test(l));
  if (to < 0) throw new Error(`${id}: no line after ${from + 1} matches ${end} in ${file}`);
  const body = lines.slice(from, to + 1);
  while (body.length && !body.at(-1).trim()) body.pop(); // an empty end marker should not ship a blank line
  const lang = file.endsWith(".py") ? "python" : "scala";
  return { file, lang, startLine: from + 1, endLine: from + body.length, code: dedent(body) };
}

copyTree(join(repo, "data/serving"), join(web, "public/data"), /\.json$/i);
copyTree(join(repo, "docs/assets/spark-ui"), join(web, "public/spark-ui"), /\.png$/i);
// MapLibre 6 derives its worker URL from its own module path, which breaks once bundled; serve the worker
// ourselves and point MapLibre at it (setWorkerUrl in network-map.tsx).
mkdirSync(join(web, "public/maplibre"), { recursive: true });
cpSync(join(web, "node_modules/maplibre-gl/dist/maplibre-gl-worker.mjs"), join(web, "public/maplibre/maplibre-gl-worker.mjs"));

const snippets = Object.fromEntries(SNIPPETS.map((s) => [s.id, excerpt(s)]));
mkdirSync(join(web, "src/generated"), { recursive: true });
writeFileSync(join(web, "src/generated/snippets.json"), JSON.stringify(snippets, null, 2) + "\n");

// Size of the codebase, for the syllabus portal: files, non-blank lines, and tests, per language.
function sources(dir, pattern) {
  if (!existsSync(join(repo, dir))) return [];
  return readdirSync(join(repo, dir), { recursive: true })
    .filter((f) => pattern.test(f) && !/node_modules|[\\/]generated[\\/]/.test(f))
    .map((f) => readFileSync(join(repo, dir, f), "utf8"));
}
function measure(texts, testPattern) {
  return {
    files: texts.length,
    lines: texts.reduce((n, t) => n + t.split(/\r?\n/).filter((l) => l.trim()).length, 0),
    tests: testPattern ? texts.reduce((n, t) => n + (t.match(testPattern) ?? []).length, 0) : undefined,
  };
}
const codeStats = {
  scala: { ...measure(sources("spark/src/main/scala", /\.scala$/)), tests: measure(sources("spark/src/test/scala", /\.scala$/), /\btest\(\s*s?"/g).tests },
  python: { ...measure(sources("python/src", /\.py$/)), tests: measure(sources("python/tests", /\.py$/), /^def test_/gm).tests },
  lab: measure(sources("python/lab", /\.py$/)),
  sql: measure(sources("spark/src/main/resources/sql", /\.sql$/)),
  web: measure(sources("web/src", /\.tsx?$/)),
};
writeFileSync(join(web, "src/generated/code-stats.json"), JSON.stringify(codeStats, null, 2) + "\n");

const files = (dir) => readdirSync(dir, { recursive: true }).filter((f) => /\.[a-z]+$/i.test(f)).length;
console.log(
  `synced ${files(join(web, "public/data"))} data files, ${files(join(web, "public/spark-ui"))} screenshots, ` +
    `${SNIPPETS.length} code excerpts`,
);
