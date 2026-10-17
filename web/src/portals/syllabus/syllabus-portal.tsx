import Link from "next/link";

import { Sticker } from "@/components/inkglass/paper";
import { Chip, Glass, KpiPuck } from "@/components/inkglass/surfaces";
import { Act, CodePanel, type SnippetId, VivaNote, WithNote } from "@/components/portal/blocks";
import codeStats from "@/generated/code-stats.json";
import {
  getArchetypes,
  getClusterSnapshot,
  getDqReport,
  getDrills,
  getInsights,
  getMlAblation,
  getMlClassification,
  getMlRegression,
  getModelCards,
  getNetworkSummary,
  getPipelineReports,
  getRddReport,
  getStreamProgress,
} from "@/lib/data";
import { compact, delta, duration, fixed, int } from "@/lib/format";
import { type PortalId, portalById } from "@/portals/registry";

type Row = {
  unit: string;
  tone: "unit" | "co";
  title: string;
  syllabus: string;
  built: string;
  proof: string;
  note: string;
  open: (PortalId | "#scala" | "/live")[];
};

function OpenLinks({ targets }: { targets: Row["open"] }) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {targets.map((t) => {
        const label = t === "#scala" ? "Scala tour ↓" : t === "/live" ? "Live page" : (portalById(t)?.title ?? t);
        const href = t.startsWith("#") || t.startsWith("/") ? t : `/portal/${t}`;
        return (
          <Link
            key={t}
            href={href}
            scroll={!t.startsWith("#")}
            className="clay inline-flex items-center rounded-full px-3 py-1 text-xs font-semibold text-fg hover:text-marker"
          >
            {label}
          </Link>
        );
      })}
    </div>
  );
}

export async function SyllabusPortal() {
  const [dq, rdd, net, insights, reg, cls, arch, abl, cards, drills, progress, pipeline, cluster] = await Promise.all([
    getDqReport(),
    getRddReport(),
    getNetworkSummary(),
    getInsights(),
    getMlRegression(),
    getMlClassification(),
    getArchetypes(),
    getMlAblation(),
    getModelCards(),
    getDrills(),
    getStreamProgress(),
    getPipelineReports(),
    getClusterSnapshot(),
  ]);
  const shuffle = Object.fromEntries(rdd.data.shuffle.map((s) => [s.operation, s.shuffleBytes]));
  const none = rdd.data.persistence.find((p) => p.level === "NONE")!;
  const memory = rdd.data.persistence.find((p) => p.level === "MEMORY_ONLY")!;
  const rain = insights.find((i) => i.data.id === "rain-impact")!;
  const worstRain = Math.min(...rain.data.rows.map((r) => Number(r.vs_dry_pct)));
  const dep = reg.data.targets.departures;
  const clsChampion = cls.data.leaderboard.find((r) => r.model === cls.data.champion)!;
  const both = abl.data.variants.find((v) => v.dropped.length === 2);
  const graphOnly = abl.data.variants.find((v) => v.dropped.length === 1 && v.dropped[0] === "graph");
  const maxDelta = Math.max(...cards.data.map((c) => Math.abs(c.deltaPct)));
  const latencies = progress.data.batches
    .filter((b) => b.scoring && b.inputRows > 0)
    .map((b) => b.scoring!.latencyS)
    .sort((a, b) => a - b);
  const p95 = latencies.length ? latencies[Math.min(latencies.length - 1, Math.floor(0.95 * latencies.length))] : 0;
  const pipelineMs = pipeline.batch.reduce((s, j) => s + j.data.durationMs, 0);
  const workers = cluster.data.spark.workers?.length ?? 2;

  const rows: Row[] = [
    {
      unit: "Unit I",
      tone: "unit",
      title: "Big data, Spark and its install",
      syllabus: "MapReduce's limits, batch vs real-time, in-memory processing, a standalone install, Spark vs Hadoop",
      built: `A batch lake and a live stream side by side, caching measured, and a Spark standalone cluster (1 master + ${workers} workers) next to the host install`,
      proof: `${int(none.secondCountMs)} → ${int(memory.secondCountMs)} ms`,
      note: "the same count, uncached and then from memory",
      open: ["rdd-lab", "cluster", "spark-ui"],
    },
    {
      unit: "Unit II",
      tone: "unit",
      title: "Scala",
      syllabus: "Types, literals, operators, inference, mutable and immutable collections, functions, lists, maps",
      built: "Every Spark job, GraphX algorithm and streaming rule is idiomatic Scala: case classes, pattern matching, Option and Try, higher-order functions, traits",
      proof: `${int(codeStats.scala.lines)} lines`,
      note: `of Scala in ${codeStats.scala.files} files, with ${codeStats.scala.tests} tests`,
      open: ["#scala", "graph-ops"],
    },
    {
      unit: "Unit III",
      tone: "unit",
      title: "RDDs and shared variables",
      syllabus: "RDD features, creation, transformations and actions, spark-shell, broadcast variables and accumulators",
      built: "RddFundamentalsJob on one raw month: map, filter, flatMap, reduceByKey vs groupByKey, join, persistence levels, lineage, broadcast, accumulators",
      proof: `${fixed(shuffle.groupByKey / shuffle.reduceByKey, 0)}× less shuffle`,
      note: "reduceByKey against groupByKey, measured by a SparkListener",
      open: ["rdd-lab", "spark-ui"],
    },
    {
      unit: "Unit III",
      tone: "unit",
      title: "GraphX",
      syllabus: "GraphX features and operations; feature extraction and transformation",
      built: "Weighted PageRank written with the Pregel API, label propagation, components, triangles, travel times, and 13 graph operators on the real graph",
      proof: `${net.data.communities.communities} districts`,
      note: `modularity ${fixed(net.data.communities.modularity, 3)}; PageRank in ${net.data.pagerank.weighted.supersteps} supersteps`,
      open: ["network", "pregel", "graph-ops"],
    },
    {
      unit: "Unit IV",
      tone: "unit",
      title: "Spark SQL",
      syllabus: "RDD to DataFrame, joins, groupBy, orderBy, aggregates, wildcards",
      built: `${insights.length} named SQL queries (self-joins, window functions, LIKE and RLIKE) and the RDD → DataFrame conversion three ways`,
      proof: `${delta(worstRain)} riders`,
      note: "in heavy rain, against dry hours: one of the 14 queries",
      open: ["sql", "rdd-lab"],
    },
    {
      unit: "Unit V",
      tone: "unit",
      title: "Kafka",
      syllabus: "Kafka fundamentals, cluster architecture, Spark integration, producer and consumer, pipelines, ingestion patterns",
      built: "A 3-broker KRaft cluster (RF 3, min ISR 2), a CDC producer, Spark reading and writing Kafka, a consumer group, a dead-letter queue",
      proof: `${int(drills.data.broker?.producerFailed ?? 0)} lost`,
      note: `messages with a broker down for ${fixed(drills.data.broker?.downS ?? 0, 0)} s mid-stream`,
      open: ["streaming", "cluster"],
    },
    {
      unit: "Unit VI",
      tone: "unit",
      title: "Machine learning with PySpark",
      syllabus: "Linear and logistic regression, decision trees, k-means, SVM, naive Bayes; confusion matrix, R², RMSE, MAE, correlation heat map",
      built: `Four PySpark lab notebooks: every listed algorithm against baselines on leakage-safe windows, and KMeans archetypes (k = ${arch.data.k})`,
      proof: `${fixed(dep.improvementVsNaive, 1)} % better`,
      note: `next-hour departures (MAE) than the seasonal-naive forecast; regime macro-F1 ${fixed(clsChampion.test.macroF1, 3)}`,
      open: ["models"],
    },
    {
      unit: "CO4",
      tone: "co",
      title: "Apply MLlib",
      syllabus: "Use Spark's machine learning library on real data",
      built: "The lab's champions rebuilt in Scala as PipelineModels, checked against the lab, then scoring every station every 30 s",
      proof: `Δ ${fixed(maxDelta, 2)} %`,
      note: "Scala retraining against the lab's September scores",
      open: ["models", "/live"],
    },
    {
      unit: "CO5",
      tone: "co",
      title: "Spark workflows",
      syllabus: "Build and run a multi-stage Spark workflow",
      built: "bronze → silver → gold → SQL → GraphX → features → models → streaming, every job timed in its own report",
      proof: duration(pipelineMs),
      note: `${pipeline.batch.length} batch jobs over ${compact(dq.data.inputRows)} raw rows; live p95 ${fixed(p95, 1)} s`,
      open: ["lake", "cluster", "streaming"],
    },
    {
      unit: "CO6",
      tone: "co",
      title: "Apply GraphX",
      syllabus: "Use graph processing to answer a real question",
      built: "Graph features (PageRank, strengths, districts) recomputed per window and measured inside the forecast",
      proof: both ? delta(both.deltaMaePct, 1) : "—",
      note: `MAE without the graph and profile features${graphOnly ? `; ${delta(graphOnly.deltaMaePct, 1)} without the graph alone` : ""}`,
      open: ["network", "models"],
    },
  ];

  const practicals: { title: string; where: string; open: Row["open"] }[] = [
    { title: "MLlib", where: "PySpark lab 01–04 and the Scala champions", open: ["models"] },
    { title: "Spark SQL", where: "14 named queries over the lake", open: ["sql"] },
    { title: "GraphX", where: "the station network suite", open: ["network", "pregel"] },
    { title: "Spark Streaming", where: "live scoring and replay analytics", open: ["streaming", "/live"] },
    { title: "Kafka producer and consumer", where: "gbfs_producer and the gateway's consumer group", open: ["streaming", "cluster"] },
  ];

  const scala: { id: SnippetId; concept: string; caption: string }[] = [
    { id: "scala-case-class", concept: "Case classes", caption: "immutable values with typed fields, equality and copy, plus their own methods" },
    { id: "scala-pattern-match", concept: "Pattern matching", caption: "a match on the algorithm name picks the estimator, with guards on the label" },
    { id: "scala-option-try", concept: "Option and Try", caption: "Try turns a missing config key into a default instead of an exception" },
    { id: "scala-higher-order", concept: "Higher-order functions", caption: "zipWithIndex, map with a pattern-matching function, count with a lambda" },
    { id: "scala-immutable", concept: "Immutable collections", caption: "maps grouped, sorted and folded without mutating anything; Option with collect" },
    { id: "scala-trait", concept: "Traits", caption: "the template every job extends, with abstract and default members" },
  ];

  return (
    <>
      <Act
        n={1}
        title="Every unit, one click from its evidence"
        lede="The CA assesses CO4, CO5 and CO6 and asks for a project that brings the syllabus together. Each row says what the unit covers, what VeloGraph built for it, and one number it measured, with the portals that show the rest."
      >
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Units covered" value="6 of 6" note="Unit I to Unit VI" />
          <KpiPuck label="Course outcomes" value="3 of 3" note="CO4 · CO5 · CO6" />
          <KpiPuck label="Code" value={compact(codeStats.scala.lines + codeStats.python.lines + codeStats.lab.lines + codeStats.web.lines)} note="lines of Scala, Python and TypeScript" />
          <KpiPuck label="Tests" value={int(codeStats.scala.tests + codeStats.python.tests)} note={`${codeStats.scala.tests} Scala · ${codeStats.python.tests} Python`} />
        </div>
        <div className="space-y-3">
          {rows.map((r) => (
            <Glass key={`${r.unit}-${r.title}`} variant="inset" className="grid items-center gap-4 p-4 lg:grid-cols-[11rem_minmax(0,1fr)_12rem_13rem]">
              <div>
                <Chip tone={r.tone}>{r.unit}</Chip>
                <p className="mt-1.5 font-display text-base leading-tight font-bold text-fg">{r.title}</p>
              </div>
              <div className="text-sm">
                <p className="text-xs text-fg-3">{r.syllabus}</p>
                <p className="mt-1 text-fg-2">{r.built}</p>
              </div>
              <div>
                <p className="font-display text-2xl leading-none font-bold text-marker">{r.proof}</p>
                <p className="mt-1 text-xs leading-snug text-fg-3">{r.note}</p>
              </div>
              <OpenLinks targets={r.open} />
            </Glass>
          ))}
        </div>
      </Act>

      <Act
        n={2}
        title="The five practicals"
        lede="The practical list of the course, each where it runs in VeloGraph and where to see it."
      >
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-5">
          {practicals.map((p, i) => (
            <Glass key={p.title} variant="inset" className="flex flex-col gap-3 p-4 text-center">
              <div className="flex justify-center">
                <Sticker accent={(["coral", "amber", "mint", "teal", "violet"] as const)[i]} tilt={i % 2 ? 3 : -3}>
                  {p.title}
                </Sticker>
              </div>
              <p className="flex-1 text-sm text-fg-2">{p.where}</p>
              <div className="flex justify-center">
                <OpenLinks targets={p.open} />
              </div>
            </Glass>
          ))}
        </div>
      </Act>

      <section id="scala" className="scroll-mt-24">
        <Act
          n={3}
          title="Unit II, in our own Scala"
          lede="Every Spark job, GraphX algorithm and streaming rule here is Scala. These excerpts are copied from the code at build time, with their file and line, so they are the code that ran."
        >
          <WithNote
            note={
              <VivaNote>
                The habits that matter in a Spark codebase: immutable values (case classes, immutable maps), small
                pure functions that Spark can ship to executors, Option and Try instead of nulls and exceptions, and
                pattern matching to choose behaviour. Each excerpt has one.
              </VivaNote>
            }
          >
            <Glass variant="inset" className="p-5">
              <p className="font-display text-sm font-bold text-fg">Unit II, concept by concept</p>
              <ul className="mt-3 space-y-2 text-sm text-fg-2">
                {scala.map((s) => (
                  <li key={s.id} className="flex gap-3">
                    <span className="w-36 shrink-0 font-semibold text-fg">{s.concept}</span>
                    <span>{s.caption}</span>
                  </li>
                ))}
              </ul>
            </Glass>
          </WithNote>
          <div className="grid items-start gap-5 xl:grid-cols-2">
            {scala.map((s) => (
              <CodePanel key={s.id} id={s.id} caption={s.concept} kicker="In our Scala" />
            ))}
          </div>
        </Act>
      </section>
    </>
  );
}
