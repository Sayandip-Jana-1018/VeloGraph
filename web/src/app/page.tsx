import Link from "next/link";

import { Doodle } from "@/components/inkglass/ink";
import { Notate } from "@/components/inkglass/ink-client";
import { Reveal, SpotlightCard } from "@/components/inkglass/motion";
import { Sticker } from "@/components/inkglass/paper";
import { Chip, KpiPuck, Marquee } from "@/components/inkglass/surfaces";
import { CityLights } from "@/components/site/city-lights";
import { DistrictSketch } from "@/components/site/district-sketch";
import { PipelineStory, type Stop } from "@/components/site/pipeline-story";
import {
  getCityLights,
  getClusterSnapshot,
  getDqReport,
  getInsights,
  getPipelineReports,
  getLiveSnapshot,
  getMlRegression,
  getNetworkSummary,
  getRddReport,
  getSparkUi,
  getStreamProgress,
} from "@/lib/data";
import { compact, delta, duration, fixed, int } from "@/lib/format";
import { cn } from "@/lib/cn";
import { scoringLatency } from "@/lib/stream-stats";
import { PORTALS, type PortalId } from "@/portals/registry";

export default async function Home() {
  const [dq, net, rdd, insights, pipeline, city, ui, reg, live, progress, cluster] = await Promise.all([
    getDqReport(),
    getNetworkSummary(),
    getRddReport(),
    getInsights(),
    getPipelineReports(),
    getCityLights(),
    getSparkUi(),
    getMlRegression(),
    getLiveSnapshot(),
    getStreamProgress(),
    getClusterSnapshot(),
  ]);
  const shuffle = Object.fromEntries(rdd.data.shuffle.map((s) => [s.operation, s.shuffleBytes]));
  const shuffleRatio = shuffle.groupByKey / shuffle.reduceByKey;
  const rain = insights.find((i) => i.data.id === "rain-impact")!;
  const worstRain = Math.min(...rain.data.rows.map((r) => Number(r.vs_dry_pct)));
  const g = net.data.graph;
  const c = net.data.communities;
  const twoD = net.data.partitioning.find((p) => p.strategy === "EdgePartition2D")!;
  const random = net.data.partitioning.find((p) => p.strategy === "RandomVertexCut")!;
  const pipelineMs = pipeline.batch.reduce((sum, j) => sum + j.data.durationMs, 0);
  const persist = Object.fromEntries(rdd.data.persistence.map((p) => [p.level, p.secondCountMs]));
  const { departures, arrivals } = reg.data.targets;
  const latency = scoringLatency(progress.data);
  const { spark, kafka } = cluster.data;

  const figure: Record<PortalId, string> = {
    lake: `${compact(dq.data.validRows)} clean trips`,
    "rdd-lab": `${fixed(shuffleRatio, 0)}× less shuffle`,
    sql: `${delta(worstRain)} riders in heavy rain`,
    "spark-ui": `${ui.data.applications.length} runs on record`,
    pregel: `${net.data.pagerank.weighted.supersteps} supersteps to converge`,
    network: `Q = ${fixed(c.modularity, 3)} · ${c.communities} districts`,
    "graph-ops": `${fixed(twoD.replicationFactor, 1)} vs ${fixed(random.replicationFactor, 1)} copies`,
    models: `${fixed(reg.data.targets.departures.improvementVsNaive, 0)} % under seasonal naive`,
    streaming: `${int(live.data.kpis.stations)} stations every 30 s`,
    cluster: `${pipeline.batch.length} jobs in ${duration(pipelineMs)}`,
    syllabus: "6 units · 3 COs, all traced",
  };
  const facts = [
    `${int(dq.data.validRows)} clean trips`,
    `${int(g.vertices)} stations`,
    `${int(g.edges)} directed routes`,
    `PageRank converged in ${net.data.pagerank.weighted.supersteps} Pregel supersteps`,
    `${c.communities} communities · modularity ${fixed(c.modularity, 3)}`,
    `heavy rain: ${delta(worstRain)} riders`,
    `reduceByKey shuffles ${fixed(shuffleRatio, 0)}× less than groupByKey`,
    `${int(net.data.triangles.triangles)} triangles of strong ties`,
    `the whole batch pipeline: ${duration(pipelineMs)}`,
  ];
  const stops: Stop[] = [
    {
      id: "lake",
      line: "Lake",
      api: "DataFrames · Parquet",
      kicker: "stop 1 · land it",
      title: "Four months of trips, cleaned in layers",
      body: "Citi Bike's trip files for June to September, its live station feed and Open-Meteo's weather land in bronze as they came. Silver keeps the valid trips and gives a reason for every one it drops; gold reshapes them into a station-by-hour panel and route edges.",
      facts: [
        { value: compact(dq.data.inputRows), label: "raw rows in" },
        { value: compact(dq.data.validRows), label: "clean trips out" },
        { value: int(dq.data.quarantinedRows), label: "quarantined, each with a reason" },
      ],
      portals: ["lake"],
    },
    {
      id: "rdds",
      line: "RDDs",
      api: "Spark core",
      kicker: "stop 2 · under the hood",
      title: "The RDDs underneath",
      body: "The same trips as a raw RDD: partitions, lazy transformations, actions and the lineage that rebuilds a lost partition. reduceByKey combines on each partition before the shuffle, and caching skips the recompute.",
      facts: [
        { value: `${fixed(shuffleRatio, 0)}×`, label: "less shuffle with reduceByKey" },
        { value: `${fixed(persist.NONE / persist.MEMORY_ONLY, 0)}×`, label: "faster recount from MEMORY_ONLY" },
      ],
      portals: ["rdd-lab", "spark-ui"],
    },
    {
      id: "sql",
      line: "Spark SQL",
      api: "Catalyst",
      kicker: "stop 3 · ask questions",
      title: "Spark SQL over the lake",
      body: "Questions written as plain SQL files and planned by Catalyst: when the city rides, where commuters go, which stations drain, and what the weather does to demand.",
      facts: [
        { value: int(insights.length), label: "insights, one SQL file each" },
        { value: delta(worstRain), label: "riders in heavy rain" },
      ],
      portals: ["sql"],
    },
    {
      id: "graphx",
      line: "GraphX",
      api: "Pregel · label propagation",
      kicker: "stop 4 · connect the dots",
      title: "Stations become a graph",
      body: "Every station is a vertex and every route a weighted edge. GraphX runs PageRank as Pregel supersteps, finds districts by label propagation, counts triangles of strong ties and measures travel times to the hubs.",
      facts: [
        { value: int(g.vertices), label: "stations" },
        { value: compact(g.edges), label: "directed routes" },
        { value: int(net.data.pagerank.weighted.supersteps), label: "supersteps for PageRank" },
        { value: int(c.communities), label: `districts, modularity ${fixed(c.modularity, 3)}` },
      ],
      portals: ["network", "pregel", "graph-ops"],
    },
    {
      id: "mllib",
      line: "MLlib",
      api: "ML Pipelines",
      kicker: "stop 5 · predict",
      title: "MLlib forecasts the next hour",
      body: "Time-based windows keep the future out of training. Weather, the calendar and the graph's own metrics feed a random forest for departures and gradient-boosted trees for arrivals, each judged against a seasonal naive forecast.",
      facts: [
        { value: `${fixed(departures.improvementVsNaive, 1)} %`, label: "lower error than seasonal naive, departures" },
        { value: `${fixed(arrivals.improvementVsNaive, 1)} %`, label: "lower error, arrivals" },
      ],
      portals: ["models"],
    },
    {
      id: "streaming",
      line: "Streaming",
      api: "Kafka · Structured Streaming",
      kicker: "stop 6 · go live",
      title: "Every 30 seconds, live",
      body: "A producer publishes the live station feed to a three-broker Kafka cluster. Structured Streaming scores every station with the champion models, flags the ones about to run empty or full and suggests moves; this site follows along over server-sent events.",
      facts: [
        { value: int(live.data.kpis.stations), label: "stations scored per micro-batch" },
        { value: `${fixed(latency.p95, 1)} s`, label: `broker to map, p95 over ${int(latency.batches)} batches` },
      ],
      portals: ["streaming"],
      page: { href: "/live", label: "The live map" },
    },
    {
      id: "cluster",
      line: "Cluster",
      api: "Standalone · KRaft",
      kicker: "stop 7 · run it for real",
      title: "On a real cluster",
      body: "A Spark master with two workers and three Kafka brokers, in Docker. The heavy batch jobs run on the host; the two streams share the workers, two cores each, while the brokers keep three copies of every partition.",
      facts: [
        { value: int(pipeline.batch.length), label: `batch jobs in ${duration(pipelineMs)}` },
        { value: `${int(spark.aliveWorkers ?? 0)} + ${int(kafka.brokers?.length ?? 0)}`, label: "Spark workers + Kafka brokers" },
      ],
      portals: ["cluster"],
    },
    {
      id: "syllabus",
      line: "Syllabus",
      api: "Units I–VI · CO4–CO6",
      kicker: "last stop · defend it",
      title: "Every unit, traced",
      body: "Each unit of the syllabus and each course outcome points to what was built, one number it produced and the portal that proves it.",
      facts: [
        { value: "6 / 6", label: "units traced" },
        { value: "3 / 3", label: "course outcomes" },
      ],
      portals: ["syllabus"],
    },
  ];

  const layout: Record<PortalId, string> = {
    network: "lg:col-span-7 lg:row-span-2",
    pregel: "lg:col-span-5",
    "graph-ops": "lg:col-span-5",
    lake: "lg:col-span-4",
    "rdd-lab": "lg:col-span-4",
    sql: "lg:col-span-4",
    "spark-ui": "lg:col-span-4",
    models: "lg:col-span-4",
    streaming: "lg:col-span-4",
    cluster: "lg:col-span-6",
    syllabus: "lg:col-span-6",
  };
  const order: PortalId[] = [
    "network",
    "pregel",
    "graph-ops",
    "lake",
    "rdd-lab",
    "sql",
    "spark-ui",
    "models",
    "streaming",
    "cluster",
    "syllabus",
  ];

  return (
    <div className="mx-auto max-w-[1400px] px-5 sm:px-8">
      {/* ---- hero: the claim on the left, the city on the right (stacked and centred on small screens) ---------- */}
      <section className="grid grid-cols-1 items-center gap-x-14 gap-y-12 pt-12 sm:pt-16 lg:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)]">
        <div className="flex flex-col items-center text-center lg:items-start lg:text-left">
          <Reveal className="flex flex-wrap justify-center gap-3 lg:justify-start">
            <Sticker accent="marker" tilt={-4}>
              Real data · {compact(dq.data.inputRows)} rows
            </Sticker>
            <Sticker accent="violet" tilt={3}>
              GraphX · MLlib · Kafka
            </Sticker>
            <Sticker accent="coral" tilt={-2}>
              INT315 · CO4 CO5 CO6
            </Sticker>
          </Reveal>
          <Reveal delay={0.05}>
            <h1 className="mt-9 font-display text-[clamp(3.3rem,6.6vw,7rem)] leading-[0.88] font-extrabold tracking-[-0.035em] text-fg">
              New York,
              <br />
              <span className="text-gleam">as a graph.</span>
            </h1>
          </Reveal>
          <Reveal delay={0.12}>
            <p className="mt-8 max-w-xl text-lg leading-relaxed text-pretty text-fg-2">
              Four months of Citi Bike trips, {int(dq.data.validRows)} of them clean, through a Spark lake, Spark SQL
              and GraphX. Every number on this site is read from what those jobs wrote.{" "}
              <Notate type="underline" color="var(--color-marker)" delay={700}>
                Open any portal to see the evidence.
              </Notate>
            </p>
          </Reveal>
          <Reveal delay={0.18} className="mt-9 flex flex-wrap justify-center gap-4 lg:justify-start">
            <Link href="#portals" className="clay rounded-full px-6 py-3 font-semibold text-marker">
              Explore the portals ↓
            </Link>
            <Link href="/portal/pregel" scroll={false} className="clay rounded-full px-6 py-3 font-semibold text-fg hover:text-marker">
              ▶ Watch Pregel converge
            </Link>
          </Reveal>
        </div>

        <Reveal delay={0.1}>
          <figure>
            <div className="map-surface glow-ring relative overflow-hidden rounded-[2.25rem]">
              <CityLights
                points={city.points}
                flows={city.flows}
                replay="/data/hero-trips.json"
                label={`Map of New York's ${int(city.points.length)} Citi Bike stations, sized by PageRank, replaying the trips of Thursday 17 September`}
                className="block h-[clamp(28rem,68vh,40rem)] w-full"
                clockClassName="pointer-events-none absolute top-5 right-6 text-right text-[0.68rem] leading-relaxed font-semibold tracking-[0.16em] whitespace-pre-line text-fg-2 uppercase tabular-nums"
              />
              <div className="pointer-events-none absolute top-0 left-0 px-6 pt-5 text-[0.68rem] font-semibold tracking-[0.16em] text-fg-3 uppercase">
                <span className="flex items-center gap-2">
                  <span className="size-2 animate-pulse rounded-full bg-marker" /> {int(city.points.length)} stations
                </span>
              </div>
              <ul className="pointer-events-none absolute inset-x-0 bottom-0 flex flex-wrap justify-center gap-x-5 gap-y-1 px-6 pb-4 text-xs text-fg-3">
                <li className="flex items-center gap-1.5">
                  <span className="size-2 rounded-full bg-glow-b" /> station, sized by PageRank
                </li>
                <li className="flex items-center gap-1.5">
                  <span className="size-2.5 rounded-full bg-marker ring-2 ring-marker/30" /> top-40 hub
                </li>
                <li className="flex items-center gap-1.5">
                  <span className="h-0.5 w-4 rounded-full bg-marker" /> a real trip, 1 in 5, at 120× speed
                </li>
              </ul>
            </div>
            <figcaption className="mt-4 text-center font-hand text-xl text-balance text-fg-2">
              one real evening, Thursday 17 September: every streak is a trip, leaving when it really left
            </figcaption>
          </figure>
        </Reveal>
      </section>

      <Reveal className="mt-16 flex flex-wrap justify-center gap-5">
        <KpiPuck label="Stations" count={{ to: g.vertices }} note="vertices" />
        <KpiPuck label="Routes" count={{ to: g.edges, format: "compact" }} note="directed edges" />
        <KpiPuck label="Clean trips" count={{ to: dq.data.validRows, format: "compact" }} note="after quality checks" />
        <KpiPuck label="Districts" count={{ to: c.communities }} note={`modularity ${fixed(c.modularity, 3)}`} />
      </Reveal>

      <Marquee items={facts} className="-mx-5 mt-20 border-y border-white/10 bg-ink-900/50 py-4 sm:-mx-8" />

      {/* ---- the pipeline as a subway line, chapter by chapter ------------------------------------------------ */}
      <PipelineStory stops={stops} />

      {/* ---- portals ------------------------------------------------------------------------------------------ */}
      <section id="portals" className="scroll-mt-28 pt-28">
        <Reveal className="mx-auto mb-14 max-w-2xl text-center">
          <p className="font-hand text-2xl text-marker">step through a portal</p>
          <h2 className="font-display text-5xl font-extrabold tracking-tight text-fg sm:text-7xl">Evidence portals</h2>
          <p className="mt-4 text-fg-2">
            One portal per module. Each opens over this page with its own address, so the report, the slides and the
            viva can link straight to it.
          </p>
        </Reveal>

        <div className="grid grid-cols-1 gap-6 lg:grid-cols-12">
          {order.map((id, i) => {
            const meta = PORTALS.find((p) => p.id === id)!;
            return (
              <Reveal key={id} delay={0.05 * i} className={cn("h-full", layout[id])}>
                <Link href={`/portal/${id}`} scroll={false} className="group block h-full">
                  <SpotlightCard className="glass glow-ring relative flex h-full min-h-[17rem] flex-col items-center justify-between overflow-hidden rounded-[var(--radius-card)] p-8 text-center transition-transform duration-500 group-hover:-translate-y-1.5">
                    <div className="flex flex-col items-center">
                      <div className="mb-4 flex flex-wrap justify-center gap-2">
                        {meta.units.map((u) => (
                          <Chip key={u} tone="unit">
                            {u}
                          </Chip>
                        ))}
                        {meta.outcomes.map((o) => (
                          <Chip key={o} tone="co">
                            {o}
                          </Chip>
                        ))}
                      </div>
                      <p className="font-hand text-xl text-marker">{meta.kicker}</p>
                      <h3
                        className={cn(
                          "font-display font-extrabold tracking-tight text-fg",
                          id === "network" ? "text-5xl sm:text-7xl" : "text-3xl",
                        )}
                      >
                        {meta.title}
                      </h3>
                      <p className="mt-3 max-w-md text-sm leading-relaxed text-fg-2">{meta.hook}</p>
                    </div>
                    {id === "network" && (
                      <figure className="mt-6 w-full">
                        <DistrictSketch className="mx-auto h-auto w-full max-w-[44rem]" />
                        <figcaption className="mt-2 font-hand text-lg text-fg-2">
                          each outline is one of the {c.communities} districts GraphX found in who rides where
                        </figcaption>
                      </figure>
                    )}
                    <div className="mt-7 flex flex-wrap items-center justify-center gap-4">
                      <Sticker accent={meta.accent} tilt={i % 2 ? 3 : -3}>
                        {figure[id]}
                      </Sticker>
                      <span className="clay rounded-full px-5 py-2 text-sm font-semibold whitespace-nowrap text-fg transition-colors group-hover:text-marker">
                        Open portal ↗
                      </span>
                    </div>
                    <Doodle
                      kind={i % 3 === 0 ? "star" : i % 3 === 1 ? "spark" : "squiggle"}
                      size={id === "network" ? 80 : 46}
                      seed={i + 3}
                      color="var(--color-marker)"
                      className="absolute top-6 right-6 opacity-30 transition-transform duration-700 group-hover:rotate-45"
                    />
                  </SpotlightCard>
                </Link>
              </Reveal>
            );
          })}
        </div>
      </section>
    </div>
  );
}
