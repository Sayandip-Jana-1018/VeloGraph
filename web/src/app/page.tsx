import Link from "next/link";

import { Doodle } from "@/components/inkglass/ink";
import { Notate } from "@/components/inkglass/ink-client";
import { Reveal, SpotlightCard } from "@/components/inkglass/motion";
import { Sticker } from "@/components/inkglass/paper";
import { Chip, KpiPuck, Marquee } from "@/components/inkglass/surfaces";
import { CityLights } from "@/components/site/city-lights";
import { DistrictSketch } from "@/components/site/district-sketch";
import {
  getCityLights,
  getDqReport,
  getInsights,
  getJobReports,
  getNetworkSummary,
  getRddReport,
  getSparkUi,
} from "@/lib/data";
import { compact, delta, duration, fixed, int } from "@/lib/format";
import { cn } from "@/lib/cn";
import { PORTALS, UPCOMING, type PortalId } from "@/portals/registry";

export default async function Home() {
  const [dq, net, rdd, insights, jobs, city, ui] = await Promise.all([
    getDqReport(),
    getNetworkSummary(),
    getRddReport(),
    getInsights(),
    getJobReports(),
    getCityLights(),
    getSparkUi(),
  ]);
  const shuffle = Object.fromEntries(rdd.data.shuffle.map((s) => [s.operation, s.shuffleBytes]));
  const shuffleRatio = shuffle.groupByKey / shuffle.reduceByKey;
  const rain = insights.find((i) => i.data.id === "rain-impact")!;
  const worstRain = Math.min(...rain.data.rows.map((r) => Number(r.vs_dry_pct)));
  const g = net.data.graph;
  const c = net.data.communities;
  const twoD = net.data.partitioning.find((p) => p.strategy === "EdgePartition2D")!;
  const random = net.data.partitioning.find((p) => p.strategy === "RandomVertexCut")!;
  const pipelineMs = jobs.reduce((sum, j) => sum + j.data.durationMs, 0);

  const figure: Record<PortalId, string> = {
    lake: `${compact(dq.data.validRows)} clean trips`,
    "rdd-lab": `${fixed(shuffleRatio, 0)}× less shuffle`,
    sql: `${delta(worstRain)} riders in heavy rain`,
    "spark-ui": `${ui.data.applications.length} runs on record`,
    pregel: `${net.data.pagerank.weighted.supersteps} supersteps to converge`,
    network: `Q = ${fixed(c.modularity, 3)} · ${c.communities} districts`,
    "graph-ops": `${fixed(twoD.replicationFactor, 1)} vs ${fixed(random.replicationFactor, 1)} copies`,
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
  const layout: Record<PortalId, string> = {
    network: "lg:col-span-7 lg:row-span-2",
    pregel: "lg:col-span-5",
    "graph-ops": "lg:col-span-5",
    lake: "lg:col-span-4",
    "rdd-lab": "lg:col-span-4",
    sql: "lg:col-span-4",
    "spark-ui": "lg:col-span-12",
  };
  const order: PortalId[] = ["network", "pregel", "graph-ops", "lake", "rdd-lab", "sql", "spark-ui"];

  return (
    <div className="mx-auto max-w-[1400px] px-5 sm:px-8">
      {/* ---- hero: the claim on the left, the city on the right (stacked and centred on small screens) ---------- */}
      <section className="grid items-center gap-x-14 gap-y-12 pt-12 sm:pt-16 lg:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)]">
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
              <CityLights points={city.points} flows={city.flows} className="block h-[clamp(28rem,68vh,40rem)] w-full" />
              <div className="pointer-events-none absolute inset-x-0 top-0 flex justify-between px-6 pt-5 text-[0.68rem] font-semibold tracking-[0.16em] text-fg-3 uppercase">
                <span className="flex items-center gap-2">
                  <span className="size-2 animate-pulse rounded-full bg-marker" /> {int(city.points.length)} stations
                </span>
                <span>{int(city.flows.length)} busiest routes · Jun–Sep 2026</span>
              </div>
              <ul className="pointer-events-none absolute inset-x-0 bottom-0 flex flex-wrap justify-center gap-x-5 gap-y-1 px-6 pb-4 text-xs text-fg-3">
                <li className="flex items-center gap-1.5">
                  <span className="size-2 rounded-full bg-glow-b" /> station, sized by PageRank
                </li>
                <li className="flex items-center gap-1.5">
                  <span className="size-2.5 rounded-full bg-marker ring-2 ring-marker/30" /> top-40 hub
                </li>
                <li className="flex items-center gap-1.5">
                  <span className="h-0.5 w-4 rounded-full bg-marker" /> a comet on a busy route
                </li>
              </ul>
            </div>
            <figcaption className="mt-4 text-center font-hand text-xl text-fg-2">
              every comet rides one of the summer&apos;s busiest real routes
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

        <div className="grid gap-6 lg:grid-cols-12">
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

        <div className="mt-6 grid gap-6 sm:grid-cols-2 lg:grid-cols-4">
          {UPCOMING.map((u, i) => (
            <Reveal key={u.title} delay={0.05 * i}>
              <div className="h-full rounded-[var(--radius-card)] border border-dashed border-white/15 p-7 text-center text-fg-3">
                <p className="font-hand text-lg text-fg-2">coming in {u.phase}</p>
                <p className="font-display text-xl font-bold text-fg-2">{u.title}</p>
                <p className="mt-1 text-sm">{u.note}</p>
              </div>
            </Reveal>
          ))}
        </div>
      </section>
    </div>
  );
}
