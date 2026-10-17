import { InkArrow } from "@/components/inkglass/ink";
import { Notate } from "@/components/inkglass/ink-client";
import { Glass, KpiPuck } from "@/components/inkglass/surfaces";
import { Act, CodePanel, Ledger, VivaNote, WithNote } from "@/components/portal/blocks";
import { getDqReport, getJobReports, PIPELINE_JOBS } from "@/lib/data";
import { compact, fixed, int, pct } from "@/lib/format";
import { jobDurationMs, jobMetric } from "@/lib/jobs";

import { PipelineChart, QuarantineChart } from "./lake-charts";

/** What each quarantine code means, in words (the codes come from SilverTripsTransforms.rejectReason). */
const MEANING: Record<string, string> = {
  MISSING_END_COORD: "The bike was never docked, so the trip has no end position.",
  DURATION_TOO_LONG: "Longer than 4 hours: almost always a bike that was not docked properly.",
  MISSING_START_COORD: "An e-bike picked up without a GPS fix.",
  IMPOSSIBLE_SPEED: "Faster than 45 km/h in a straight line.",
  DURATION_TOO_SHORT: "Shorter than a minute: a false start.",
};

const OUTPUT: Record<(typeof PIPELINE_JOBS)[number], string> = {
  "bronze-trips": "raw CSV → bronze/trips (typed Parquet, by month)",
  "silver-trips": "bronze → silver/trips + quarantine + dq-report.json",
  "silver-stations": "trips + GBFS → silver/stations",
  "silver-weather": "Open-Meteo JSON → silver/weather_hourly",
  "silver-calendar": "dates + US holidays → silver/calendar",
  "gold-station-hour": "silver → gold/station_hour (zero-filled panel)",
  "gold-routes": "silver → gold/route_edges",
  "rdd-fundamentals": "raw text → rdd-fundamentals.json",
  insights: "14 SQL files → insights/*.json",
  "graph-metrics": "routes → graph/station_metrics + network-*.json",
};

export async function LakePortal() {
  const [dq, jobs] = await Promise.all([getDqReport(), getJobReports()]);
  const d = dq.data;
  const panelRows = jobMetric(jobs, "gold-station-hour", "rows");
  const routeEdges = jobMetric(jobs, "gold-routes", "edges");
  const graphStations = jobMetric(jobs, "graph-metrics", "stations");
  const matchPct = jobMetric(jobs, "silver-stations", "endpointMatchPct");
  const pipelineMs = PIPELINE_JOBS.reduce((sum, j) => sum + jobDurationMs(jobs, j), 0);

  const stages = [
    { name: "Raw", tag: "CSV", rows: d.inputRows, note: "4 monthly downloads, every column text" },
    { name: "Bronze", tag: "Parquet", rows: jobMetric(jobs, "bronze-trips", "rows"), note: "typed, 0 malformed rows, reconciled" },
    { name: "Silver", tag: "Parquet", rows: d.validRows, note: `${int(d.quarantinedRows)} quarantined with a reason` },
    { name: "Gold", tag: "Parquet", rows: panelRows, note: `station × hour panel · ${compact(routeEdges)} route edges` },
    { name: "Graph", tag: "Parquet", rows: graphStations, note: "one row per station, every GraphX metric" },
  ];

  return (
    <>
      <Act
        n={1}
        title="Four layers, each one cleaner"
        lede="A medallion lake: bronze keeps every raw row, silver keeps only valid trips (and explains every one it drops), gold reshapes them for analysis, and the graph layer adds GraphX metrics."
      >
        <div className="grid grid-cols-1 gap-4 md:grid-cols-5">
          {stages.map((s, i) => (
            <div key={s.name} className="relative">
              <Glass className="h-full p-5">
                <p className="font-hand text-lg text-marker">{s.tag}</p>
                <p className="font-display text-2xl font-extrabold text-fg">{s.name}</p>
                <p className="mt-3 text-2xl font-semibold text-fg">{compact(s.rows)}</p>
                <p className="text-xs text-fg-3">rows · {int(s.rows)}</p>
                <p className="mt-3 text-sm leading-snug text-fg-2">{s.note}</p>
              </Glass>
              {i < stages.length - 1 && (
                <InkArrow
                  width={44}
                  height={30}
                  from={[2, 22]}
                  to={[40, 12]}
                  bend={-0.4}
                  className="absolute top-1/2 -right-7 z-10 hidden -translate-y-1/2 md:block"
                />
              )}
            </div>
          ))}
        </div>
      </Act>

      <Act
        n={2}
        title={`Why ${int(d.quarantinedRows)} trips were set aside`}
        lede={
          <>
            Silver checks every trip against a fixed sequence of rules (the code below) and tags a failing trip with
            the first rule it breaks, so nothing disappears silently. Only{" "}
            <Notate type="circle" color="var(--color-coral)" padding={6}>
              {pct(d.quarantinedPct, 3)}
            </Notate>{" "}
            of the input failed.
          </>
        }
      >
        <WithNote
          note={
            <VivaNote>
              Bronze stores every raw row as typed Parquet; silver applies the quality rules. Each rejected trip keeps
              the first rule it broke, so I can say exactly why {int(d.quarantinedRows)} trips ({pct(d.quarantinedPct, 3)})
              are missing: mostly bikes that were never docked.
            </VivaNote>
          }
        >
          <QuarantineChart reasons={d.byReason.map((r) => ({ ...r, meaning: MEANING[r.reason] ?? r.reason }))} />
        </WithNote>
        <CodePanel id="lake-reject-reason" caption="the quarantine rules, as one Column expression" />
      </Act>

      <Act n={3} title="What a clean trip looks like" lede="The valid trips, summarised by the same silver job.">
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Median ride" value={`${fixed(d.durationSeconds.p50 / 60, 1)} min`} note={`95 % under ${fixed(d.durationSeconds.p95 / 60, 0)} min`} />
          <KpiPuck label="Median distance" value={`${fixed(d.distanceMeters.p50 / 1000, 2)} km`} note="straight line" />
          <KpiPuck label="E-bikes" value={pct(d.validTripShares.ebikePct, 1)} note="of valid trips" />
          <KpiPuck label="Members" value={pct(d.validTripShares.memberPct, 1)} note="annual subscribers" />
          <KpiPuck label="Round trips" value={pct(d.validTripShares.roundTripPct, 1)} note="end where they start" />
          <KpiPuck label="Stations matched" value={pct(matchPct, 1)} note="trip endpoints found in GBFS" />
        </div>
      </Act>

      <Act
        n={4}
        title="The whole pipeline, timed"
        lede={`Ten Spark jobs take the lake from raw CSV to graph metrics in ${fixed(pipelineMs / 60_000, 1)} minutes of compute on the laptop. Every job writes a report with its timings and row counts.`}
      >
        <PipelineChart jobs={PIPELINE_JOBS.map((j) => ({ job: j, ms: jobDurationMs(jobs, j), output: OUTPUT[j] }))} />
        <CodePanel id="lake-bronze-read" caption="bronze: an explicit schema, and bad rows kept instead of dropped" />
      </Act>

      <Ledger
        rows={[
          { label: "Raw rows read", value: int(d.inputRows), path: "dq-report.json · data.inputRows" },
          { label: "Valid trips", value: int(d.validRows), path: "dq-report.json · data.validRows" },
          { label: "Quarantined", value: `${int(d.quarantinedRows)} (${pct(d.quarantinedPct, 3)})`, path: "dq-report.json · data.quarantinedRows" },
          { label: "Gold panel rows", value: int(panelRows), path: "jobs/gold-station-hour.json · metrics.rows" },
          { label: "Route edges", value: int(routeEdges), path: "jobs/gold-routes.json · metrics.edges" },
          { label: "Pipeline compute", value: `${fixed(pipelineMs / 1000, 0)} s`, path: "jobs/*.json · data.durationMs" },
        ]}
      />
    </>
  );
}
