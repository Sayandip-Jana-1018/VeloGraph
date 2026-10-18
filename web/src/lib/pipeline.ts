// The batch pipeline in run order, by roadmap phase (ARCHITECTURE §6). Every job writes serving/jobs/<job>.json.
// The PySpark lab notebooks sit between feature-build and train-champion but are not Spark jobs with a report.
export const STAGES = [
  { phase: "P1", label: "Lake", jobs: ["bronze-trips", "silver-trips", "silver-stations", "silver-weather", "silver-calendar"] },
  { phase: "P2", label: "Gold, RDD and SQL", jobs: ["gold-station-hour", "gold-routes", "rdd-fundamentals", "insights"] },
  { phase: "P3", label: "GraphX", jobs: ["graph-metrics"] },
  { phase: "P4", label: "MLlib", jobs: ["station-archetypes", "feature-build", "train-champion"] },
  { phase: "P5", label: "Live features", jobs: ["live-features"] },
  { phase: "P6", label: "Web exports", jobs: ["hero-trips"] },
] as const;

/** Streaming jobs run until stopped: their report covers one run, not a pipeline step. */
export const STREAMING_JOBS = ["live-station-scoring", "trip-replay-analytics"] as const;

export const BATCH_JOBS: string[] = STAGES.flatMap((s) => [...s.jobs]);

export function stageOf(job: string) {
  return STAGES.find((s) => (s.jobs as readonly string[]).includes(job));
}
