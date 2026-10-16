import type { JobReport } from "./schemas";

/** A numeric metric from a job report; a missing one is a contract break, so it fails the build. */
export function jobMetric(jobs: JobReport[], job: string, key: string): number {
  const value = jobs.find((j) => j.data.job === job)?.data.metrics[key];
  if (typeof value !== "number") throw new Error(`jobs/${job}.json has no numeric metric "${key}"`);
  return value;
}

export function jobDurationMs(jobs: JobReport[], job: string): number {
  const report = jobs.find((j) => j.data.job === job);
  if (!report) throw new Error(`missing jobs/${job}.json`);
  return report.data.durationMs;
}
