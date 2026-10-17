import type { StreamProgress } from "./schemas";

/** The q-quantile by nearest rank (no interpolation): always a latency that was really measured. */
export function quantile(xs: number[], q: number) {
  const s = [...xs].sort((a, b) => a - b);
  return s.length ? s[Math.min(s.length - 1, Math.floor(q * s.length))] : 0;
}

/**
 * Broker append → snapshot latency of the live scoring query, over the micro-batches that carried rows (an empty
 * batch has nothing to be late). The home page and the streaming portal quote the same numbers from here.
 */
export function scoringLatency(progress: StreamProgress["data"]) {
  const scoring = progress.batches.filter((b) => b.query === "live-station-scoring");
  const latencies = scoring.flatMap((b) => (b.scoring && b.inputRows > 0 ? [b.scoring.latencyS] : []));
  return { scoring, batches: latencies.length, p50: quantile(latencies, 0.5), p95: quantile(latencies, 0.95) };
}
