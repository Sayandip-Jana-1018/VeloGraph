import "server-only";

import { readdir, readFile } from "node:fs/promises";
import path from "node:path";

import { cacheLife } from "next/cache";
import type { z } from "zod";

import * as S from "./schemas";

/**
 * Server-side loaders for the serving exports in public/data (copied there by `pnpm sync`). Each loader is a
 * cached function, so pages prerender at build time and the JSON is read and validated once.
 */
async function load<T extends z.ZodType>(file: string, schema: T): Promise<z.infer<T>> {
  const full = path.join(process.cwd(), "public", "data", file);
  let text: string;
  try {
    text = await readFile(full, "utf8");
  } catch {
    throw new Error(`Missing ${file}: run the batch job that writes it, then \`pnpm sync\` in web/.`);
  }
  const parsed = schema.safeParse(JSON.parse(text));
  if (!parsed.success) throw new Error(`${file} does not match its contract: ${parsed.error.message}`);
  return parsed.data;
}

export async function getDqReport() {
  "use cache";
  cacheLife("max");
  return load("dq-report.json", S.DqReport);
}

export const PIPELINE_JOBS = [
  "bronze-trips",
  "silver-trips",
  "silver-stations",
  "silver-weather",
  "silver-calendar",
  "gold-station-hour",
  "gold-routes",
  "rdd-fundamentals",
  "insights",
  "graph-metrics",
] as const;

export async function getJobReports() {
  "use cache";
  cacheLife("max");
  return Promise.all(PIPELINE_JOBS.map((job) => load(`jobs/${job}.json`, S.JobReport)));
}

export async function getRddReport() {
  "use cache";
  cacheLife("max");
  return load("rdd-fundamentals.json", S.RddReport);
}

export async function getInsightIndex() {
  "use cache";
  cacheLife("max");
  return load("insights/index.json", S.InsightIndex);
}

export async function getInsights() {
  "use cache";
  cacheLife("max");
  const index = await getInsightIndex();
  return Promise.all(index.data.map((entry) => load(`insights/${entry.id}.json`, S.Insight)));
}

export async function getNetworkSummary() {
  "use cache";
  cacheLife("max");
  return load("network-summary.json", S.NetworkSummary);
}

export async function getNetworkCommunities() {
  "use cache";
  cacheLife("max");
  return load("network-communities.json", S.NetworkCommunities);
}

export async function getNetworkPregel() {
  "use cache";
  cacheLife("max");
  return load("network-pregel.json", S.NetworkPregel);
}

/** Spark's own dashboards, from the history server's REST API (velograph.ops.spark_ui_export). */
export async function getSparkUi() {
  "use cache";
  cacheLife("max");
  return load("spark-ui.json", S.SparkUi);
}

/** The Spark UI screenshots copied by `pnpm sync`, in file-name order. */
export async function getSparkUiShots() {
  "use cache";
  cacheLife("max");
  const files = await readdir(path.join(process.cwd(), "public", "spark-ui"));
  return files.filter((f) => f.endsWith(".png")).sort();
}

/** Every station with its graph metrics, in PageRank order. */
export async function getNetworkNodes() {
  "use cache";
  cacheLife("max");
  return load("network-nodes.json", S.NetworkNodes);
}

/**
 * For the home page's animated city of lights: every station as [lon, lat, pagerank] and the busiest flows as
 * [from index, to index, trips], compact enough to ship with the page.
 */
export async function getCityLights() {
  "use cache";
  cacheLife("max");
  const [nodes, flows] = await Promise.all([getNetworkNodes(), load("network-flows.json", S.NetworkFlows)]);
  const index = new Map(nodes.data.map((n, i) => [n.id, i]));
  const round = (x: number, d: number) => Math.round(x * 10 ** d) / 10 ** d;
  return {
    points: nodes.data.map((n) => [round(n.lon, 5), round(n.lat, 5), round(n.pagerank, 7)] as [number, number, number]),
    flows: flows.data
      .slice(0, 500)
      .flatMap((f) => {
        const [a, b] = [index.get(f.from), index.get(f.to)];
        return a === undefined || b === undefined ? [] : [[a, b, f.trips] as [number, number, number]];
      }),
  };
}
