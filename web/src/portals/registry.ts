import type { Accent } from "@/components/inkglass/paper";

/** Evidence portals (ADR 011): one per module, each with its own URL `/portal/<id>`. */
export const PORTAL_IDS = [
  "lake",
  "rdd-lab",
  "sql",
  "spark-ui",
  "pregel",
  "network",
  "graph-ops",
  "models",
  "streaming",
  "cluster",
  "syllabus",
] as const;
export type PortalId = (typeof PORTAL_IDS)[number];

export type PortalMeta = {
  id: PortalId;
  title: string;
  kicker: string;
  hook: string;
  units: string[];
  outcomes: string[];
  accent: Accent;
  sources: string[];
};

export const PORTALS: PortalMeta[] = [
  {
    id: "lake",
    title: "The lake",
    kicker: "Bronze → silver → gold",
    hook: "20.6 million raw rows in, a reconciled, quality-checked lake out, with every rejected trip explained.",
    units: ["Unit I", "Unit IV"],
    outcomes: ["CO5"],
    accent: "teal",
    sources: ["dq-report.json", "jobs/*.json"],
  },
  {
    id: "rdd-lab",
    title: "RDD lab",
    kicker: "Unit III, measured",
    hook: "groupByKey vs reduceByKey, caching levels, the accumulator trap and lineage, on one real month.",
    units: ["Unit I", "Unit III"],
    outcomes: ["CO5"],
    accent: "coral",
    sources: ["rdd-fundamentals.json"],
  },
  {
    id: "sql",
    title: "Spark SQL",
    kicker: "14 questions, 14 queries",
    hook: "Rain, rush hours, e-bikes and wildcards: each insight with its chart and the exact SQL that made it.",
    units: ["Unit IV"],
    outcomes: ["CO5"],
    accent: "amber",
    sources: ["insights/*.json"],
  },
  {
    id: "spark-ui",
    title: "Spark UI",
    kicker: "Proof from the history server",
    hook: "Spark's own dashboards redrawn from its REST API: job timelines, shuffle stages, SQL plans and Pregel supersteps.",
    units: ["Unit I", "Unit III", "Unit IV"],
    outcomes: ["CO5", "CO6"],
    accent: "pink",
    sources: ["spark-ui.json", "docs/assets/spark-ui/*.png"],
  },
  {
    id: "pregel",
    title: "Pregel, step by step",
    kicker: "Watch PageRank converge",
    hook: "A real Williamsburg neighbourhood, every superstep replayed: messages, merges, and votes to halt.",
    units: ["Unit III"],
    outcomes: ["CO6"],
    accent: "violet",
    sources: ["network-pregel.json", "network-summary.json"],
  },
  {
    id: "network",
    title: "The network",
    kicker: "GraphX on 2,490 stations",
    hook: "Hubs, the morning-evening shift, 15 communities, travel times and where riders go.",
    units: ["Unit III"],
    outcomes: ["CO6"],
    accent: "mint",
    sources: ["network-nodes.json", "network-flows.json", "network-paths.json", "network-communities.json"],
  },
  {
    id: "graph-ops",
    title: "GraphX operators",
    kicker: "Every operator, checked",
    hook: "Thirteen operators on the real graph, and why EdgePartition2D needs half the vertex copies.",
    units: ["Unit III"],
    outcomes: ["CO6"],
    accent: "marker",
    sources: ["network-summary.json"],
  },
  {
    id: "models",
    title: "Models",
    kicker: "MLlib, measured honestly",
    hook: "Station archetypes by KMeans, next-hour demand against the seasonal-naive baseline, drain-or-fill classes, and what GraphX adds.",
    units: ["Unit VI"],
    outcomes: ["CO4", "CO6"],
    accent: "coral",
    sources: ["archetypes.json", "ml-features.json", "ml-*.json", "model-cards.json"],
  },
  {
    id: "streaming",
    title: "Streaming",
    kicker: "Kafka in, Spark every 30 s",
    hook: "The live feed through three Kafka brokers into Structured Streaming: micro-batches, watermarks, output modes, and a broker pulled mid-stream.",
    units: ["Unit V"],
    outcomes: ["CO4", "CO5"],
    accent: "teal",
    sources: ["live/snapshot.json", "live/stream-progress.json", "live/replay-analytics.json", "live/drills.json"],
  },
  {
    id: "cluster",
    title: "The cluster",
    kicker: "Spark and Kafka, live",
    hook: "Workers, executors, brokers, leaders and in-sync replicas as they are now, and the whole pipeline timed end to end.",
    units: ["Unit I", "Unit V"],
    outcomes: ["CO5"],
    accent: "amber",
    sources: ["cluster.json", "jobs/*.json"],
  },
  {
    id: "syllabus",
    title: "Syllabus",
    kicker: "Every unit, traced",
    hook: "Units I to VI and CO4 to CO6, each with what was built, one measured number, and the portals that prove it.",
    units: ["Units I–VI"],
    outcomes: ["CO4", "CO5", "CO6"],
    accent: "violet",
    sources: ["every export above", "the code itself"],
  },
];

export function portalById(id: string): PortalMeta | undefined {
  return PORTALS.find((p) => p.id === id);
}
