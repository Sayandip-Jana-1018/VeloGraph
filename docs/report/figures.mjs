// The report's diagrams as SVG, drawn for print (light background), with their numbers read from the exports.
// `node figures.mjs` writes figures/*.svg; render.py turns them into PNGs for the Word document.
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { facts } from "./facts.mjs";

const C = {
  ink: "#1f2340",
  sub: "#4a5070",
  line: "#7b819f",
  batch: "#0f766e",
  batchBg: "#e6f4f1",
  stream: "#b45309",
  streamBg: "#fdf0e1",
  graph: "#6d28d9",
  graphBg: "#efe9fc",
  web: "#1d4ed8",
  webBg: "#e8eefc",
  grey: "#f3f4f8",
};
const FONT = "Segoe UI, Calibri, Arial, sans-serif";

const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

function svg(width, height, body) {
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" font-family="${FONT}">
<defs>
  <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="${C.line}"/></marker>
</defs>
<rect width="${width}" height="${height}" fill="#ffffff"/>
${body}
</svg>`;
}

/** A rounded box with a bold title and lines of text under it. */
function box(x, y, w, h, title, lines = [], { fill = C.grey, stroke = C.line, color = C.ink, size = 17 } = {}) {
  const text = lines
    .map((l, i) => `<text x="${x + w / 2}" y="${y + 48 + i * 22}" text-anchor="middle" font-size="${size - 2}" fill="${C.sub}">${esc(l)}</text>`)
    .join("");
  return `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="14" fill="${fill}" stroke="${stroke}" stroke-width="2"/>
<text x="${x + w / 2}" y="${y + 26}" text-anchor="middle" font-size="${size}" font-weight="700" fill="${color}">${esc(title)}</text>${text}`;
}

function arrow(x1, y1, x2, y2, label = "", { dashed = false, dy = -8 } = {}) {
  const mid = label
    ? `<text x="${(x1 + x2) / 2}" y="${(y1 + y2) / 2 + dy}" text-anchor="middle" font-size="13" fill="${C.sub}">${esc(label)}</text>`
    : "";
  return `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="${C.line}" stroke-width="2.2" ${dashed ? 'stroke-dasharray="7 6"' : ""} marker-end="url(#arrow)"/>${mid}`;
}

function lane(x, y, w, h, label, color) {
  return `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="18" fill="none" stroke="${color}" stroke-width="1.5" stroke-dasharray="4 5"/>
<text x="${x + 16}" y="${y + 22}" font-size="14" font-weight="700" fill="${color}" letter-spacing="1.5">${esc(label.toUpperCase())}</text>`;
}

function architecture(f) {
  const W = 1600;
  const H = 820;
  return svg(
    W,
    H,
    [
      lane(300, 30, 1000, 360, "Batch: host, Spark local[20]", C.batch),
      lane(300, 420, 1000, 370, "Streaming: Docker cluster", C.stream),
      box(20, 60, 250, 96, "Citi Bike trips", [`CSV, Jun-Sep 2026`, `${f.data.inputRowsM} M rows`]),
      box(20, 180, 250, 96, "Open-Meteo", ["hourly weather", "+ US holidays"]),
      box(20, 520, 250, 96, "GBFS live feed", [`${f.data.liveStations} stations`, "station status, every 60 s"]),
      box(330, 80, 300, 150, "Parquet lake", ["bronze: every raw row", `silver: ${f.data.validRowsM} M clean trips`, "gold: station x hour, routes"], { fill: C.batchBg, stroke: C.batch }),
      box(330, 260, 300, 104, "Graph layer", ["lake/graph/station_metrics", "PageRank, districts, strength"], { fill: C.graphBg, stroke: C.graph }),
      box(690, 60, 280, 72, "Spark SQL", [`${f.sql.queries} named queries`], { fill: C.batchBg, stroke: C.batch }),
      box(690, 146, 280, 72, "RDD lab", ["shuffles, persistence, lineage"], { fill: C.batchBg, stroke: C.batch }),
      box(690, 232, 280, 72, "GraphX", [`Pregel PageRank, ${f.graph.communities} districts`], { fill: C.graphBg, stroke: C.graph }),
      box(690, 318, 280, 62, "MLlib", ["archetypes, features, champions"], { fill: C.graphBg, stroke: C.graph }),
      box(1030, 140, 250, 150, "Serving exports", ["data/serving/*.json", "one envelope: meta + data", "every number the site shows"], { fill: C.grey }),
      box(330, 520, 230, 120, "Producer", ["Python, CDC deltas", "acks=all, idempotent", "dead-letter topic"], { fill: C.streamBg, stroke: C.stream }),
      box(610, 470, 300, 150, "Kafka, KRaft", ["3 brokers, replication 3", "min in-sync replicas 2", "station status: 6 partitions"], { fill: C.streamBg, stroke: C.stream }),
      box(610, 650, 300, 120, "Structured Streaming", ["LiveStationScoringJob", "every 30 s: score, alert, move", "master + 2 workers"], { fill: C.streamBg, stroke: C.stream }),
      box(1030, 560, 250, 130, "Gateway", ["FastAPI", "REST + server-sent events", "cluster topology"], { fill: C.streamBg, stroke: C.stream }),
      box(1340, 250, 240, 330, "Website", ["Next.js 16", "11 evidence portals", "live map + alerts", "Ctrl K station search", "works offline from", "the exports"], { fill: C.webBg, stroke: C.web, color: C.web }),
      arrow(270, 108, 330, 130),
      arrow(270, 228, 330, 190),
      arrow(630, 150, 690, 96),
      arrow(630, 160, 690, 182),
      arrow(630, 300, 690, 268),
      arrow(630, 320, 690, 349),
      arrow(480, 230, 480, 260),
      arrow(970, 96, 1030, 180),
      arrow(970, 268, 1030, 230),
      arrow(970, 349, 1030, 265),
      arrow(1280, 215, 1340, 300),
      arrow(270, 568, 330, 578),
      arrow(560, 578, 610, 552),
      arrow(760, 620, 760, 650, "", {}),
      arrow(910, 735, 1030, 650),
      `<text x="935" y="752" font-size="13" fill="${C.sub}">predictions, alerts,</text><text x="935" y="770" font-size="13" fill="${C.sub}">snapshot.json</text>`,
      arrow(1280, 625, 1340, 540),
      // Models and live features flow from the batch side into the stream: routed around the Kafka box.
      `<path d="M 960 380 L 960 668 L 914 668" fill="none" stroke="${C.line}" stroke-width="2.2" stroke-dasharray="7 6" marker-end="url(#arrow)"/>`,
      `<text x="972" y="520" font-size="13" fill="${C.sub}">champion models</text><text x="972" y="538" font-size="13" fill="${C.sub}">+ live features</text>`,
    ].join("\n"),
  );
}

function deployment(f) {
  const W = 1600;
  const H = 760;
  return svg(
    W,
    H,
    [
      `<rect x="20" y="20" width="1560" height="720" rx="24" fill="#fafbfd" stroke="${C.line}" stroke-width="2"/>`,
      `<text x="44" y="56" font-size="22" font-weight="800" fill="${C.ink}">One laptop: Windows 11, 16 GB RAM</text>`,
      `<rect x="44" y="80" width="560" height="630" rx="18" fill="#ffffff" stroke="${C.batch}" stroke-width="2"/>`,
      `<text x="64" y="112" font-size="18" font-weight="700" fill="${C.batch}">Host processes</text>`,
      box(64, 130, 520, 110, "Spark batch jobs", ["spark-submit, local[20], JDK 17", `${f.batch.jobs} jobs, ${f.batch.minutes} min end to end`], { fill: C.batchBg, stroke: C.batch }),
      box(64, 260, 250, 110, "GBFS recorder", ["every 60 s", "gzipped JSON lines"]),
      box(334, 260, 250, 110, "GBFS producer", ["live feed to Kafka", "or a recorded replay"], { fill: C.streamBg, stroke: C.stream }),
      box(64, 390, 250, 110, "Website :3000", ["Next.js production", "build"], { fill: C.webBg, stroke: C.web, color: C.web }),
      box(334, 390, 250, 110, "Gateway :8000", ["FastAPI, REST + SSE", "Kafka consumer group"], { fill: C.streamBg, stroke: C.stream }),
      arrow(314, 445, 334, 445),
      box(64, 520, 520, 160, "data/  (shared with the containers)", ["raw: CSVs, GBFS recordings, weather", "lake: bronze, silver, gold, graph (Parquet)", "models, checkpoints, stream, serving JSON"]),
      `<rect x="640" y="80" width="920" height="630" rx="18" fill="#ffffff" stroke="${C.stream}" stroke-width="2"/>`,
      `<text x="660" y="112" font-size="18" font-weight="700" fill="${C.stream}">Docker VM, 6 GB (WSL 2)</text>`,
      box(660, 130, 280, 120, "kafka-1", ["broker + controller", "host port 9092"], { fill: C.streamBg, stroke: C.stream }),
      box(960, 130, 280, 120, "kafka-2", ["broker + controller", "host port 9094"], { fill: C.streamBg, stroke: C.stream }),
      box(1260, 130, 280, 120, "kafka-3", ["broker + controller", "host port 9096"], { fill: C.streamBg, stroke: C.stream }),
      box(660, 270, 880, 70, "KRaft quorum: no ZooKeeper. Replication factor 3, min in-sync replicas 2.", [], { size: 16 }),
      box(660, 370, 420, 140, "spark-master", ["standalone master :7077", "UI :8080, app UIs :4040-4042", "drivers run here (client mode)"], { fill: C.batchBg, stroke: C.batch }),
      box(1120, 370, 200, 140, "worker 1", [`2 cores`, "900 MB executors", "UI :8081"], { fill: C.batchBg, stroke: C.batch }),
      box(1340, 370, 200, 140, "worker 2", [`2 cores`, "900 MB executors", "UI :8082"], { fill: C.batchBg, stroke: C.batch }),
      box(660, 540, 420, 140, "kafka-ui :8089", ["brokers, topics, partitions", "consumer groups"]),
      box(1120, 540, 420, 140, "spark-history :18080", ["event logs of every run", "(started on demand)"]),
      arrow(584, 300, 660, 190),
      `<text x="590" y="262" font-size="13" fill="${C.sub}">produce</text>`,
      arrow(660, 232, 584, 430),
      `<text x="606" y="402" font-size="13" fill="${C.sub}">consume</text>`,
    ].join("\n"),
  );
}

function lake(f) {
  const W = 1600;
  const H = 430;
  const gold = f.raw.jobs["gold-station-hour"].metrics;
  const stages = [
    ["Raw CSV", `${f.data.inputRows} rows`, "4 monthly downloads, text"],
    ["Bronze", `${f.data.inputRows} rows`, "typed Parquet, 0 malformed"],
    ["Silver", `${f.data.validRows} trips`, `${f.data.quarantined} quarantined`],
    ["Gold", "station x hour panel", `${int(gold.departures)} departures, routes`],
    ["Graph", `${f.graph.vertices} stations`, "every GraphX metric"],
  ];
  const w = 270;
  const parts = stages.map(([t, a, b], i) =>
    box(30 + i * 312, 70, w, 140, t, [a, b], {
      fill: i === 4 ? C.graphBg : C.batchBg,
      stroke: i === 4 ? C.graph : C.batch,
      size: 20,
    }),
  );
  const arrows = stages.slice(1).map((_, i) => arrow(30 + i * 312 + w, 140, 30 + (i + 1) * 312, 140));
  const reasons = f.data.reasons
    .map((r, i) => `<text x="${60 + i * 380}" y="330" font-size="16" fill="${C.ink}"><tspan font-weight="700">${int(r.rows)}</tspan> ${esc(r.reason.toLowerCase().replace(/_/g, " "))}</text>`)
    .join("");
  return svg(
    W,
    H,
    [
      `<text x="30" y="44" font-size="20" font-weight="800" fill="${C.ink}">The medallion lake, with the rows at every layer</text>`,
      ...parts,
      ...arrows,
      `<rect x="30" y="250" width="1540" height="130" rx="14" fill="#fff7ed" stroke="${C.stream}" stroke-width="1.5"/>`,
      `<text x="60" y="290" font-size="17" font-weight="700" fill="${C.stream}">Quarantine: ${f.data.quarantined} trips (${f.data.quarantinedPct}), each kept with the rule it broke</text>`,
      reasons,
      `<text x="60" y="362" font-size="14" fill="${C.sub}">Silver keeps only valid trips and records why every other row was dropped (SilverTripsTransforms.rejectReason).</text>`,
    ].join("\n"),
  );
}

function microBatch(f) {
  const W = 1600;
  const H = 560;
  const steps = [
    ["Kafka source", ["station status, 6 partitions", "startingOffsets = latest"]],
    ["Parse + clean", ["from_json, explicit schema", "watermark 10 min, dedupe"]],
    ["State", ["newest status per station", "kept on the driver"]],
    ["Features", ["live station features,", "weather, calendar"]],
    ["Three models", ["departures: random forest", "arrivals: GBT, regime: SVM"]],
    ["Rules", ["bikes in 60 min, 6 states", "alerts, rebalancing moves"]],
  ];
  const w = 230;
  const parts = steps.map(([t, l], i) => box(30 + i * 258, 90, w, 130, t, l, { fill: C.streamBg, stroke: C.stream, size: 18 }));
  const arrows = steps.slice(1).map((_, i) => arrow(30 + i * 258 + w, 155, 30 + (i + 1) * 258, 155));
  return svg(
    W,
    H,
    [
      `<text x="30" y="50" font-size="20" font-weight="800" fill="${C.ink}">One micro-batch of LiveStationScoringJob (trigger: 30 s, foreachBatch)</text>`,
      ...parts,
      ...arrows,
      box(260, 300, 300, 110, "Kafka sinks", ["predictions (every station)", "alerts (state changes)"], { fill: C.grey }),
      box(650, 300, 300, 110, "Parquet", ["stream/predictions", "partitioned by date and batch"], { fill: C.grey }),
      box(1040, 300, 300, 110, "snapshot.json", ["replaced atomically", "read by the gateway"], { fill: C.grey }),
      arrow(1430, 220, 410, 300, ""),
      arrow(1430, 220, 800, 300, ""),
      arrow(1430, 220, 1190, 300, ""),
      `<text x="30" y="470" font-size="16" fill="${C.ink}">Measured over a ${f.stream.minutes}-minute run: ${f.stream.batches} micro-batches, broker to snapshot p50 ${f.stream.p50} s and p95 ${f.stream.p95} s; the producer delivered ${f.stream.delivered} messages with ${f.stream.failed} failures.</text>`,
      `<text x="30" y="500" font-size="16" fill="${C.ink}">States: EMPTY / FULL now, EMPTY_RISK / FULL_RISK within 60 minutes, OK, OFFLINE, against a margin of max(2, 10 % of the docks).</text>`,
    ].join("\n"),
  );
}

function mlWindows(f) {
  const W = 1600;
  const H = 400;
  const months = ["Jun", "Jul", "Aug", "Sep"];
  const x0 = 200;
  const mw = 330;
  const head = months
    .map((m, i) => `<rect x="${x0 + i * mw}" y="70" width="${mw - 6}" height="40" rx="8" fill="${C.grey}"/><text x="${x0 + i * mw + mw / 2}" y="97" text-anchor="middle" font-size="17" font-weight="700" fill="${C.ink}">${m} 2026</text>`)
    .join("");
  // Profile windows are trailing eight weeks; the rows they describe start the day after (leakage check).
  const day = (d) => {
    const [, m, dd] = d.split("-").map(Number);
    return x0 + (m - 6) * mw + ((dd - 1) / 30.5) * mw;
  };
  const rows = f.ml.windows.map((w, i) => {
    const y = 140 + i * 110;
    return [
      `<text x="30" y="${y + 30}" font-size="17" font-weight="700" fill="${C.ink}">${w.split === "fit" ? "Training" : "Test"}</text>`,
      `<rect x="${day(w.profile.from)}" y="${y}" width="${day(w.profile.to) - day(w.profile.from)}" height="44" rx="8" fill="${C.graphBg}" stroke="${C.graph}" stroke-width="1.5"/>`,
      `<text x="${(day(w.profile.from) + day(w.profile.to)) / 2}" y="${y + 28}" text-anchor="middle" font-size="15" fill="${C.graph}">profile: ${w.profile.from} to ${w.profile.to}</text>`,
      `<rect x="${day(w.rows.from)}" y="${y}" width="${day(w.rows.to) - day(w.rows.from) + mw / 30}" height="44" rx="8" fill="${C.batchBg}" stroke="${C.batch}" stroke-width="1.5"/>`,
      `<text x="${(day(w.rows.from) + day(w.rows.to)) / 2}" y="${y + 28}" text-anchor="middle" font-size="15" fill="${C.batch}">rows: ${w.rows.from} to ${w.rows.to}</text>`,
    ].join("");
  });
  return svg(
    W,
    H,
    [
      `<text x="30" y="44" font-size="20" font-weight="800" fill="${C.ink}">Leakage-safe windows: features only ever look back</text>`,
      head,
      ...rows,
      `<text x="30" y="370" font-size="15" fill="${C.sub}">Each split's profile features (trailing eight weeks, and GraphX metrics recomputed on that window) end the day before its first row; September is never used for tuning.</text>`,
    ].join("\n"),
  );
}

function int(n) {
  return new Intl.NumberFormat("en-US").format(n);
}

const f = facts();
const out = join(import.meta.dirname, "figures");
mkdirSync(out, { recursive: true });
const figures = { architecture, deployment, lake, "micro-batch": microBatch, "ml-windows": mlWindows };
for (const [name, draw] of Object.entries(figures)) {
  writeFileSync(join(out, `${name}.svg`), draw(f));
  console.log("figures/" + name + ".svg");
}
