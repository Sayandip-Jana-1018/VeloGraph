// The VeloGraph project report: `node build.mjs` writes submission/VeloGraph-Report.docx; export.ps1 updates its
// fields (contents, lists of figures and tables) in Word and saves the PDF next to it.
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import {
  AlignmentType,
  BorderStyle,
  Document,
  ExternalHyperlink,
  Footer,
  Header,
  NumberFormat,
  Packer,
  PageBorderDisplay,
  PageBorderOffsetFrom,
  PageNumber,
  Paragraph,
  Tab,
  Table,
  TableCell,
  TableOfContents,
  TableRow,
  TabStopType,
  TextRun,
  WidthType,
} from "docx";

import { facts, fixed, int, pct } from "./facts.mjs";
import {
  bullets,
  callout,
  COLORS,
  figure,
  figurePair,
  front,
  gap,
  h1,
  h1Plain,
  mark,
  plainImage,
  refOf,
  h2,
  h3,
  link,
  noBorders,
  numbering,
  p,
  runs,
  styles,
  table,
  TEXT_DXA,
} from "./kit.mjs";

const ROOT = join(import.meta.dirname, "..", "..");
const SITE = "https://velograph-sj.vercel.app";
const REPO = "https://github.com/Sayandip-Jana-1018/VeloGraph";
/** A justified paragraph with one clickable address in it: `before`, the link, then `after`. */
const withLink = (before, url, after) =>
  new Paragraph({
    alignment: AlignmentType.JUSTIFIED,
    children: [...runs(before), link(url.replace("https://", ""), url), ...runs(after)],
  });
const EVIDENCE = join(ROOT, "docs", "assets", "evidence");
const SPARKUI = join(ROOT, "docs", "assets", "spark-ui");
const FIG = join(import.meta.dirname, "figures");
const shot = (name) => join(EVIDENCE, name);

const STUDENT = { name: "Sayandip Jana", reg: "12308059", roll: "09", section: "K3M2328" };
const TEACHER = { name: "Pardeep Kumar", title: "Assistant Professor" };
const COURSE = { code: "INT315", name: "Cluster Computing" };
const DATE = "18 October 2026";

const f = facts();
const code = JSON.parse(readFileSync(join(ROOT, "web", "src", "generated", "code-stats.json"), "utf8"));
const backtestPath = join(ROOT, "data", "serving", "live-backtest.json");
const backtest = existsSync(backtestPath) ? JSON.parse(readFileSync(backtestPath, "utf8")).data : null;
const call = (risk) => backtest?.calls.find((c) => c.risk === risk);

// ---- cover -----------------------------------------------------------------------------------------------------
const centered = (text, run = {}, spacing = { after: 80 }) =>
  new Paragraph({ alignment: AlignmentType.CENTER, spacing, children: [new TextRun({ text, ...run })] });

function detailsTable() {
  const half = Math.floor(TEXT_DXA / 2);
  const cell = (lines) =>
    new TableCell({
      width: { size: half, type: WidthType.DXA },
      borders: noBorders,
      margins: { top: 60, bottom: 60, left: 120, right: 120 },
      children: lines.map(
        ([t, bold, color], i) =>
          new Paragraph({
            alignment: AlignmentType.CENTER,
            spacing: { after: i === 0 ? 100 : 40 },
            children: [new TextRun({ text: t, bold, color: color ?? "222222", size: i === 0 ? 20 : 22 })],
          }),
      ),
    });
  return new Table({
    width: { size: TEXT_DXA, type: WidthType.DXA },
    columnWidths: [half, TEXT_DXA - half],
    rows: [
      new TableRow({
        children: [
          cell([
            ["SUBMITTED BY", true, COLORS.teal],
            [STUDENT.name, true],
            [`Registration No.: ${STUDENT.reg}`],
            [`Roll No.: ${STUDENT.roll}`],
            [`Section: ${STUDENT.section}`],
          ]),
          cell([
            ["SUBMITTED TO", true, COLORS.teal],
            [TEACHER.name, true],
            [TEACHER.title],
            ["Lovely Professional University"],
          ]),
        ],
      }),
    ],
  });
}

// The university's logo, from lpu.in (docs/assets/brand is git-ignored: it is LPU's artwork, not ours).
const LOGO = join(ROOT, "docs", "assets", "brand", "lpu-logo.png");
const cover = [
  existsSync(LOGO)
    ? plainImage(LOGO, 440)
    : centered("LOVELY PROFESSIONAL UNIVERSITY", { bold: true, size: 36, color: COLORS.ink }, { before: 120, after: 40 }),
  centered("PROJECT REPORT", { bold: true, size: 28, color: COLORS.ink }, { before: 120, after: 60 }),
  centered(`${COURSE.name} (${COURSE.code})`, { bold: true, size: 26, color: COLORS.teal }, { after: 260 }),
  centered("VeloGraph", { bold: true, size: 64, color: COLORS.ink }, { after: 40 }),
  centered("Real-Time Bike-Share Intelligence on Apache Spark", { bold: true, size: 30, color: COLORS.ink }, { after: 100 }),
  centered(
    "Demand forecasting with MLlib, network analytics with GraphX, and live risk scoring with Kafka and Spark Structured Streaming",
    { size: 21, color: "4A5070" },
    { after: 240 },
  ),
  plainImage(shot("01-home-hero.jpg"), 440),
  detailsTable(),
  centered(`Date of submission: ${DATE}`, { size: 22, color: "4A5070" }, { before: 240, after: 0 }),
];

// ---- front matter ----------------------------------------------------------------------------------------------
/** A link set large and centred under a small label, for the overview page. */
const bigLink = (label, url, size) => [
  new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { before: 160, after: 40 },
    children: [new TextRun({ text: label, bold: true, size: 20, color: COLORS.teal })],
  }),
  new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { after: 120 },
    children: [new ExternalHyperlink({ link: url, children: [new TextRun({ text: url.replace("https://", ""), style: "Hyperlink", bold: true, size })] })],
  }),
];

const overview = [
  front("Project Overview", true),
  ...bigLink("LIVE WEBSITE", SITE, 52),
  ...bigLink("SOURCE CODE ON GITHUB", REPO, 40),
  h3("Problem statement"),
  p(
    "Bike-share stations run empty or full within the hour, which strands riders, and operators move bikes by van, so they need to know which stations are about to fail before it happens. Given four months of historical trips and the live station feed, the project builds a system that (a) forecasts next-hour departures and arrivals at every station, (b) combines those forecasts with the live feed to flag stations that are about to run empty or full, (c) suggests rebalancing moves, and (d) explains the structure of the network, all on a real Spark cluster and a real Kafka cluster, with every result measured on real data.",
  ),
  h3("Solution"),
  ...bullets([
    `**Batch (Spark):** a medallion lake of ${f.data.validRowsM} million clean trips, ${f.sql.queries} Spark SQL insights, and an RDD lab that measures shuffles, caching and shared variables.`,
    `**Network (GraphX):** ${f.graph.vertices} stations as a weighted graph; PageRank written with Pregel, and ${f.graph.communities} districts found by label propagation.`,
    `**Forecasts (MLlib):** leakage-safe features; a random forest, gradient-boosted trees and a linear SVM, each judged against a seasonal naive baseline.`,
    `**Live (Kafka and Structured Streaming):** the GBFS feed through three Kafka brokers to Spark, which re-scores all ${f.data.liveStations} stations every 30 seconds, raises alerts and suggests moves.`,
    "**Website (Next.js):** an evidence portal for every module, reading only the results the jobs wrote.",
  ]),
  h3("Datasets and where they come from"),
  ...table(
    "The datasets and their sources",
    ["Dataset", "What it holds", "Source"],
    [
      [
        "Citi Bike trip histories",
        `${f.data.inputRows} trips, June to September 2026, as monthly CSV archives`,
        [{ text: "s3.amazonaws.com/tripdata", url: "https://s3.amazonaws.com/tripdata/index.html" }, ", listed at ", { text: "citibikenyc.com/system-data", url: "https://citibikenyc.com/system-data" }],
      ],
      [
        "Citi Bike live feed (GBFS 2.3)",
        `station information and status for ${f.data.liveStations} stations, refreshed every minute`,
        [{ text: "gbfs.citibikenyc.com/gbfs/2.3/gbfs.json", url: "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json" }],
      ],
      [
        "Open-Meteo weather",
        "hourly New York weather for the four months, and the current hour for the live job",
        [
          { text: "open-meteo.com", url: "https://open-meteo.com/" },
          ": the ",
          { text: "Historical Weather API", url: "https://open-meteo.com/en/docs/historical-weather-api" },
          " and the ",
          { text: "Forecast API", url: "https://open-meteo.com/en/docs" },
        ],
      ],
    ],
    { widths: [1.15, 1.6, 1.75], fontSize: 18 },
  ),
];

const glance = [
  front("VeloGraph at a Glance"),
  p(
    `VeloGraph turns ${f.data.inputRowsM} million real Citi Bike trips and the live New York station feed into a forecast for every station, every 30 seconds. It runs on a Spark cluster and a three-broker Kafka cluster, and a website shows the evidence for every part of it. The screenshots on these two pages were taken from the running system, in the website's six colour themes.`,
  ),
  ...table(
    "The project in numbers (all read from the exports the jobs wrote)",
    ["What", "Measured"],
    [
      ["Trips processed", `${f.data.inputRows} raw rows, ${f.data.validRows} clean (Jun-Sep 2026)`],
      ["Batch pipeline", `${f.batch.jobs} Spark jobs, ${f.batch.minutes} minutes of job time`],
      ["Station graph (GraphX)", `${f.graph.vertices} vertices, ${f.graph.edges} directed edges, ${f.graph.communities} districts`],
      ["Demand forecast (MLlib)", `${f.ml.dep.improvement} % lower error than the seasonal naive forecast`],
      ["Live scoring", `${f.data.liveStations} stations every 30 s; broker to map p95 ${f.stream.p95} s`],
      ["Code", `${int(code.scala.lines)} lines of Scala, ${int(code.python.lines)} of Python, ${int(code.web.lines)} of TypeScript`],
    ],
    { widths: [1, 2.2] },
  ),
  ...figurePair(
    [
      { path: shot("05-live-map.jpg"), note: "The live map: every station by state, with alerts and moves" },
      { path: shot("07-cluster-topology.jpg"), note: "The cluster, live: Spark workers and Kafka brokers" },
    ],
    "The live system: risk on the map, and the cluster that computes it",
  ),
  ...figurePair(
    [
      { path: shot("14-portal-network.jpg"), note: "GraphX: stations sized by PageRank, in districts" },
      { path: shot("16-portal-models.jpg"), note: "MLlib: archetypes and models against baselines" },
    ],
    "Two of the eleven evidence portals: GraphX and MLlib",
  ),
  ...figurePair(
    [
      { path: shot("02-home-pipeline-story.jpg"), note: "The pipeline, told as a subway line" },
      { path: shot("13-portal-pregel.jpg"), note: "Pregel, superstep by superstep" },
    ],
    "The website explains each module where it is shown",
  ),
  ...figurePair(
    [
      { path: shot("18-portal-streaming.jpg"), note: "Streaming: from a polled feed to a stream" },
      { path: shot("19-portal-streaming.jpg"), note: "Pull the plug: the failure drills" },
    ],
    "The streaming portal: the live path and its drills",
  ),
  ...figurePair(
    [
      { path: shot("03-home-portals.jpg"), note: "One evidence portal per module" },
      { path: shot("04-station-search.jpg"), note: "Ctrl+K search over every station" },
    ],
    "Finding your way: the portals and the station search",
  ),
  callout(
    "Where every number comes from",
    "Each figure, table and number in this report, and on the website, is read from the JSON files the Spark jobs wrote (data/serving). Nothing is typed in by hand, and anything replayed is labelled as a replay.",
  ),
];

const declaration = [
  front("Declaration"),
  p(
    `I, ${STUDENT.name} (Registration No. ${STUDENT.reg}, Roll No. ${STUDENT.roll}, Section ${STUDENT.section}), hereby declare that this project report, entitled "VeloGraph: Real-Time Bike-Share Intelligence on Apache Spark", is submitted for the course ${COURSE.code} ${COURSE.name} at Lovely Professional University.`,
  ),
  p(
    "The system described in this report was built and run for this project. The data, measurements, figures and screenshots it presents are genuine: every number was produced by the system on real data, and the sources of the data and of every tool used are acknowledged in the report.",
  ),
  gap(600),
  p(`Date: ${DATE}`, { align: AlignmentType.LEFT }),
  gap(300),
  p(`${STUDENT.name}`, { align: AlignmentType.RIGHT, run: { bold: true } }),
  p(`Registration No. ${STUDENT.reg}`, { align: AlignmentType.RIGHT }),
];

const certificate = [
  front("Certificate"),
  p(
    `This is to certify that the project report entitled "VeloGraph: Real-Time Bike-Share Intelligence on Apache Spark", submitted by ${STUDENT.name} (Registration No. ${STUDENT.reg}, Section ${STUDENT.section}) for the course ${COURSE.code} ${COURSE.name}, is a record of the project work carried out by the student for the course.`,
  ),
  gap(1200),
  p(`${TEACHER.name}`, { align: AlignmentType.RIGHT, run: { bold: true } }),
  p(TEACHER.title, { align: AlignmentType.RIGHT }),
  p("Lovely Professional University", { align: AlignmentType.RIGHT }),
  gap(200),
  p("Signature: ____________________", { align: AlignmentType.RIGHT }),
];

const acknowledgement = [
  front("Acknowledgement"),
  p(
    `I would like to thank ${TEACHER.name}, ${TEACHER.title}, for teaching ${COURSE.name} and for the guidance that shaped this project. The course's units on Spark, GraphX, Spark SQL, Kafka and machine learning with Spark gave the project its structure.`,
  ),
  p("I am grateful to Lovely Professional University for the opportunity to build a complete system rather than an isolated exercise."),
  p(
    "The project rests on open data and open-source software. The trip data and the live station feed are published by NYC Bike Share, LLC (Citi Bike); the weather data comes from Open-Meteo.com. The system is built on Apache Spark, Apache Kafka, Python, Scala and Next.js, and I thank the communities that maintain them.",
  ),
  gap(400),
  p(STUDENT.name, { align: AlignmentType.RIGHT, run: { bold: true } }),
];

const abstract = [
  h1("Abstract"),
  p(
    `Bike-share systems lose riders when stations run empty or full, and operators rebalance bikes by truck on short notice. This project, VeloGraph, builds a complete cluster-computing system around that problem for New York's Citi Bike network of about ${f.data.liveStations} stations.`,
  ),
  p(
    `A batch pipeline of ${f.batch.jobs} Apache Spark jobs turns ${f.data.inputRows} raw trip records from June to September 2026 into a medallion lake (bronze, silver, gold) with documented data-quality rules. Spark SQL answers ${f.sql.queries} analytical questions. GraphX models the stations as a graph of ${f.graph.vertices} vertices and ${f.graph.edges} directed edges; weighted PageRank, written with the Pregel API, converges in ${f.graph.supersteps} supersteps, and label propagation finds ${f.graph.communities} districts with a modularity of ${f.graph.modularity}.`,
  ),
  p(
    `MLlib models, trained on leakage-safe time windows, forecast next-hour departures and arrivals for every station. The champion random forest has a mean absolute error of ${f.ml.dep.mae} trips per station-hour on the held-out September data: ${f.ml.dep.improvement} % below a seasonal naive forecast. This is short of the 10 % aimed for, and it is reported as measured.`,
  ),
  p(
    `A three-broker Kafka cluster carries the live GBFS station feed to Spark Structured Streaming on a Docker Spark cluster. Every 30 seconds the streaming job scores all stations with the champion models, flags the stations about to run empty or full, and suggests rebalancing moves. Over a ${f.stream.minutes}-minute run, a station report reached the live map in ${f.stream.p50} s at the median and ${f.stream.p95} s at the 95th percentile, and the stream kept flowing while a Kafka broker was stopped.${
      backtest && backtest.calls.some((c) => c.risk === "EMPTY_RISK")
        ? ` Checked against the recorded feed an hour later, ${pct(backtest.calls.find((c) => c.risk === "EMPTY_RISK").rate * 100, 0)} of its "empties soon" alerts came true.`
        : ""
    }`,
  ),
  p(
    "A web application presents every module as an evidence portal backed by the jobs' own exports, and the report maps every unit of the INT315 syllabus and course outcomes CO4 to CO6 to what was built.",
  ),
  p("**Keywords:** Apache Spark, cluster computing, GraphX, Pregel, MLlib, Apache Kafka, Structured Streaming, bike sharing, demand forecasting.", { align: AlignmentType.LEFT }),
];

const contents = [
  front("Contents"),
  new TableOfContents("Contents", { hyperlink: true, headingStyleRange: "1-2" }),
  front("List of Figures"),
  new TableOfContents("Figures", { hyperlink: true, captionLabelIncludingNumbers: "Figure" }),
  front("List of Tables"),
  new TableOfContents("Tables", { hyperlink: true, captionLabelIncludingNumbers: "Table" }),
];

// ---- chapter 1 -------------------------------------------------------------------------------------------------
const ch1 = [
  h1Plain("1. Introduction"),
  h2("1.1 Background and motivation"),
  p(
    "Public bike-share systems are among the most data-rich forms of urban transport. Every trip is logged with its start and end station and time, and every station publishes its live bike and dock counts through the General Bikeshare Feed Specification (GBFS). New York's Citi Bike is the largest system in North America; in the four summer months studied here its riders made more than twenty million trips.",
  ),
  p(
    "The operational problem is imbalance. Commuters drain residential stations in the morning and fill office districts; the flow reverses in the evening. A station that is empty turns riders away, and one that is full stops them from ending their trip. Operators move bikes by truck, so they need to know which stations are about to fail, and in time to act.",
  ),
  p(
    "The data volume (tens of millions of trips), the graph structure of trips between stations, the need for predictive models and the live, continuously arriving feed make the problem a natural fit for cluster computing: batch processing, graph processing, machine learning and stream processing on one platform, Apache Spark, with Apache Kafka as the streaming backbone.",
  ),
  h2("1.2 Problem statement"),
  p(
    "Given four months of historical trips and the live station feed, build a system that (a) forecasts next-hour departures and arrivals at every station, (b) combines those forecasts with the live feed to flag stations that are about to run empty or full, (c) suggests rebalancing moves, and (d) explains the structure of the network, all on a real Spark cluster and a real Kafka cluster, with every result measured on real data.",
  ),
  h2("1.3 Objectives"),
  ...bullets([
    "Ingest the June to September 2026 Citi Bike trip data into a layered (medallion) data lake with explicit, documented data-quality rules.",
    "Demonstrate Spark's core abstractions (RDDs, DataFrames, Spark SQL), measuring the effects of shuffles and caching.",
    "Model the station network with GraphX: weighted PageRank written with the Pregel API, communities, triangles and travel times.",
    "Train and evaluate MLlib models for next-hour demand and for the drain-or-fill regime, always against baselines, with time-based splits that prevent leakage.",
    "Score the live GBFS feed every 30 seconds with Kafka and Spark Structured Streaming on a Docker Spark cluster, with alerts and rebalancing suggestions, and test its resilience.",
    "Present every module through a web application whose numbers are read from the jobs' own exports, and map the work to the INT315 syllabus.",
  ]),
  h2("1.4 Scope and constraints"),
  p(
    `The project was built and run on one laptop (16 GB of memory, Windows 11). The Kafka and Spark clusters run in Docker inside a 6 GB virtual machine, and heavy batch jobs run on the host with Spark's local mode, which uses the same code. The historical data covers ${f.data.months} months (June to September 2026); September is held out for testing and is never used to tune a model.`,
  ),
  h2("1.5 Organisation of the report"),
  p(
    "Chapter 2 introduces the cluster-computing concepts the project uses. Chapter 3 describes the data, Chapter 4 the system design and Chapter 5 the implementation of each module. Chapter 6 presents the results, Chapter 7 maps the work to the syllabus and course outcomes, Chapter 8 discusses the challenges met, and Chapter 9 concludes.",
  ),
];

// ---- chapter 2 -------------------------------------------------------------------------------------------------
const ch2 = [
  h1Plain("2. Background: Cluster Computing with Apache Spark"),
  h2("2.1 From MapReduce to Spark"),
  p(
    "Hadoop MapReduce made large-scale batch processing on commodity clusters practical, but every MapReduce job reads its input from disk and writes its output back to disk [2]. Multi-step workloads, such as iterative algorithms (PageRank, machine learning) and interactive queries, pay that disk cost at every step. MapReduce is also batch-only; it has no notion of a continuous stream.",
  ),
  p(
    "Apache Spark generalises the model [1]. A program is a directed acyclic graph (DAG) of transformations over distributed datasets, evaluated lazily and executed in memory where possible. Intermediate results can be cached, so iterative algorithms run many times faster than chains of MapReduce jobs. The same engine serves batch processing, SQL, graph processing (GraphX), machine learning (MLlib) and streaming (Structured Streaming).",
  ),
  h2("2.2 Spark architecture"),
  ...bullets([
    "**Driver:** the process that runs the application's `main` function, plans the DAG into stages and tasks, and schedules them.",
    "**Cluster manager:** allocates resources. VeloGraph uses Spark's own standalone manager (one master, two workers) for the cluster and local mode on the host.",
    "**Executors:** JVM processes on the workers that run tasks and keep cached data.",
    "**Jobs, stages and tasks:** an action triggers a job; the job is split into stages at shuffle boundaries (wide dependencies); each stage runs one task per partition [12].",
  ]),
  h2("2.3 RDDs, DataFrames and Spark SQL"),
  p(
    "The resilient distributed dataset (RDD) is Spark's core abstraction: an immutable, partitioned collection that records its lineage (how it was derived), so a lost partition can be recomputed instead of replicated [1]. Transformations (map, filter, reduceByKey) are lazy; actions (count, collect) run them. Narrow transformations stay within a partition, while wide ones such as reduceByKey and join need a shuffle across the cluster.",
  ),
  p(
    "DataFrames add a schema to distributed data, and Spark SQL lets the same data be queried with SQL. Both go through the Catalyst optimiser, which rewrites the logical plan (predicate pushdown, column pruning, join selection) and generates efficient code with whole-stage code generation (Tungsten) [3].",
  ),
  h2("2.4 GraphX and the Pregel model"),
  p(
    "GraphX represents a property graph as two distributed collections: vertices and edges, each with attributes [4]. Large graphs are split by vertex-cut partitioning: edges are assigned to partitions, and vertices are replicated where their edges are. Iterative graph algorithms are expressed in the Pregel model: in each superstep every active vertex runs a vertex program on the messages it received, then sends messages along its edges; the computation stops when no messages are sent [5]. PageRank, connected components and label propagation all fit this model.",
  ),
  h2("2.5 MLlib"),
  p(
    "MLlib is Spark's machine-learning library [9]. Its Pipeline API chains feature transformers (assemblers, scalers, encoders) and estimators (linear models, decision trees, random forests, gradient-boosted trees, support vector machines, naive Bayes, k-means) into one object that can be fitted, evaluated and saved. Training is distributed across the executors.",
  ),
  h2("2.6 Apache Kafka"),
  p(
    "Kafka is a distributed, partitioned, replicated commit log [10]. Producers append records to topics; each topic is split into partitions, and the record key decides the partition, so records with the same key stay in order. Each partition has a leader and followers on other brokers; with replication factor 3 and a minimum of 2 in-sync replicas, a write acknowledged by the leader (acks=all) survives the loss of a broker. Consumers in a consumer group share the partitions and commit their offsets. Recent versions replace ZooKeeper with KRaft, a built-in consensus quorum of controllers [13].",
  ),
  h2("2.7 Structured Streaming"),
  p(
    "Structured Streaming treats a stream as an unbounded table and runs the same DataFrame operations on it in micro-batches [11]. Event time (when a thing happened) is distinguished from processing time (when Spark sees it); a watermark tells Spark how long to wait for late data before finalising a window. Output modes (append, update, complete) decide what each micro-batch emits, and checkpoints of offsets and state let a restarted query continue exactly where it stopped.",
  ),
];

// ---- chapter 3 -------------------------------------------------------------------------------------------------
const ch3 = [
  h1Plain("3. Data"),
  h2("3.1 Citi Bike trip data"),
  p(
    `Citi Bike publishes one CSV archive of trips per month [14]. Each trip has a ride id, the bike type (classic or electric), start and end times, start and end stations with coordinates, and whether the rider is a member or a casual user. The project uses June to September 2026: ${f.data.inputRows} rows in all. Table ${mark("months", "table")} shows the volume by month.`,
  ),
  ...table(
    "Trip records by month (bronze rows, valid rows after the silver rules, quarantined rows)",
    ["Month", "Raw (bronze) rows", "Valid trips", "Quarantined"],
    f.data.byMonth.map((m) => [`${m.ym.slice(0, 4)}-${m.ym.slice(4)}`, int(m.bronzeRows), int(m.validRows), int(m.quarantinedRows)]),
    { widths: [1, 1.4, 1.4, 1.2], align: ["left", "right", "right", "right"] },
  ),
  h2("3.2 The live GBFS feed"),
  p(
    `GBFS is the open standard through which bike-share systems publish live data as JSON [15]. VeloGraph reads Citi Bike's station_information (names, positions, capacity) and station_status (bikes and docks available, last report time) for ${f.data.liveStations} stations. A recorder saves the feed every 60 seconds, so that live features can be replayed and the forecasts checked later; a producer publishes it to Kafka.`,
  ),
  h2("3.3 Weather and calendar"),
  p(
    "Hourly weather for New York (temperature, apparent temperature, precipitation, wind, humidity, cloud cover) comes from the Open-Meteo archive and forecast APIs [16]. A calendar dimension marks weekends and US public holidays. Both are joined to the station-hour panel and used as model features.",
  ),
  h2("3.4 Data quality"),
  p(
    `The silver layer applies explicit rules and keeps every rejected row in a quarantine table with the rule it broke, rather than dropping it silently. ${f.data.quarantined} trips (${f.data.quarantinedPct}) were quarantined; Table ${mark("reasons", "table")} lists the reasons. Trip endpoints were matched to the station registry for ${f.data.endpointMatchPct} of trips.`,
  ),
  ...table(
    "Quarantine reasons in the silver layer",
    ["Rule", "Trips", "Share of all rows"],
    f.data.reasons.map((r) => [r.reason.replace(/_/g, " ").toLowerCase(), int(r.rows), pct(r.pct, 3)]),
    { widths: [2, 1, 1], align: ["left", "right", "right"] },
  ),
];

// ---- chapter 4 -------------------------------------------------------------------------------------------------
const ch4 = [
  h1Plain("4. System Design"),
  h2("4.1 Architecture"),
  p(
    `VeloGraph has a batch path and a streaming path that meet in a serving layer (Figure ${mark("arch", "figure")}). On the batch path, Spark jobs on the host build the lake and the analytics: Spark SQL, the RDD lab, GraphX and MLlib. On the streaming path, the GBFS feed flows through Kafka to Spark Structured Streaming on the Docker cluster, which uses the models and features from the batch path. Both paths write JSON exports, and a small gateway relays the live results to the website over server-sent events.`,
  ),
  ...figure(join(FIG, "architecture.png"), "System architecture: the batch path (top) and the streaming path (bottom)"),
  h2("4.2 Deployment on one laptop"),
  p(
    `Figure ${mark("deploy", "figure")} shows where each part runs. The Kafka brokers, Kafka UI, and the Spark master and workers run as Docker containers in a 6 GB virtual machine; the data folder is shared with them. Heavy batch jobs run on the host, where they can use all cores and more memory, with the containers stopped. The streaming jobs run on the cluster, with two cores each.`,
  ),
  ...figure(join(FIG, "deployment.png"), "Deployment: host processes and the Docker cluster on one 16 GB laptop"),
  h2("4.3 Technology stack"),
  ...table(
    "Technology stack",
    ["Area", "Choice", "Role"],
    [
      ["Processing", "Apache Spark 3.5.8, Scala 2.12", "batch jobs, Spark SQL, GraphX, MLlib, Structured Streaming"],
      ["Streaming backbone", "Apache Kafka 4.3.1 (KRaft)", "3 brokers, replication factor 3, min in-sync replicas 2"],
      ["Ingestion and API", "Python 3.11 (uv), confluent-kafka, FastAPI", "downloads, GBFS recorder and producer, gateway with SSE"],
      ["Machine-learning lab", "PySpark notebooks (jupytext)", "model exploration on the same Spark engine"],
      ["Storage", "Parquet on the local file system", "partitioned lake tables; JSON serving exports"],
      ["Cluster", "Docker Compose", "Spark standalone master and 2 workers, Kafka x3, Kafka UI, history server"],
      ["Website", "Next.js 16, React 19, TypeScript, deck.gl, ECharts", "evidence portals, live map, charts"],
      ["Build and quality", "sbt, pytest, ruff, ESLint, SonarCloud", `${int(code.scala.tests)} Scala tests, ${code.python.tests} Python tests, quality gate`],
    ],
    { widths: [1.1, 1.6, 2.4] },
  ),
  h2("4.4 Serving layer and data contracts"),
  p(
    "Every job ends by writing a job report (start, end, duration, rows in and out, Spark application id) and, where the website needs it, a serving export. Every export uses one envelope: a meta block (schema name and version, generation time, the producing job, the source date range, the row count) and a data block. The website validates each file against a schema (zod) at build time, so a job that changes its output cannot silently break a page. Because the site renders from these files, it works without the cluster or the network; the gateway adds the live view when it is running.",
  ),
];

// ---- chapter 5 -------------------------------------------------------------------------------------------------
const lb = (t) => [...f.ml.baselines(t).filter((b) => b.model === "seasonal-naive"), ...f.ml.leaderboard(t)];
const ch5 = [
  h1Plain("5. Implementation"),
  p(
    "Each Spark job is one Scala object with a fixed shape: parse arguments, open a session, apply pure DataFrame transformations (kept in separate objects so that unit tests can run them on small inputs), write idempotently, and record a job report. Explicit schemas are used on every read, built-in functions are preferred to user-defined functions, and every random operation has a fixed seed.",
  ),
  h2("5.1 The medallion lake"),
  p(
    `The bronze layer stores every raw row as typed Parquet, partitioned by month, and reconciles its row count with the download manifest. The silver layer validates trips (end coordinates present, a duration between one minute and four hours, a plausible speed), deduplicates them with a window function and quarantines the rest with a reason. The gold layer builds a zero-filled station-by-hour panel (${int(f.raw.jobs["gold-station-hour"].metrics.departures)} departures, reconciled with the trips) and the route edges for the graph (Figure ${mark("lake", "figure")}).`,
  ),
  ...figure(join(FIG, "lake.png"), "The medallion lake with the rows at every layer, and the quarantine"),
  h2("5.2 RDD fundamentals"),
  p(
    `A dedicated job reads one raw month as plain text with the RDD API (${f.rdd.partitions} partitions, ${f.rdd.trips} trips) to make Spark's core behaviour measurable. Counting trips per station with groupByKey shuffled ${f.rdd.groupBytes}; reduceByKey, which combines values on each partition before the shuffle, moved ${f.rdd.reduceBytes}: ${f.rdd.shuffleRatio} times less. A join shuffled ${f.rdd.joinBytes}, while a broadcast join of the same data shuffled nothing.`,
  ),
  p(
    `Persistence was measured by counting the same RDD twice. Uncached, the second count took ${f.rdd.uncachedMs} ms because Spark recomputed it from the file; with MEMORY_ONLY it took ${f.rdd.memoryMs} ms (${f.rdd.cacheSpeedup} times faster), at the cost of ${f.rdd.memoryBytes} of objects (${f.rdd.serBytes} serialised with MEMORY_ONLY_SER). An accumulator incremented inside a transformation counted ${f.rdd.accInMap} trips after two actions instead of ${f.rdd.accInAction}: transformations re-run on every action, which is why accumulators belong in actions.`,
  ),
  ...figurePair(
    [
      { path: join(SPARKUI, "02-groupbykey-job-dag.png"), note: "groupByKey: the job's DAG in the Spark UI" },
      { path: join(SPARKUI, "03-reducebykey-job-dag.png"), note: "reduceByKey: the same count, less shuffle" },
    ],
    "The two jobs of the shuffle comparison as Spark's own history server shows them",
  ),
  h2("5.3 Spark SQL"),
  p(
    `${f.sql.queries} analytical questions are written as named SQL files and run over temporary views of the silver and gold tables (Table ${mark("sql", "table")}); together they ran in ${f.sql.totalSeconds} s. They use joins (including self-joins), grouping and ordering, window functions, and the LIKE and RLIKE wildcard operators. One result: in heavy rain, trips per hour fall by ${f.sql.worstRain} against dry hours.`,
  ),
  ...table(
    "The Spark SQL insights",
    ["Insight", "Question answered"],
    f.sql.titles.map((t) => [t.title, t.question]),
    { widths: [1.2, 3], fontSize: 18 },
  ),
  h2("5.4 GraphX network analytics"),
  p(
    `The stations form a directed, weighted property graph: ${f.graph.vertices} vertices and ${f.graph.edges} edges, one per route ridden, weighted by trips (${f.graph.trips} in all). The graph is dense (density ${f.graph.density}) and largely reciprocal (${f.graph.reciprocity} of edges have a reverse edge).`,
  ),
  ...bullets([
    `**Weighted PageRank with the Pregel API [5], [6]:** each superstep, a vertex adds the incoming rank shares (vprog) and sends its rank change along its out-edges in proportion to their trips (sendMsg); messages to one vertex are summed (mergeMsg). It converged in ${f.graph.supersteps} supersteps (${f.graph.pagerankSeconds} s) and agrees with GraphX's built-in PageRank (Spearman ${f.graph.spearman}). The top stations: ${f.graph.top.slice(0, 3).join("; ")}.`,
    `**Communities:** label propagation [7] found ${f.graph.lpaCommunities} raw communities (modularity ${f.graph.lpaModularity}, as defined by Newman [8]); merging small ones gave ${f.graph.communities} districts with modularity ${f.graph.modularity}.`,
    `**Structure:** ${f.graph.scc} strongly connected components and ${f.graph.triangles} triangles of strong ties; the average clustering coefficient is ${f.graph.clustering}, against ${f.graph.randomClustering} for a random graph of the same density.`,
    `**Partitioning:** with ${f.graph.partitions} partitions, EdgePartition2D copies each vertex ${f.graph.rf2D} times on average, against ${f.graph.rfRandom} for a random vertex cut.`,
  ]),
  ...figure(shot("15-portal-graph-ops.jpg"), "The GraphX operators portal: the property graph, its operators and the vertex cuts"),
  h2("5.5 Machine learning with MLlib"),
  p(
    `KMeans on the shape of every station's day (four hourly curves) chose k = ${f.ml.k} by silhouette (${f.ml.silhouette}): ${f.ml.archetypes.join(", ")}. The feature table has ${f.ml.features} features: calendar and weather for the hour, station attributes, the archetype, GraphX metrics and trailing-window demand profiles.`,
  ),
  p(
    `Leakage is prevented by time (Figure ${mark("windows", "figure")}): every profile feature, and the GraphX metrics themselves, are computed on a window that ends the day before the rows they describe. Models are tuned on August and tested once on September.`,
  ),
  ...figure(join(FIG, "ml-windows.png"), "Leakage-safe windows: the features of each split only look back"),
  p(
    `Table ${mark("departures", "table")} shows the departures leaderboard on the September test set. The champion random forest has a mean absolute error of ${f.ml.dep.mae} trips per station-hour, ${f.ml.dep.improvement} % below the seasonal naive forecast (${f.ml.dep.naiveMae}), which predicts the station's own mean for that weekday and hour over the previous eight weeks. For arrivals, gradient-boosted trees reach ${f.ml.arr.mae} (${f.ml.arr.improvement} % below naive).`,
  ),
  ...table(
    "Next-hour departures on the September test set (lower MAE and RMSE are better)",
    ["Model", "MAE", "RMSE", "R²"],
    lb("departures").map((r) => [r.model, r.mae, r.rmse, r.r2]),
    { widths: [2, 1, 1, 1], align: ["left", "right", "right", "right"] },
  ),
  p(
    `A classifier also predicts the regime of each station-hour: draining, stable or filling. The champion, a one-vs-rest linear support vector machine, reaches a macro-F1 of ${f.ml.cls.macroF1}, against ${f.ml.cls.naiveF1} for the seasonal naive rule and ${f.ml.cls.majorityF1} for always predicting the majority class.`,
  ),
  p(
    `An ablation study retrains the departure model without groups of features (Table ${mark("ablation", "table")}). Removing the graph features alone raises the error a little; removing both the graph and the demand profiles raises it sharply, so the network features carry information the profiles do not.`,
  ),
  ...table(
    "Ablation: test MAE of the departure model when feature groups are removed",
    ["Variant", "Features", "MAE", "Change"],
    f.ml.ablation.map((a) => [a.name, String(a.features), a.mae, a.delta]),
    { widths: [2.2, 1, 1, 1], align: ["left", "right", "right", "right"] },
  ),
  p(
    `The models were explored in four PySpark notebooks and then rebuilt in Scala as MLlib PipelineModels. The Scala training job refuses to finish unless it reproduces the notebook's test error within 1 %. In the latest run it came within ${f.ml.scala.maxDelta} % (departures ${f.ml.scala.depDelta} %, arrivals ${f.ml.scala.arrDelta} %, regime ${f.ml.scala.regimeDelta} %): forests and boosted trees draw their samples per input partition, and the feature table had been rebuilt since the notebooks ran.`,
  ),
  h2("5.6 Streaming with Kafka and Structured Streaming"),
  p(
    `A Python producer polls the GBFS feed and publishes only the stations that changed (change data capture), with a periodic full snapshot, to a six-partition topic keyed by station, so each station's reports stay in order. It uses acks=all and idempotence; malformed records go to a dead-letter topic. Over the latest run it delivered ${f.stream.delivered} messages with ${f.stream.failed} failures (acknowledgement p95 ${f.stream.ackP95} ms).`,
  ),
  p(
    `The scoring job reads the topic with Structured Streaming, applies a 10-minute watermark and drops duplicates within it, and processes each 30-second micro-batch with foreachBatch (Figure ${mark("batch", "figure")}): it keeps the newest status of every station, builds the model features for the current and next hour, scores them with the three champion models and applies the rules.`,
  ),
  ...figure(join(FIG, "micro-batch.png"), "One micro-batch of the live scoring job"),
  p(
    "The rules project each station's bikes 60 minutes ahead from the forecast net flow and classify it into one of six states against a margin of max(2, 10 % of the docks): empty or full now, at risk of either within the hour, fine, or offline. A station that enters the empty or full group raises an alert, and a rebalancer pairs stations about to run empty with stations that will have spare bikes in the same GraphX district, within 2.5 km and at most 12 bikes per move.",
  ),
  ...figure(shot("06-live-station-drawer.jpg"), "The live map during a run: states, alerts, suggested moves and one station's forecast"),
  p(
    `Resilience was tested on the running system. In the broker drill, one of the three brokers was stopped for ${fixed(f.stream.broker.downS, 0)} s in the middle of the stream: its partitions moved to the other brokers, no partition lost its leader, the producer delivered ${int(f.stream.broker.producerDelivered)} messages with ${int(f.stream.broker.producerFailed)} failures, ${int(f.stream.broker.batchesWhileDown)} micro-batches ran meanwhile, and the replicas were back in sync ${fixed(f.stream.broker.recoveredS, 1)} s after the restart. In the restart drill, the driver was killed with kill -9: the restarted job's first batch began at offset ${int(f.stream.restart.firstStart)}, exactly where the last committed batch had ended, so nothing was lost or counted twice.`,
  ),
  ...figurePair(
    [
      { path: shot("23-spark-structured-streaming.jpg"), note: "Spark's Structured Streaming UI for the job" },
      { path: shot("24-kafka-ui-brokers.jpg"), note: "Kafka UI: three brokers, all partitions in sync" },
    ],
    "The streaming system in its own tools",
  ),
  p(
    `A second streaming job replays one evening of trips with deliberately late messages to show event time at work: of ${int(f.stream.replay.late.sent)} late trips, ${int(f.stream.replay.late.accepted)} still reached their window and ${int(f.stream.replay.late.droppedByWatermark)} were dropped by the 15-minute watermark, in both append and update output modes.`,
  ),
  h2("5.7 The web application"),
  p(
    "The website, built with Next.js, renders every page from the serving exports. Each module has an evidence portal: the lake, the RDD lab, Spark SQL, the Spark UI (its charts redrawn from the history server's REST API), Pregel step by step, the network, GraphX operators, the models, streaming, the cluster and the syllabus. Each portal has the same layout: the concept, an exact chart, the code that produced it and the numbers with the export they came from. The live map follows the gateway over server-sent events and falls back to the last recorded snapshot when the gateway is down.",
  ),
  p(
    "On a production build, Lighthouse (desktop) scores the ten pages without a WebGL map 93 to 98 for performance and 100 for accessibility, and scrolling stays within a 16.7 ms frame budget (6.3 ms per frame on the home page).",
  ),
  ...figurePair(
    [
      { path: shot("11-portal-sql.jpg"), note: "Spark SQL: fourteen questions, one query each" },
      { path: shot("12-portal-spark-ui.jpg"), note: "The Spark UI, redrawn from the history server" },
    ],
    "Two more portals: Spark SQL and the Spark UI",
  ),
  h2("5.8 Automation, testing and quality"),
  ...bullets([
    "`scripts/pipeline.ps1` runs every batch stage in order, with each job's tested driver memory, and prints the timings. A full run on 17 October took 50.9 minutes, and it reproduced every row count and metric of the run before; only the timings changed.",
    "`scripts/demo.ps1` starts the whole live system with one command: the Docker cluster, the producer, the scoring job, the gateway and the website. In testing it was live in 73 seconds from a running Docker.",
    "`scripts/benchmark.ps1` runs the RDD lab on the Docker cluster next to the laptop run (Section 6.2).",
    `The Scala jobs have ${int(code.scala.tests)} unit tests and the Python code ${code.python.tests}; the website is type-checked, linted and built. The repository is analysed by SonarCloud, whose quality gate (reliability, security, maintainability) passes.`,
  ]),
];

// ---- chapter 6 -------------------------------------------------------------------------------------------------
const okRate = (key) => (call("OK") ? (call("OK")[key] / Math.max(1, call("OK").n)) * 100 : 0);
const backtestPara = backtest
  ? [
      p(
        `The live forecasts were then checked against reality. Every prediction the scoring job stored was compared with what the GBFS recorder saw ${backtest.horizonMin} minutes later: ${int(backtest.predictions)} predictions from ${backtest.window.from} to ${backtest.window.to} (New York time). The projected bike counts were off by ${fixed(backtest.mae, 2)} bikes on average, while simply assuming that nothing changes was off by ${fixed(backtest.persistenceMae, 2)}. ${
          backtest.improvementPct >= 0
            ? `The projection is ${fixed(backtest.improvementPct, 1)} % more accurate than no change.`
            : `So the projection itself is ${fixed(-backtest.improvementPct, 1)} % less accurate than no change: over one hour most stations change little, and the forecast's noise outweighs its signal.`
        }`,
      ),
      ...(call("EMPTY_RISK") && call("FULL_RISK") && call("OK")
        ? [
            p(
              `The risk calls are where the model earns its place. Of ${int(call("EMPTY_RISK").n)} "empties soon" calls, ${pct(call("EMPTY_RISK").rate * 100, 0)} came true within the hour, and of ${int(call("FULL_RISK").n)} "fills soon" calls, ${pct(call("FULL_RISK").rate * 100, 0)}. Among stations the model called fine, only ${pct(okRate("cameTrue"), 1)} emptied and ${pct(okRate("becameFull"), 1)} filled. Flagged stations are often already close to their margin, which makes a hit easier, but the gap is ${fixed(call("EMPTY_RISK").rate * 100 / Math.max(0.1, okRate("cameTrue")), 0)}-fold for empty stations: the alerts point operators at the right stations.`,
            ),
          ]
        : []),
    ]
  : [];

// Section numbers and table marks are handed out as the chapter is built, so this runs at its place in ch6.
let section6 = 0;
const h2in6 = (title) => h2(`6.${++section6} ${title}`);
const benchSection = () =>
  f.bench
  ? [
      h2in6("Laptop against cluster"),
      p(
        `The RDD lab was run twice on the same September data: on the laptop in local mode (${f.bench.localMaster}) and on the Docker cluster (${f.bench.clusterMaster}, two workers with two cores each). ${f.bench.same ? "The answers were identical, down to every shuffle byte." : "The answers differed between the two runs."} Table ${mark("bench", "table")} compares the timings: the whole job took ${f.bench.localS} s on the laptop and ${f.bench.clusterS} s on the cluster.`,
      ),
      ...table(
        "The RDD lab on the laptop and on the Docker cluster (wall-clock time per step)",
        ["Step", "Laptop", "Cluster"],
        f.bench.rows,
        { widths: [2.4, 1, 1], align: ["left", "right", "right"] },
      ),
      p(
        `The cache rows explain most of the difference. On the cluster, MEMORY_ONLY could keep only ${f.bench.memCluster} of the ${f.bench.memLocal} of deserialised objects, so the "cached" count recomputed most partitions from their lineage and took ${f.bench.memClusterTime}. Serialised, the whole RDD fits in ${f.bench.serCluster}, and its second count took ${f.bench.serClusterTime}. On a small cluster MEMORY_ONLY_SER is the better storage level, and a cluster pays off only once the data outgrows one machine.`,
      ),
    ]
  : [];

const ch6 = [
  h1Plain("6. Results and Discussion"),
  h2in6("Batch pipeline"),
  p(`Table ${mark("jobs", "table")} lists the batch jobs in run order with their measured durations: ${f.batch.minutes} minutes in all on the host.`),
  ...table(
    "Batch jobs and their durations (host, local mode, Jun-Sep 2026 data)",
    ["Job", "Duration (s)"],
    f.batch.perJob.map((j) => [j.name, String(j.seconds)]),
    { widths: [3, 1], align: ["left", "right"] },
  ),
  ...benchSection(),
  h2in6("Models against baselines"),
  p(
    `The demand models beat the seasonal naive forecast by ${f.ml.dep.improvement} % (departures) and ${f.ml.arr.improvement} % (arrivals) in mean absolute error. The target set at the start was 10 %, and it was not met. Two reasons stand out. First, next-hour demand at a single station is small and noisy: ${fixed(f.raw.jobs["gold-station-hour"].metrics.zeroDeparturePct, 1)} % of station-hours have no departures at all. Second, the seasonal naive forecast is itself strong, since bike demand repeats week to week. The gain is consistent, and the ablation shows that the graph features contribute to it.`,
  ),
  ...(f.ml.evaluation
    ? [
        p(
          `These are the notebooks' numbers. The models the live job actually loads can be checked with one command, \`submit.ps1 evaluate-champions\`, which reloads the saved PipelineModels and scores the ${f.ml.evaluation.rows} station-hours of September against the same baselines. Figure ${mark("evaluation", "figure")} is its console output, as it ran on 18 October. The deployed departures model has an MAE of ${f.ml.evaluation.dep.mae} against ${f.ml.evaluation.dep.naive} for the seasonal naive forecast (${f.ml.evaluation.dep.better} % better), and the arrivals model ${f.ml.evaluation.arr.mae} against ${f.ml.evaluation.arr.naive} (${f.ml.evaluation.arr.better} % better).`,
        ),
        // Both figures follow this text, so the training log is the figure after the evaluation.
        p(
          `The classifier shows why accuracy alone misleads on unbalanced classes. Always predicting "stable", the majority class, would score an accuracy of ${f.ml.evaluation.majorityAccuracy}, higher than the model's ${f.ml.evaluation.accuracy}, but it never finds a draining or filling station, so its macro-F1 is only ${f.ml.evaluation.majorityF1}. The model, trained with class weights, finds almost half of the draining and filling station-hours, at a macro-F1 of ${f.ml.evaluation.macroF1}. Figure ${refOf("evaluation") + 1} shows the training job's own log lines: each model and its check against the notebook.`,
        ),
        ...figure(shot("26-terminal-evaluate-champions.png"), "The saved champion models scored on September: the console output of evaluate-champions"),
        ...figure(shot("27-terminal-train-champion-log.png"), "The champion training run of 17 October: each model checked against the notebook"),
      ]
    : []),
  h2in6("The live system"),
  p(
    `Over a ${f.stream.minutes}-minute run, the scoring job processed ${f.stream.batches} micro-batches. From the broker's append time to the snapshot the website reads, the median latency was ${f.stream.p50} s and the 95th percentile ${f.stream.p95} s, against a goal of 90 s; most of that time is the 30-second trigger and the feed's own reporting delay.`,
  ),
  ...backtestPara,
  callout(
    "Honest reporting",
    "Every number in this report and on the website is read from the files the jobs wrote. Replayed data is labelled as replay, the 10 % model target that was missed is reported as missed, and the test month was never used for tuning.",
  ),
  h2in6("Limitations"),
  ...bullets([
    "The clusters run on one laptop, so the cluster demonstrates distribution and fault tolerance rather than scale; the batch data (about 20 million trips) fits a single machine's local mode.",
    "The models forecast one hour ahead from historical patterns and the weather; events such as concerts or closures are not in the features.",
    "The live recordings have gaps where the laptop slept, so the live backtest covers only the periods the recorder saw.",
  ]),
];

// ---- chapter 7 -------------------------------------------------------------------------------------------------
const ch7 = [
  h1Plain("7. Syllabus and Course Outcome Mapping"),
  p(`Table ${mark("syllabus", "table")} maps every unit of the INT315 syllabus and the assessed course outcomes to what VeloGraph built and to one measured result.`),
  ...table(
    "Syllabus units and course outcomes against the project",
    ["Unit / CO", "Syllabus topic", "What VeloGraph built", "Evidence"],
    [
      ["Unit I", "MapReduce limits, batch vs real-time, in-memory processing, Spark install", "A batch lake and a live stream side by side, a standalone Spark cluster and the host install", `cached recount ${f.rdd.cacheSpeedup}x faster`],
      ["Unit II", "Scala: types, collections, functions", "All jobs in idiomatic Scala: case classes, pattern matching, Option/Try, higher-order functions", `${int(code.scala.lines)} lines, ${int(code.scala.tests)} tests`],
      ["Unit III", "RDDs, operations, shared variables", "The RDD lab: reduceByKey vs groupByKey, persistence levels, lineage, broadcast, accumulators", `${f.rdd.shuffleRatio}x less shuffle`],
      ["Unit III", "GraphX features and operations", "Weighted PageRank with Pregel, label propagation, components, triangles, travel times", `${f.graph.communities} districts, Q = ${f.graph.modularity}`],
      ["Unit IV", "Spark SQL: DataFrames, joins, aggregates, wildcards", `${f.sql.queries} named SQL queries with joins, windows, LIKE and RLIKE`, `heavy rain: ${f.sql.worstRain} fewer trips`],
      ["Unit V", "Kafka: architecture, producers, consumers, Spark integration", "A 3-broker KRaft cluster, CDC producer, consumer group, dead-letter topic, Spark reading and writing Kafka", `${int(f.stream.broker.producerFailed)} failed deliveries with a broker down`],
      ["Unit VI", "Machine learning with PySpark", "Four PySpark notebooks: regression, classification, clustering, ablation, against baselines", `${f.ml.dep.improvement} % better than naive`],
      ["CO4", "Apply MLlib", "Champion models rebuilt in Scala and scoring every station every 30 s", `within ${f.ml.scala.maxDelta} % of the notebooks`],
      ["CO5", "Build Spark workflows", "bronze, silver, gold, SQL, GraphX, features, models, streaming, each job timed", `${f.batch.jobs} jobs, ${f.batch.minutes} min`],
      ["CO6", "Apply GraphX", "Graph features computed per window and measured inside the forecast", `ablation (Table ${refOf("ablation")})`],
    ],
    { widths: [0.7, 1.6, 2.4, 1.3], fontSize: 17 },
  ),
  p("The five practicals listed in the course map as follows: MLlib to the models portal and the notebooks; Spark SQL to the 14 insights; GraphX to the network suite; Spark Streaming to the live scoring and replay jobs; and the Kafka producer and consumer to the GBFS producer and the gateway's consumer group."),
];

// ---- chapter 8 -------------------------------------------------------------------------------------------------
const ch8 = [
  h1Plain("8. Challenges and Lessons Learned"),
  ...bullets([
    "**Memory on a 16 GB machine.** The Docker VM was capped at 6 GB and sized container by container; heavy batch jobs run on the host with the containers stopped, each with a measured driver-memory setting.",
    "**Clock skew.** The Windows clock ran 92 s slow while the Docker VM's was correct, so latencies measured across the two were wrong. Latency is therefore measured on the cluster against the broker's append time, and the producer corrects itself from the feed's own Date and Age headers.",
    "**Spark on Windows.** Spark needs Hadoop's native helpers (winutils) and Java 17; a session script sets them for one terminal without changing the machine.",
    "**Separating already-empty from about-to-empty.** A single risk flag was swamped by stations that were already empty, so the rules were split into six states, and alerts fire only when a station enters the empty or full group.",
    "**Restarts and stale state.** A restarted job compared its first batch with an hours-old snapshot and raised hundreds of false alerts; it now resumes from a snapshot only if it is less than 35 minutes old.",
    "**Resilience testing finds real bugs.** Stopping a broker while the website was recorded revealed a crash in the gateway's Kafka admin client; requests are now serialised and the gateway is restarted automatically if it exits.",
    "**Leakage is easy to introduce.** Graph metrics computed on all four months leak the future into the training set; recomputing them per time window fixed it.",
  ]),
];

// ---- chapter 9 -------------------------------------------------------------------------------------------------
const ch9 = [
  h1Plain("9. Conclusion and Future Work"),
  p(
    `VeloGraph shows the parts of cluster computing working together on one real problem. A Spark batch pipeline turns ${f.data.inputRowsM} million raw trips into a clean lake. Spark SQL and GraphX explain how the city rides, and MLlib forecasts the next hour better than a strong baseline. Kafka and Structured Streaming apply those forecasts to the live feed every 30 seconds on a fault-tolerant cluster. Every result is measured, and every result can be inspected on the website.`,
  ),
  h3("Future work"),
  ...bullets([
    "Run the same jobs on a multi-node cloud cluster, and on the full multi-year trip history.",
    "Add event and transit-disruption data to the features, and forecast several hours ahead.",
    "Turn suggested moves into an optimised truck route (a vehicle-routing problem) over the GraphX districts.",
    "Store the lake in a table format with time travel (Delta Lake or Apache Iceberg), and host the live backend on a cloud VM, so the public site's live pages update in real time.",
  ]),
];

// ---- references and appendices ---------------------------------------------------------------------------------
const refs = [
  "M. Zaharia et al., \"Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing,\" in Proc. NSDI, 2012.",
  "J. Dean and S. Ghemawat, \"MapReduce: Simplified Data Processing on Large Clusters,\" in Proc. OSDI, 2004.",
  "M. Armbrust et al., \"Spark SQL: Relational Data Processing in Spark,\" in Proc. ACM SIGMOD, 2015.",
  "J. E. Gonzalez et al., \"GraphX: Graph Processing in a Distributed Dataflow Framework,\" in Proc. OSDI, 2014.",
  "G. Malewicz et al., \"Pregel: A System for Large-Scale Graph Processing,\" in Proc. ACM SIGMOD, 2010.",
  "L. Page, S. Brin, R. Motwani and T. Winograd, \"The PageRank Citation Ranking: Bringing Order to the Web,\" Stanford InfoLab, 1999.",
  "U. N. Raghavan, R. Albert and S. Kumara, \"Near linear time algorithm to detect community structures in large-scale networks,\" Physical Review E, 76(3), 2007.",
  "M. E. J. Newman, \"Modularity and community structure in networks,\" PNAS, 103(23), 2006.",
  "X. Meng et al., \"MLlib: Machine Learning in Apache Spark,\" Journal of Machine Learning Research, 17(34), 2016.",
  "J. Kreps, N. Narkhede and J. Rao, \"Kafka: a Distributed Messaging System for Log Processing,\" in Proc. NetDB, 2011.",
  "M. Armbrust et al., \"Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark,\" in Proc. ACM SIGMOD, 2018.",
  "Apache Spark documentation, version 3.5, https://spark.apache.org/docs/3.5.8/.",
  "Apache Kafka documentation, https://kafka.apache.org/documentation/.",
  "Citi Bike System Data, NYC Bike Share, LLC, https://citibikenyc.com/system-data.",
  "General Bikeshare Feed Specification (GBFS), MobilityData, https://gbfs.org/.",
  "Open-Meteo weather API, https://open-meteo.com/.",
];

/** A reference's text, with any URL in it as a clickable link (a trailing full stop stays outside the link). */
const refRuns = (ref) =>
  ref
    .split(/(https?:\/\/\S*[^\s.,;])/)
    .filter(Boolean)
    .map((part) => (/^https?:\/\//.test(part) ? link(part, part) : new TextRun(part)));

const references = [h1Plain("References"), ...refs.map((r, i) => new Paragraph({ spacing: { after: 100 }, indent: { left: 540, hanging: 540 }, children: [new TextRun(`[${i + 1}]`), new TextRun({ children: [new Tab()] }), ...refRuns(r)], tabStops: [{ type: TabStopType.LEFT, position: 540 }] }))];

const appendixA = [
  h1Plain("Appendix A. How to Run VeloGraph"),
  p("All commands run in Windows PowerShell from the repository root, after `. .\\scripts\\env.ps1` (which sets Java 17, Spark, sbt and Hadoop's helpers for that terminal only)."),
  ...table(
    "Main commands",
    ["Purpose", "Command"],
    [
      ["Check the toolchain", "`.\\scripts\\doctor.ps1`"],
      ["Build the Spark jar", "`cd spark; sbt --batch assembly`"],
      ["Run the batch pipeline", "`.\\scripts\\pipeline.ps1`"],
      ["Start the live demo", "`.\\scripts\\demo.ps1` (stop: `-Stop`)"],
      ["Run one Spark job", "`.\\scripts\\submit.ps1 <job>` (`-Mode cluster` for Docker)"],
      ["Resilience drills", "`cd python; uv run python -m velograph.ops.drills broker`"],
      ["Tests", "`sbt --batch test` · `uv run pytest` · `pnpm build`"],
    ],
    { widths: [1.2, 2.4] },
  ),
  withLink("The source code, with a README, an architecture document, decision records and a runbook, is in the project repository, ", REPO, "."),
];

const appendixB = [
  h1Plain("Appendix B. Screenshot Gallery"),
  p("These screenshots were taken from the running system on 18 October 2026 by a script (velograph.ops.evidence_shots), in the website's six colour themes, so they show the real pages and the real cluster tools."),
  ...figurePair(
    [
      { path: shot("09-portal-lake.jpg"), note: "The lake portal" },
      { path: shot("10-portal-rdd-lab.jpg"), note: "The RDD lab portal" },
    ],
    "Portals for the lake and the RDD lab",
  ),
  ...figurePair(
    [
      { path: shot("17-portal-models.jpg"), note: "Will a station drain or fill? The classifier" },
      { path: shot("08-cluster-pipeline.jpg"), note: "The pipeline, end to end, on the cluster page" },
    ],
    "The drain-or-fill classifier and the timed pipeline",
  ),
  ...figurePair(
    [
      { path: shot("20-syllabus.jpg"), note: "The syllabus portal" },
      { path: shot("25-kafka-ui-topics.jpg"), note: "Kafka UI: the topics" },
    ],
    "Syllabus traceability and the Kafka topics",
  ),
  ...figurePair(
    [
      { path: shot("21-spark-master.jpg"), note: "Spark master UI" },
      { path: shot("22-spark-app-jobs.jpg"), note: "The scoring job's Spark jobs" },
    ],
    "The Spark cluster's own web interfaces during a live run",
  ),
  ...figurePair(
    [
      { path: join(SPARKUI, "07-sql-plan-with-exchanges.png"), note: "A Catalyst physical plan" },
      { path: join(SPARKUI, "08-graphx-pregel-supersteps.png"), note: "Pregel: one Spark job per superstep" },
    ],
    "Spark SQL and GraphX in the Spark history server",
  ),
];

// ---- assembly --------------------------------------------------------------------------------------------------
const header = new Header({
  children: [
    new Paragraph({
      border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: COLORS.rule, space: 4 } },
      tabStops: [{ type: TabStopType.RIGHT, position: TEXT_DXA }],
      children: [
        new TextRun({ text: "VeloGraph: Real-Time Bike-Share Intelligence on Apache Spark", size: 17, color: COLORS.sub }),
        new TextRun({ children: [new Tab(), `${COURSE.code} ${COURSE.name}`], size: 17, color: COLORS.sub }),
      ],
    }),
  ],
});
const footer = new Footer({
  children: [
    new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [new TextRun({ children: [PageNumber.CURRENT], size: 18, color: COLORS.sub })],
    }),
  ],
});
const margins = { top: 1440, right: 1440, bottom: 1440, left: 1440, header: 600, footer: 600 };
const doc = new Document({
  creator: STUDENT.name,
  title: "VeloGraph: Real-Time Bike-Share Intelligence on Apache Spark",
  description: `${COURSE.code} ${COURSE.name} project report`,
  styles,
  numbering,
  sections: [
    {
      properties: {
        page: {
          margin: margins,
          borders: {
            pageBorderTop: { style: BorderStyle.DOUBLE, size: 6, color: COLORS.ink, space: 20 },
            pageBorderBottom: { style: BorderStyle.DOUBLE, size: 6, color: COLORS.ink, space: 20 },
            pageBorderLeft: { style: BorderStyle.DOUBLE, size: 6, color: COLORS.ink, space: 20 },
            pageBorderRight: { style: BorderStyle.DOUBLE, size: 6, color: COLORS.ink, space: 20 },
            pageBorders: { display: PageBorderDisplay.ALL_PAGES, offsetFrom: PageBorderOffsetFrom.PAGE },
          },
        },
      },
      children: cover,
    },
    {
      properties: { page: { margin: margins, pageNumbers: { start: 1, formatType: NumberFormat.LOWER_ROMAN } } },
      headers: { default: header },
      footers: { default: footer },
      children: [...overview, ...glance, ...declaration, ...certificate, ...acknowledgement, ...abstract, ...contents],
    },
    {
      properties: { page: { margin: margins, pageNumbers: { start: 1, formatType: NumberFormat.DECIMAL } } },
      headers: { default: header },
      footers: { default: footer },
      children: [...ch1, ...ch2, ...ch3, ...ch4, ...ch5, ...ch6, ...ch7, ...ch8, ...ch9, ...references, ...appendixA, ...appendixB],
    },
  ],
});

const out = join(ROOT, "submission");
mkdirSync(out, { recursive: true });
const target = join(out, "VeloGraph-Report.docx");
writeFileSync(target, await Packer.toBuffer(doc));
console.log(target);
