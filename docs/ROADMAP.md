# VeloGraph — Roadmap

From an empty folder (Wed 14 Oct 2026) to submission on **Sat 24 Oct 2026**.

**How this file is used.** Every work session: read [CLAUDE.md](../CLAUDE.md) → continue the first unchecked task of
the current phase → verify by running it → write measured numbers on its **Evidence** line → tick it → update the
Status block in CLAUDE.md. Gates are hard checkpoints. Design details live in [ARCHITECTURE.md](ARCHITECTURE.md); this
file says *what* and *when*.

Legend: `[ ]` todo · `[x]` done · 🎓 what you must be able to explain in the viva once the phase is done.

---

## 1. What "done" means

| Component | Marks | How VeloGraph earns it |
|---|---|---|
| Implementation | 15 | A working end-to-end system that visibly runs on a Spark cluster and a Kafka cluster: Spark SQL, GraphX, MLlib and Structured Streaming over ~20M real trips plus a live feed |
| Report | 5 | 25–30 pages: architecture, methodology, honest results against baselines, syllabus traceability, screenshots |
| Presentation + viva | 10 | 12–15 slides, a rehearsed 6–7 min live demo with an offline fallback, answers from a prepared Q&A bank |

## 2. Coverage matrix — the grading lens

The CA brief assesses **CO4, CO5, CO6** and asks for a project that "integrates multiple components of the syllabus".
Every row must be demonstrable, not just mentioned.

| Syllabus | VeloGraph feature | Evidence | Demo moment |
|---|---|---|---|
| **Unit I** — MapReduce limits, batch vs real-time, stream & in-memory processing, standalone install, Spark vs Hadoop | Batch lake and real-time path side by side; cache vs no-cache timings; Spark standalone cluster (1 master + 2 workers) plus the host install; report chapter on Spark vs Hadoop/MapReduce | `rdd-fundamentals.json`, `infra/docker-compose.yml` | `/cluster`: live master + workers |
| **Unit II** — Scala types, literals, operators, inference, (im)mutable collections, functions, lists/maps/streams | All core jobs in idiomatic Scala; `scala-tour.md` maps each concept to real lines of our code | `spark/src/**` | walk through a job in IntelliJ |
| **Unit III** — RDD features, creation, functions, operations, spark-shell, shared variables | `RddFundamentalsJob` (transformations, actions, persistence, lineage, broadcast, accumulators); spark-shell walkthrough | `rdd-fundamentals.json` | type the walkthrough live |
| **Unit III** — GraphX features & operations; feature extraction & transformation | GraphX suite (PageRank, LPA, CC/SCC, triangles, paths, operator tour); ML feature transformers | `lake/graph/*`, `network-*.json` | `/network` |
| **Unit IV** — Spark SQL: RDD → DataFrame, joins, groupBy, orderBy, aggregates, wildcards | `InsightsJob`: 14 named queries (self-join, window functions, `LIKE` / `RLIKE`); three RDD → DataFrame conversions | `resources/sql/*.sql`, `insights/*.json` | `/insights` → View SQL |
| **Unit V** — Kafka fundamentals, cluster architecture, installation, Spark integration, producer/consumer, pipeline, ingestion patterns | 3-broker KRaft cluster (RF 3, min ISR 2) + Kafka UI; producers (polling + CDC, replay, record/replay), Spark consumers, gateway consumer group, DLQ | `infra/`, `python/velograph/ingest/` | stop a broker live |
| **Unit VI** — PySpark ML: LinReg, LogReg, DT, K-means, SVM, NB; confusion matrix, R², RMSE, MAE, correlation heat map | PySpark Lab 01–04 + Scala archetypes and champion models | `python/lab/*.ipynb`, `ml-*.json` | `/models` |
| **CO4** — apply MLlib | Demand regressors, regime classifier, archetypes, live scoring | model cards | `/models`, `/live` |
| **CO5** — Spark workflows | bronze → silver → gold → graph → features → models → streaming, orchestrated and timed | `serving/jobs/*.json` | `/cluster` job timeline |
| **CO6** — apply GraphX | GraphX suite + graph features measured inside the model (ablation) | `ml-ablation.json` | `/network` + ablation chart |

The five listed practicals: MLlib → `/models` · Spark SQL → `/insights` · GraphX → `/network` · Spark Streaming →
`/live` · Kafka producer & consumer → `gbfs_producer` + gateway consumer group.

## 3. Schedule

| Day | Date | Claude builds | You (≈ 30–45 min) | Gate |
|---|---|---|---|---|
| D0 | Wed 14 Oct | Analysis, design, this roadmap | Review the plan | — |
| D1 | Thu 15 Oct | P0 foundations; start downloads + GBFS recorder; P1 on the Sep slice | Watch the cluster come up; open Spark UI and Kafka UI | — |
| D2 | Fri 16 Oct | P1 for Jun–Sep; P2 gold, RDD, Spark SQL | Run one insight query yourself in `spark-shell` | **A** |
| D3 | Sat 17 Oct | P3 GraphX | Read the PageRank/Pregel notes; explore results | — |
| D4 | Sun 18 Oct | P4 ML: archetypes, features, PySpark lab, champion | Re-run a lab notebook; read the metrics notes | — |
| D5 | Mon 19 Oct | P5 Kafka + Structured Streaming + gateway | Stop a broker yourself during a live run | **B** |
| D6 | Tue 20 Oct | P6 web I: design system, shell, Home, Live | Design feedback | — |
| D7 | Wed 21 Oct | P6 web II: Network, Models, Insights, Cluster, Syllabus | Design feedback | **C** |
| D8 | Thu 22 Oct | P7 hardening, automation, README, live backtest, screenshots | Run `demo.ps1` alone from a fresh terminal | — |
| D9 | Fri 23 Oct | P8 report, slides, viva kit, backup video | Two timed rehearsals | **D** |
| D10 | Sat 24 Oct | Buffer only — fixes from rehearsal | Final rehearsal → **submit** | — |

---

## 4. Phases

### P0 · Foundations — D1

Goal: a reproducible workstation and a running Kafka + Spark cluster on this laptop.

- [x] **P0.1 Repository** — `git init`; `.gitignore` (data/, models, checkpoints, `.venv`, `node_modules`, `target`,
  `.bsp`, `.idea`, `.env*`); `.gitattributes` (LF for `*.sh`, CRLF for `*.ps1`); `.editorconfig`; README stub; folder
  skeleton from CLAUDE.md.
- [x] **P0.2 Toolchain (user-level only)** — Temurin JDK 17.0.20.1 zip → `%USERPROFILE%\.jdks\temurin-17` (IntelliJ
  picks it up); sbt 1.12.15 zip → `%USERPROFILE%\.local\sbt-1.12.15`; `scripts/env.ps1` sets `JAVA_HOME`,
  `SPARK_HOME`, `HADOOP_HOME`, `PATH`, `PYSPARK_PYTHON` for the current session only.
- [x] **P0.3 Scala project** — `build.sbt` (Spark 3.5.8 `Provided`; `spark-sql-kafka-0-10` bundled; Typesafe Config;
  scopt; ScalaTest); sbt-assembly with a merge strategy; scalafmt; `SparkJob` base trait; `AppConfig`; `SmokeJob` that
  writes and re-reads Parquet on Windows (proves winutils + JDK 17).
- [x] **P0.4 Python project** — `uv init` on Python 3.11; deps (pyspark 3.5.8, confluent-kafka, httpx, tenacity,
  pyarrow, pandas, matplotlib, seaborn, jupyterlab, fastapi, uvicorn, sse-starlette, typer, rich); ruff; package
  skeleton; PySpark reads the Parquet written in P0.3.
- [x] **P0.5 Infrastructure** — `infra/docker-compose.yml` with profiles `kafka` (3 KRaft nodes + kafka-ui), `spark`
  (master + 2 workers) and `history`; memory caps from ARCHITECTURE §3; healthchecks; shared `./data` mount;
  `infra/kafka/create-topics.sh`. About 1.7 GB of images to pull.
- [x] **P0.6 Smoke tests** — SparkPi on `spark://spark-master:7077` with both workers in the UI; produce/consume
  round-trip through all three brokers; Kafka UI shows 3 brokers and the controller quorum.
- [x] **P0.7 `scripts/doctor.ps1`** — one green/red checklist for everything above.

**Acceptance:** doctor all green; `docker stats` ≤ 7 GB with the `kafka` and `spark` profiles up (budget later fixed at
the 6 GB VM, [ADR 009](adr/009-docker-vm-6gb-budget.md)).
**Evidence (15 Oct 2026):** `doctor.ps1` all required checks green; Docker uses 2.11 GiB of the 5.8 GB VM with 7
containers up. JDK and sbt zips were SHA-256 verified. Fat jar 6.1 MB (66 MB before excluding the Hadoop and codec
jars Spark already ships). Tests: Scala 4/4, Python 6/6. `SmokeJob` (1M-row Parquet round trip with no `xxhash64`
collisions, GraphX PageRank, MLlib regression slope 3.0000) succeeded on host `local[20]` in 40.4 s and on
`spark://spark-master:7077` in 28.9 s. PySpark 3.5.8 read the cluster-written Parquet (1,000,000 rows, Arrow
`toPandas`). SparkPi on 2 workers / 4 cores: 9 s. Kafka: 3 KRaft voters (leader node 2, follower lag 0), 7 topics
with RF 3 and min ISR 2, 2 partition leaders per broker, host round trip 300/300 messages with `acks=all`.
🎓 Driver, cluster manager, executors, tasks · local vs standalone vs YARN · broker, topic, partition, replica, ISR,
controller · why KRaft replaced ZooKeeper.

### P1 · Data acquisition, bronze & silver — D1–D2

Goal: trusted, typed trips with a measurable data-quality report.

- [x] **P1.1 Trip downloader** — `velograph.ingest.download_trips`: resumable HTTP Range downloads, size/ETag check,
  unzip (skip `__MACOSX`), `data/raw/citibike/manifest.json`. Order: 2026-09 first (dev slice), then 2026-06..08 in the
  background (≈ 4 GB total). 20,606,696 rows in 22 CSV parts (3.8 GB) in 30 min; S3 dropped the June transfer at
  226 MB once and the downloader resumed from that byte.
- [x] **P1.2 Weather + calendar** — Open-Meteo hourly archive for Central Park (40.7812, −73.9665), 1 Jun → yesterday,
  `timezone=America/New_York` (`velograph.ingest.download_weather`): 3,240 hours, 1 Jun – 13 Oct, no gaps.
  `SilverCalendarJob` builds `silver/calendar` (214 days, Jun–Dec) from the US federal holidays bundled in the jar,
  flagging both 3 Jul (observed) and 4 Jul.
- [x] **P1.3 Station information snapshot** — GBFS `station_information` (capacity, coordinates, `short_name` ↔ UUID).
  Captured once per NYC day by the recorder (P1.4): 2,519 stations on 14 Oct 2026.
- [x] **P1.4 GBFS recorder** *(approved 15 Oct; runs in the background while the laptop is awake)* — polls every 60 s
  (the feed's ttl), writes changed stations only plus periodic snapshots, gzipped JSONL per NYC day, and Open-Meteo
  current weather every 15 min. Feeds the offline demo and the live backtest (P7.4). Control: `scripts/recorder.ps1`.
  First 24 min: 1 snapshot (2,519 stations, 68 KB) + 11 deltas averaging 461 changed stations, ≈ 12 MB/day.
- [x] **P1.5 `BronzeIngestJob`** — explicit all-text schema with `_corrupt_record`, `_source_file`, `_ingested_at`,
  partitioned by `ym`; reconciles its per-month row counts with the downloader's manifest and fails on any difference.
- [x] **P1.6 `SilverTripsJob`** — typing, duration/distance/speed, 15 DQ rules with reason codes → quarantine, dedupe
  on `ride_id`, station-id normalisation → `serving/dq-report.json`. Reason counts come from exact aggregations;
  accumulators are shown where Spark guarantees them (inside an action, P2.3).
- [x] **P1.7 `SilverStationsJob` + `SilverWeatherJob`** — station dimension (median coordinates, first/last seen, GBFS
  capacity/UUID join) and the hourly weather table.

**Acceptance:** Sep slice bronze → silver < 10 min on host; quarantine < 2 %; ≥ 95 % of trip station ids resolve to
GBFS; Jun–Sep fully processed.
**Evidence (15 Oct 2026):** Sep slice bronze 30 s + silver 80 s on host `local[20]`. Full run: bronze 20,606,696 rows
(Jun 5,384,468 · Jul 4,993,137 · Aug 5,246,236 · Sep 4,982,855; 0 malformed lines; reconciled with the manifest) in
69 s; silver 20,528,598 valid trips, **0.379 % quarantined** (missing end coordinates 49,559 · longer than 4 h 16,590 ·
e-bike without start GPS 11,945 · impossible speed 4) in 3 min 10 s. Station ids: 100 ids lost their trailing zero
in 378,743 trip endpoints (`5980.1` = `5980.10`), and GBFS itself stores 3 that way; normalising both sides merged
97 duplicate stations (2,587 → 2,490) and raised the GBFS endpoint match from 98.88 % to **99.59 %** (2,463 of 2,490
stations; the other 27 have closed). Valid trips: 72.8 % e-bike, 79.7 % member, 2.2 % round trips, median ride
9.5 min / 1.56 km. Tests: Scala 15/15, Python 7/7.
🎓 Parquet (columnar, compression, predicate pushdown) · partitioning & pruning · schema-on-read · lazy evaluation ·
accumulators (and why they are only trustworthy inside actions).

### P2 · Gold layer, RDD fundamentals, Spark SQL — D2 → Gate A

- [x] **P2.1 `GoldStationHourJob`** — zero-filled station × hour panel (≈ 6–7M rows) with weather + calendar; each
  station only over its active span (first to last day with a trip), totals reconciled with the trips.
- [x] **P2.2 `GoldRoutesJob`** — directed route edges with AM-peak / PM-peak / weekend splits, median duration, member
  and e-bike share.
- [x] **P2.3 `RddFundamentalsJob`** → `rdd-fundamentals.json`: `textFile`, `map` / `filter` / `flatMap`, `reduceByKey`
  vs `groupByKey` (shuffle bytes), `join`, `persist` levels with timings, lineage via `toDebugString`, broadcast lookup,
  accumulators, RDD → DataFrame three ways (`toDF`, `createDataFrame(rdd, schema)`, case-class reflection). Shuffle
  bytes come from task metrics through a `SparkListener` (`ShuffleMeter`) and match the Spark UI to the byte.
- [x] **P2.4 `InsightsJob`** — 14 named `.sql` files → `serving/insights/<id>.json` (rows + SQL text + chart hint):
  1. hourly demand, weekday vs weekend · 2. member vs casual (count, median duration via `percentile_approx`, e-bike
  share) · 3. top-20 departure stations · 4. top-20 routes with names (self-join) · 5. round-trip share by station ·
  6. rain impact (join with weather) · 7. temperature bands · 8. weekly e-bike share (window) · 9. station imbalance
  ranking · 10. wildcard search (`LIKE '%Park%'`, `LIKE 'W % St'`, `RLIKE`) · 11. duration histogram & outliers ·
  12. busiest hour per station (`RANK() OVER`) · 13. week-over-week growth (`LAG`) · 14. e-bike vs classic speed.
- [x] **P2.5 Spark UI evidence + spark-shell walkthrough** — DAG with shuffle boundaries and Storage tab with a cached
  RDD → `docs/assets/`; `docs/viva/spark-shell-walkthrough.md` you can type live. Seven history-server screenshots in
  `docs/assets/spark-ui/`, captured by `velograph.ops.ui_screenshots` (headless Edge via Playwright); the walkthrough
  was rehearsed end to end in `spark-shell`.

**Gate A:** all gold tables + 14 insight exports for Jun–Sep; every query < 60 s.
**Evidence (15 Oct 2026):** **Gate A met.** `gold/station_hour`: 6,879,984 rows (2,490 stations, 2,928 hours), 72 s;
20,528,041 departures and 20,528,595 arrivals reconciled exactly with the trips; 44.7 % of station-hours have no
departure; busiest hour 9 Ave & W 33 St, 29 Sep 17:00, 190 departures. `gold/route_edges`: 1,308,071 directed edges
(216,418 with ≥ 20 trips, 2,403 self-loops), 62 s; the top 1 % of routes carry 22 % of trips. RDD job on Sep 2026
(30 partitions, 4,979,938 trips): accumulator 4,982,860 inside an action vs 9,965,720 inside a re-run `map`;
`groupByKey` shuffled 4,979,938 records / 12.5 MB vs `reduceByKey` 69,390 / 0.73 MB (17× less); shuffle join 99.1 MB
vs broadcast 0 B; re-count 2,055 ms uncached vs 86 ms `MEMORY_ONLY` (1,332 MB) vs 491 ms `MEMORY_ONLY_SER` (397 MB)
vs 610 ms `DISK_ONLY`. Insights: all 14 in 77 s total, slowest 11.8 s; e.g. heavy rain cuts daytime demand by 69 %.
Tests: Scala 28/28 (all 14 queries run against a fixture lake), Python 7/7.
🎓 Narrow vs wide transformations · jobs, stages, tasks · Catalyst & Tungsten · cache vs persist · broadcast joins ·
RDD vs DataFrame vs Dataset.

### P3 · GraphX network intelligence — D3

- [x] **P3.1 Graph construction** — stations → `VertexRDD` (`VertexId` = `xxhash64(station_id)`, uniqueness
  asserted), routes → `EdgeRDD`, `EdgePartition2D`, Kryo; graph stats (V, E, density, degree distribution).
- [x] **P3.2 PageRank** — weighted PageRank written with the Pregel API + built-in `pageRank`; Spearman ρ between them.
- [x] **P3.3 Morning vs evening** — weighted PageRank on weekday 07–10 and 16–19 subgraphs → `pr_shift`.
- [x] **P3.4 Flow strength & imbalance** — `aggregateMessages`, `outerJoinVertices`.
- [x] **P3.5 Communities** — weighted label propagation (Pregel, deterministic ties) on the strong-edge subgraph +
  built-in `LabelPropagation` baseline; modularity Q for both. Added: a greedy modularity merge
  (Clauset–Newman–Moore) on top, because label propagation alone scored below the baseline.
- [x] **P3.6 Structure** — connected & strongly connected components; triangle count → local clustering coefficient.
- [x] **P3.7 Paths** — `ShortestPaths` hops to the top-10 hubs; Pregel SSSP weighted by median ride time from 20
  landmarks; personalized PageRank for landmarks.
- [x] **P3.8 Outputs + tests** — `lake/graph/station_metrics`, `network-*.json`; ScalaTest on toy graphs with known
  answers. Also: the GraphX operator tour (G10), a superstep-by-superstep Pregel explainer for the `pregel` portal,
  a Spark UI screenshot of the supersteps and a rehearsed GraphX spark-shell walkthrough.

**Acceptance:** top hubs are recognisable transit/office hubs; communities are spatially coherent; morning sinks
concentrate in Midtown/FiDi; tests green.

**Evidence (15 Oct 2026):** **acceptance met.** `GraphMetricsJob` runs in 6.0 min on host `local[20]`.

- **Graph:** 2,490 stations and 1,305,668 routes plus 2,403 round-trip self-loops (20.5M trips). Density 0.21,
  reciprocity 74 %, 78 dangling stations.
- **Vertex cut** (16 partitions): EdgePartition2D needs 6.9 copies per vertex (bound 7) against 15.6 for random,
  canonical-random and 1D cuts; edge imbalance 1.13.
- **Weighted PageRank** (Pregel, tol 1e-4): converged in 49 supersteps (40 s); all 2,490 vertices active until the
  last few supersteps (2,401, then 2,076). Spearman ρ against the built-in = 0.91, top-20 overlap 10/20.
- **Top hubs:** Bedford Ave/Metropolitan (Williamsburg), Chelsea Piers, W 21 St & 6 Ave, Atlantic Terminal (Hanson
  Pl), Columbus Circle, Union Square, Penn Station (9 Ave & W 33 St, W 31 St & 7 Ave).
- **AM vs PM:** 84 % of the 50 highest AM-PageRank stations are in Midtown or Lower Manhattan, against 10 % of the 50
  highest PM ones.
- **Communities:**

  | Method | Communities | Q |
  |---|---|---|
  | Weighted label propagation | 59 | 0.702 |
  | Built-in `LabelPropagation` | 36 | 0.734 |
  | Label propagation + 55 modularity merges | **15** | **0.788** |

  The final 15 are recognisable districts, and Governors Island is its own community. They cover 93 % of stations.
  Median distance to the community centre is 1.7 km, against 6.6 km with shuffled labels.
- **Components:** one weakly connected component. Strongly connected: a giant component of 2,412 stations plus 78
  arrival-only stations, 77 of them Jersey City or Hoboken (their departures are in the separate JC dataset).
- **Triangles:** 26,995 on the strong graph; average clustering 0.31 against 0.006 for a random graph of the same
  density.
- **Paths:** hops to the 10 hubs: 1 → 481, 2 → 525, 3 → 675 stations, 179 unreachable; travel-time SSSP from 20
  landmarks in 6.5 s (median: 270 stations within 15 min); personalized PageRank for all 20 landmarks in one Pregel
  run (27 s).
- **Operator tour:** 13 operators, every claim asserted, e.g. `groupEdges` merges 2,206,763 parallel edges back into
  1,305,668.
- **Explainer:** 46 supersteps recorded; the driver replay equals GraphX's final state.
- **Event log:** 22 MB with block-update logging off and zstd compression (it was 1.1 GB).
- **Tests:** Scala 45/45, including 17 new GraphX tests (toy graphs + a synthetic city); Python 7/7.

🎓 Property graph · vertex-cut partitioning · Pregel supersteps (vprog / sendMsg / mergeMsg) · PageRank & damping · why
weights change rankings · LPA & modularity · triangles & clustering coefficient.

### P4 · Machine learning — D4

- [ ] **P4.1 `StationArchetypesJob`** — 96-dim hourly share profiles (weekday/weekend × departures/arrivals) → KMeans
  k = 3..10 by silhouette → named archetypes; PCA 2-D for display.
- [ ] **P4.2 `FeatureBuildJob`** — leakage-safe windows (ARCHITECTURE §8.2), trailing hour-of-week profiles, weather,
  calendar, station + graph + archetype features → `gold/ml_features`; assertions that every profile window ends
  before its fit/test window starts.
- [ ] **P4.3 Lab 01 · EDA (PySpark)** — distributions, demand curves, Pearson + Spearman heat map.
- [ ] **P4.4 Lab 02 · Regression** — global-mean and seasonal-naive baselines; LinearRegression (elastic net),
  DecisionTree, RandomForest, GBT; tuning on the last fit week; R², RMSE, MAE on Sep; residuals; error by hour and
  archetype.
- [ ] **P4.5 Lab 03 · Classification** — regime threshold τ chosen from the data; LogisticRegression (multinomial),
  DecisionTree, RandomForest, Gaussian NaiveBayes, LinearSVC via OneVsRest; class weights; confusion matrices,
  accuracy, macro-F1, per-class precision/recall.
- [ ] **P4.6 Lab 04 · Ablation** — champion with/without graph features and with/without profiles.
- [ ] **P4.7 `TrainChampionJob` (Scala)** — retrain the chosen configs, save `PipelineModel`s + model cards; assert
  test metrics match the lab within ±1 %.

**Acceptance:** champion beats seasonal-naive MAE on Sep (target ≥ 10 %; otherwise iterate at most ½ day, then report
honestly); confusion matrix + macro-F1 exported; ablation quantified.
**Evidence:** _pending_
🎓 Regression vs classification vs clustering · R², RMSE, MAE · reading a confusion matrix; precision vs recall ·
overfitting and time-based validation · DT vs RF vs GBT · silhouette · StringIndexer, OneHotEncoder, VectorAssembler,
StandardScaler, Pipeline.

### P5 · Kafka + Structured Streaming — D5 → Gate B

- [ ] **P5.1 Topics** — bootstrap script: partitions, RF 3, `min.insync.replicas=2`, retention, compaction for weather.
- [ ] **P5.2 `gbfs_producer`** — TTL-aware polling, CDC, idempotent producer (`acks=all`), weather publishing, DLQ,
  `--source recorded` replay.
- [ ] **P5.3 `LiveStationScoringJob`** — ARCHITECTURE §9.4: watermark + `dropDuplicatesWithinWatermark`, broadcast
  enrichment, champion models, projected availability, risk, regime, rebalancing moves; Kafka + Parquet + atomic
  snapshot sinks; checkpointing.
- [ ] **P5.4 Replay analytics** — `trip_replayer` + `TripReplayAnalyticsJob` (tumbling & sliding windows, watermarks,
  late-data metrics, update vs append).
- [ ] **P5.5 Gateway** — FastAPI: `/api/health`, `/api/live/snapshot`, `/api/live/stream` (SSE, consumer group),
  `/api/cluster`.
- [ ] **P5.6 Resilience drills** — stop a broker mid-stream; kill and restart the streaming job (resumes from its
  checkpoint). Record outcomes and screenshots.

**Gate B:** live GBFS → snapshot p95 < 90 s; 30-min run without errors; both drills pass; the snapshot carries risks
and suggested moves.
**Evidence:** _pending_
🎓 Producer/consumer model · key → partition → ordering · consumer groups & offsets · acks, ISR, idempotence ·
micro-batches & triggers · event vs processing time · watermarks · output modes · checkpoints & exactly-once.

### P6 · Web experience — D6–D7 → Gate C

Design: "Inkglass" with one evidence portal per module ([ADR 011](adr/011-inkglass-design-and-portals.md)).
Maximalist canvas, glass portals, neumorphic (clay) controls, hand-drawn ink annotations; data marks stay exact.

- [x] **P6.1 Foundation** — Next.js 16 (App Router, TS strict), Tailwind 4, Radix primitives (instead of shadcn/ui
  copies), motion, roughjs, rough-notation, perfect-freehand; Inkglass tokens for the canvas, glass, clay and ink
  layers (palette validated with the `dataviz` skill) and six colour presets; fonts via `next/font`; zod contracts for
  every serving file; `pnpm sync`. The ⌘K station search moves to P6.4 with the home page.
- [x] **P6.2 Portal system** — `@portal` parallel + intercepting routes (`/portal/<id>`); the portal layout (header
  chips + source, hero, acts, "How Spark did it" code panel, "Say it in the viva" sticky note, numbers ledger); ink
  kit (sketch frames, notations, freehand arrows, sticky notes, stickers, tape); clay kit (toggle, segmented control,
  slider/knob, stepper, KPI puck).
- [x] **P6.3 Portals for P1–P3** — `lake` (medallion + DQ funnel), `rdd-lab` (shuffle duel, persistence race,
  accumulator trap, lineage sketch, RDD → DataFrame), `sql` (14 insights in one gallery with View SQL and deep links),
  `spark-ui` (the history server's charts redrawn from its REST API, plus pinned screenshots), `pregel` (superstep
  step-through), `network` (PageRank map, AM ↔ PM morph, communities, travel times, "where riders go"), `graph-ops`
  (operator cards, vertex-cut comparison).
- [ ] **P6.4 `/` Home** — night-time trip replay hero (deck.gl TripsLayer on one real evening), KPI band, scroll story
  of the pipeline whose chapters open the portals, ⌘K station search. _So far:_ two-column hero with the city of
  lights, KPI pucks, fact ticker, portal bento grid with the district sketch.
- [ ] **P6.5 `/live` + `streaming` portal** — risk map with pulsing halos, alert ticker (SSE), KPIs, station drawer
  (status, forecast, archetype, community, PageRank), suggested moves as arcs, live/replay toggle.
- [ ] **P6.6 `/models` + `models` portal** — leaderboards vs baselines, confusion matrix, predicted vs actual,
  residuals, importances, ablation, archetype profiles + PCA, heat map.
- [ ] **P6.7 `/cluster` + `/syllabus`** — live topology (brokers, partitions, leaders, ISR; Spark master, workers,
  apps), job timeline; traceability table in which every row opens its portal.
- [ ] **P6.8 Polish** — ≥ 1280 px primary + tablet, WCAG AA contrast, reduced motion and reduced transparency,
  loading/empty/error/offline states, Lighthouse ≥ 90 on non-map pages.

**Gate C:** every page and portal renders real exports; the site works with the gateway down; the production build
passes.
**Evidence (P6.1–P6.3, 16 Oct):**
- **Portals:** 7, all reading real exports.
- **Build and checks:** `pnpm build` passes (19 prerendered pages); `pnpm lint` reports 0 problems; `pnpm
  typecheck` passes.
- **Spark UI export:** `spark-ui.json` holds 10 applications, 34 RDD jobs, a 47-node SQL plan with 5 exchanges and
  50 Pregel superstep jobs. Its shuffle bytes match `rdd-fundamentals.json` to the byte.
- **Scrolling** (`velograph.ops.scroll_bench`, ADR 012):
  - the mean frame fell from 25.5–29.9 ms to 6.2–7.5 ms;
  - frames slower than 33 ms fell from 13–36 % to ≤ 0.9 %;
  - live backdrop blurs in the RDD lab fell from 21 to 3.

  Measured on the home page and the `rdd-lab`, `sql` and `network` overlays.

### P7 · Hardening & automation — D8

- [ ] **P7.1 One-command flows** — `pipeline.ps1` (all batch stages with timings), `demo.ps1` (cluster, topics,
  producer, streaming, gateway, web; prints URLs), `sync-web-data.ps1`.
- [ ] **P7.2 Tests green** — Scala, Python, export contracts, web typecheck + build.
- [ ] **P7.3 Docs** — README (quick start, architecture, screenshots) + `docs/RUNBOOK.md` (OOM, ports, winutils, Docker,
  Kafka).
- [ ] **P7.4 Live backtest** — projected vs actual availability from the recordings → `live-backtest.json` (MAE in
  bikes, hit rate of empty-risk alerts).
- [ ] **P7.5 Evidence pack** — screenshots (UI, Spark UI, history server, Kafka UI) + benchmark table (local vs
  cluster, cache effects).

**Acceptance:** from a fresh terminal, `demo.ps1` reaches a working Live page in < 5 min.
**Evidence:** _pending_

### P8 · Report, slides, viva — D9 → Gate D

- [ ] **P8.1 Report** (25–30 pp): abstract · introduction & objectives · background (Spark vs Hadoop/MapReduce,
  bike-share rebalancing) · dataset · architecture · methodology (ETL, SQL, GraphX, MLlib, streaming) · results ·
  syllabus/CO mapping · challenges & lessons · conclusion & future work · references · appendix (runbook,
  screenshots). Tool and format agreed with you at the start of P8.
- [ ] **P8.2 Slides** (12–15) aligned with the demo script.
- [ ] **P8.3 Viva kit** — ≥ 80 Q&A (Units I–VI + design "why" questions), one-page cheat sheet, `scala-tour.md` (each
  Unit II concept → a real line of our code).
- [ ] **P8.4 Backup demo video** (3–4 min screen recording) in case the live demo fails.
- [ ] **P8.5 Rehearsals** — two timed runs; viva-day checklist.

**Gate D:** you can run the demo end-to-end alone and answer 20 random questions from the bank.
**Evidence:** _pending_

---

## 5. Stretch goals (only after Gate C)

| ID | Idea | Value |
|---|---|---|
| S1 | HDFS profile (namenode + datanode) holding the gold layer; Spark reads `hdfs://` | Hadoop ecosystem integration (Unit I) |
| S2 | SQL playground: read-only Spark SQL from the browser (FastAPI + PySpark, LIMIT 500, 20 s timeout) | live Unit IV demo |
| S3 | Shortage-diffusion Pregel: simulate how an empty station pushes demand to its neighbours | deeper CO6 |
| S4 | MapReduce vs Spark benchmark (Hadoop Streaming vs RDD vs DataFrame on one aggregation) | CO1 evidence |
| S5 | Static deployment of the website (e.g. Vercel) — only with your approval | shareable link in the report |
| S6 | Delta Lake sink with time travel | lakehouse talking point |
| S7 | 3-D force-directed community graph view | visual wow |

## 6. Cut list — if we fall behind

Cut in this order, telling you each time: S1–S7 → `/syllabus` merges into `/cluster` → the 14 SQL sub-portals merge
into one gallery → `TripReplayAnalyticsJob` (keep
the live path) → AM ↔ PM morph animation (keep static maps) → hyper-parameter grids (keep defaults + one tuned model)
→ isochrones.

**Never cut:** the live scoring path · GraphX PageRank, imbalance, communities, components · regression and
classification leaderboards with baselines and confusion matrix · report, slides, viva kit.

## 7. Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R1 | 16 GB RAM → OOM / swapping | Medium | High | ARCHITECTURE §3 budget, heap caps, heavy ETL with containers stopped, demo checklist (close IntelliJ) |
| R2 | Spark-on-Windows quirks (winutils, JDK 21, paths) | Medium | Medium | JDK 17; winutils already present; no spaces in paths; the cluster runs in Linux containers |
| R3 | Docker Desktop / WSL2 won't start | Low | High | Kafka natively inside WSL Ubuntu; Spark local mode |
| R4 | No internet or GBFS down at the viva | Medium | High | recorded replay, static website, backup video |
| R5 | Slow download (≈ 4 GB) | Medium | Medium | Sep slice first; resumable background downloads |
| R6 | Weak model metrics | Medium | Medium | baselines, ½-day feature iteration budget, honest framing |
| R7 | Kafka 4.x client incompatibility | Low | Medium | one-line switch to `apache/kafka:3.9.2` |
| R8 | Ten-day deadline | Medium | High | gates, cut list, backend vertical slice by D5 |
| R9 | Viva questions on parts you didn't write | Medium | High | 🎓 checkpoints per phase, Q&A bank, two rehearsals |
| R10 | System Python 3.14 breaks PySpark | High if ignored | Medium | uv venv on Python 3.11 |

## 8. Demo script (draft; finalised in P8)

| Time | Beat | Screen |
|---|---|---|
| 0:00 | Hook with a real number from our data (e.g. share of stations draining in the 8–9 am weekday peak) | Home hero |
| 0:40 | Architecture in one breath | Home pipeline story / slide |
| 1:20 | The cluster is real: Spark master + 2 workers, 3 Kafka brokers | `/cluster`, Spark UI, Kafka UI |
| 2:10 | Live right now: at-risk stations, alerts streaming, suggested moves | `/live` |
| 3:20 | Why it happens: hubs, AM ↔ PM shift, communities | `/network` |
| 4:20 | Proof: leaderboard vs baseline, confusion matrix, GraphX ablation | `/models` |
| 5:20 | Spark SQL with a wildcard query | `/insights` |
| 5:50 | Resilience: stop broker 2 → the stream keeps flowing | terminal + `/live` |
| 6:30 | Impact and future work | slide |

## 9. Submission checklist — Sat 24 Oct

- [ ] Source code (Git repo or zip, without `data/`) with README run instructions
- [ ] Report (PDF)
- [ ] Slides
- [ ] Backup demo video
- [ ] Final rehearsal done
