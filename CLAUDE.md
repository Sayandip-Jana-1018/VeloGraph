# CLAUDE.md — VeloGraph

**VeloGraph** — real-time bike-share intelligence on Apache Spark.
INT315 Cluster Computing · CA project (30 marks) · **individual** · **due Sat 24 Oct 2026**.

Start every session by reading this file, then [docs/ROADMAP.md](docs/ROADMAP.md) (what to do next) and the relevant
sections of [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (how it must be built). Continue with the first unchecked task
of the current phase.

## Mission

Forecast next-hour departures and arrivals for each of NYC's ~2,500 Citi Bike stations, fuse them with the live GBFS
feed to flag empty/full risk and suggest rebalancing moves, and explain the network with GraphX.

Graded on **CO4 (MLlib), CO5 (Spark workflows), CO6 (GraphX)** at Bloom L6 "Create":
implementation 15 · report 5 · presentation + viva 10. The student defends every module alone, so being able to
explain each piece simply is a first-class requirement, not an afterthought.

## Non-negotiables

1. **Real data, honestly labelled.** Trips Jun–Sep 2026 + live GBFS + Open-Meteo. Anything replayed is labelled
   "replay" in the UI and report. Never invent numbers: UI and report read metrics from `data/serving/*.json`
   written by jobs.
2. **GraphX in Scala.** CO6 work uses `org.apache.spark.graphx` — no networkx/GraphFrames substitutes.
3. **Leakage-safe ML.** Time-based windows (ARCHITECTURE §8.2), trailing-window profiles, a baseline next to every
   model; the test month (Sep 2026) is never used for tuning.
4. **Offline-safe demo.** Every live feature has a recorded-replay fallback; the website renders fully from static JSON.
5. **16 GB machine.** Docker stack ≤ ~7 GB (ARCHITECTURE §3). Heavy batch runs on host `local[*]` with the Spark
   containers stopped.
6. **No global machine changes** (PATH, JAVA_HOME, registry, `.wslconfig`, Docker settings) without asking.
   `scripts/env.ps1` sets everything for the current session only.
7. **Ask before outward-facing or long-lived actions:** deployments, publishing, account sign-ups, new remotes,
   background processes that keep running. Say what and why. Standing approvals (from the user, 15 Oct 2026):
   push every commit on `main` to `origin` = github.com/Sayandip-Jana-1018/VeloGraph; keep the GBFS recorder
   running. Never force-push.
8. **Never commit** `data/`, models, checkpoints, `.env*`, secrets or large binaries.

## Stack (pinned — change only via an ADR)

| Area | Version | Note |
|---|---|---|
| JDK | Temurin 17.0.20.1 | Spark 3.5 supports Java 8/11/17; the host's default JDK 21 is not used for Spark |
| Spark | 3.5.8, Scala 2.12.18 | host `C:\spark`; Docker `apache/spark:3.5.8-scala2.12-java17-python3-ubuntu` |
| Build | sbt 1.12.15, sbt-assembly 2.5.0, scalafmt | sbt 2.0 avoided until plugins/IDE catch up |
| Kafka | `apache/kafka:4.3.1`, KRaft, 3 nodes | fallback `3.9.2`; UI `ghcr.io/kafbat/kafka-ui:v1.5.0` |
| Python | 3.11 via uv 0.12 · pyspark 3.5.8 · confluent-kafka 2.16 · FastAPI 0.143 | system Python 3.14 is too new for PySpark 3.5 |
| Web | Next.js 16.4 · React 19.3 · Tailwind 4.3 · shadcn/ui · motion 14 · deck.gl 9.4 · MapLibre 6.14 · react-map-gl 8.1 · ECharts 6.1 · zod | Node 24, pnpm 9 |
| Hadoop | winutils + hadoop.dll from `C:\hadoopsetup\hadoop-3.2.4` | only for Spark-on-Windows file IO |

## Repository map (target)

```
spark/     Scala sbt project — package `velograph` (common, etl, sql, graph, ml, streaming, export)
python/    uv project — package `velograph` (ingest, api) + lab/ PySpark notebooks
web/       Next.js app
infra/     docker-compose.yml (kafka x3, kafka-ui, spark master/workers/history) + topic bootstrap
conf/      dev.conf, cluster.conf (overrides for application.conf)
scripts/   env.ps1, doctor.ps1, pipeline.ps1, demo.ps1, sync-web-data.ps1, validate_exports.py
docs/      ROADMAP, ARCHITECTURE, adr/, viva/, report/, presentation/, assets/
data/      (git-ignored) raw, lake, models, stream, checkpoints, serving, spark-events
```

## Commands

✅ = verified working; the rest are planned.

| Purpose | Command |
|---|---|
| ✅ Session environment | `. .\scripts\env.ps1` |
| ✅ Health check | `.\scripts\doctor.ps1` (`-SkipCluster`, `-Quick`) |
| ✅ Build fat jar | `cd spark; sbt --batch assembly` → `spark/target/scala-2.12/velograph-spark.jar` (6.1 MB) |
| ✅ Unit tests | `cd spark; sbt --batch test` · `cd python; uv run pytest` |
| ✅ Run a Spark job | `.\scripts\submit.ps1 <job>` (host `local[20]`) · `-Mode cluster` (Docker) |
| ✅ Cluster up / down | `docker compose -f infra/docker-compose.yml --profile kafka --profile spark up -d` / `down` |
| ✅ Trip download | `cd python; uv run python -m velograph.ingest.download_trips 202609 202606 202607 202608` |
| ✅ Weather download | `cd python; uv run python -m velograph.ingest.download_weather --start 2026-06-01` |
| ✅ Bronze + silver | `.\scripts\submit.ps1 bronze-trips` (`-- --months 202609`) · `silver-trips -DriverMemory 6g` · `silver-stations` · `silver-weather` · `silver-calendar` |
| ✅ Gold, RDD, SQL | `.\scripts\submit.ps1 gold-station-hour -DriverMemory 6g` · `gold-routes` · `rdd-fundamentals` · `insights` |
| ✅ GraphX suite | `.\scripts\submit.ps1 graph-metrics -DriverMemory 6g` (~8 min) → `lake/graph/station_metrics` + `serving/network-*.json` |
| ✅ ML features | `.\scripts\submit.ps1 station-archetypes -DriverMemory 6g` (~3 min) · `feature-build -DriverMemory 7g` (~7 min) → `gold/station_archetypes`, `gold/ml_features` |
| ✅ ML lab | `cd python; uv run jupytext --to ipynb --execute lab/01_eda.py` (then `02_regression`, `03_classification`, `04_ablation`; one at a time: each takes an 8 GB driver) |
| ✅ Champion models | `.\scripts\submit.ps1 train-champion -DriverMemory 8g` (~3 min) → `data/models/*`, `serving/model-cards.json`; fails unless it reproduces the lab within 1 % |
| ✅ Model evaluation | `.\scripts\submit.ps1 evaluate-champions -DriverMemory 6g` (~30 s): reloads `data/models/*`, prints the September metrics beside their baselines → `serving/model-evaluation.json` |
| ✅ Live features | `.\scripts\submit.ps1 live-features -DriverMemory 6g` (~5 min) → `gold/live_stations`, `gold/live_profiles` (after `station-archetypes`) |
| ✅ GBFS → Kafka | `cd python; uv run python -m velograph.ingest.gbfs_producer` (live) · `--source recorded --day 2026-10-16 --start 07:30 --speed 20` (replay); stop with `data\run\gbfs_producer.stop` or Ctrl+C |
| ✅ Live scoring | `.\scripts\submit.ps1 live-station-scoring -Mode cluster -- --duration-min 30` (`--fresh` drops the checkpoint, `--source replay`); stop with `data\run\live-station-scoring.stop` |
| ✅ Trip replay | `.\scripts\submit.ps1 trip-replay-analytics -Mode cluster -- --duration-min 8 --fresh`, then `cd python; uv run python -m velograph.ingest.trip_replayer` once `serving/live/replay-analytics.json` exists |
| ✅ Gateway | `cd python; uv run uvicorn velograph.api.main:app --port 8000` → `/api/health`, `/api/live/snapshot`, `/api/live/stream` (SSE), `/api/cluster` |
| ✅ Cluster snapshot | `cd python; uv run python -m velograph.ops.cluster_snapshot` (best while a streaming job runs) → `serving/cluster.json` |
| ✅ Resilience drills | `cd python; uv run python -m velograph.ops.drills broker --broker 2 --down-s 120` · `... drills restart` (live scoring running) → `serving/live/drills.json` |
| ✅ History server | `docker compose -f infra/docker-compose.yml --profile history up -d spark-history` → http://localhost:18080 |
| ✅ Spark UI screenshots | `cd python; uv run python -m velograph.ops.ui_screenshots` (history server up) → `docs/assets/spark-ui/` |
| ✅ Live-feed recorder | `.\scripts\recorder.ps1 start` / `status` / `stop` |
| ✅ Spark UI export | `cd python; uv run python -m velograph.ops.spark_ui_export` (history server up) → `serving/spark-ui.json` |
| ✅ Hero replay | `.\scripts\submit.ps1 hero-trips` (~15 s) → `serving/hero-trips.json` (the evening in `velograph.hero`) |
| ✅ Live backtest | `.\scripts\submit.ps1 live-backtest` (after live runs, with the recorder on) → `serving/live-backtest.json` |
| ✅ Evidence screenshots | `cd python; uv run python -m velograph.ops.evidence_shots` (demo running) → `docs/assets/evidence/` |
| ✅ Demo video | `cd python; uv run python -m velograph.ops.demo_video` (demo running) → `submission/VeloGraph-demo.webm` |
| ✅ Web data sync | `cd web; pnpm sync` (serving JSON, screenshots, MapLibre worker, code snippets, the ⌘K station index → `public/`, `src/generated/`) |
| ✅ Web dev server | `cd web; pnpm dev` → http://localhost:3000 (stale page after an edit: stop it, delete `web/.next/dev`, restart) |
| ✅ Web checks | `cd web; pnpm typecheck` · `pnpm lint` · `pnpm build` |
| ✅ Scroll benchmark | `cd python; uv run python -m velograph.ops.scroll_bench` (web server up; ADR 012 frame budget) |
| ✅ Lighthouse | `cd web; pnpm build; pnpm start`, then `pnpm dlx lighthouse@13.5.0 http://localhost:3000/ --preset=desktop "--chrome-path=C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"` (the first runs after a reboot are noisy: repeat them) |
| ✅ Batch pipeline | `.\scripts\pipeline.ps1` (`-Stages graph,ml`, `-WithLabs`, `-KeepCluster`): 16 steps in ~51 min, containers stopped |
| ✅ Laptop vs cluster | `.\scripts\benchmark.ps1` (Spark containers up; after a laptop run of `rdd-fundamentals`) → `serving/benchmarks/` |
| ✅ Report | diagrams: `node docs/report/figures.mjs`, then `cd python; uv run python ../docs/report/render.py`; then `node docs/report/build.mjs` → `submission/VeloGraph-Report.docx` and `.\docs\report\export.ps1` (Word: fields, PDF); `uv run --with pymupdf python ../docs/report/layout.py` narrows figures that leave page gaps |
| ✅ Live demo | `.\scripts\demo.ps1` (live in ~75 s) · `-Replay -Day 2026-10-16 -Start 20:30` · `-Stop` (`-Down`) |
| Hosting | [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md): the website on Vercel (`cd web; pnpm sync; pnpm dlx vercel deploy --prod`), the live backend on a cloud VM; the gateway reads `VELOGRAPH_CORS_ORIGINS` |

Windows gotchas, learned the hard way:
- Quote `-D` flags in PowerShell (`sbt '-Dsbt.color=false'`); unquoted, PowerShell splits them at the dot.
- In Windows PowerShell 5.1, native stderr plus `$ErrorActionPreference='Stop'` aborts scripts on Spark's first
  log line; run native tools through `Invoke-Native` (see `scripts/submit.ps1`) and check `$LASTEXITCODE`.
- In Git Bash, prefix `docker exec` (or any command given a `/path` argument, such as a URL path) with
  `MSYS_NO_PATHCONV=1`, or `/opt/...` becomes `C:/Program Files/Git/opt/...`.
- Host Spark UIs use port 4050+; 4040–4042 are published by the Docker master for cluster drivers.
- `Set-Content -Encoding utf8` in PowerShell 5.1 writes a BOM, which the Scala REPL rejects as illegal characters;
  write files for tools with `[IO.File]::WriteAllText(path, text, (New-Object Text.UTF8Encoding $false))`.
- `spark-shell -i file` stays open after the file; end rehearsal copies with `sys.exit(0)`.
- `jupytext --sync` reads files in the Windows code page; run it with `PYTHONUTF8=1`.
- PySpark's `OneVsRestModel` scores with Python UDFs, and a Python worker can drop its socket on Windows
  ("Connection reset by peer"). Lab 03 writes one-vs-rest out with built-in column functions instead.
- The Windows clock runs 92 s slow (its time service is off) while the Docker VM's is right. Never compare a host
  timestamp with a container one: latency is measured on the cluster against the broker's append time, and the
  producer corrects itself from the feed's `Date` + `Age` headers. Waiting for a file written in a container by
  its mtime fails for the same reason: wait for the file to appear instead.
- Don't rebuild the fat jar while a cluster job runs: the driver and executors load classes from it lazily.
- Long heredocs break the Bash tool's parser; write patch scripts to the scratchpad and run them.
- A `threading.Thread` subclass must not define `_stop` or `_bootstrap` (they are Thread internals): the gateway
  consumer owns a thread instead of being one.

## Engineering conventions

**Scala / Spark**
- One `object XxxJob extends SparkJob` per job: parse args → session → pure transforms → write → job report
  (`data/serving/jobs/<job>.json`: timings, rows in/out, app id).
- Transforms are pure `DataFrame => DataFrame` (or typed `Dataset[T]`) functions in `*Transforms` objects, so they can
  be unit-tested with a local session.
- Explicit `StructType` on every read; no `inferSchema` in jobs.
- No `collect()` / `toPandas()` on unbounded data. Bounded driver-side work (≤ 10k rows) carries a comment stating
  its bound.
- Built-in functions over UDFs; `spark.sql.shuffle.partitions` set per job; partition by date; 128–256 MB files.
- Typesafe Config + `VELOGRAPH_*` env overrides; no hard-coded paths or dates (ML windows live in config).
- Idempotent writes (`overwrite` + dynamic partition overwrite); fixed seeds for every random operation.
- scalac `-deprecation -feature -unchecked -Xlint`, zero warnings; scalafmt before commit.
- Use Scala idioms deliberately (case classes, `Option`/`Try`, pattern matching, immutable collections,
  higher-order functions): they are Unit II material and the viva will probe them.
- Time: all trip times are New York wall-clock (`spark.sql.session.timeZone=America/New_York`), but the JVM
  default zone here is IST. Never turn a collected `java.sql.Timestamp` into text on the driver; format it in Spark
  (`date_format`) so the session zone applies. In tests, compare formatted strings, not `Timestamp.valueOf`.

**Python** — 3.11 venv via uv; ruff for lint + format; type hints; `logging`, not print; retries with exponential
backoff on HTTP; producers flush on SIGINT; notebooks re-run cleanly top to bottom.

**Web** — TypeScript strict; server components by default, client components only for maps, charts and live views;
design tokens as CSS variables (no ad-hoc hex in components); zod-validated data loading; every view has
loading/empty/error/offline states. Load the `dataviz` skill before writing chart code or choosing chart colours.
Design language "Inkglass" ([ADR 011](docs/adr/011-inkglass-design-and-portals.md)): maximalist canvas, glass
portals, neumorphic controls, hand-drawn annotations; data marks stay exact. **Everything we build is showcased in
the UI**: each module gets an evidence portal (`/portal/<id>`), so every job should write portal-ready exports.

**Naming** — Kafka topics `velograph.<domain>.<entity>.v<N>` with kebab-case segments, never `_` (Kafka metric
names treat `.` and `_` alike); lake tables `lake/{bronze,silver,gold,graph}/<table>`;
serving files `kebab-case.json` wrapped in the `meta` envelope (ARCHITECTURE §10).

**Code quality** — small cohesive modules, intention-revealing names, comments that explain *why*, no dead code, no
TODO without a roadmap ID.

**Git** — conventional commits (`feat(graph): weighted PageRank via Pregel`), one per completed task group; never
push without approval. No `Co-Authored-By` or other AI-attribution lines in commit messages or PR descriptions
(the user's decision, 18 Oct 2026; the history was rewritten once, with the user's approval, to remove them).

## Working a roadmap task

1. Re-read its acceptance criteria. Build the thinnest end-to-end version on the dev slice (Sep 2026), then scale to
   Jun–Sep.
2. Verify by running it. Record evidence (row counts, timings, metrics) on the task's **Evidence** line: numbers,
   not adjectives.
3. Tick the box, update **Status** below, and append 2–4 Q&A about what was just built to `docs/viva/qa-bank.md`.
4. A design change means a short ADR in `docs/adr/` plus an ARCHITECTURE update in the same commit.
5. Behind schedule: apply the cut list (ROADMAP §6) and tell the user. Never drop scope silently.
6. End each working day with a short note to the user: what was built, what to study (the phase's 🎓 list).

## Status

- **Phase:** P3 done 15 Oct 2026; P6.1–P6.3 done early on 16 Oct (evidence in ROADMAP). The web app has 7 evidence
  portals over real exports, six colour presets, a two-column hero with the city of lights, and the Spark UI's charts
  redrawn from the history server's REST API (`spark-ui.json`). The design was approved by the user on 16 Oct.
  Scrolling keeps the ADR 012 frame budget (frame cost 25–30 ms → 6–7.5 ms).
- **P4 done 16 Oct** (evidence in ROADMAP):
  - archetypes (k = 3);
  - leakage-safe `gold/ml_features`, with GraphX rerun per window (ADR 013);
  - Labs 01–04 in `python/lab/`;
  - champions: a random forest for departures, absolute-loss GBT for arrivals and a one-vs-rest linear-SVM regime
    classifier, which the Scala `TrainChampionJob` reproduces within its 1 % gate. It was exact at P4, and within
    0.6 % after the 17 Oct rerun rebuilt the feature table.

  The demand models beat the seasonal naive forecast by 6.6 % (departures) and 6.3 % (arrivals). The 10 % target is
  not met, and this is reported honestly. The `models` portal shows it all.
- **P5 done 17 Oct, Gate B met** (evidence in ROADMAP; [ADR 014](docs/adr/014-live-scoring-state-risk-and-latency.md)):
  - the GBFS producer (CDC, `acks=all`, idempotent, recorded replay);
  - `LiveFeaturesJob` (profile-l features);
  - `LiveStationScoringJob` on the Docker cluster (watermark, dedupe, three champions, six states, moves; Kafka,
    Parquet and snapshot sinks);
  - `TripReplayAnalyticsJob` (append and update modes, late data);
  - the FastAPI gateway (SSE, cluster topology);
  - both drills passed.

  Broker → snapshot p95 37.5 s over a 32-minute run. The `/live` page and the `streaming` portal show it all (P6.5).
- **P6.7 done 17 Oct:** the `cluster` and `syllabus` portals, at `/cluster` and `/syllabus` and in the nav. The
  site has 11 portals.
- **P6 done 17 Oct, Gate C met** (evidence in ROADMAP; [ADR 015](docs/adr/015-home-replay-pipeline-story-station-search.md)):
  - the home page replays one real evening on the city of lights and tells the pipeline as an 8-stop subway line;
  - the ⌘K station search sits in every page's header;
  - layouts are checked at 1280, 1024, 768 and 390 px, with a menu below 1024 px;
  - reduced motion hydrates cleanly, and there are branded 404 and error pages;
  - Lighthouse (desktop) scores performance 93–98 and accessibility 100 on the 10 pages without a WebGL map.
- **P7 done 18 Oct** (evidence in ROADMAP):
  - `demo.ps1` is live in 73 s; `pipeline.ps1` runs 16 steps in 50.9 min, and a rerun changes only timings;
  - tests: Scala 77, Python 30, contracts on 63 exports;
  - the live backtest: 89.8 % of EMPTY_RISK and 85.5 % of FULL_RISK calls came true, but the bike counts are 5.8 %
    worse than "no change";
  - 25 evidence screenshots, and the laptop-versus-cluster benchmark (act 7 of `rdd-lab`).
- **P8.1–P8.4 done 18 Oct** (evidence in ROADMAP):
  - the report: 32 pages (an overview page with the live and GitHub links right after the cover), Word + PDF, built by `docs/report/build.mjs` (with `layout.py` for full pages), in
    `submission/` (git-ignored);
  - the LPU logo, in the git-ignored `docs/assets/brand/`;
  - the 17-slide deck, a Slides artifact;
  - the demo video `submission/VeloGraph-demo.webm`;
  - the viva kit: Q&A Q1–Q95, `cheat-sheet.md`, `scala-tour.md`, and `demo-voiceover.md` (the video's 23
    captions with their times and a narration for each);
  - `evaluate-champions` and two console captures of the models.
- **Website live** at https://velograph-sj.vercel.app (Vercel, deployed from `web/` with the CLI; recorded data).
- **Next task:** P8.5 rehearsals (the user's), then submission.
  - The containers are stopped; `demo.ps1` starts them (and `benchmark.ps1` needs the Spark profile up).
  - The GBFS recorder keeps running. It records only while the laptop is awake, and the recordings have gaps where
    it slept; the offline demo needs one full evening.
  - The producer, gateway and scoring job are started on demand (commands above).
- **Open questions:** none. Option for the user: raising `.wslconfig` `memory=` to 8 GB would add cluster headroom
  (ADR 009); not needed so far.
