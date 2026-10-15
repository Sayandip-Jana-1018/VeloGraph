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
| ✅ History server | `docker compose -f infra/docker-compose.yml --profile history up -d spark-history` → http://localhost:18080 |
| ✅ Spark UI screenshots | `cd python; uv run python -m velograph.ops.ui_screenshots` (history server up) → `docs/assets/spark-ui/` |
| ✅ Live-feed recorder | `.\scripts\recorder.ps1 start` / `status` / `stop` |
| Batch pipeline | `.\scripts\pipeline.ps1 -Mode dev -Stages bronze,silver,gold,sql,graph,ml` |
| Live demo | `.\scripts\demo.ps1` |
| Web dev server | `cd web; pnpm dev` |

Windows gotchas, learned the hard way:
- Quote `-D` flags in PowerShell (`sbt '-Dsbt.color=false'`); unquoted, PowerShell splits them at the dot.
- In Windows PowerShell 5.1, native stderr plus `$ErrorActionPreference='Stop'` aborts scripts on Spark's first
  log line; run native tools through `Invoke-Native` (see `scripts/submit.ps1`) and check `$LASTEXITCODE`.
- In Git Bash, prefix `docker exec` with `MSYS_NO_PATHCONV=1`, or `/opt/...` becomes `C:/Program Files/Git/opt/...`.
- Host Spark UIs use port 4050+; 4040–4042 are published by the Docker master for cluster drivers.
- `Set-Content -Encoding utf8` in PowerShell 5.1 writes a BOM, which the Scala REPL rejects as illegal characters;
  write files for tools with `[IO.File]::WriteAllText(path, text, (New-Object Text.UTF8Encoding $false))`.
- `spark-shell -i file` stays open after the file; end rehearsal copies with `sys.exit(0)`.

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
push without approval.

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

- **Phase:** P3 done 15 Oct 2026 (evidence in ROADMAP), P0–P3 on D1, two days ahead of schedule. `GraphMetricsJob`
  runs the GraphX suite in 6 min: Pregel weighted PageRank (49 supersteps, ρ = 0.91 against the built-in), AM/PM
  shift (84 % of morning sinks in Midtown/Lower Manhattan), 15 communities (Q = 0.788 against the built-in's 0.734),
  components (77 Jersey City/Hoboken stations are arrival-only), triangles, hops, travel times, personalized
  PageRank and an operator tour. It writes `lake/graph/station_metrics` and six portal-ready `network-*.json` files.
  The UI direction is decided: "Inkglass", with an evidence portal per module (ADR 011).
- **Next task:** your call. Either P6.1–P6.3 early (web foundation, portal system, portals for P1–P3), for design
  feedback while the backend phases continue, or P4.1 `StationArchetypesJob` (ML) as scheduled. Stop the Spark
  containers before heavy host runs. The GBFS recorder keeps running in the background.
- **Open questions:** none. Option for the user: raising `.wslconfig` `memory=` to 8 GB would add cluster headroom
  (ADR 009); not needed so far.
