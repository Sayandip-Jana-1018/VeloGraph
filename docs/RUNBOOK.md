# VeloGraph runbook

What to run, where it listens, and what to do when something breaks. Commands assume Windows PowerShell 5.1 from the
repository root, with `. .\scripts\env.ps1` run once in the terminal (it changes nothing outside that terminal).

## Daily commands

| Task | Command |
|---|---|
| Check the toolchain | `.\scripts\doctor.ps1` (`-Quick`, `-SkipCluster`) |
| Live demo, one command | `.\scripts\demo.ps1` · recorded evening: `-Replay -Day 2026-10-16 -Start 20:30` · stop: `-Stop` (`-Down` too) |
| Whole batch pipeline | `.\scripts\pipeline.ps1` (`-Stages graph,ml`, `-WithLabs`) |
| One Spark job | `.\scripts\submit.ps1 <job>` on the host · `-Mode cluster` on Docker |
| Live-feed recorder | `.\scripts\recorder.ps1 start` / `status` / `stop` |
| Web data after a job | `cd web; pnpm sync` |
| Tests | `cd spark; sbt --batch test` · `cd python; uv run pytest` · `cd web; pnpm typecheck; pnpm lint; pnpm build` |
| Evidence screenshots | `cd python; uv run python -m velograph.ops.evidence_shots` (demo running) |
| Backup demo video | `cd python; uv run python -m velograph.ops.demo_video` (demo running) |

## Ports

| Port | Service |
|---|---|
| 3000 | website (Next.js) |
| 8000 | gateway (FastAPI: REST + server-sent events) |
| 8080 / 7077 | Spark master UI / RPC |
| 8081, 8082 | Spark worker UIs |
| 4040–4042 | Spark UIs of applications on the cluster (published by the master container) |
| 4050+ | Spark UIs of jobs run on the host |
| 8089 | Kafka UI |
| 9092, 9094, 9096 | Kafka brokers 1–3 (host listeners) |
| 18080 | Spark history server (`--profile history`) |

## Memory (16 GB laptop)

- **Docker:** the Docker VM is capped at 6 GB in `%USERPROFILE%\.wslconfig` (ADR 009). Three brokers, Kafka UI,
  the master and two workers use about 3.6 GB with both streams running.
- **Heavy batch jobs** run on the host with the containers stopped. `pipeline.ps1` stops them itself. Driver
  memory per job is in `pipeline.ps1`: 6–8 GB for silver, gold, GraphX and the ML jobs.
- **Each PySpark lab notebook** takes an 8 GB driver: run them one at a time.
- **If a job dies with `OutOfMemoryError`:** close other heavy applications (IntelliJ, browsers with many tabs),
  stop the containers, and re-run it alone. Every job writes idempotently, so re-running is safe.

## When something fails

| Symptom | Cause | Fix |
|---|---|---|
| `demo.ps1`: "Docker is not running" | Docker Desktop is closed | start Docker Desktop, wait for the whale icon, re-run |
| `demo.ps1` waits more than 6 minutes | the scoring job did not start | read `data\logs\demo-scoring.log`; check `docker ps` and the master UI on :8080 |
| The website shows "Recorded" instead of "Live" | the gateway or the scoring job is down | `demo.ps1` restarts the gateway itself if it exits; check `data\logs\demo-gateway*.log` |
| The cluster page lists two brokers | a broker container stopped | `docker start velograph-kafka-<n>`; the replicas resync in seconds |
| A Spark job fails with "winutils" or "HADOOP_HOME" | Spark on Windows needs Hadoop's native helpers | run `. .\scripts\env.ps1` first; it points HADOOP_HOME at `C:\hadoopsetup\hadoop-3.2.4` |
| `spark-submit` picks JDK 21 | the terminal skipped `env.ps1` | run `. .\scripts\env.ps1` (JDK 17 for this terminal only) |
| A script stops at Spark's first log line | Windows PowerShell 5.1 treats native stderr as an error | run native tools through `Invoke-Native` (see `scripts\submit.ps1`) |
| Latency or timestamps look 90 s off | the Windows clock runs 92 s slow; the Docker VM's is right | never compare a host time with a container time; latency is measured on the cluster |
| A rebuilt jar breaks a running cluster job | executors load classes from the jar lazily | stop cluster jobs before `sbt assembly` |
| `/live` shows yesterday's numbers | the website reads the exports copied at build time | `cd web; pnpm sync`, then rebuild or restart the dev server |
| The web dev server serves a stale page | Turbopack's dev cache | stop it, delete `web\.next\dev`, start it again |

## Resilience drills

```powershell
cd python
uv run python -m velograph.ops.drills broker --broker 2 --down-s 120   # stop a broker mid-stream, then bring it back
uv run python -m velograph.ops.drills restart                          # kill -9 the scoring driver, check the offsets
```

Both record their samples in `data/serving/live/drills.json`, which the streaming portal draws.

## Gateway and the broker drill

During two broker drills the gateway died with a native crash (exit code 139) in its Kafka client. It happened while
overlapping requests reached the Kafka admin client just as a broker went down. Three changes followed:
- the gateway sends one admin request at a time and reuses each reading for 5 s;
- a probe that fails returns `{"ok": false}` instead of a 500;
- `demo.ps1` runs the gateway in a restart loop.

Four pollers through a 45-second outage then got a 200 on every request (58 requests), with no restart needed.

The same native crash ended one drill run, just after the drill restarted the broker. Windows logged it as an access
violation in `ntdll.dll`, a heap fault inside the native Kafka client library. A drill writes its record only when
it completes, so `drills.json` always holds the last drill that finished. If a drill stops early, check that the
broker is back up (`docker ps`) and run the drill again.
