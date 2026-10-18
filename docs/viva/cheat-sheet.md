# VeloGraph: viva cheat sheet

**In one breath.** Four months of real Citi Bike trips (20.6 million rows) go into a medallion lake on Spark. From
the lake come Spark SQL insights, a GraphX model of the network and MLlib forecasts. In parallel, the live GBFS
feed flows through three Kafka brokers into Structured Streaming on a Docker Spark cluster. Every 30 seconds it
scores every station into a risk state, raises alerts and suggests moves. A Next.js site with 11 evidence portals
shows it all. Every number comes from a JSON file a job wrote.

## Numbers to know

| Area | Numbers |
|---|---|
| Data | 20,606,696 raw rows; 20,528,598 clean; 78,098 quarantined (0.379 %), each with a reason. 2,490 stations in the graph, 2,519 in the live feed. |
| RDDs | reduceByKey shuffles 17× less than groupByKey (0.7 vs 12.5 MB). A cached recount is 28× faster (2,089 → 74 ms). A broadcast join shuffles 0 B, a shuffle join 99.1 MB. An accumulator inside a map counts every line twice. |
| Spark SQL | 14 queries. Heavy rain: −69 % trips (6 such hours). E-bikes: 72 % of trips. |
| GraphX | 1,305,668 directed edges. PageRank: 49 supersteps, Spearman 0.910 against the built-in. 59 label-propagation communities → 15 districts, modularity 0.788. Clustering 0.307 (random graph: 0.0057). EdgePartition2D: 6.9 replicas a vertex (random cut: 15.6). |
| MLlib | k = 3 archetypes (silhouette 0.211); 31 features. Departures: random forest, MAE 1.460 against 1.562, 6.6 % better. Arrivals: GBT, 1.467 against 1.566, 6.3 % better. Regime: linear SVM, macro-F1 0.519. Ablation: without graph +1.6 %; without graph and profiles +26.3 %. |
| Deployed models | `evaluate-champions` on 1,717,896 September station-hours: departures MAE 1.464 (naive 1.562); arrivals 1.476 (naive 1.566). Regime accuracy 0.659, macro-F1 0.519; always "stable" scores accuracy 0.736 but macro-F1 0.283. Within 0.6 % of the notebooks. |
| Streaming | 30 s trigger, 10 min watermark. p50 21.4 s, p95 38.2 s over 55 micro-batches (31 min). 14,248 messages sent, 0 failed. |
| Drills | Broker 2 down 75 s: 0 failed sends, 3 batches meanwhile, back in sync in 10.4 s. kill -9 of the driver: resumed at offset 38,030, with nothing lost or repeated. |
| Backtest | 156,902 predictions. "Empties soon" came true 89.8 % (fine stations: 6.4 %). "Fills soon" came true 85.5 % (fine stations: 11.2 %). Bike counts: 1.41 off, against 1.33 for "no change". |
| Laptop vs cluster | RDD lab: 67.5 s on the laptop, 99.0 s on the cluster. On the cluster MEMORY_ONLY fitted only 497 of 1,332 MB; MEMORY_ONLY_SER fitted all of it. |
| Engineering | 77 Scala and 30 Python tests; 63 exports validated. `demo.ps1` is live in 73 s; `pipeline.ps1` takes 50.9 min. Lighthouse accessibility 100. |

## Say it in one line

- **RDD**: an immutable, partitioned collection that remembers its lineage, so a lost partition is recomputed, not
  replicated.
- **Narrow vs wide**: narrow transformations (map, filter) need one parent partition. Wide ones (groupByKey, join)
  need a shuffle, which starts a new stage.
- **Job → stage → task**: an action starts a job. Shuffles cut the job into stages, and each stage runs one task per
  partition.
- **Lazy evaluation**: transformations only build the plan; an action runs it.
- **Catalyst and Tungsten**:
  - Catalyst rewrites the query plan: it pushes predicates down, prunes columns and picks the join.
  - Tungsten generates code and manages memory compactly.
- **Storage levels**:
  - MEMORY_ONLY stores objects: fast, but large.
  - MEMORY_ONLY_SER stores bytes: about a third of the size, with some CPU to read.
  - DISK_ONLY spills everything to disk.
- **Shared variables**:
  - a broadcast variable gives every executor a read-only copy;
  - an accumulator is reliable only when updated inside actions.
- **Pregel**: in each superstep, `vprog` updates a vertex, `sendMsg` sends along its edges and `mergeMsg` combines
  what arrives. A vertex with nothing to say votes to halt.
- **PageRank**: a random rider teleports with α = 0.15 or follows a route in proportion to its trips.
- **Modularity**: the edge weight inside communities minus what chance would give. Above 0.3 is strong; 0.788 is
  very strong.
- **Vertex cut**: GraphX partitions edges and copies vertices. Fewer copies means less traffic each superstep.
- **Leakage**: a feature that knows the future. It is prevented by time windows, profiles that look back only, and
  GraphX rerun per window.
- **Seasonal naive**: the station's mean for that weekday and hour over the previous eight weeks. It is the
  baseline every model must beat.
- **MAE vs RMSE**: MAE is the typical miss. RMSE punishes big misses more. MAE rewards forecasting the median.
- **Kafka**:
  - a topic is split into partitions; an offset is a position in a partition;
  - a consumer group shares the partitions among its members;
  - RF 3 keeps three copies, and min ISR 2 needs two in sync to accept a write;
  - with acks=all, the in-sync replicas confirm every write;
  - KRaft replaces ZooKeeper.
- **Structured Streaming**:
  - a micro-batch runs on each trigger;
  - the watermark bounds lateness;
  - output modes are append, update and complete;
  - the checkpoint holds the offset and commit logs;
  - foreachBatch runs any batch code on each micro-batch.
- **Standalone cluster**:
  - the master hands out cores, and workers host executors;
  - the driver runs the plan;
  - in client mode the driver runs where you submit.

## The demo, in order

Home hero (one real evening) → the architecture → `/cluster` (2 workers, 3 brokers) → `/live` (risk, alerts, moves)
→ the network portal → the models portal → Spark SQL with a wildcard → the broker drill → the conclusion slide.

**If something fails:**
- the website renders offline from static JSON;
- `demo.ps1 -Replay -Day 2026-10-16 -Start 20:30` replays a recorded evening;
- the backup video is `submission/VeloGraph-demo.webm`.

## Honest answers, ready

- **The 10 % target:** missed, with 6.6 % and 6.3 %. Nearly half of all station-hours (44.7 %) have zero departures,
  and the seasonal baseline is strong.
- **Hour-ahead bike counts:** they lose to "no change", but the risk alerts work: 90 % came true.
- **Scale:** both clusters run on one laptop. They demonstrate distribution and fault tolerance, not scale. The
  local-versus-cluster benchmark shows the cost of a small cluster.
- **The recordings:** they have gaps where the laptop slept, so the backtest covers only part of two days.
