# Viva Q&A bank

Short, honest answers about what VeloGraph actually does, with our own numbers. Grows by a few questions every time a
roadmap task is finished. Practise answering out loud in under a minute each.

---

## P0 · Foundations (cluster, toolchain, ingestion)

**Q1. What are the parts of a Spark application, and where did each run in your project?**
The *driver* runs `main`, turns transformations into a DAG of stages and schedules tasks. The *cluster manager*
hands out resources — ours is Spark's standalone Master. *Executors* are JVMs on the worker nodes that run tasks
and cache data. On our Docker cluster the driver ran inside `velograph-spark-master` (client mode) and the
executors on `spark-worker-1` and `spark-worker-2`; SparkPi used all 4 cores across both workers.

**Q2. Why Spark standalone and not YARN?**
Unit I covers installing Spark as a standalone user, and standalone is Spark's own cluster manager: one master,
several workers, no Hadoop required. YARN is Hadoop's resource manager, used when Spark shares a Hadoop cluster.
The code does not change between them — only `--master` does.

**Q3. What is the difference between `local[20]` and `spark://spark-master:7077`?**
`local[20]` runs the driver and 20 task threads inside one JVM on the laptop. `spark://...` sends tasks to
executor JVMs on separate worker machines (containers here). The same jar runs in both modes: our smoke job took
40.4 s on the host and 28.9 s on the cluster.

**Q4. Explain broker, topic, partition, replica, leader and ISR using your cluster.**
A *broker* is a Kafka server; we run three. A *topic* is a named stream, e.g. `velograph.gbfs.station-status.v1`.
A *partition* is an ordered, append-only log and the unit of parallelism; that topic has 6. Each partition has
3 *replicas*, one per broker. One replica is the *leader* that serves reads and writes, and the others follow; our
6 partitions have 2 leaders on each broker. The *ISR* (in-sync replicas) are the replicas that are fully caught up.

**Q5. What is KRaft, and why is there no ZooKeeper?**
KRaft is Kafka's built-in Raft consensus: controller nodes keep the cluster metadata in a replicated log, which
replaced ZooKeeper (Kafka 4 removed ZooKeeper entirely). Each of our 3 nodes is both broker and controller. The
quorum leader was node 2, and with 3 voters the cluster keeps working if any one node is lost (2 of 3 is a
majority).

**Q6. What do `acks=all` and `min.insync.replicas=2` guarantee?**
The producer only gets a success once the leader *and* all in-sync replicas have stored the record, and if fewer
than 2 replicas are in sync the write is refused rather than silently stored on one machine. With replication
factor 3, one broker can fail without losing acknowledged data.

**Q7. Why an idempotent producer?**
After a network error the producer retries, which could write the same message twice. With
`enable.idempotence=true` every message carries a producer id and sequence number, so the broker drops duplicates.

**Q8. Why does the topic use the station id as the message key?**
Kafka sends all messages with the same key to the same partition, and order is guaranteed within a partition. So
every update for one station arrives in order, while different stations are processed in parallel.

**Q9. Why JDK 17 when the laptop's default is JDK 21?**
Spark 3.5 officially supports Java 8, 11 and 17. JDK 17 is a long-term-support release and is also what the
Docker Spark image uses, so host and cluster behave the same.

**Q10. Why does Spark on Windows need `winutils.exe`?**
Spark does file IO through Hadoop's FileSystem API, which on Windows needs native helpers (`winutils.exe`,
`hadoop.dll`) for permissions and file operations. Without them, writing Parquet fails.

**Q11. Your jar is only 6 MB. What is in it?**
Our code, the Kafka connector with `kafka-clients`, Typesafe Config and scopt. Spark, Scala and Hadoop are
*provided* by the runtime (`C:\spark` or the Docker image), so they stay out. The first build was 66 MB, because
the Kafka connector dragged in Hadoop client jars that Spark already ships; excluding them brought it to 6.1 MB.

**Q12. How does the trip downloader cope with failures?**
It downloads into a `.part` file and resumes with HTTP Range requests, retries with exponential backoff, verifies
the byte size and records every file in `manifest.json`. This happened for real: S3 dropped the June archive at
226 MB, and the downloader carried on from that byte instead of starting again.

**Q13. What does the GBFS recorder store, and why change data capture?**
It polls the live feed every 60 s. Instead of saving all 2,519 stations every time, it writes only the stations
whose bikes or docks changed. A full snapshot is written at start-up, every 6 hours and at the start of each day,
so any day can be replayed on its own. The recordings make the demo work offline, and later let us compare our
forecasts with what actually happened.

---

## P1 · Data acquisition, bronze and silver

**Q14. What are bronze, silver and gold, and why three layers?**
*Bronze* is a faithful copy of the source: every CSV field kept as text, plus lineage (source file, month, ingestion
time), so ingestion never fails and we can always reprocess. *Silver* is cleaned, typed and validated trips plus a
quarantine of rejected rows. *Gold* (P2) holds analysis-ready aggregates such as the station × hour panel. Each
layer can be rebuilt from the one before it.

**Q15. Why convert CSV to Parquet?**
Parquet is columnar: a query reads only the columns it needs, and per-column compression shrank our 3.8 GB of CSV to
1.0 GB in bronze. It also stores the types, and keeps min/max statistics that let Spark skip whole blocks
(predicate pushdown).

**Q16. What is schema-on-read, and where do you use it?**
Bronze stores every field as text; types are applied only when silver reads it. A bad value becomes null in silver
and is quarantined instead of crashing the load. The CSV reader in PERMISSIVE mode also keeps any line with the
wrong number of fields whole in `_corrupt_record` (our unit test proves it; the real data had 0 such lines).

**Q17. What is lazy evaluation, and why did you `persist` in the silver job?**
Transformations only build a plan; nothing runs until an *action* such as a write or a count. The silver job runs
three actions on the same classified data: write silver, write the quarantine, compute the quality report. Without
`persist`, Spark would re-read bronze, re-parse and re-run the duplicate-detection shuffle three times. We used
`MEMORY_AND_DISK_SER`: partitions that did not fit in memory were spilled to disk, which is what the
`MemoryStore` warnings in the log mean.

**Q18. Why repartition by `start_date` before the partitioned write?**
`partitionBy("start_date")` creates one folder per day, and every task writes a file into each folder it has rows
for. Without repartitioning that is thousands of tiny files. Repartitioning by the date sends each day to one task,
so silver has exactly one file per day: 123 files for 31 May – 30 Sep.

**Q19. What is dynamic partition overwrite, and why does silver not use it?**
It replaces only the partitions present in the new data, so re-ingesting September into bronze touches only
`ym=202609`. Silver is rebuilt in one pass instead, because month files contain trips that start in the
neighbouring month: the September file's first trip started on 31 Aug at 20:13. Overwriting dates month by month
would have silently replaced 31 August with just those few trips.

**Q20. Tell us about a data-quality problem you found.**
Station ids. Citi Bike ids have two decimals, but 100 ids lost their trailing zero in 378,743 trip endpoints
(`5980.1` and `5980.10` are both "E 17 St & Broadway"), and the live GBFS registry stores 3 ids the same way.
Unfixed, one station becomes two graph vertices. Normalising both sides merged 97 duplicates (2,587 → 2,490
stations) and raised the GBFS match from 98.88 % to 99.59 % of trip endpoints.

**Q21. What did the quarantine catch?**
78,098 of 20,606,696 rows (0.379 %): 49,559 trips without end coordinates (bikes never docked), 16,590 longer than
4 hours, 11,945 e-bike trips without a start GPS fix and 4 impossible speeds. The count is about 19,500 every month,
so these are systematic patterns rather than one bad file.

**Q22. Why not use accumulators for the quality counts?**
Spark guarantees accumulator updates only inside actions; inside transformations a retried or recomputed task can
add twice. Our counts come from `groupBy` aggregations, which are exact. Accumulators are demonstrated where they are
reliable, in the RDD job (P2.3).

**Q23. How do you know no rows were lost?**
Bronze compares its per-month row counts with the downloader's manifest, which counted the CSV lines, and fails on
any difference. In silver, valid plus quarantined equals bronze exactly: 20,528,598 + 78,098 = 20,606,696.

**Q24. What time-zone bug did you hit?**
Trip times are New York wall-clock and Spark's session zone is `America/New_York`, but this laptop's JVM runs in
IST. A timestamp collected to the driver as `java.sql.Timestamp` prints in the JVM zone, so New York midnight
showed as 09:30. Rule since then: format timestamps inside Spark with `date_format`, never on the driver.

**Q25. Why do you pass a schema when reading your own Parquet tables?**
Parquet files store their column types, but partition columns live in folder names, and Spark guesses their type:
`ym=202609` came back as an integer and broke the reconciliation. Declared schemas (`TripSchemas`) fix the types of
partition columns too.

---

## P2 · Gold tables, RDD fundamentals, Spark SQL

**Q26. What is the gold station × hour panel, and why "zero-filled"?**
One row per station per hour with its departures, arrivals, net flow, weather and calendar flags: 6,879,984 rows for
2,490 stations over 2,928 hours. A plain `groupBy` only returns hours that had a trip, but a demand model must also
learn from the 44.7 % of station-hours with no departures at all, so we cross-join stations with hours and left-join
the counts in. A station only gets rows between its first and last day with a trip, so a station that opened in
August is not padded with three months of fake zeros.

**Q27. How do you know the panel is right?**
Every run reconciles it: the panel's 20,528,041 departures and 20,528,595 arrivals must equal the trip counts for
the same span, or the job fails. Unit tests check a hand-made trip that ends after midnight on the last day: its
departure counts, its arrival (outside the span) does not.

**Q28. Show us a narrow and a wide transformation.**
`map` and `filter` are narrow: each output partition depends on one input partition, so they run in the same stage.
`reduceByKey`, `groupByKey` and `join` are wide: they need records with the same key together, so Spark shuffles
data between executors and starts a new stage. In the Spark UI DAG (`docs/assets/spark-ui/02-...png`) the arrow
from stage 13 to stage 14 is that shuffle.

**Q29. Why is `reduceByKey` better than `groupByKey`?**
`reduceByKey` combines values inside each partition before the shuffle. Counting departures per station over
4,979,938 September trips, `groupByKey` shuffled all 4,979,938 records (12.5 MB); `reduceByKey` shuffled only
69,390 partial sums (0.73 MB), 17 times less. The answers were identical.

**Q30. What are shared variables?**
Broadcast variables and accumulators. A broadcast ships read-only data (our 2.5k station names) to each executor
once; enriching every trip with its station name that way shuffled 0 bytes, against 99 MB for a shuffle join. An
accumulator is a counter that tasks add to and only the driver reads. Spark guarantees each update once only inside
actions: in a `map` that two actions re-ran, our accumulator counted 9,965,720 lines instead of 4,982,860.

**Q31. What do the storage levels cost and buy you?**
Re-counting 4,979,938 parsed trips took 2,055 ms uncached, 86 ms from `MEMORY_ONLY` (1,332 MB of objects), 491 ms
from `MEMORY_ONLY_SER` (397 MB of serialized bytes, slower because it must deserialize) and 610 ms from `DISK_ONLY`.
That is in-memory processing (Unit I) measured.

**Q32. What is lineage, and why does it make RDDs fault tolerant?**
Each RDD remembers the transformations that built it (`toDebugString` prints them, back to the `HadoopRDD` that read
the file). If an executor is lost, Spark recomputes only the lost partitions from that recipe, so no data has to be
replicated.

**Q33. Three ways to turn an RDD into a DataFrame?**
`rdd.toDF()` on an RDD of case classes (column names by reflection), `spark.createDataFrame(rowRdd, schema)` with an
explicit `StructType`, and `rdd.toDF("a", "b")` on an RDD of tuples. Our RDD job does all three and checks that the
counts agree and that SQL over the result matches the RDD (4,045,528 member trips either way).

**Q34. Walk us through one of your SQL insights.**
Rain impact: sum departures per hour from the gold panel (working days, 07:00–20:59), join each hour to its weather,
bucket the precipitation, average per bucket and compare with dry hours using `FIRST_VALUE(...) OVER (ORDER BY
bucket)`. Light rain costs 8 %, moderate 33 %, heavy 69 % of trips. It uses a CTE, a join, `GROUP BY`, `ORDER BY`,
`CASE` and a window function.

**Q35. Which SQL wildcards did you use?**
`%` matches any run of characters (`name LIKE '%Park%'`: 106 stations), `_` matches exactly one (`name LIKE 'E _
St%'`: East streets with a one-digit number, 23 stations), plus `RLIKE` for regular expressions
(`'^[0-9]+ Ave'`: 247 stations on numbered avenues) and `NOT LIKE '%&%'` for stations that are not street corners.

**Q36. The log warns "No Partition Defined for Window operation". Is that a problem?**
A window with no `PARTITION BY`, like `SUM(...) OVER ()` or `LAG(...) OVER (ORDER BY week)`, has to see every row,
so Spark moves them into one partition. That would be a bottleneck on millions of rows. Ours run on at most a few
dozen rows that are already aggregated, so it costs nothing, and the slowest insight still finishes in 11.8 s.

## P3 · GraphX network intelligence

**Q37. What is a property graph, and what is yours?**
A directed multigraph in which every vertex and every edge carries a user-defined attribute. Ours has 2,490 stations
as vertices (`StationAttr`: id, name, lat, lon, capacity) and 1,308,071 routes as edges (`RouteAttr`: trips,
working-day AM- and PM-peak trips, weekend trips, median ride time), built from `gold/route_edges`. 2,403 edges are
round trips (self-loops). It is dense: 21 % of all possible station pairs have at least one trip, and the median
station sends riders to 495 different stations.

**Q38. GraphX needs Long vertex ids. How did you get them?**
`xxhash64(station_id)`, Spark's built-in 64-bit hash, so every job derives the same id without a lookup table. The
build asserts that 2,490 stations give 2,490 distinct ids and that every route endpoint is a known station.
Otherwise GraphX would silently create a vertex with a `null` attribute for an unknown endpoint.

**Q39. What is vertex-cut partitioning, and why EdgePartition2D?**
GraphX partitions *edges*. A vertex is copied to every partition that holds one of its edges, so it is the vertices
that get cut. The replication factor, copies per vertex on average, is the memory cost and the cost of shipping
vertex attributes to the edges in every superstep. We measured four strategies on 16 partitions:

| Strategy | Copies per vertex |
|---|---|
| RandomVertexCut | 15.6 |
| CanonicalRandomVertexCut | 15.6 |
| EdgePartition1D | 15.6 |
| EdgePartition2D | 6.9 |

EdgePartition2D places each edge on a 4 × 4 grid by (source block, destination block), so a vertex can only appear
in its row and its column: at most 2·√16 − 1 = 7 copies. The price is a little edge imbalance: the largest partition
holds 1.13× the mean.

**Q40. Explain Pregel.**
Bulk-synchronous message passing in supersteps:

1. Every vertex that received messages runs `vprog` on their merged value.
2. `sendMsg` runs on the edges (triplets) of the vertices updated in step 1, emitting new messages.
3. `mergeMsg` combines messages headed to the same vertex, like a combiner before the shuffle.

A vertex that receives nothing stays inactive ("votes to halt"). Pregel stops when no message is left, or at
`maxIterations`. Each superstep is one Spark job (`isEmpty at Pregel.scala` in the Spark UI).

Our weighted PageRank converged in 49 supersteps (40 s). All 2,490 vertices stayed active almost to the end; then
2,401 and finally 2,076 were still sending in the last two supersteps. The `pregel` portal replays a hub's
8-station neighbourhood superstep by superstep, through to the final ones where the messages drop from 56 to 49, 7
and 0.

**Q41. How does your PageRank differ from GraphX's built-in one, and why do weights matter?**
- **Built-in:** a station's rank is split equally over its distinct routes.
- **Ours:** it is split in proportion to trips, p(u→v) = trips / out-strength. With equal weights, ours reproduces the
  built-in exactly (a unit test asserts it to 10⁻¹²), so the difference between them is purely the weights.

The two rankings agree broadly (Spearman ρ = 0.91), but only 10 of the top 20 are shared:

- **Rises with weights:** Melrose Ave & E 150 St in the Bronx climbs from rank 930 to 64. It has few distinct routes
  but heavy local traffic.
- **Falls with weights:** Jay St & Tech Pl drops from 49 to 663. It connects to many stations, but each route carries
  few trips.

The weighted top 10 are real hubs: Bedford Ave (Williamsburg), Chelsea Piers, W 21 St, Atlantic Terminal (Hanson
Pl), Columbus Circle (Broadway & W 58 St), Union Square (E 17 St) and Penn Station (9 Ave & W 33 St, W 31 St & 7 Ave).

**Q42. What do the damping factor and dangling vertices mean?**
- **Damping.** With probability α = 0.15 the random rider "teleports" to a random station instead of riding on
  (damping 0.85). This guarantees convergence and a minimum rank for every station.
- **Dangling vertices.** These have no outgoing route; we have 78. They pass nothing on, so their mass leaks. As GraphX
  does, we renormalize the ranks to sum to 1 at the end.

**Q43. What did the morning-vs-evening PageRank show?**
We ran the same weighted PageRank on working-day 07–10 trips only, then on 16–19 trips only, and defined
`pr_shift = (am − pm) / (am + pm)`:

- **+1** means a morning magnet: riders arrive there in the morning (offices).
- **−1** means an evening magnet: riders arrive there in the evening (homes).

Of the 50 stations with the highest morning PageRank, 84 % are in Midtown or Lower Manhattan; of the 50 highest
evening ones, only 10 % are. The strongest morning magnets are Greenwich St & Hubert St and Park Ave by Grand Central.
The strongest evening magnets are Grand Army Plaza, Astoria and Greenpoint.

**Q44. How did you compute strength and imbalance?**
One `aggregateMessages` pass. Every route sends its trips to its source (out-strength) and its destination
(in-strength); a round trip goes to its own station; `mergeMsg` adds them. `TripletFields.EdgeOnly` tells GraphX
that the function reads only the edge attribute, so vertex attributes are never shipped to the edge partitions.
`outerJoinVertices` then attaches the sums to the stations. The biggest four-month sink is Greenwich St & W Houston
St (+2,383); the biggest source is E 33 St & Park Ave (−2,445).

**Q45. How do your communities work, and what is modularity?**

*How they are built*

1. Label propagation runs in Pregel on the strong-tie graph: 17,541 ties, where a tie means at least 20 trips and
   being among one of the two stations' top-10 destinations.
2. Every station starts with its own label and repeatedly adopts the label carrying the most trips among its
   neighbours. A tie keeps the current label, otherwise the smallest label wins.
3. Stations take turns by id parity, which damps the flip-flopping of synchronous label propagation.

That gives 59 communities with Q = 0.702. GraphX's unweighted built-in LabelPropagation scored higher (Q = 0.734),
because label propagation stops at a local optimum.

4. So we add a greedy modularity merge (Clauset–Newman–Moore). It repeatedly joins the two linked communities whose
   union raises Q the most, ΔQ = w_ab/m − D_a·D_b/(2m²), until no merge helps.

*Result:* 15 communities, Q = 0.788, covering 93 % of stations.

*What modularity means.* Q = Σ_c [ L_c/m − (D_c/2m)² ] measures how much more trip weight falls inside communities
than a random graph with the same degrees would put there.

*Why we believe the communities.* They are recognisable districts: the Bronx, Park Slope and Prospect Park, Jackson
Heights, Bushwick, Astoria and LIC, Midtown, Williamsburg, Downtown Brooklyn, Bay Ridge. Governors Island is a
community of its own. A station is a median 1.7 km from its community's centre, against 6.6 km with shuffled labels.

**Q46. Connected vs strongly connected components: what did you find?**
- **Weakly connected** (direction ignored): one component; every station is linked to every other.
- **Strongly connected** (directions respected): a giant component of 2,412 stations plus 78 singletons, the 78
  stations riders only arrive at.
  - 77 of them are Jersey City and Hoboken stations: NYC riders end trips there, but trips starting there are in
    Citi Bike's separate Jersey City dataset, which we did not load.
  - The 78th, Bay St & Court St in Red Hook, received exactly one trip in four months.

That is a data-coverage finding we would not have seen without SCC.

**Q47. Triangles and the clustering coefficient?**
A triangle is three stations that are strongly tied pairwise. A station's local clustering coefficient,
2T / (d·(d − 1)), is the share of its strong-neighbour pairs that are tied to each other. The network has 26,995
triangles. Average clustering is 0.31, against 0.006 for a random graph of the same density, so strong ties are
local and neighbourhood-shaped.

**Q48. ShortestPaths vs your Pregel SSSP?**
- **GraphX `ShortestPaths`** counts hops to landmark vertices. To the top-10 hubs along strong ties: 481 stations are
  1 hop away, 525 are 2 and 675 are 3; 179 cannot reach a hub at all.
- **Our Pregel SSSP** is weighted. It is Bellman-Ford with one relaxation round per superstep, run from 20 landmarks
  at once: each vertex holds a map from landmark to best seconds. It uses routes with ≥ 10 trips, each weighted by
  its median ride time, and takes 6.5 s.

From the median landmark, 270 stations are within 15 minutes by bike. Those travel times feed the isochrone map.

**Q49. What does personalized PageRank answer, and how did you compute 20 at once?**
It is the same random walk, but teleports always return to the starting station. So it answers "of riders who
start here, where do they end up?". Every vertex carries a 20-element rank vector, one entry per landmark, and a
single Pregel run (27 s) computes all 20. For example, riders from Queens Plaza most often end up at 1 Ave & E 62
St, across the Queensboro Bridge.

**Q50. Why must `partitionBy` come before `groupEdges`?**
`groupEdges` merges parallel edges only within one partition. `partitionBy` sends all edges with the same (src,
dst) to the same partition, because every strategy is a function of the two ids. Our operator tour splits each route
into AM, PM and other edges (2,206,763 parallel edges), partitions them, merges them back into 1,305,668 routes, and
asserts the trip total is unchanged.

**Q51. Pregel has no hook between supersteps. How does the website show every superstep?**
The `Program` (vprog, sendMsg, mergeMsg) is a plain serializable value. GraphX runs it once on the neighbourhood,
then the driver replays the same functions under Pregel's activation rules and records every superstep. The job
asserts that the replay ends in exactly GraphX's state, so the step-through is the real algorithm, not an animation.

**Q52. A Scala gotcha from this phase?**
`Graph(vertices, edges, edgeStorageLevel = ..., vertexStorageLevel = ...)` failed with *"Cannot throw exception
because x$5 is null"*. With named arguments, scalac typed the default `defaultVertexAttr: VD =
null.asInstanceOf[VD]` with VD = `Nothing`, and casting null to `Nothing` compiles to `throw null`. The fix was
explicit type arguments, `Graph[StationAttr, RouteAttr](...)`. It is a Unit II lesson on type inference and the
bottom type `Nothing`.

## P6 · Web app (portals, Spark UI, frame budget)

**Q53. How does a portal open as an overlay but also work as its own page?**
Next.js has two routing features for this. A parallel route (`@portal`) is a second slot in the root layout. An
intercepting route (`(.)portal/[id]`) catches a click on `/portal/lake` from inside the app and renders it into that
slot, over the current page, inside a Radix dialog. The URL still changes, so Back closes it. On a refresh or a
direct link nothing is intercepted, and `/portal/[id]` renders the same body as a full page. The report and the
slides can therefore link straight to a portal.

**Q54. Where do the Spark UI charts on the website come from?**
From the Spark history server's REST API, the same data its web pages draw. `velograph.ops.spark_ui_export` reads
`/api/v1` and writes `spark-ui.json`. It takes applications, jobs, stages with their task-time quantiles, executors,
and SQL executions with their plan graph. The portal redraws the job timeline, each shuffle as a stage DAG labelled
with bytes, and the physical plan with its WholeStageCodegen clusters. The bytes match the RDD job's own
SparkListener count exactly: groupByKey 12,511,454 B against reduceByKey 734,004 B.

**Q55. Why did removing the glass blur make scrolling four times faster?**
`backdrop-filter: blur()` re-blurs whatever is behind a panel every time that changes. It changed on every frame:
the page scrolled, and the background animated behind 9 to 21 blurred panels. A frame cost 25–30 ms, about 35 fps,
with up to 36 % of frames slower than 33 ms. Now:
- glass is translucency with a lit edge, and only the header keeps a real blur;
- the background moves with GPU transforms only;
- animations pause under an open portal;
- charts are built only near the viewport.

A frame now costs 6–7.5 ms. We measured it with a script that scrolls one step per frame in headless Edge.

**Q56. Pages are "prerendered at build time". What does that mean here?**
With Cache Components, a loader marked `'use cache'` (for example `getNetworkSummary`) runs once during `next
build`: it reads the JSON, validates it with zod, and the result is stored. The build then generates 19 pages ahead
of time, including the home page and every portal, in both overlay and page form. A visit is served a ready page
instead of re-reading the data, and nothing needs the internet or the Spark cluster at run time.

## P4 · Machine learning (MLlib, PySpark lab)

**Q57. What are the station archetypes, and how was k chosen?**
Each station's day is a profile with four curves: departures and arrivals, on working days and on days off, in
3-hour bins, scaled to sum to 1, so it describes shape and not size. KMeans clustered 2,283 stations for
k = 3–10. Each k took the best of five runs by within-cluster cost, and the k with the best silhouette won,
among those whose smallest cluster holds at least 2 % of stations. That was k = 3, silhouette 0.211:
- **Morning origin** (1,084 stations): riders leave in the morning;
- **Strong morning destination** (235): AM arrivals 2.5× departures, such as Park Ave & E 42 St;
- **Leisure** (1,065): a day off as busy as a working day.

A silhouette of 0.21 means station rhythms are a continuum, not tight groups.

**Q58. How do you stop features from leaking the future?**
The splits are by time:
- **Fit:** August rows, whose features come from 6 Jun – 31 Jul.
- **Test:** September rows, whose features come from 7 Jul – 31 Aug.

`MlConfig` cannot even be constructed with a profile window that does not end before its rows start. The
feature job then checks the same on the data it read (31 Jul < 1 Aug, 31 Aug < 1 Sep). The graph features are
recomputed per window, because the P3 graph includes September. The archetype model is learned on the fit window
and applied to both, so an archetype id means the same in August and September.

**Q59. Why recompute PageRank for the ML features when GraphX already ran in P3?**
P3's graph was built from all four months of trips. Its PageRank for a station partly reflects September, so a
September prediction would be using September. `GraphFeatures` runs the same algorithms (weighted PageRank, AM/PM
PageRank, strengths, clustering coefficient, label propagation) on each 8-week window. Two adjustments keep the
windows comparable:
- the strong-tie rule is scaled to the window (≥ 9 trips instead of ≥ 20);
- districts are matched to the fit window's ids by majority overlap (14 of 14).

**Q60. How was the drain/fill threshold τ chosen?**
From the data, on the fit rows only. τ is the 70th percentile of |net flow| / capacity, which came out at 0.0526.
So an hour drains a 30-dock station when it loses more than 1.58 bikes net, which in whole bikes means 2 or more.
That leaves 13.8 % draining, 72.5 % stable and 13.7 % filling hours in August, and September keeps the same τ.

**Q61. How good are the demand forecasts, honestly?**
For departures in September, the random forest has an MAE of 1.46 trips per station-hour (R² 0.745). The seasonal
naive forecast (each station's own mean for that weekday and hour over the previous eight weeks) has 1.56. That is
6.6 % better. Arrivals are 6.3 % better, with gradient-boosted trees trained on absolute loss. Both fall short of
the 10 % we aimed for. The baseline is strong because it
already knows each station's weekly rhythm. The models add the part habit does not explain: weather, holidays, and
how similar stations behave. Without a same-day signal, such as the previous hour's flow (which the live GBFS feed
provides), that part is small. We report the gap rather than tune on September.

**Q62. Why did the random forest win departures, and how was it chosen?**
Each candidate trained on 1–24 August and was scored on 25–31 August:
- linear regression (three regularisation settings);
- decision trees (depth 8 and 12);
- random forests (30 trees, depth 10 and 12);
- gradient-boosted trees (depth 5 and 7, plus absolute loss).

For departures, the random forest at depth 12 had the lowest validation MAE (1.421), so it is the champion.
September was only used to report it. Averaging 30 trees, each grown on half the rows, cuts the variance a single deep tree has: the
decision tree's validation MAE is 1.470.

**Q63. The models are judged on MAE but trained on squared error. Is that a mismatch?**
Partly. Squared error aims at the mean, but for counts that are mostly zero the forecast with the lowest MAE is the
median, which is lower. Spark's GBT can train on absolute loss directly, so the lab's grid includes it. For departures it
reached 1.429 on the validation week, close to the forest's 1.421 but not better. For arrivals it was the best
(1.431 against 1.432), so the arrivals champion is an absolute-loss GBT, with a September MAE of 1.467.

**Q64. Why macro-F1, and what does the confusion matrix say?**
73.6 % of September hours are stable, so a model that always answers "stable" is 73.6 % accurate but useless. Its
macro-F1, the mean of the three per-class F1 scores, is only 0.28. Every classifier trains with class weights
`n / (k · n_class)`, which give the three classes equal total weight. The champion is chosen on macro-F1:
- **Champion:** a linear SVM, one-vs-rest, with validation macro-F1 0.5210, a hair ahead of the random forest's
  0.5209. Its September macro-F1 is 0.519 (accuracy 65.9 %).
- **Seasonal-naive regime:** the regime implied by the profile's expected net flow, at 0.508. So the models add
  little there.

The matrix shows why. Stable hours are recognised well (precision 0.865, recall 0.726). For draining and filling,
the models catch almost half (recall 0.46–0.47) but raise twice as many false alarms (precision 0.32). An hour's net
flow of a few bikes is noisy, and calling its sign is hard.

**Q65. How does one-vs-rest turn a binary SVM into a three-class one?**
It trains one linear SVM per class: "draining or not", "stable or not", "filling or not". Each SVM's raw output is
the signed distance to its separating hyperplane. The class whose SVM is furthest on the positive side wins. The lab
writes that comparison with built-in column functions (`vector_to_array` and `greatest`): PySpark's own
`OneVsRestModel` scores with Python UDFs, and a Python worker crashed on Windows. The Scala champion job uses Spark's
JVM `OneVsRest`, which is the same method.

**Q66. What do the GraphX features add to the forecasts?**
The ablation refits the departures champion on August with whole feature groups removed and scores September:
- without the GraphX features, MAE rises 1.6 %;
- without the hour-of-week profiles, 6.4 %;
- without both, 26.3 %;
- without weather, 7.6 %.

So the graph features matter little while the profiles are there, because both describe how busy a station is.
Without the profiles, though, PageRank and strengths carry most of that signal: removing both costs far more than
either alone. That is CO6 measured inside CO4.

**Q67. The lab is in PySpark and the production models in Scala. How do you know they are the same models?**
`TrainChampionJob` rebuilds the lab's feature pipeline stage for stage:
- the same StringIndexer, one-hot and VectorAssembler order;
- the same algorithm and parameters, declared in config;
- the same input splits: the lab ran local[16], so the job pins `spark.sql.files.minPartitionNum = 16`, because
  forests sample per partition.

It refits on August and must match the lab's September score within 1 %, or it fails. It matched to five decimals
for all three models (departures MAE 1.45950, arrivals MAE 1.46738, regime macro-F1 0.51883), and saved them with
model cards carrying their windows, parameters and git commit.

## P5 · Kafka and Structured Streaming

**Q68. Why are the station messages keyed by station id?**
Kafka sends a message to a partition by hashing its key, so every update of one station lands in the same
partition. Kafka keeps order only within a partition, so a station's updates arrive in the order they were sent.
With 2,519 stations spread over 6 partitions, each partition carried 15.6–18.2 % of the messages, which also keeps
the three brokers evenly loaded.

**Q69. What do `acks=all`, `min.insync.replicas = 2` and idempotence each do?**
- **`acks=all`:** the leader confirms a write only once every in-sync replica has it.
- **`min.insync.replicas = 2`:** the write is refused if fewer than 2 replicas are in sync, so a confirmed message
  always exists on at least two brokers.
- **Idempotence:** the producer numbers its messages, so a retry after a lost acknowledgement does not write a
  duplicate.

In the drill, broker 2 was stopped for two minutes. Its partitions' leadership moved to broker 3 within 7 s, every
partition ran on 2 replicas, and the producer had 352 deliveries and 0 failures.

**Q70. What is a consumer group, and who keeps the offsets?**
- **A consumer group** is a set of consumers that share a topic's partitions, each partition read by one member.
  The gateway is the group `velograph-gateway` and commits its offsets to Kafka, so a restarted gateway resumes
  where it stopped. `/api/cluster` shows its lag.
- **Spark does not commit to Kafka.** It keeps the offsets of every micro-batch in its own checkpoint, next to the
  state they produced, so the two can never disagree.

**Q71. What is change data capture in your producer, and why send full snapshots too?**
GBFS gives the whole station list every minute, but about 300 of 2,519 stations change between polls. The producer
remembers each station's inventory and publishes only those that changed, about a seventh of the data. Every
30 publishes it sends everything, so a consumer that starts later can rebuild every station from the last half
hour of the log. That is exactly what the scoring job does on start.

**Q72. What happens every 30 seconds in the scoring job?**
The trigger fires and Spark plans a micro-batch:
1. It writes the end offsets it will read to the checkpoint's offset log.
2. It reads those records, drops duplicates within the watermark, and hands the batch to `foreachBatch`.
3. Inside it, the driver merges the newest report of each station into its state, then builds the model rows for
   this hour and the next.
4. Three PipelineModels predict departures, arrivals and regime. The rules project the next 60 minutes and the
   rebalancer plans moves.
5. Predictions and alerts go to Kafka, the batch to Parquet, then the snapshot to disk.
6. Spark commits the batch. A batch took 4–10 s on two cores.

**Q73. Event time or processing time: which does the job use, and why?**
- **Event time** is when something happened: here the station's `last_reported`. Watermarks and deduplication use
  it, so a report sent twice (by a restarted producer, say) is recognised as the same event.
- **Processing time** is when Spark sees the record. It is what latency is about.

The broker stamps each record with its own append time, and the job measures latency from it. The laptop's clock
was 92 s slow, and comparing two different clocks would have measured nothing.

**Q74. What does the watermark do in `dropDuplicatesWithinWatermark`?**
To drop a duplicate, Spark must remember every key it has seen, and without a limit that state grows forever. The
watermark ("10 minutes behind the newest event time seen") tells Spark when it may forget a key. Any record older
than the watermark is dropped as late. The state stayed at about 1,000–2,000 rows instead of growing.

**Q75. Append and update mode: what is the difference, and where do you show it?**
- **Append** writes a result row once, when it can no longer change. For a window, that is when the watermark
  passes its end. The 15-minute station windows of the replay job reached Kafka only then.
- **Update** writes a row every time it changes. The 60-minute sliding windows were rewritten 5 to 12 times as
  trips arrived.

Append needs a watermark for aggregations; complete mode (not used) would rewrite the whole result every time.

**Q76. Your watermark is 15 minutes, yet trips 25 minutes late were still counted. Why?**
In a windowed aggregation, Spark judges lateness by the window, not by the row. A row is dropped only when the
watermark has already passed the end of the window it belongs to, because only then has that window's state been
written and forgotten. A trip at 17:01 belongs to the 17:00–17:15 window. That window closes when the watermark
passes 17:15, that is when trips of 17:30 have arrived. With 10 minutes of event time per trigger, it can come
later still. In the replay, 1,318 trips were sent late:
- 823 still counted;
- 372 were dropped;
- the rest waited in windows still open.

**Q77. What happens if the driver is killed in the middle of a batch?**
The offset log already holds that batch's offsets, but the commit log does not, so on restart Spark runs the same
batch again with the same offsets. The sinks make that safe:
- the Parquet file is written under the batch id and overwrites itself;
- the snapshot is replaced as a whole;
- Kafka predictions may appear twice (an at-least-once sink), and consumers can recognise them by station and time.

The restart drill kills the driver with `kill -9`, restarts it, and checks the offset log: the first new batch must
start where the last committed one ended.

**Q78. The models predict whole hours. How do you get "the next 60 minutes"?**
At minute m of the hour, f = m / 60 of the hour has passed. The next 60 minutes are the remaining (1 − f) of this
hour and the first f of the next. So net flow = (1 − f) × (arrivals − departures)(this hour) +
f × (arrivals − departures)(next hour), with each hour's weather taken from the forecast. Bikes now plus that net
flow, kept between 0 and the docks, is the projection.

**Q79. Why are the live features from 6 Aug to 30 Sep, not the eight weeks before today?**
Citi Bike publishes a month's trip file only after the month ends, so on 17 October the last published trips are
September's. The live window is the last eight weeks that exist. It is a leakage-safe window like the training ones,
since it ends before the live rows start, and the job checks that on the data. GraphX reruns on it, and its
districts take the fit window's ids, the numbering the models learned.

**Q80. Why does the map show "empty now" and "empty within 60 minutes" separately?**
On a Friday at 9 pm, 37 % of stations are already empty or full: the feed shows that without any model. What the
models add is the stations that are fine now and will not be in an hour, for example 80 forecast to empty and
34 to fill. One "at risk" flag would have hidden those among a thousand others. Solid dots are the feed and
pulsing rings the forecast, and an alert fires only when a station enters the empty or the full group.
