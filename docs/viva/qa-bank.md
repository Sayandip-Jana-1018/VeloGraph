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
