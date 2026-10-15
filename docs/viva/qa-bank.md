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
