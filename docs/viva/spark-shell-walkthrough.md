# spark-shell walkthrough (Unit III — "invoking the spark shell")

A live demo you can type in front of the examiner in about five minutes. The code is in
[spark-shell-walkthrough.scala](spark-shell-walkthrough.scala); paste one numbered block at a time. Every number
below comes from a real run on 15 Oct 2026 (September 2026 data), so you know what to expect before you press Enter.

**Start it** (from the repository root, in PowerShell):

```powershell
. .\scripts\env.ps1
spark-shell --master "local[8]" --driver-memory 4g --conf spark.ui.port=4050
```

Inside the shell, `sc` (the SparkContext) and `spark` (the SparkSession) already exist; that is what "invoking the
spark shell" gives you. While it runs, the Spark UI at http://localhost:4050 shows every job you trigger.

---

### 1 · An RDD from a text file

```scala
val lines = sc.textFile("data/raw/citibike/202609")
println(s"partitions: ${lines.getNumPartitions}")
println(s"first line: ${lines.first()}")
```

Expected: `partitions: 30`, then the CSV header line.
**Say:** "textFile creates an RDD with one element per line. Spark splits the 1 GB month into 30 partitions,
roughly one per 32 MB block, and each partition is processed by one task."

### 2 · Lazy transformations, then an action

```scala
val rows = lines.map(_.split(",", -1).map(_.replace("\"", ""))).filter(f => f(0) != "ride_id")
println(s"trips: ${rows.count()}")
println(s"lines without 13 fields: ${rows.filter(_.length != 13).count()}")
```

Expected: `trips: 4982855` and `lines without 13 fields: 0`.
**Say:** "`map` and `filter` are transformations: nothing runs until `count()`, the action. 4,982,855 is exactly
the number our downloader counted. Zero rows have the wrong number of fields, so a plain split is safe here; the
real job uses a quote-aware parser anyway."

### 3 · A pair RDD and `reduceByKey`

```scala
val departures = rows.map(f => (f(4), 1)).reduceByKey(_ + _)
departures.takeOrdered(5)(Ordering.by[(String, Int), Int](-_._2)).foreach(println)
```

Expected:

```
(W 21 St & 6 Ave,17306)
(Pier 61 at Chelsea Piers,16876)
(W 31 St & 7 Ave,15315)
(E 17 St & Broadway,15253)
(9 Ave & W 33 St,14982)
```

**Say:** "`reduceByKey` adds up the ones inside each partition before the shuffle, so only per-station subtotals
cross the network. On this month that is 69,390 records instead of 4,979,938 with `groupByKey`: 0.7 MB instead
of 12.5 MB (see `rdd-fundamentals.json`)."

### 4 · Persistence

```scala
rows.cache()
spark.time(rows.count())
spark.time(rows.count())
```

Expected: about `Time taken: 3125 ms`, then about `1130 ms` (your times will differ a little).
**Say:** "`cache()` is lazy too. The first count computes and stores the partitions; the second reads them from
memory. In our RDD job, re-counting took 2,055 ms uncached and 86 ms cached in MEMORY_ONLY."

### 5 · Lineage

```scala
println(departures.toDebugString)
```

Expected (abridged):

```
(30) ShuffledRDD[6] at reduceByKey
 +-(30) MapPartitionsRDD[5] at map
    |   MapPartitionsRDD[3] at filter
    |   MapPartitionsRDD[2] at map
    |   data/raw/citibike/202609 MapPartitionsRDD[1] at textFile
    |   data/raw/citibike/202609 HadoopRDD[0] at textFile
```

**Say:** "This is the lineage graph. If an executor dies, Spark replays exactly these steps for the lost
partitions: that is RDD fault tolerance, with no data replication needed. The indentation change marks the
shuffle, which is where a new stage begins."

### 6 · Shared variables

```scala
val ebike      = sc.broadcast(Set("electric_bike"))
val ebikeTrips = sc.longAccumulator("e-bike trips")
rows.foreach(f => if (ebike.value.contains(f(1))) ebikeTrips.add(1))
println(s"e-bike trips: ${ebikeTrips.value}")
```

Expected: `e-bike trips: 3618815`.
**Say:** "A broadcast variable ships read-only data to every executor once, instead of with every task. An
accumulator lets tasks add to a counter the driver reads. I update it inside `foreach`, an action, because Spark
only guarantees exactly-once updates there; inside a `map`, a recomputed task would add twice, and our RDD job
shows exactly that: 9,965,720 counted instead of 4,982,860."

### 7 · RDD → DataFrame → Spark SQL

```scala
val df = rows.map(f => (f(1), f(12))).toDF("bike", "rider")
df.createOrReplaceTempView("sep_trips")
spark.sql("SELECT bike, rider, COUNT(*) AS trips FROM sep_trips GROUP BY bike, rider ORDER BY trips DESC").show()
```

Expected:

```
+-------------+------+-------+
|         bike| rider|  trips|
+-------------+------+-------+
|electric_bike|member|2899274|
| classic_bike|member|1148336|
|electric_bike|casual| 719541|
| classic_bike|casual| 215704|
+-------------+------+-------+
```

**Say:** "`toDF` turns an RDD of tuples into a DataFrame (Unit IV, converting RDDs to DataFrames). Once it is a
temp view, plain SQL works on it, and the Catalyst optimizer plans it. The e-bike rows add up to the
accumulator's 3,618,815."

### 8 · The cleaned lake table and a wildcard

```scala
val silver = spark.read.parquet("data/lake/silver/trips")
println(s"silver trips from a 'Park' station: ${silver.where("start_station_name LIKE '%Park%'").count()}")
```

Expected: `silver trips from a 'Park' station: 1259069` (Jun–Sep), the same number as the wildcard insight.
**Say:** "This reads our silver Parquet table: columnar, typed and cleaned. `%` in LIKE matches any text; `_`
would match exactly one character."

Leave with `:quit`.

---

**If something goes wrong in the viva:** if the port is busy, Spark moves to 4051 automatically. "Path does not
exist" means you started the shell outside the repository root, so `cd` there and retry. If a count is slow, the
laptop is busy; cached blocks (step 4) make the rest fast again.
