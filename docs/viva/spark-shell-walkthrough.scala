// VeloGraph spark-shell walkthrough (Unit III, "invoking the spark shell"). From the repository root:
//   . .\scripts\env.ps1
//   spark-shell --master "local[8]" --driver-memory 4g --conf spark.ui.port=4050
// then paste one numbered block at a time; :quit leaves the shell. Expected output and explanations:
// docs/viva/spark-shell-walkthrough.md. While it runs, the Spark UI is at http://localhost:4050.

// 1. An RDD from a text file: one element per line, spread over partitions.
val lines = sc.textFile("data/raw/citibike/202609")
println(s"partitions: ${lines.getNumPartitions}")
println(s"first line: ${lines.first()}")

// 2. Transformations are lazy; count() is the action that makes Spark run them.
//    The NYC files are plain CSV; the first field of the header line is "ride_id".
val rows = lines.map(_.split(",", -1).map(_.replace("\"", ""))).filter(f => f(0) != "ride_id")
println(s"trips: ${rows.count()}")
println(s"lines without 13 fields: ${rows.filter(_.length != 13).count()}") // 0: no name contains a comma

// 3. A pair RDD: departures per start station (field 4 is the station name), top 5.
val departures = rows.map(f => (f(4), 1)).reduceByKey(_ + _)
departures.takeOrdered(5)(Ordering.by[(String, Int), Int](-_._2)).foreach(println)

// 4. Persistence: the second count reuses the cached partitions.
rows.cache()
spark.time(rows.count()) // computes the RDD and caches it
spark.time(rows.count()) // served from memory

// 5. Lineage: the recipe Spark replays to rebuild a lost partition.
println(departures.toDebugString)

// 6. Shared variables: a broadcast lookup, and an accumulator updated inside an action.
val ebike      = sc.broadcast(Set("electric_bike"))
val ebikeTrips = sc.longAccumulator("e-bike trips")
rows.foreach(f => if (ebike.value.contains(f(1))) ebikeTrips.add(1))
println(s"e-bike trips: ${ebikeTrips.value}")

// 7. RDD -> DataFrame -> Spark SQL.
val df = rows.map(f => (f(1), f(12))).toDF("bike", "rider")
df.createOrReplaceTempView("sep_trips")
spark.sql("SELECT bike, rider, COUNT(*) AS trips FROM sep_trips GROUP BY bike, rider ORDER BY trips DESC").show()

// 8. The cleaned lake table, queried with a SQL wildcard.
val silver = spark.read.parquet("data/lake/silver/trips")
println(s"silver trips from a 'Park' station: ${silver.where("start_station_name LIKE '%Park%'").count()}")
