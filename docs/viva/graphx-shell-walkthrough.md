# GraphX in spark-shell (Unit III — "GraphX features and operations")

A live demo of about four minutes, the GraphX companion to [spark-shell-walkthrough.md](spark-shell-walkthrough.md).
The code is in [graphx-shell-walkthrough.scala](graphx-shell-walkthrough.scala); paste one numbered block at a time.
Every number below comes from a rehearsal on 15 Oct 2026 (Jun–Sep 2026 data). The whole script runs in about 45 s.

**Start it** (from the repository root, in PowerShell):

```powershell
. .\scripts\env.ps1
spark-shell --master "local[8]" --driver-memory 4g --conf spark.ui.port=4050
```

---

### 1 · A property graph from two lake tables

Expected: `stations: 2490, routes with at least 20 trips: 216418`.
**Say:** "A property graph is vertices and edges that each carry an attribute. Here a vertex is a station, with its
name as the attribute, and an edge is a route with its trip count. GraphX needs Long vertex ids, so `xxhash64` turns
each station id into one. `partitionBy(EdgePartition2D)` puts each edge on a 4 × 4 grid by source and destination, so
every vertex is copied to at most seven partitions."

### 2 · Triplets

```
Vernon Blvd & 50 Ave -> 55 Ave & Center Blvd: 4020 trips
55 Ave & Center Blvd -> Vernon Blvd & 50 Ave: 3925 trips
N 6 St & Bedford Ave -> S 2 St & Kent Ave: 3090 trips
```

**Say:** "A triplet is an edge plus both endpoint attributes, which is why I can print names next to the trip
count. The busiest route of the summer is a short hop along the Long Island City waterfront, in both directions."

### 3 · Degrees

Expected: `Cleveland Pl & Spring St: 945 busy routes`.
**Say:** "`degrees` counts edges in plus out per vertex. On this filtered graph that means routes with at least 20
trips."

### 4 · aggregateMessages

```
W 21 St & 6 Ave: 66390 arrivals
Pier 61 at Chelsea Piers: 65435 arrivals
9 Ave & W 33 St: 57577 arrivals
```

**Say:** "`aggregateMessages` is the GraphX primitive underneath everything else. Each edge sends a message, here its
trips to its destination, and the messages for one vertex are merged, here by adding. That gives arrivals per
station in one pass."

### 5 · Built-in PageRank

```
  4.91  E Fordham Rd & Webster Ave
  3.82  Shore Pkwy Greenway & Bay Ridge Ave
  3.73  Broadway & 74 St
  3.67  1 Ave & E 68 St
  3.56  Melrose Ave & E 150 St
```

**Say:** "The built-in `pageRank` is unweighted: a route used 20 times counts as much as one used 4,000 times. Our
job's weighted PageRank, written with the Pregel API, ranks the real hubs first: Bedford Ave, Chelsea Piers, Penn
Station. That is step 7."

### 6 · Connected components and triangles

Expected: `components: 165, largest: 2326 stations`, then `triangles: 5885895`.
**Say:** "`connectedComponents` labels each vertex with the smallest vertex id it is connected to, by repeated
Pregel supersteps. On busy routes only, 164 stations are cut off from the main network. `triangleCount` counts
closed triples, which is how the clustering coefficient is computed."

### 7 · The full suite's result

```
+------------------------------+---------------------+--------------------+------------+-------------------+
|name                          |pagerank_w           |pr_shift            |community_id|clustering_coef    |
+------------------------------+---------------------+--------------------+------------+-------------------+
|Metropolitan Ave & Bedford Ave|0.0018996653093393647|-0.36864485665483127|9           |0.10505212510024058|
|Pier 61 at Chelsea Piers      |0.001840844687975533 |0.29089208409530803 |11          |0.10799726589200273|
|W 21 St & 6 Ave               |0.0018074493571180992|0.4409243249817633  |11          |0.12836438923395446|
|Hanson Pl & Ashland Pl        |0.0017839958749920105|-0.195688377473747  |8           |0.08935574229691877|
|N 7 St & Driggs Ave           |0.0017555257231579502|-0.28735176063963397|9           |0.16721311475409836|
+------------------------------+---------------------+--------------------+------------+-------------------+
```

**Say:** "This is what `GraphMetricsJob` writes for every station.

- `pagerank_w` is our weighted PageRank.
- `pr_shift` compares morning with evening PageRank. W 21 St is a morning magnet (+0.44); Bedford Ave, in
  Williamsburg, is an evening magnet (−0.37).
- `community_id` is the rebalancing zone found by label propagation plus modularity merging.
- `clustering_coef` is the share of a station's strong neighbours that are tied to each other."

Leave with `:quit`.

---

**If something goes wrong:**

- **Slow first block.** Step 1 reads two Parquet tables, so give it 10–15 s.
- **Port busy.** Spark moves the UI to 4051 automatically.
- **"Path does not exist".** The shell was started outside the repository root.
