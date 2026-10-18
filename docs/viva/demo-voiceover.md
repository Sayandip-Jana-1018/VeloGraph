# Demo video: captions and voice-over script

The video is `submission/VeloGraph-demo.webm`: 3:58, 1280 × 720, with no sound. Each time below is when that
caption appears, measured from the video itself.

The narration under each caption fits its time window at a calm pace, about 2.5 words a second. Read it as
written or in your own words. To record:
1. Play the video, and speak each line when its caption appears.
2. In any editor (Clipchamp on Windows, CapCut, DaVinci Resolve), lay the voice track under the video.

## Script

**0:02 – 0:06** · *"VeloGraph: 20.5 million real Citi Bike trips through Spark, GraphX, MLlib and Kafka"*
> This is VeloGraph: real New York bike-share data on Apache Spark.

**0:06 – 0:13** · *"The hero replays one real evening (Thu 17 Sep): every streak is a real trip, at 120x speed"*
> The map replays one real evening, Thursday the seventeenth of September. Every streak is a real trip.

**0:13 – 0:17** · *"Headline numbers, all read from what the Spark jobs wrote"*
> Every number here is read from what the Spark jobs wrote.

**0:17 – 0:43** · *"The pipeline as a subway line: each stop is a Spark module with its own evidence portal"*
> The home page tells the pipeline as a subway line. The lake turns 20.6 million raw rows into clean Parquet.
> Then come RDDs and Spark SQL, GraphX for the station network, and MLlib for the forecasts. Kafka with Structured
> Streaming carries the live feed, on a real cluster. The last stop maps everything to the syllabus.

**0:43 – 0:46** · *"Eleven evidence portals, one per module"*
> There is one evidence portal for every module.

**0:46 – 0:56** · *"Ctrl K: search any of the 2,490 stations"*
> Control K searches all 2,490 stations. Here I look up Bedford and open it on the live map.

**0:56 – 1:02** · *"Live: Kafka -> Spark Structured Streaming on the cluster, every station re-scored every 30 s"*
> This is live. Kafka feeds Spark Structured Streaming, which re-scores every station every thirty seconds.

**1:02 – 1:11** · *"Red: empty or about to be; blue: full or about to be; yellow arcs: suggested rebalancing moves"*
> Red stations are empty or about to empty, blue ones are full or about to fill, and the yellow arcs are
> suggested van moves.

**1:11 – 1:19** · *"The real cluster: Spark master + 2 workers, 3 Kafka brokers (KRaft), live every 10 s"*
> Behind it is a real cluster: a Spark master with two workers, and three Kafka brokers in KRaft mode.

**1:19 – 1:45** · *"Resilience drill: stopping Kafka broker 2 now, while the stream runs"*
> Now a resilience drill: I'm stopping Kafka broker 2 in the middle of the stream. Watch the partition grid. The
> controller moves broker 2's partitions to brokers 1 and 3 within seconds. Every write needs only two in-sync
> replicas, and the producer uses acks equal to all, so nothing is lost and the stream keeps running.

**1:45 – 1:52** · *"Broker 2 is down: its partitions moved to brokers 1 and 3; every partition keeps 2 in-sync copies"*
> Broker 2 is down, and every partition still has a leader and two in-sync copies.

**1:52 – 1:58** · *"Kafka UI during the outage: 2 brokers online"*
> Kafka's own UI agrees: two brokers online, and no partition offline.

**1:58 – 2:03** · *"Spark's own UI: the streaming query keeps processing micro-batches"*
> Spark's Structured Streaming page shows the query still processing batches.

**2:03 – 2:09** · *"Spark master: the scoring application running on the two workers"*
> And the Spark master shows the scoring application on both workers.

**2:09 – 2:23** · *"The live map keeps updating through the outage (acks=all, replication factor 3)"*
> The live map kept updating through the whole outage. In this drill the producer delivered 400 messages with zero
> failures, and Spark finished three micro-batches while the broker was down.

**2:23 – 2:37** · *"GraphX: stations as vertices, trips as edges; PageRank by Pregel, 15 districts"*
> Now GraphX. Stations are vertices and trips are weighted edges. PageRank, written with the Pregel API, finds the
> hubs, and label propagation splits the city into fifteen districts.

**2:37 – 2:50** · *"MLlib: next-hour demand models judged against a seasonal naive baseline"*
> MLlib forecasts next-hour departures and arrivals at every station. Every model is judged on September, a month
> never used for tuning, against a seasonal naive baseline.

**2:50 – 2:56** · *"Spark SQL: 14 insights, each with the exact query that produced it"*
> Spark SQL answers fourteen questions, each with its exact query.

**2:56 – 3:07** · *"Pregel, superstep by superstep: PageRank converging on real stations"*
> This is Pregel step by step, on real Williamsburg stations. Each superstep sends rank along the routes, until
> every station votes to halt.

**3:07 – 3:25** · *"Streaming evidence: latency, watermarks, late data and the drills"*
> The streaming portal holds the evidence for the live path: the latency from Kafka to the map, and the watermark
> and late data in the trip replay. It also holds both failure drills, the stopped broker and the killed driver.

**3:25 – 3:37** · *"Broker 2 is back: replicas resynchronise and leadership is handed back"*
> Back on the cluster page, broker 2 has returned. Its replicas resynchronise and the controller hands leadership
> back, so the partitions are balanced again.

**3:37 – 3:54** · *"Every unit of the INT315 syllabus and CO4-CO6, traced to what was built"*
> Finally, the syllabus page traces every unit of INT315 and the three course outcomes, MLlib, Spark workflows and
> GraphX, to what was built and to one measured number.

**3:54 – 3:58** · *"VeloGraph - Sayandip Jana - INT315 Cluster Computing"*
> That's VeloGraph. I'm Sayandip Jana. Thank you.
