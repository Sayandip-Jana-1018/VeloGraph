# ADR 014 — Live scoring: station state, six states, and latency on one clock

- **Status:** accepted, 17 Oct 2026. Refines ARCHITECTURE §9.3–§9.5, which were written before the stream existed.

## Context

The live path has to:

- score every station every 30 s;
- survive a restart without waiting half an hour for data;
- tell an operator something the feed alone does not;
- prove its latency.

Building it turned up five problems the design had not foreseen:

- **Every station, every batch.** A micro-batch holds only the stations that changed since the last trigger (about
  300 of 2,519), but the snapshot needs all of them. `startingOffsets = latest` also means a fresh job sees nothing
  until the next change.
- **Risk drowned the forecast.** On a Friday evening 37 % of stations are already empty or full. A single "at risk"
  flag gave 1,015 stations, and the 109 that only the models foresaw were lost among them.
- **One busy neighbourhood took every move.** Ranked by expected riders, all 40 suggested moves landed in Lower
  Manhattan.
- **Two clocks.** The Windows host's clock is 92 s slow (its time service is off), while the Docker VM's is right.
  Latency measured across them was negative on the host and inflated on the cluster.
- **One cluster, two streams.** Each streaming app took all four cores, so the replay job queued behind the live one.

## Decision

1. **The driver holds every station's newest status.** It is a bounded collect: one row per station, at most ~2.5k.
   - On start, the state is rebuilt from the last 35 minutes of the station-status log, a batch read by broker
     timestamp. The producer writes a full snapshot at least every 30 minutes, so that window always holds one.
   - Each micro-batch merges the newest report per station. The Kafka offsets stay in the checkpoint, and every
     sink is idempotent per batch id. A replayed batch therefore lands on the same state.
   - A streaming aggregation in complete mode could hold the same table in Spark's state store. It was not used:
     the rebalancer needs the whole table on the driver anyway, and a plain merge of at most ~2.5k rows is simpler
     to explain and to unit-test.
2. **Six states.** Each is judged against a margin of max(2, 10 % of the docks):
   - **EMPTY** and **FULL:** the feed says so now.
   - **EMPTY_RISK** and **FULL_RISK:** fine now, but the 60-minute projection crosses the margin. This is what the
     models add.
   - **OFFLINE** and **OK.**

   An alert fires when a station enters the empty or the full family, so a forecast that comes true does not alert
   twice. The first batch with no history only sets the baseline. A restart reloads the previous states from the
   last snapshot.
3. **Moves one district at a time.** The rebalancer takes the neediest empty station of each GraphX district in
   turn (ranked by expected departures), then the second of each, and so on.
   - Each move takes bikes from a donor in the same district: a full station first, then the nearest.
   - A move is at most 12 bikes and 2.5 km, and a plan holds at most 40 moves.
4. **Latency is measured on one clock.**
   - The station-status topic uses `message.timestamp.type = LogAppendTime`, so a record carries the broker's time.
   - The job measures from the oldest record of a batch to the snapshot write. That is the cluster's clock on both
     ends, because the driver runs in the Spark master container.
   - The producer corrects its own clock to the feed server's, from the `Date` and `Age` headers (the feed sits
     behind CloudFront). So `fetched_at` and the TTL timing agree with `last_reported`.
5. **`spark.cores.max = 2` per streaming app,** so live scoring and replay analytics run side by side on one
   executor each.
6. **Live features come from profile L,** the last eight weeks of published trips (6 Aug – 30 Sep):
   - GraphX reruns on that window, and its districts are aligned to the fit window's ids.
   - The profile-A KMeans clusters are applied to it. Rerunning `StationArchetypesJob` with the extra window left
     all 4,811 profile-A and profile-B rows identical.

## Consequences

- **Restarts.** A restarted job scores all 2,519 stations in its first batch: `bootstrap: 2519 stations from the
  last 35 min, 2519 previous risks`.
- **What the snapshot shows.** It separates what is happening from what is about to happen, for example 322 empty
  now and 80 more forecast. The map draws the forecast as pulsing rings.
- **Latency.** On the cluster, broker → snapshot measured 17.0 s at p50 and 37.9 s at p95 in the first 6-minute
  run, against the 90 s gate. A host run shows the clock skew instead, and is not used as evidence.
- **The demo laptop's clock.** Syncing it is still worth doing (Settings → Time & language → Date & time → Sync
  now), but nothing in the pipeline depends on it any more.
