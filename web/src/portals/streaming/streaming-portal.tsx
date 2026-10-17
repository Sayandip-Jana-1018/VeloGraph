import { Glass, KpiPuck } from "@/components/inkglass/surfaces";
import { Act, CodePanel, Ledger, VivaNote, WithNote } from "@/components/portal/blocks";
import {
  getDrills,
  getLiveFeatures,
  getLiveSnapshot,
  getProducerStats,
  getReplayAnalytics,
  getStreamProgress,
} from "@/lib/data";
import { fixed, int, pct } from "@/lib/format";

import { LiveBoard } from "./live-board";
import { StreamLoop } from "./stream-loop";
import {
  AppendWindowsChart,
  BatchCharts,
  BrokerDrillChart,
  PartitionsChart,
  UpdateWindowsChart,
  WatermarkChart,
} from "./streaming-charts";

const GATE_S = 90;
const STATUS_TOPIC = "velograph.gbfs.station-status.v1";

function quantile(xs: number[], q: number) {
  const s = [...xs].sort((a, b) => a - b);
  return s.length ? s[Math.min(s.length - 1, Math.floor(q * s.length))] : 0;
}

function Funnel({ rows }: { rows: { label: string; value: number; note: string; tone: string }[] }) {
  const max = Math.max(1, ...rows.map((r) => r.value));
  return (
    <Glass variant="inset" className="space-y-3 p-5">
      <h3 className="font-display text-base font-bold text-fg">Where the late trips went</h3>
      {rows.map((r) => (
        <div key={r.label}>
          <div className="flex justify-between text-sm">
            <span className="text-fg-2">{r.label}</span>
            <span className="font-semibold text-fg tabular-nums">{int(r.value)}</span>
          </div>
          <div className="mt-1 h-2.5 rounded-full bg-white/6">
            <div className="h-full rounded-full" style={{ width: `${(r.value / max) * 100}%`, background: r.tone }} />
          </div>
          <p className="mt-0.5 text-xs text-fg-3">{r.note}</p>
        </div>
      ))}
    </Glass>
  );
}

export async function StreamingPortal() {
  const [{ data: snapshot }, { data: progress }, { data: producer }, { data: replay }, { data: drills }, { data: features }] =
    await Promise.all([getLiveSnapshot(), getStreamProgress(), getProducerStats(), getReplayAnalytics(), getDrills(), getLiveFeatures()]);

  const scoring = progress.batches.filter((b) => b.query === "live-station-scoring");
  const withRows = scoring.filter((b) => b.scoring && b.inputRows > 0);
  const latencies = withRows.map((b) => b.scoring!.latencyS);
  const p50 = quantile(latencies, 0.5);
  const p95 = quantile(latencies, 0.95);
  const rows = scoring.reduce((s, b) => s + b.inputRows, 0);
  const processing = scoring.filter((b) => b.scoring).map((b) => b.scoring!.processingMs);
  const c = producer.counts;
  const late = replay.late;
  const open = Math.max(0, late.sent - late.accepted - late.droppedByWatermark);
  const station15 = replay.queries.find((q) => q.outputMode === "append");
  const system60 = replay.queries.find((q) => q.outputMode === "update");
  const broker = drills.broker;
  const restart = drills.restart;

  return (
    <>
      <Act
        n={1}
        title="From a polled feed to an ordered stream"
        lede="GBFS is a file that changes every minute, not a stream. The producer polls it as often as its ttl allows and publishes only the stations whose bikes or docks changed. Each message goes to Kafka keyed by station, so one station's updates stay in order."
      >
        <StreamLoop producer={producer} snapshot={snapshot} batches={scoring} />
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Station messages" value={int(c.station_messages ?? 0)} note={`${int(c.snapshots ?? 0)} snapshots + ${int(c.deltas ?? 0)} deltas`} />
          <KpiPuck label="Acknowledged" value={int(producer.delivered)} note={`${int(producer.failed)} failed · acks=all, idempotent`} />
          <KpiPuck label="Ack latency" value={`${fixed(producer.ackMs.p95, 0)} ms`} note={`p95; p50 ${fixed(producer.ackMs.p50, 0)} ms`} />
          {producer.clockOffsetS !== null && (
            <KpiPuck label="Clock offset" value={`${producer.clockOffsetS > 0 ? "+" : ""}${fixed(producer.clockOffsetS, 0)} s`} note="this laptop vs the feed's server" />
          )}
        </div>
        <WithNote
          note={
            <VivaNote>
              Change data capture: a station is published only when its bikes or docks change, and every 30 polls a
              full snapshot goes out, so a consumer that starts late can rebuild every station from the log. acks=all
              waits for the in-sync replicas, and idempotence lets the broker drop a retried duplicate.
            </VivaNote>
          }
        >
          <PartitionsChart byPartition={producer.byPartition} topic={STATUS_TOPIC} />
        </WithNote>
        <CodePanel id="stream-producer-cdc" caption="gbfs_producer: one poll becomes changed-station messages, keyed by station" />
      </Act>

      <Act
        n={2}
        title="Thirty seconds at a time"
        lede={`Spark reads the topic as an unbounded table and works through it in micro-batches, one every 30 s, on the Docker cluster. Event time is the station's own report time. A 10-minute watermark bounds the state that dropDuplicatesWithinWatermark keeps.`}
      >
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Broker → snapshot" value={`${fixed(p95, 1)} s`} note={`p95 over ${int(withRows.length)} batches with data; p50 ${fixed(p50, 1)} s`} />
          <KpiPuck label="Gate B" value={p95 < GATE_S ? "passed" : "missed"} note={`p95 under ${GATE_S} s`} />
          <KpiPuck label="Micro-batches" value={int(scoring.length)} note={`${int(rows)} rows in`} />
          <KpiPuck label="Scoring time" value={`${fixed(quantile(processing, 0.5) / 1000, 1)} s`} note="median, 2 cores, all stations" />
        </div>
        <BatchCharts batches={scoring} limitS={GATE_S} />
        <WithNote
          note={
            <VivaNote>
              A trigger every 30 s makes a micro-batch of whatever arrived. Offsets are written to the checkpoint
              before the batch runs and committed after it, so a crash replays the batch rather than losing or
              doubling it. The watermark lets Spark forget duplicates older than 10 minutes.
            </VivaNote>
          }
        >
          <WatermarkChart batches={scoring} delay="10 minutes" />
        </WithNote>
        <CodePanel id="stream-watermark" caption="LiveStationScoringJob: watermark, deduplication and a micro-batch sink" />
      </Act>

      <Act
        n={3}
        title="What one micro-batch decides"
        lede={`Per station, the champions forecast departures and arrivals for this hour and the next. The next 60 minutes blend them: (1 − f) of this hour plus f of the next, where f is how far into the hour we are. Bikes now plus that net flow, against a margin of max(2, 10 % of the docks), says who empties or fills.`}
      >
        <LiveBoard initial={snapshot} />
        <WithNote
          note={
            <VivaNote>
              The live features follow the same rule as training: station, graph and archetype features from the
              last eight weeks of trips ({features.profile.from} to {features.profile.to}), with the district ids of
              the fit window. The models never see a feature computed on the hours they predict.
            </VivaNote>
          }
        >
          <div className="grid gap-4 sm:grid-cols-2">
            <Glass variant="inset" className="p-4 text-sm text-fg-2">
              <p className="font-display text-sm font-bold text-fg">Live features (gold/live_stations)</p>
              <p className="mt-2">
                {int(features.stations)} stations, {int(features.withProfile)} with their own hour-of-week profile;{" "}
                {int(features.profileRows)} profile rows. Districts matched to the fit window: {features.communitiesMatched} of{" "}
                {features.communitiesMatched + features.communitiesNew}. Leakage check: {features.leakage.lastProfileDay} &lt;{" "}
                {features.leakage.firstRowDay} {features.leakage.passed ? "✓" : "✗"}.
              </p>
            </Glass>
            <Glass variant="inset" className="p-4 text-sm text-fg-2">
              <p className="font-display text-sm font-bold text-fg">Sinks of every batch</p>
              <p className="mt-2">
                Predictions for all stations and alerts for state changes go to Kafka. The batch is written to Parquet
                under its batch id, so a replayed batch overwrites itself. Last comes the snapshot this page reads,
                renamed into place in one step.
              </p>
            </Glass>
          </div>
        </WithNote>
        <CodePanel id="stream-risk" caption="LiveRules: 60-minute projection and the state it implies" />
        <CodePanel id="stream-rebalance" caption="Rebalancer: one district at a time, busiest empty stations first" />
      </Act>

      <Act
        n={4}
        title="Event time, late trips and output modes"
        lede={`One real evening (${int(late.rows)} trips, 16:00 to 20:00 on Thu 17 Sep) replayed into Kafka at 60× speed. ${pct((late.sent / Math.max(1, late.rows)) * 100, 0)} of the trips were held back 5 to 50 minutes, as if their dock had lost its connection. Two queries read the same stream, one in each output mode.`}
      >
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Trips replayed" value={int(late.rows)} note="event time = trip start" />
          <KpiPuck label="Sent late" value={int(late.sent)} note="counted with observe()" />
          <KpiPuck label="Still counted" value={int(late.accepted)} note="their window was still open" />
          <KpiPuck label="Dropped as late" value={int(late.droppedByWatermark)} note="Spark's own counter" />
        </div>
        <div className="grid gap-5 lg:grid-cols-2">
          <UpdateWindowsChart system={replay.system} />
          <AppendWindowsChart stations={replay.stations} />
        </div>
        <WithNote
          note={
            <VivaNote>
              A late trip is not judged by its own time but by its window&apos;s: Spark drops it only once the
              watermark has passed the end of the window it belongs to. That is why trips up to about half an hour
              late still counted here. Append writes a window once, when it can no longer change; update rewrites it
              at every change.
            </VivaNote>
          }
        >
          <Funnel
            rows={[
              { label: "Sent late by the replayer", value: late.sent, note: "5 to 50 minutes after their start", tone: "#9085e9" },
              { label: "Counted in a closed window", value: late.accepted, note: "the watermark had not yet passed their window's end", tone: "#3987e5" },
              { label: "Dropped by the watermark", value: late.droppedByWatermark, note: "counted after the partial aggregation before the shuffle", tone: "#e66767" },
              { label: "In windows still open", value: open, note: "the last windows wait for a later trip to move the watermark", tone: "#5f5a8c" },
            ]}
          />
        </WithNote>
        <CodePanel id="stream-replay-windows" caption="TripReplayAnalyticsJob: departures per station in tumbling 15-minute windows" />
      </Act>

      {(broker || restart) && (
        <Act
          n={5}
          title="Pull the plug"
          lede="Two drills on the running system: stop one of the three Kafka brokers in the middle of the stream, then kill the streaming driver outright and start it again."
        >
          {broker && (
            <>
              <div className="flex flex-wrap justify-center gap-4">
                <KpiPuck label="Broker down" value={`${fixed(broker.downS, 0)} s`} note={`broker ${broker.broker} of 3`} />
                <KpiPuck label="Failed deliveries" value={int(broker.producerFailed)} note={`${int(broker.producerDelivered)} acknowledged meanwhile`} />
                <KpiPuck label="Batches meanwhile" value={int(broker.batchesWhileDown)} note={`max latency ${fixed(broker.maxLatencyWhileDownS ?? 0, 1)} s`} />
                <KpiPuck label="Back in sync" value={broker.recoveredS !== null ? `${fixed(broker.recoveredS, 1)} s` : "—"} note="all partitions at 3 replicas again" />
              </div>
              <WithNote
                note={
                  <VivaNote>
                    Every partition has 3 replicas and needs 2 in sync to accept a write. When a broker stops, the
                    controller moves its partitions&apos; leadership to the others within seconds, writes go on with 2
                    replicas, and the broker catches up from the leaders when it returns.
                  </VivaNote>
                }
              >
                <BrokerDrillChart samples={broker.samples} downS={broker.downS} />
              </WithNote>
            </>
          )}
          {restart && (
            <Glass variant="inset" className="p-5 text-sm text-fg-2">
              <p className="font-display text-base font-bold text-fg">
                kill -9, then restart: {restart.passed ? "resumed exactly where it stopped" : "check the offsets"}
              </p>
              <p className="mt-2">
                The last batch committed before the kill was #{restart.lastCommitted}, ending at offset {int(restart.lastCommittedEnd)}{" "}
                (summed over the 6 partitions).{" "}
                {restart.rerunBatches.length > 0
                  ? `Batch ${restart.rerunBatches.join(", ")} had its offsets logged but was not committed, so it ran again with the same offsets. `
                  : ""}
                After the restart, batch #{restart.firstAfterRestart} started at {restart.firstStart !== null ? int(restart.firstStart) : "—"} and{" "}
                {restart.batchesAfterRestart} batches have committed since: no gap, no overlap.
              </p>
            </Glass>
          )}
        </Act>
      )}

      <Ledger
        rows={[
          { label: "Station messages published (CDC)", value: int(c.station_messages ?? 0), path: "live/producer-stats.json · counts" },
          { label: "Producer ack latency, p50 / p95", value: `${fixed(producer.ackMs.p50, 0)} / ${fixed(producer.ackMs.p95, 0)} ms`, path: "live/producer-stats.json · ackMs" },
          { label: "Broker → snapshot, p50 / p95", value: `${fixed(p50, 1)} / ${fixed(p95, 1)} s`, path: "live/stream-progress.json · batches[].scoring.latencyS" },
          { label: "Stations scored in the last batch", value: int(snapshot.kpis.stations), path: "live/snapshot.json · kpis.stations" },
          {
            label: "Empty / full, now and forecast",
            value: `${int(snapshot.kpis.empty)} + ${int(snapshot.kpis.emptyRisk)} · ${int(snapshot.kpis.full)} + ${int(snapshot.kpis.fullRisk)}`,
            path: "live/snapshot.json · kpis",
          },
          { label: "Replay: late trips sent / counted / dropped", value: `${int(late.sent)} / ${int(late.accepted)} / ${int(late.droppedByWatermark)}`, path: "live/replay-analytics.json · late" },
          ...(station15 && system60
            ? [{ label: "Replay queries (append / update)", value: `${station15.batches} / ${system60.batches} batches`, path: "live/replay-analytics.json · queries" }]
            : []),
          ...(broker ? [{ label: "Broker drill: failed deliveries", value: int(broker.producerFailed), path: "live/drills.json · broker" }] : []),
        ]}
      />
    </>
  );
}
