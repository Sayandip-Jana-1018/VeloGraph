import { InkArrow } from "@/components/inkglass/ink";
import { fixed, int } from "@/lib/format";
import type { LiveSnapshot, ProducerStats, ProgressEntry } from "@/lib/schemas";

type Stop = { kicker: string; title: string; lines: string[]; accent?: boolean };

function Node({ stop }: { stop: Stop }) {
  return (
    <div className={`glass-inset h-full rounded-2xl p-4 ${stop.accent ? "ring-1 ring-marker/40" : ""}`}>
      <p className="text-[0.62rem] font-semibold tracking-[0.16em] text-fg-3 uppercase">{stop.kicker}</p>
      <p className="mt-1 font-display text-sm leading-tight font-bold text-fg">{stop.title}</p>
      <ul className="mt-2 space-y-0.5 text-[0.72rem] leading-snug text-fg-2">
        {stop.lines.map((l) => (
          <li key={l}>{l}</li>
        ))}
      </ul>
    </div>
  );
}

/**
 * The live loop as one row of stops, each with the numbers the last run measured: from the polled feed to the
 * browser. On a narrow screen the row folds into a column and the arrows turn down.
 */
export function StreamLoop({
  producer,
  snapshot,
  batches,
}: {
  producer: ProducerStats["data"];
  snapshot: LiveSnapshot["data"];
  batches: ProgressEntry[];
}) {
  const c = producer.counts;
  const polls = (c.snapshots ?? 0) + (c.deltas ?? 0);
  const perPoll = polls ? (c.station_messages ?? 0) / polls : 0;
  const scored = batches.filter((b) => b.scoring);
  const lastScoring = scored.at(-1)?.scoring;
  const stops: Stop[] = [
    {
      kicker: "Source",
      title: "Citi Bike GBFS",
      lines: [`${int(snapshot.kpis.stations)} stations`, "station_status, ttl 60 s", "Open-Meteo weather"],
    },
    {
      kicker: "Python producer",
      title: "Poll → change data capture",
      lines: [`~${int(perPoll)} changed stations a poll`, "full snapshot every 30 polls", `acks=all · p95 ${fixed(producer.ackMs.p95, 0)} ms`],
    },
    {
      kicker: "Kafka · KRaft",
      title: "3 brokers, RF 3",
      lines: ["station-status: 6 partitions", "min.insync.replicas = 2", "key = station"],
      accent: true,
    },
    {
      kicker: "Spark Structured Streaming",
      title: "Every 30 s on the cluster",
      lines: ["watermark 10 min · dedupe", "3 champion models", lastScoring ? `scored in ${fixed(lastScoring.processingMs / 1000, 1)} s` : "foreachBatch"],
      accent: true,
    },
    {
      kicker: "Sinks",
      title: "Kafka · Parquet · snapshot",
      lines: ["predictions + alerts topics", "stream/predictions (Parquet)", "snapshot.json, replaced atomically"],
    },
    {
      kicker: "FastAPI gateway",
      title: "Consumer group → SSE",
      lines: ["group velograph-gateway", "/api/live/stream", "this page"],
    },
  ];
  return (
    <div className="grid items-stretch gap-2 md:grid-cols-[repeat(5,minmax(0,1fr)_auto)_minmax(0,1fr)]">
      {stops.map((stop, i) => (
        <div key={stop.title} className="contents">
          <Node stop={stop} />
          {i < stops.length - 1 && (
            <div className="grid place-items-center py-1 md:py-0" aria-hidden>
              <InkArrow width={30} height={22} from={[3, 11]} to={[27, 11]} bend={0.15} className="hidden md:block" />
              <InkArrow width={22} height={26} from={[11, 3]} to={[11, 23]} bend={0.15} className="md:hidden" />
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
