import { Glass, KpiPuck } from "@/components/inkglass/surfaces";
import { Act, VivaNote, WithNote } from "@/components/portal/blocks";
import type { RddBenchmark } from "@/lib/data";
import { bytes, duration, int } from "@/lib/format";
import type { RddReport } from "@/lib/schemas";

const OPERATIONS = [
  ["groupByKey", "groupByKey"],
  ["reduceByKey", "reduceByKey"],
  ["join", "Shuffle join"],
  ["broadcast", "Broadcast join"],
] as const;

const LEVELS = [
  ["NONE", "Recount, not cached"],
  ["MEMORY_ONLY", "Recount, MEMORY_ONLY"],
  ["MEMORY_ONLY_SER", "Recount, MEMORY_ONLY_SER"],
  ["DISK_ONLY", "Recount, DISK_ONLY"],
] as const;

const shuffle = (r: RddReport, operation: string) => r.data.shuffle.find((s) => s.operation === operation);
const level = (r: RddReport, name: string) => r.data.persistence.find((p) => p.level === name);

/** The same lab job on the laptop and on the Docker cluster: the answers agree to the byte, the timings do not. */
export function LaptopOrCluster({ n, bench }: { n: number; bench: RddBenchmark }) {
  const { local, cluster, localJob, clusterJob } = bench;
  const rows = [
    { step: "The whole job", laptop: localJob.data.durationMs, cluster: clusterJob.data.durationMs },
    ...OPERATIONS.map(([operation, step]) => ({
      step,
      laptop: shuffle(local, operation)?.millis ?? 0,
      cluster: shuffle(cluster, operation)?.millis ?? 0,
    })),
    ...LEVELS.map(([name, step]) => ({
      step,
      laptop: level(local, name)?.secondCountMs ?? 0,
      cluster: level(cluster, name)?.secondCountMs ?? 0,
    })),
  ];
  const sameAnswers =
    local.data.trips === cluster.data.trips &&
    local.data.shuffle.every((s) => {
      const other = shuffle(cluster, s.operation);
      return other?.shuffleBytes === s.shuffleBytes && other?.result === s.result;
    });
  const objects = level(local, "MEMORY_ONLY")?.memoryBytes ?? 0;
  const fitted = level(cluster, "MEMORY_ONLY");
  const serialized = level(cluster, "MEMORY_ONLY_SER");

  return (
    <Act
      n={n}
      title="Laptop or cluster?"
      lede="The same September job, run on the laptop in local mode and on the Docker cluster, a master and two workers. The answers match; the timings tell the story of a small cluster."
    >
      <div className="flex flex-wrap justify-center gap-4">
        <KpiPuck
          label="Same answers"
          value={sameAnswers ? "to the byte" : "they differ"}
          note={`${int(local.data.trips)} trips, every shuffle's bytes and result`}
        />
        <KpiPuck
          label="MEMORY_ONLY on the cluster"
          value={`${bytes(fitted?.memoryBytes ?? 0)} of ${bytes(objects)}`}
          note="of objects fitted in executor memory"
        />
      </div>
      <WithNote
        note={
          <VivaNote>
            On the cluster, MEMORY_ONLY could keep only {bytes(fitted?.memoryBytes ?? 0)} of the {bytes(objects)} of
            objects, so Spark recomputed the rest and the &quot;cached&quot; recount took{" "}
            {duration(fitted?.secondCountMs ?? 0)}. Serialized, the whole RDD fits in{" "}
            {bytes(serialized?.memoryBytes ?? 0)} and the recount took {duration(serialized?.secondCountMs ?? 0)}. On a
            small cluster, MEMORY_ONLY_SER is the better choice.
          </VivaNote>
        }
      >
        <Glass variant="inset" className="overflow-x-auto p-5">
          <table className="w-full text-sm">
            <caption className="mb-3 text-left font-display text-base font-bold text-fg">Wall-clock time, per step</caption>
            <thead>
              <tr className="text-left text-xs text-fg-3">
                <th scope="col" className="py-1 pr-4 font-semibold">Step</th>
                <th scope="col" className="py-1 pr-4 text-right font-semibold">
                  Laptop <span className="font-mono font-normal">{localJob.data.master}</span>
                </th>
                <th scope="col" className="py-1 text-right font-semibold">Docker cluster</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.step} className="border-t border-white/5">
                  <th scope="row" className="py-1.5 pr-4 text-left font-normal text-fg-2">
                    {r.step}
                  </th>
                  <td className="py-1.5 pr-4 text-right text-fg tabular-nums">{duration(r.laptop)}</td>
                  <td className="py-1.5 text-right text-fg tabular-nums">{duration(r.cluster)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="mt-3 font-mono text-xs text-fg-3">
            {localJob.data.appId} · {clusterJob.data.appId} on {clusterJob.data.master}
          </p>
        </Glass>
      </WithNote>
    </Act>
  );
}
