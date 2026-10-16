import { InkArrow } from "@/components/inkglass/ink";
import { Glass } from "@/components/inkglass/surfaces";
import { bytes, duration, int } from "@/lib/format";
import type { StageView } from "@/lib/schemas";

const LABEL: Record<string, string> = {
  groupByKey: "every (station, 1) record crosses the shuffle",
  reduceByKey: "only per-partition subtotals cross the shuffle",
  join: "both sides are shuffled by key, then joined",
  broadcast: "the lookup ships once to every task: no shuffle",
};

function Stage({ s }: { s: StageView }) {
  const rows: [string, string][] = [
    ["tasks", int(s.numTasks)],
    ["ran for", duration(s.durationMs)],
  ];
  if (s.inputBytes) rows.push(["input", bytes(s.inputBytes)]);
  const shuffle = s.shuffleWriteBytes
    ? { label: "shuffle write", size: s.shuffleWriteBytes, records: s.shuffleWriteRecords }
    : s.shuffleReadBytes
      ? { label: "shuffle read", size: s.shuffleReadBytes, records: s.shuffleReadRecords }
      : null;
  return (
    <div className="glass-inset w-full rounded-2xl p-4">
      <p className="flex items-baseline justify-between gap-3">
        <span className="font-mono text-xs text-fg-3">stage {s.stageId}</span>
        <span className="truncate text-sm font-semibold text-fg" title={s.name}>
          {s.name.split(" at ")[0]}
        </span>
      </p>
      <dl className="mt-2 space-y-0.5 text-xs">
        {rows.map(([k, v]) => (
          <div key={k} className="flex justify-between gap-3">
            <dt className="text-fg-3">{k}</dt>
            <dd className="text-fg-2 tabular-nums">{v}</dd>
          </div>
        ))}
      </dl>
      {shuffle && (
        <div className="mt-3 rounded-xl bg-marker/10 px-3 py-2 text-center ring-1 ring-marker/30">
          <p className="text-[0.62rem] font-semibold tracking-[0.14em] text-fg-3 uppercase">{shuffle.label}</p>
          <p className="text-lg leading-tight font-semibold text-marker tabular-nums">{bytes(shuffle.size)}</p>
          <p className="text-[0.7rem] text-fg-2 tabular-nums">{int(shuffle.records)} records</p>
        </div>
      )}
    </div>
  );
}

/**
 * Each measured job as Spark's DAG view draws it, from the history server's stage data: the map side (stages
 * that write shuffle files) on the left, the reduce side (stages that fetch them) on the right, and the arrow
 * between them is the shuffle boundary, labelled with the bytes that crossed it.
 */
export function StageFlow({ shuffles }: { shuffles: { group: string; jobId: number; stages: StageView[] }[] }) {
  return (
    <div className="grid gap-5 lg:grid-cols-2">
      {shuffles.map((job) => {
        const mapSide = job.stages.filter((s) => s.shuffleWriteBytes > 0);
        const reduceSide = job.stages.filter((s) => s.shuffleWriteBytes === 0);
        const crossed = mapSide.reduce((sum, s) => sum + s.shuffleWriteBytes, 0);
        return (
          <Glass key={job.group} className="flex flex-col p-6">
            <p className="text-center font-display text-lg font-bold text-fg">
              <span className="text-marker">{job.group}</span> <span className="font-sans text-sm font-normal text-fg-3">· job {job.jobId}</span>
            </p>
            <p className="mb-5 text-center font-hand text-lg leading-snug text-fg-2">{LABEL[job.group] ?? job.group}</p>
            {mapSide.length === 0 ? (
              <div className="mx-auto my-auto w-full max-w-[16rem] space-y-3">
                {reduceSide.map((s) => (
                  <Stage key={s.stageId} s={s} />
                ))}
                <p className="text-center font-hand text-lg text-marker">one stage, nothing shuffled</p>
              </div>
            ) : (
              <div className="my-auto grid items-center gap-3 sm:grid-cols-[minmax(0,1fr)_5.5rem_minmax(0,1fr)]">
                <div className="space-y-3">
                  {mapSide.map((s) => (
                    <Stage key={s.stageId} s={s} />
                  ))}
                </div>
                <div className="flex flex-col items-center text-center">
                  <span className="font-hand text-lg leading-none text-marker">shuffle</span>
                  <InkArrow width={80} height={30} from={[4, 17]} to={[74, 13]} bend={-0.25} color="var(--color-marker)" className="rotate-90 sm:rotate-0" />
                  <span className="text-xs font-semibold text-fg tabular-nums">{bytes(crossed)}</span>
                </div>
                <div className="space-y-3">
                  {reduceSide.map((s) => (
                    <Stage key={s.stageId} s={s} />
                  ))}
                </div>
              </div>
            )}
          </Glass>
        );
      })}
    </div>
  );
}
