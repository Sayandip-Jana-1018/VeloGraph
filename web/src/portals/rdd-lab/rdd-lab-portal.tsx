import { InkArrow } from "@/components/inkglass/ink";
import { Notate } from "@/components/inkglass/ink-client";
import { sketch } from "@/components/inkglass/rough";
import { Glass, KpiPuck } from "@/components/inkglass/surfaces";
import { Act, CodePanel, Ledger, VivaNote, WithNote } from "@/components/portal/blocks";
import { getRddReport } from "@/lib/data";
import { bytes, duration, fixed, int } from "@/lib/format";

import { HourlyChart, PersistenceChart, ShuffleChart } from "./rdd-charts";

/** One step of `toDebugString`, read from the job's real output. */
type LineageStep = { rdd: string; op: string; partitions?: string; source?: string; cached?: string; afterShuffle: boolean };

function parseLineage(lines: string[]): LineageStep[] {
  const steps: LineageStep[] = [];
  lines.forEach((line, i) => {
    const rdd = line.match(/(\w+RDD\[\d+\]) at (\w+)/);
    if (rdd) {
      const source = line.match(/(data\/raw\/[^ ]+)/)?.[1]; // keep the repo-relative part of local paths only
      const name = line.match(/(trips \d+ \([A-Z_]+\))/)?.[1];
      steps.push({
        rdd: rdd[1],
        op: rdd[2],
        partitions: line.match(/\((\d+)\)/)?.[1],
        source: source ?? name,
        afterShuffle: i === 0,
      });
    }
    const cached = line.match(/CachedPartitions: (\d+); MemorySize: ([\d.]+ \w+)/);
    if (cached && steps.length) steps[steps.length - 1].cached = `${cached[1]} partitions cached · ${cached[2]}`;
  });
  return steps.reverse(); // source first
}

/** groupByKey vs reduceByKey as a sketch: how many records cross the network between three map partitions. */
function ShuffleSketch({ crossing, label }: { crossing: number; label: string }) {
  const parts = [30, 95, 160];
  const ink = (seed: number) => ({ stroke: "#000", strokeWidth: 1.4, roughness: 1.4, seed });
  const boxes = [...parts.map((y, i) => sketch.rect(10, y, 90, 50, ink(i + 1))), ...[60, 130].map((y, i) => sketch.rect(330, y, 90, 50, ink(i + 9)))].flat();
  const dots = Array.from({ length: crossing }, (_, i) => {
    const t = (i + 0.5) / crossing;
    const x = 130 + 170 * ((i * 0.37) % 1);
    const y = 40 + 160 * t;
    return sketch.ellipse(x, y, crossing > 6 ? 9 : 22, crossing > 6 ? 9 : 22, { ...ink(40 + i), fill: "#000", fillStyle: "solid" });
  }).flat();
  return (
    <figure className="flex flex-col items-center">
      <svg aria-hidden viewBox="0 0 430 230" className="w-full max-w-[26rem]">
        {boxes.map((p, i) => (
          <path key={i} d={p.d} fill="none" stroke="var(--color-chalk)" strokeWidth={1.4} />
        ))}
        {dots.map((p, i) => (
          <path key={`d${i}`} d={p.d} fill={p.fill ? "var(--color-coral)" : "none"} stroke="var(--color-coral)" strokeWidth={1.2} />
        ))}
        {parts.map((y, i) => (
          <text key={i} x={55} y={y + 31} textAnchor="middle" className="font-hand" fontSize={18} fill="var(--color-chalk)">
            map {i + 1}
          </text>
        ))}
        {[60, 130].map((y, i) => (
          <text key={i} x={375} y={y + 31} textAnchor="middle" className="font-hand" fontSize={18} fill="var(--color-chalk)">
            reduce {i + 1}
          </text>
        ))}
      </svg>
      <figcaption className="font-hand text-xl text-marker">{label}</figcaption>
    </figure>
  );
}

export async function RddLabPortal() {
  const { data: r } = await getRddReport();
  const op = Object.fromEntries(r.shuffle.map((s) => [s.operation, s]));
  const ratio = op.groupByKey.shuffleBytes / op.reduceByKey.shuffleBytes;
  const lineage = parseLineage(r.lineage);
  const persist = Object.fromEntries(r.persistence.map((p) => [p.level, p]));

  return (
    <>
      <Act
        n={1}
        title="The shuffle duel"
        lede={`Departures per station on ${int(r.trips)} parsed September trips, computed twice. Same answer, very different network traffic.`}
      >
        <div className="grid items-center gap-6 lg:grid-cols-2">
          <Glass className="grid gap-2 p-6 sm:grid-cols-2">
            <ShuffleSketch crossing={18} label="groupByKey: every (station, 1) record travels" />
            <ShuffleSketch crossing={3} label="reduceByKey: only subtotals travel" />
          </Glass>
          <div className="space-y-4">
            <p className="font-display text-[clamp(2.4rem,5vw,4rem)] leading-none font-extrabold text-fg">
              <Notate type="circle" color="var(--color-coral)" padding={10} strokeWidth={2.5}>
                {fixed(ratio, 0)}×
              </Notate>{" "}
              less shuffle
            </p>
            <p className="max-w-md text-fg-2">
              groupByKey shuffled {bytes(op.groupByKey.shuffleBytes)} in {duration(op.groupByKey.millis)}; reduceByKey
              added the ones up inside each partition first and shuffled {bytes(op.reduceByKey.shuffleBytes)} in{" "}
              {duration(op.reduceByKey.millis)}. Bytes come from Spark&apos;s own task metrics, through a SparkListener.
            </p>
          </div>
        </div>
        <ShuffleChart
          runs={r.shuffle}
          title="Shuffle bytes written, per operation"
          subtitle="join vs broadcast: enriching every trip with its station name"
        />
        <WithNote
          note={
            <VivaNote>
              reduceByKey combines values inside each partition before the shuffle, like a combiner in MapReduce. So
              only one subtotal per station per partition crosses the network: {bytes(op.reduceByKey.shuffleBytes)} instead
              of {bytes(op.groupByKey.shuffleBytes)}.
            </VivaNote>
          }
        >
          <CodePanel id="rdd-shuffle" caption="the same count, two ways" />
        </WithNote>
      </Act>

      <Act
        n={2}
        title="The persistence race"
        lede="Caching is lazy: the first count computes and stores the partitions, the second reads them back. Serialized storage trades a little CPU for a third of the memory."
      >
        <PersistenceChart runs={r.persistence} />
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Uncached recount" value={duration(persist.NONE.secondCountMs)} note="recomputed from the file" />
          <KpiPuck label="MEMORY_ONLY" value={duration(persist.MEMORY_ONLY.secondCountMs)} note={`${bytes(persist.MEMORY_ONLY.memoryBytes)} of objects`} />
          <KpiPuck label="MEMORY_ONLY_SER" value={duration(persist.MEMORY_ONLY_SER.secondCountMs)} note={`${bytes(persist.MEMORY_ONLY_SER.memoryBytes)} serialized`} />
          <KpiPuck label="DISK_ONLY" value={duration(persist.DISK_ONLY.secondCountMs)} note={`${bytes(persist.DISK_ONLY.diskBytes)} on disk`} />
        </div>
        <CodePanel id="rdd-persistence" caption="one fresh lineage per storage level" />
      </Act>

      <Act
        n={3}
        title="The accumulator trap"
        lede="Shared variable #1. Counting lines with an accumulator inside an action is exact; inside a transformation, every recomputation counts again."
      >
        <div className="grid gap-6 md:grid-cols-2">
          <Glass className="p-6">
            <p className="font-hand text-xl text-mint">inside foreach (an action)</p>
            <p className="mt-1 text-5xl font-semibold text-fg">{int(r.accumulators.inAction)}</p>
            <p className="mt-2 text-sm text-fg-2">
              = {int(r.lines)} lines: {int(r.accumulators.headerLines)} headers + {int(r.trips)} trips +{" "}
              {int(r.accumulators.rejectedLines)} rejected lines
            </p>
          </Glass>
          <Glass className="relative p-6">
            <p className="font-hand text-xl text-coral">inside map, then two count() calls</p>
            <p className="mt-1 text-5xl font-semibold text-fg">
              <Notate type="strike-through" color="var(--color-coral)" iterations={1}>
                {int(r.accumulators.inTransformationAfterTwoActions)}
              </Notate>
            </p>
            <p className="mt-2 text-sm text-fg-2">
              exactly ×{fixed(r.accumulators.inTransformationAfterTwoActions / r.accumulators.inAction, 0)}: the uncached
              map ran once per action
            </p>
          </Glass>
        </div>
        <WithNote
          note={
            <VivaNote tilt={1.2}>
              Spark guarantees each task&apos;s accumulator update once only inside actions. In a transformation, a
              re-run task adds again, which is why I got exactly double.
            </VivaNote>
          }
        >
          <CodePanel id="rdd-accumulators" caption="the trap, reproduced on purpose" />
        </WithNote>
        <CodePanel id="rdd-broadcast" caption="shared variable #2: a broadcast lookup instead of a shuffle join" />
      </Act>

      <Act
        n={4}
        title="Lineage: the recipe for a lost partition"
        lede="toDebugString of the departures RDD, read bottom to top. If an executor dies, Spark replays exactly these steps for the missing partitions: fault tolerance without replicating the data."
      >
        <Glass className="p-6">
          <ol className="flex flex-col items-stretch gap-2 md:flex-row md:items-center">
            {lineage.map((s, i) => (
              <li key={s.rdd} className="flex items-center gap-2 md:flex-1">
                {s.afterShuffle && (
                  <span className="mx-1 flex flex-col items-center font-hand text-lg leading-tight text-coral">
                    <span aria-hidden>║</span>shuffle
                    <span className="text-sm text-fg-3">new stage</span>
                  </span>
                )}
                <div className={`glass-inset flex-1 rounded-xl px-3 py-2.5 ${s.cached ? "ring-1 ring-mint/60" : ""}`}>
                  <p className="font-mono text-xs text-fg-3">{s.rdd}</p>
                  <p className="font-display text-base font-bold text-fg">.{s.op}()</p>
                  {s.source && <p className="truncate font-mono text-[0.7rem] text-fg-3">{s.source}</p>}
                  {s.cached && <p className="mt-1 text-xs text-mint">{s.cached}</p>}
                  {s.partitions && <p className="text-xs text-fg-3">{s.partitions} partitions</p>}
                </div>
                {i < lineage.length - 1 && !lineage[i + 1].afterShuffle && (
                  <InkArrow width={30} height={20} from={[2, 12]} to={[28, 10]} bend={0} className="hidden shrink-0 md:block" />
                )}
              </li>
            ))}
          </ol>
        </Glass>
        <CodePanel id="rdd-lineage" caption="printing the lineage" />
      </Act>

      <Act n={5} title="Transformations and actions on the cached RDD" lede={`${int(r.inputPartitions)} input partitions, ${int(r.trips)} trips, ${int(r.members)} by members.`}>
        <HourlyChart hours={r.tripsByHour} />
      </Act>

      <Act
        n={6}
        title="RDD → DataFrame, three ways"
        lede="The same trips become a DataFrame by reflection on a case class, with an explicit schema, and from tuples; Spark SQL on the result agrees with the RDD count of member trips."
      >
        <div className="grid gap-4 md:grid-cols-4">
          {[
            ["reflection", "rdd.toDF() on case classes"],
            ["explicitSchema", "createDataFrame(rowRdd, schema)"],
            ["tuples", 'rdd.toDF("a", "b")'],
            ["sqlMembers", "SQL: member trips"],
          ].map(([key, how]) => (
            <Glass key={key} className="p-5">
              <p className="font-mono text-xs text-fg-3">{how}</p>
              <p className="mt-2 text-3xl font-semibold text-fg">{int(r.rddToDataFrame[key])}</p>
              <p className="text-xs text-fg-2">{key === "sqlMembers" ? `RDD API said ${int(r.members)}` : "rows"}</p>
            </Glass>
          ))}
        </div>
      </Act>

      <Ledger
        rows={[
          { label: "groupByKey shuffle", value: bytes(op.groupByKey.shuffleBytes), path: "rdd-fundamentals.json · shuffle[groupByKey].shuffleBytes" },
          { label: "reduceByKey shuffle", value: bytes(op.reduceByKey.shuffleBytes), path: "rdd-fundamentals.json · shuffle[reduceByKey].shuffleBytes" },
          { label: "Shuffle join vs broadcast", value: `${bytes(op.join.shuffleBytes)} vs ${bytes(op.broadcast.shuffleBytes)}`, path: "rdd-fundamentals.json · shuffle[join|broadcast]" },
          { label: "Accumulator (action / map)", value: `${int(r.accumulators.inAction)} / ${int(r.accumulators.inTransformationAfterTwoActions)}`, path: "rdd-fundamentals.json · accumulators" },
          { label: "Cached recount (MEMORY_ONLY)", value: duration(persist.MEMORY_ONLY.secondCountMs), path: "rdd-fundamentals.json · persistence[MEMORY_ONLY]" },
          { label: "Parsed trips", value: int(r.trips), path: "rdd-fundamentals.json · trips" },
        ]}
      />
    </>
  );
}
