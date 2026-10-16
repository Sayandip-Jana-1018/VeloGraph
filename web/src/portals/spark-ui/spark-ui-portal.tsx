import { KpiPuck } from "@/components/inkglass/surfaces";
import { Act, CodePanel, VivaNote, WithNote } from "@/components/portal/blocks";
import { getNetworkSummary, getRddReport, getSparkUi, getSparkUiShots } from "@/lib/data";
import { bytes, duration, fixed, int } from "@/lib/format";

import { ShotWall, type Shot } from "./shot-wall";
import { AppsChart, JobTimelineChart, SuperstepJobsChart, TaskBoxplot } from "./spark-charts";
import { SqlPlan } from "./sql-plan";
import { StageFlow } from "./stage-flow";

const MiB = (n: number) => `${fixed(n / 1024 / 1024, 1)} MiB`;
const KiB = (n: number) => `${fixed(n / 1024, 1)} KiB`;
const sum = <T,>(xs: T[], f: (x: T) => number) => xs.reduce((s, x) => s + f(x), 0);

export async function SparkUiPortal() {
  const [{ data: ui }, { data: rdd }, { data: net }, files] = await Promise.all([
    getSparkUi(),
    getRddReport(),
    getNetworkSummary(),
    getSparkUiShots(),
  ]);
  const op = Object.fromEntries(rdd.shuffle.map((s) => [s.operation, s]));
  const serialized = rdd.persistence.find((p) => p.level === "MEMORY_ONLY_SER")!;
  const pr = net.pagerank.weighted;

  const shuffled = Object.fromEntries(ui.rdd.shuffles.map((j) => [j.group, sum(j.stages, (s) => s.shuffleWriteBytes)]));
  const taskSpread = ui.rdd.shuffles.flatMap((j) =>
    j.stages.map((s) => ({ label: `${j.group} · stage ${s.stageId}`, quantiles: s.taskRunTimeMs })),
  );
  const rddTasks = sum(ui.rdd.executors, (e) => e.tasks);
  const codegenStages = new Set(ui.sql.nodes.flatMap((n) => (n.codegen === null ? [] : [n.codegen]))).size;
  const firstRunMs = sum(ui.pregel.firstRun, (j) => j.durationMs);

  const captions: Record<string, Omit<Shot, "file">> = {
    "01-history-applications.png": {
      title: "Every run, on record",
      caption: "Each spark-submit writes an event log; the history server lists every application with its duration.",
      look: "one row per job of the pipeline",
      unit: "Unit I",
    },
    "02-groupbykey-job-dag.png": {
      title: "groupByKey: the DAG",
      caption: `textFile → flatMap → map run in one stage; the shuffle starts a second stage for groupByKey. Shuffle write: ${MiB(op.groupByKey.shuffleBytes)}.`,
      look: "the arrow crossing between the two pink stage boxes is the shuffle",
      unit: "Unit III",
    },
    "03-reducebykey-job-dag.png": {
      title: "reduceByKey: same shape",
      caption: "Two stages again, but values are combined inside each map task before anything is shuffled.",
      look: "same DAG, compare the Shuffle Write column",
      unit: "Unit III",
    },
    "04-groupbykey-shuffle-write.png": {
      title: "groupByKey: the bytes",
      caption: `The map-side stage writes ${MiB(op.groupByKey.shuffleBytes)} of (station, 1) records to the shuffle.`,
      look: "Shuffle Write in the summary metrics",
      unit: "Unit III",
    },
    "05-reducebykey-shuffle-write.png": {
      title: "reduceByKey: a fraction",
      caption: `With map-side combining the same stage writes only ${KiB(op.reduceByKey.shuffleBytes)}.`,
      look: `the same column, ${fixed(op.groupByKey.shuffleBytes / op.reduceByKey.shuffleBytes, 0)}× smaller`,
      unit: "Unit III",
    },
    "06-storage-cached-rdd.png": {
      title: "A cached RDD",
      caption: `trips ${rdd.month} persisted as MEMORY_ONLY_SER: every partition cached, ${MiB(serialized.memoryBytes)} in memory.`,
      look: "Fraction Cached 100 %",
      unit: "Unit III",
    },
    "07-sql-plan-with-exchanges.png": {
      title: "Catalyst's physical plan",
      caption: "The wildcard insight: Scan parquet → Filter → HashAggregate, compiled together by WholeStageCodegen.",
      look: "the Filter row counts: 106 stations for '%Park%'",
      unit: "Unit IV",
    },
    "08-graphx-pregel-supersteps.png": {
      title: "Pregel = one job per superstep",
      caption: `Weighted PageRank ran ${pr.supersteps} supersteps, each an isEmpty job of about ${fixed(pr.millis / pr.supersteps / 1000, 1)} s; skipped stages are cached lineage being reused.`,
      look: "isEmpty at Pregel.scala, over and over",
      unit: "Unit III",
    },
  };
  const shots: Shot[] = files.filter((f) => captions[f]).map((file) => ({ file, ...captions[file] }));

  return (
    <>
      <Act
        n={1}
        title="The history server, redrawn"
        lede="Every chart on this page is drawn from the Spark history server's REST API, the same data its own pages show. A script reads /api/v1 for each pipeline run and exports it; hover any mark, or switch a chart to its table."
      >
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Applications" count={{ to: ui.applications.length }} note="one per pipeline job" />
          <KpiPuck label="Pipeline time" value={duration(sum(ui.applications, (a) => a.durationMs))} note="sum of all runs" />
          <KpiPuck label="Spark" value={ui.applications[0].sparkVersion} note="local[20] on the host" />
        </div>
        <AppsChart apps={ui.applications} />
      </Act>

      <Act
        n={2}
        title="Every job of the RDD lab"
        lede={`Each action (count, collect, foreach) starts a job: ${ui.rdd.jobs.length} jobs and ${int(rddTasks)} tasks in one application. The four coloured jobs are the shuffle experiments taken apart in the next act.`}
      >
        <JobTimelineChart jobs={ui.rdd.jobs} />
      </Act>

      <Act
        n={3}
        title="The shuffle, stage by stage"
        lede={`An arrow is a shuffle boundary. groupByKey ships every record (${bytes(shuffled.groupByKey)}); reduceByKey combines inside each map task first and ships ${bytes(shuffled.reduceByKey)}, ${fixed(shuffled.groupByKey / shuffled.reduceByKey, 0)}× less. A broadcast join ships nothing.`}
      >
        <StageFlow shuffles={ui.rdd.shuffles} />
        <TaskBoxplot stages={taskSpread} />
      </Act>

      <Act
        n={4}
        title="Catalyst's physical plan"
        lede={`SQL query ${ui.sql.executionId} of the insights job (${duration(ui.sql.durationMs)}): ${ui.sql.nodes.length} operators, ${ui.sql.exchanges} exchanges and ${codegenStages} whole-stage codegen blocks. Data flows top to bottom; hover an operator for its metrics.`}
      >
        <SqlPlan plan={ui.sql} />
        <div className="flex justify-center">
          <VivaNote tilt={1}>
            An Exchange is a shuffle inside a SQL plan. The tinted boxes are WholeStageCodegen: Catalyst fuses those
            operators into one generated Java function, so rows are never handed from one operator object to the next.
          </VivaNote>
        </div>
      </Act>

      <Act
        n={5}
        title="Pregel is a loop of Spark jobs"
        lede={`The GraphX job ran ${ui.pregel.totalJobs} Spark jobs; ${ui.pregel.superstepJobs} of them are Pregel's per-superstep check for remaining messages. The first PageRank run alone is ${ui.pregel.firstRun.length} of them, ${duration(firstRunMs)} in total.`}
      >
        <SuperstepJobsChart steps={ui.pregel.firstRun} />
      </Act>

      <Act
        n={6}
        title="Pinned up: the original pages"
        lede="The same evidence as Spark itself drew it. A script asks the REST API which job and stage to open, then captures the history server's own pages."
      >
        <ShotWall shots={shots} />
      </Act>

      <Act n={7} title="How the shuffle bytes were measured" lede="The RDD job counts shuffle bytes itself, with a SparkListener, and they match the history server to the byte.">
        <WithNote
          note={
            <VivaNote>
              A shuffle is where Spark cuts a job into stages: data is written to disk on the map side and fetched over
              the network by the reduce side. The UI shows it as the arrow between two stage boxes.
            </VivaNote>
          }
        >
          <CodePanel id="sparkui-shuffle-meter" caption="a SparkListener adds up shuffle bytes per task" />
        </WithNote>
      </Act>
    </>
  );
}
