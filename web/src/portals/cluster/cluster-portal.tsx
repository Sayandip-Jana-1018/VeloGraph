import { Glass, KpiPuck } from "@/components/inkglass/surfaces";
import { Act, Ledger, VivaNote, WithNote } from "@/components/portal/blocks";
import Link from "next/link";

import { getClusterSnapshot, getDqReport, getPipelineReports } from "@/lib/data";
import { compact, duration, fixed, int } from "@/lib/format";
import { STAGES, stageOf } from "@/lib/pipeline";

import { PipelineWaterfall, type TimelineJob } from "./cluster-charts";
import { ClusterTopology } from "./topology";

const where = (master: string) => (master.startsWith("spark://") ? "Docker cluster" : `host ${master}`);

const UIS = [
  { name: "Spark master", url: "http://localhost:8080", note: "workers, cores, running and finished applications" },
  { name: "Spark driver", url: "http://localhost:4040", note: "jobs, stages and the Structured Streaming tab of a running cluster job" },
  { name: "History server", url: "http://localhost:18080", note: "every finished application, from the event logs (history profile)" },
  { name: "Kafka UI", url: "http://localhost:8089", note: "brokers, topics, partitions, consumer groups, messages" },
];

export async function ClusterPortal() {
  const [snapshot, { batch }, dq] = await Promise.all([getClusterSnapshot(), getPipelineReports(), getDqReport()]);
  const jobs: TimelineJob[] = batch.map(({ data: j }) => {
    const stage = stageOf(j.job)!;
    return { job: j.job, phase: stage.phase, label: stage.label, ms: j.durationMs, where: where(j.master), appId: j.appId };
  });
  const totalMs = jobs.reduce((s, j) => s + j.ms, 0);
  const slowest = [...jobs].sort((a, b) => b.ms - a.ms)[0];
  const s = snapshot.data;
  const workers = s.spark.workers ?? [];
  const vmGb = (s.containers.vmMemoryMb ?? 0) / 1024;

  return (
    <>
      <Act
        n={1}
        title="Two clusters on one laptop"
        lede="A Spark standalone cluster (a master and two workers) and a three-broker Kafka cluster run in Docker, inside a 6 GB virtual machine. With the gateway running, this view follows them every 10 seconds; without it, it shows the recording below."
      >
        <ClusterTopology initial={s} recordedAt={snapshot.meta.generatedAt} />
        <WithNote
          note={
            <VivaNote>
              Spark: the driver plans the work, the master hands out the workers&apos; cores, and each worker runs an
              executor that does the tasks. Kafka: each partition has a leader broker that takes the writes and
              followers that copy it; the in-sync ones can take over. The controller (KRaft, no ZooKeeper) decides
              who leads.
            </VivaNote>
          }
        >
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            {UIS.map((u) => (
              <a key={u.name} href={u.url} target="_blank" rel="noreferrer" className="block">
                <Glass variant="inset" className="h-full p-4 transition-colors hover:bg-white/6">
                  <p className="flex items-center justify-between font-display text-sm font-bold text-fg">
                    {u.name}
                    <span className="font-mono text-[0.7rem] font-normal text-marker">{u.url.replace("http://", "")} ↗</span>
                  </p>
                  <p className="mt-1 text-xs text-fg-2">{u.note}</p>
                </Glass>
              </a>
            ))}
          </div>
        </WithNote>
      </Act>

      <Act
        n={2}
        title="The pipeline, end to end"
        lede={`${jobs.length} batch jobs turn the raw trip files into the lake, the insights, the graph, the features and the models. Each job writes a report when it ends (serving/jobs), so the whole run is measured, not estimated.`}
      >
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Batch jobs" value={int(jobs.length)} note={`in ${STAGES.length} phases`} />
          <KpiPuck label="End to end" value={duration(totalMs)} note="the sum of the jobs' run times" />
          <KpiPuck label="Longest job" value={duration(slowest.ms)} note={slowest.job} />
          <KpiPuck label="Raw rows in" value={compact(dq.data.inputRows)} note="trip records, June to September" />
        </div>
        <PipelineWaterfall jobs={jobs} phases={STAGES.map((st) => ({ phase: st.phase, label: st.label }))} />
        <Glass variant="inset" className="flex flex-wrap items-center justify-between gap-4 p-5 text-sm text-fg-2">
          <p className="max-w-3xl">
            Two more jobs run on the cluster but never end on their own: live scoring every 30 seconds, and the replay
            analytics. They are measured by their micro-batches, not their run time.
          </p>
          <Link href="/portal/streaming" className="clay inline-flex items-center rounded-full px-4 py-2 text-sm font-semibold text-fg hover:text-marker">
            Open the streaming portal
          </Link>
        </Glass>
      </Act>

      <Act
        n={3}
        title="Where each job runs, and why"
        lede="The same jar runs in both places; only the configuration changes (ARCHITECTURE §3). Scanning gigabytes of Parquet goes fastest on the laptop's own disk and 24 threads. The cluster is where the streams live and where the distributed run is seen in the Spark UIs."
      >
        <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
          <Glass variant="inset" className="p-5 text-sm text-fg-2">
            <p className="font-display text-base font-bold text-fg">On the host · local[20]</p>
            <ul className="mt-2 space-y-1">
              <li>Spark 3.5.8 on JDK 17, 20 of 24 threads, a driver of up to 8 GB.</li>
              <li>Native NTFS reads: bronze, silver and gold, GraphX, features and training.</li>
              <li>The Spark containers are stopped first, to keep RAM for the driver.</li>
            </ul>
          </Glass>
          <Glass variant="inset" className="p-5 text-sm text-fg-2">
            <p className="font-display text-base font-bold text-fg">On the Docker cluster · spark://spark-master:7077</p>
            <ul className="mt-2 space-y-1">
              <li>
                {workers.length} workers × {workers[0]?.cores ?? 2} cores, one {workers[0]?.memoryMb ?? 900} MB slot each; a
                768 MB executor per worker.
              </li>
              <li>The streaming jobs take 2 cores each (spark.cores.max), so both run side by side.</li>
              <li>Kafka: 3 brokers with a 256 MB heap each, inside the same {fixed(vmGb, 1)} GB VM.</li>
            </ul>
          </Glass>
        </div>
        <WithNote
          note={
            <VivaNote>
              Why not run everything on the cluster? Docker on Windows reads the shared folder slowly, and the VM has
              6 GB for Kafka, the master, two workers and their executors. So the multi-gigabyte batch work runs on the
              host and writes compact gold tables, and the cluster reads those, plus the live stream.
            </VivaNote>
          }
        >
          <Glass variant="inset" className="p-5 text-sm text-fg-2">
            <p className="font-display text-sm font-bold text-fg">The memory budget (ADR 009)</p>
            <p className="mt-2">
              The VM has {fixed(vmGb, 1)} GB. Every container has a hard cap: each broker 640 MB, Kafka UI 448 MB, the
              master 1,280 MB (it also hosts the client-mode driver), each worker 1,408 MB (its daemon plus one
              executor). With both streaming jobs running the recording shows{" "}
              {fixed((s.containers.containers ?? []).reduce((sum, c) => sum + c.memUsedMb, 0) / 1024, 2)} GB in use.
            </p>
          </Glass>
        </WithNote>
      </Act>

      <Ledger
        rows={[
          { label: "Batch jobs, end to end", value: `${jobs.length} jobs · ${duration(totalMs)}`, path: "jobs/*.json · durationMs" },
          { label: "Spark workers · cores · memory", value: `${workers.length} · ${s.spark.cores ?? 0} · ${int(s.spark.memoryMb ?? 0)} MB`, path: "cluster.json · spark" },
          { label: "Kafka brokers · controller", value: `${(s.kafka.brokers ?? []).length} · broker ${s.kafka.controller ?? "?"}`, path: "cluster.json · kafka" },
          { label: "Applications finished on the cluster", value: int(s.spark.completedApps ?? 0), path: "cluster.json · spark.completedApps" },
          { label: "Docker VM memory", value: `${fixed(vmGb, 1)} GB`, path: "cluster.json · containers.vmMemoryMb" },
        ]}
      />
    </>
  );
}
