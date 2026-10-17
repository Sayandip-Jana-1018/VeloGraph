"use client";

import { useEffect, useState } from "react";

import { SERIES } from "@/components/charts/theme";
import { Glass } from "@/components/inkglass/surfaces";
import { cn } from "@/lib/cn";
import { duration, fixed, int } from "@/lib/format";
import { ClusterLive, type ClusterData, type SparkApp } from "@/lib/schemas";
import { GATEWAY } from "@/portals/streaming/use-live";

type Connection = "connecting" | "live" | "recorded";

/** Broker identity: the validated categorical order, broker 1 in slot 1. Each cell also prints the number. */
const brokerColor = (id: number) => SERIES[(id - 1) % SERIES.length];

const appName = (name: string | null) => (name ?? "?").replace(/^velograph-/, "");

const TOPIC_LABEL: Record<string, string> = {
  "velograph.gbfs.station-status.v1": "station-status",
  "velograph.weather.current.v1": "weather (compacted)",
  "velograph.trips.replay.v1": "trips.replay",
  "velograph.trip-metrics.v1": "trip-metrics",
  "velograph.predictions.v1": "predictions",
  "velograph.alerts.v1": "alerts",
  "velograph.dlq.v1": "dlq",
};

/**
 * The cluster as the gateway sees it now: every 10 s while it answers, every 30 s while it does not (then the
 * recorded snapshot stays on screen).
 */
function useCluster(initial: ClusterData) {
  const [data, setData] = useState(initial);
  const [connection, setConnection] = useState<Connection>("connecting");
  const [at, setAt] = useState<string | null>(null);
  useEffect(() => {
    let alive = true;
    let timer = 0;
    const refresh = async () => {
      let next = 30_000;
      try {
        const response = await fetch(`${GATEWAY}/api/cluster`, { cache: "no-store" });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const parsed = ClusterLive.safeParse(await response.json());
        if (!parsed.success) throw new Error("the cluster report does not match its contract");
        if (!alive) return;
        setData(parsed.data);
        setAt(parsed.data.generatedAt);
        setConnection("live");
        next = 10_000;
      } catch {
        if (alive) setConnection("recorded");
      }
      if (alive) timer = window.setTimeout(refresh, next);
    };
    void refresh();
    return () => {
      alive = false;
      window.clearTimeout(timer);
    };
  }, []);
  return { data, connection, at };
}

function Bar({ used, total, label }: { used: number; total: number; label: string }) {
  const share = total > 0 ? Math.min(1, used / total) : 0;
  return (
    <div>
      <div className="flex justify-between text-[0.7rem]">
        <span className="text-fg-3">{label}</span>
        <span className="text-fg-2 tabular-nums">
          {int(used)} / {int(total)}
        </span>
      </div>
      <div className="mt-1 h-1.5 rounded-full bg-white/8" role="img" aria-label={`${label}: ${int(used)} of ${int(total)}`}>
        <div className="h-full rounded-full bg-teal" style={{ width: `${share * 100}%` }} />
      </div>
    </div>
  );
}

function AppRow({ app, running }: { app: SparkApp; running: boolean }) {
  return (
    <li className="flex items-center justify-between gap-3 rounded-xl px-2 py-1.5 text-xs hover:bg-white/5">
      <span className="flex min-w-0 items-center gap-2">
        <span className={cn("size-1.5 shrink-0 rounded-full", running ? "animate-pulse bg-mint" : "bg-white/25")} aria-hidden />
        <span className="truncate text-fg">{appName(app.name)}</span>
      </span>
      <span className="shrink-0 text-fg-3 tabular-nums">
        {app.cores ?? "?"} cores · {app.memoryPerExecutorMb ?? "?"} MB · {app.durationMs !== null ? duration(app.durationMs) : "—"}
      </span>
    </li>
  );
}

function SparkPanel({ spark }: { spark: ClusterData["spark"] }) {
  if (!spark.ok) {
    return (
      <Glass variant="inset" className="p-5 text-sm text-fg-2">
        <p className="font-display text-base font-bold text-fg">Spark standalone</p>
        <p className="mt-2">The master is not answering ({spark.error}). Start it with the spark profile of infra/docker-compose.yml.</p>
      </Glass>
    );
  }
  return (
    <Glass variant="inset" className="space-y-4 p-5">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <p className="font-display text-base font-bold text-fg">Spark standalone</p>
        <span className="font-mono text-[0.7rem] text-fg-3">
          {spark.url} · {spark.status}
        </span>
      </div>
      <div className="grid gap-3 sm:grid-cols-2">
        {(spark.workers ?? []).map((w, i) => (
          <div key={w.id} className="clay rounded-2xl p-3.5">
            <p className="flex items-center justify-between text-sm font-semibold text-fg">
              worker {i + 1}
              <span className="rounded-full border border-mint/50 px-2 py-0.5 text-[0.62rem] font-semibold text-mint">{w.state}</span>
            </p>
            <p className="font-mono text-[0.65rem] text-fg-3">{w.host}</p>
            <div className="mt-2.5 space-y-2">
              <Bar used={w.coresUsed} total={w.cores} label="cores in use" />
              <Bar used={w.memoryUsedMb} total={w.memoryMb} label="executor memory, MB" />
            </div>
          </div>
        ))}
      </div>
      <div>
        <p className="text-[0.68rem] font-semibold tracking-[0.14em] text-fg-3 uppercase">Running now</p>
        {(spark.activeApps ?? []).length === 0 ? (
          <p className="mt-1 text-xs text-fg-3">No application is running.</p>
        ) : (
          <ul className="mt-1">
            {(spark.activeApps ?? []).map((a) => (
              <AppRow key={a.id} app={a} running />
            ))}
          </ul>
        )}
      </div>
      <div>
        <p className="text-[0.68rem] font-semibold tracking-[0.14em] text-fg-3 uppercase">
          Finished on this cluster ({int(spark.completedApps ?? 0)})
        </p>
        <ul className="mt-1 max-h-44 overflow-auto pr-1">
          {(spark.completed ?? []).map((a) => (
            <AppRow key={a.id} app={a} running={false} />
          ))}
        </ul>
      </div>
    </Glass>
  );
}

function KafkaPanel({ kafka }: { kafka: ClusterData["kafka"] }) {
  if (!kafka.ok) {
    return (
      <Glass variant="inset" className="p-5 text-sm text-fg-2">
        <p className="font-display text-base font-bold text-fg">Kafka</p>
        <p className="mt-2">No broker is answering ({kafka.error}).</p>
      </Glass>
    );
  }
  const topics = kafka.topics ?? [];
  const leads = new Map<number, number>();
  for (const t of topics) for (const p of t.partitions) leads.set(p.leader, (leads.get(p.leader) ?? 0) + 1);
  const width = Math.max(...topics.map((t) => t.partitions.length));
  const lag = kafka.gatewayLag ? Object.values(kafka.gatewayLag.lag).reduce((s, v) => s + v, 0) : null;
  return (
    <Glass variant="inset" className="space-y-4 p-5">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <p className="font-display text-base font-bold text-fg">Kafka · KRaft</p>
        <span className="font-mono text-[0.7rem] text-fg-3">cluster {kafka.clusterId}</span>
      </div>
      <div className="grid gap-3 sm:grid-cols-3">
        {(kafka.brokers ?? []).map((b) => (
          <div key={b.id} className="clay rounded-2xl p-3.5">
            <p className="flex items-center gap-2 text-sm font-semibold whitespace-nowrap text-fg">
              <span className="grid size-5 shrink-0 place-items-center rounded-full text-[0.65rem] font-bold text-ink-950" style={{ background: brokerColor(b.id) }}>
                {b.id}
              </span>
              broker {b.id}
            </p>
            <p className="mt-0.5 font-mono text-[0.65rem] text-fg-3">
              {b.host}:{b.port}
            </p>
            <p className="mt-2 flex flex-wrap items-center gap-1.5 text-xs text-fg-2">
              leads {int(leads.get(b.id) ?? 0)} partitions
              {kafka.controller === b.id && (
                <span className="rounded-full border border-marker/60 px-2 py-0.5 text-[0.6rem] font-semibold text-marker">controller</span>
              )}
            </p>
          </div>
        ))}
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <caption className="sr-only">Leader broker and in-sync replicas of every partition</caption>
          <thead>
            <tr>
              <th className="py-1 pr-3 text-left font-semibold text-fg-3">topic</th>
              {Array.from({ length: width }, (_, i) => (
                <th key={i} className="px-1 py-1 text-center font-mono font-normal text-fg-3">
                  p{i}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {topics.map((t) => (
              <tr key={t.name} className="border-t border-white/5">
                <td className="py-1.5 pr-3 whitespace-nowrap text-fg-2">{TOPIC_LABEL[t.name] ?? t.name}</td>
                {Array.from({ length: width }, (_, i) => {
                  const p = t.partitions[i];
                  if (!p) return <td key={i} />;
                  const full = p.isr.length === p.replicas.length;
                  return (
                    <td key={i} className="px-1 py-1 text-center" title={`leader ${p.leader} · replicas ${p.replicas.join(", ")} · in sync ${p.isr.join(", ")}`}>
                      <span
                        className="mx-auto grid size-6 place-items-center rounded-lg text-[0.7rem] font-bold text-ink-950"
                        style={{ background: p.leader > 0 ? brokerColor(p.leader) : "#4a4766" }}
                      >
                        {p.leader > 0 ? p.leader : "–"}
                      </span>
                      <span className={cn("mt-0.5 block text-[0.6rem] tracking-[0.1em]", full ? "text-fg-3" : "text-coral")}>
                        {"●".repeat(p.isr.length)}
                        {"○".repeat(Math.max(0, p.replicas.length - p.isr.length))}
                      </span>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-fg-3">
        Cell: the partition&apos;s leader broker; dots: its replicas, filled when in sync (● ● ● = all 3).
        {lag !== null && ` Gateway consumer group lag: ${int(lag)} messages.`}
      </p>
    </Glass>
  );
}

function ContainerPanel({ docker }: { docker: ClusterData["containers"] }) {
  if (!docker.ok || !docker.containers) {
    return <p className="text-center text-xs text-fg-3">Container memory is not available ({docker.error}).</p>;
  }
  const used = docker.containers.reduce((s, c) => s + c.memUsedMb, 0);
  const vm = docker.vmMemoryMb ?? 0;
  return (
    <Glass variant="inset" className="p-5">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <p className="font-display text-base font-bold text-fg">Memory of the Docker VM</p>
        <span className="text-xs text-fg-2 tabular-nums">
          {fixed(used / 1024, 2)} of {fixed(vm / 1024, 1)} GB in use · capped per container (ADR 009)
        </span>
      </div>
      <div className="mt-3 grid gap-x-6 gap-y-2.5 sm:grid-cols-2 lg:grid-cols-4">
        {docker.containers.map((c) => (
          <Bar key={c.name} used={c.memUsedMb} total={c.memLimitMb} label={`${c.name}, MB`} />
        ))}
      </div>
    </Glass>
  );
}

/** Spark and Kafka side by side, then the containers they run in. */
export function ClusterTopology({ initial, recordedAt }: { initial: ClusterData; recordedAt: string }) {
  const { data, connection, at } = useCluster(initial);
  const live = connection === "live";
  return (
    <div className="space-y-5">
      <div className="flex justify-center">
        <span
          className={cn(
            "inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-semibold",
            live ? "border-mint/60 text-mint" : "border-white/15 text-fg-2",
          )}
          title={live ? undefined : `The gateway at ${GATEWAY} is not answering: this is the recorded snapshot.`}
        >
          <span className={cn("size-2 rounded-full bg-current", live && "animate-pulse")} aria-hidden />
          {live
            ? `Live from the gateway · ${new Date(at!).toLocaleTimeString("en-GB")} · every 10 s`
            : connection === "connecting"
              ? "Connecting to the gateway…"
              : `Recorded ${new Date(recordedAt).toLocaleString("en-GB", { dateStyle: "medium", timeStyle: "short" })}`}
        </span>
      </div>
      <div className="grid items-start gap-5 lg:grid-cols-2">
        <SparkPanel spark={data.spark} />
        <KafkaPanel kafka={data.kafka} />
      </div>
      <ContainerPanel docker={data.containers} />
    </div>
  );
}
