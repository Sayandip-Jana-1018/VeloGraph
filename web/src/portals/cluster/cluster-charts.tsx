"use client";

import { ChartCard } from "@/components/charts/chart-card";
import { BAR_END_H, SERIES, TEXT, tooltipRow } from "@/components/charts/theme";
import { duration, fixed } from "@/lib/format";

export type TimelineJob = { job: string; phase: string; label: string; ms: number; where: string; appId: string };

/**
 * The batch pipeline end to end, as if run in one go: each job starts where the one before it ended, coloured by
 * roadmap phase (fixed categorical order). The real runs were spread over three days of building.
 */
export function PipelineWaterfall({ jobs, phases }: { jobs: TimelineJob[]; phases: { phase: string; label: string }[] }) {
  const minutes = jobs.map((j) => j.ms / 60_000);
  const offsets = minutes.map((_, i) => minutes.slice(0, i).reduce((s, m) => s + m, 0));
  const total = minutes.reduce((s, m) => s + m, 0);
  const color = (phase: string) => SERIES[phases.findIndex((p) => p.phase === phase) % SERIES.length];
  return (
    <ChartCard
      title={`The batch pipeline end to end: ${fixed(total, 1)} minutes`}
      subtitle="Each job starts where the one before it ended; colour is the roadmap phase"
      height={Math.max(320, 26 * jobs.length + 60)}
      option={{
        grid: { left: 8, right: 64, top: 32, bottom: 8, containLabel: true },
        legend: { top: 0, data: phases.map((p) => `${p.phase} · ${p.label}`) },
        xAxis: { type: "value", axisLabel: { formatter: (v: number) => `${v} min` } },
        yAxis: { type: "category", inverse: true, data: jobs.map((j) => j.job), axisLabel: { color: TEXT.secondary } },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number; seriesName: string }) => {
            const j = jobs[p.dataIndex];
            return (
              tooltipRow(color(j.phase), `${j.job} · ${j.phase}`, duration(j.ms)) +
              `<div style="color:${TEXT.secondary}">${j.where} · ${j.appId}</div>` +
              `<div style="color:${TEXT.secondary}">minute ${fixed(offsets[p.dataIndex], 1)} → ${fixed(offsets[p.dataIndex] + minutes[p.dataIndex], 1)}</div>`
            );
          },
        },
        series: [
          // The invisible part of each row: where the job starts.
          { name: "start", type: "bar", stack: "t", silent: true, itemStyle: { color: "transparent" }, data: offsets, tooltip: { show: false } },
          ...phases.map((p) => ({
            name: `${p.phase} · ${p.label}`,
            type: "bar",
            stack: "t",
            barMaxWidth: 16,
            itemStyle: { color: color(p.phase), ...BAR_END_H },
            data: jobs.map((j, i) => (j.phase === p.phase ? minutes[i] : "-")),
            label: {
              show: true,
              position: "right",
              color: TEXT.secondary,
              formatter: (x: { dataIndex: number }) => duration(jobs[x.dataIndex].ms),
            },
          })),
        ],
      }}
      table={{
        columns: ["Job", "Phase", "Ran on", "Duration", "Minutes", "Spark app"],
        numeric: [false, false, false, true, false, false],
        rows: jobs.map((j, i) => [
          j.job,
          `${j.phase} · ${j.label}`,
          j.where,
          duration(j.ms),
          `${fixed(offsets[i], 1)} → ${fixed(offsets[i] + minutes[i], 1)}`,
          j.appId,
        ]),
      }}
    />
  );
}
