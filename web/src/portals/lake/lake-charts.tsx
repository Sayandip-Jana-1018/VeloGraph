"use client";

import { BAR_END_H, SERIES, TEXT, tooltipRow } from "@/components/charts/theme";
import { ChartCard } from "@/components/charts/chart-card";
import { duration, escapeHtml, fixed, int } from "@/lib/format";

type Reason = { reason: string; rows: number; pct: number; meaning: string };

/** Why trips were quarantined: one series (slot 1), so no legend; every bar labelled at its tip. */
export function QuarantineChart({ reasons }: { reasons: Reason[] }) {
  const sorted = [...reasons].sort((a, b) => a.rows - b.rows); // ECharts draws the first category at the bottom
  return (
    <ChartCard
      title="Quarantined trips by reason"
      subtitle="Each trip gets the first rule it breaks; rules run in a fixed order"
      height={260}
      option={{
        grid: { left: 8, right: 64, top: 8, bottom: 8, containLabel: true },
        xAxis: { type: "value", axisLabel: { formatter: (v: number) => int(v) } },
        yAxis: { type: "category", data: sorted.map((r) => r.reason) },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const r = sorted[p.dataIndex];
            return (
              tooltipRow(SERIES[0], escapeHtml(r.reason), `${int(r.rows)} trips`) +
              `<div style="color:${TEXT.secondary};max-width:240px;white-space:normal">${escapeHtml(r.meaning)}</div>`
            );
          },
        },
        series: [
          {
            type: "bar",
            data: sorted.map((r) => r.rows),
            itemStyle: { color: SERIES[0], ...BAR_END_H },
            label: { show: true, position: "right", color: TEXT.secondary, formatter: (p: { value: number }) => int(p.value) },
          },
        ],
      }}
      table={{
        columns: ["Reason", "Trips", "Share of input", "Meaning"],
        numeric: [false, true, true, false],
        rows: [...reasons].map((r) => [r.reason, int(r.rows), `${fixed(r.pct, 3)} %`, r.meaning]),
      }}
    />
  );
}

/** The batch pipeline as run on 15 Oct: how long each job took (CO5, Spark workflows). */
export function PipelineChart({ jobs }: { jobs: { job: string; ms: number; output: string }[] }) {
  const ordered = [...jobs].reverse(); // first job at the top
  return (
    <ChartCard
      title="Every batch job, timed"
      subtitle="Host local[20]; each bar is one spark-submit, read from serving/jobs/<job>.json"
      height={360}
      option={{
        grid: { left: 8, right: 70, top: 8, bottom: 8, containLabel: true },
        xAxis: { type: "value", axisLabel: { formatter: (v: number) => `${v} s` } },
        yAxis: { type: "category", data: ordered.map((j) => j.job) },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const j = ordered[p.dataIndex];
            return (
              tooltipRow(SERIES[0], escapeHtml(j.job), duration(j.ms)) +
              `<div style="color:${TEXT.secondary}">${escapeHtml(j.output)}</div>`
            );
          },
        },
        series: [
          {
            type: "bar",
            data: ordered.map((j) => Math.round(j.ms / 100) / 10),
            itemStyle: { color: SERIES[0], ...BAR_END_H },
            label: { show: true, position: "right", color: TEXT.secondary, formatter: (p: { value: number }) => `${p.value} s` },
          },
        ],
      }}
      table={{
        columns: ["Job", "Duration", "Output"],
        numeric: [false, true, false],
        rows: jobs.map((j) => [j.job, duration(j.ms), j.output]),
      }}
    />
  );
}
