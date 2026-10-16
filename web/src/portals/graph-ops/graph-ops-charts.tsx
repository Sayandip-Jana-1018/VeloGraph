"use client";

import { ChartCard } from "@/components/charts/chart-card";
import { BAR_END_H, SERIES, TEXT, tooltipRow } from "@/components/charts/theme";
import { duration, escapeHtml, fixed, int } from "@/lib/format";

type Partitioning = { strategy: string; partitions: number; replicationFactor: number; edgeImbalance: number; maxEdgesPerPartition: number };

/** Vertex copies per strategy: one measure, nominal categories (slot 1), labelled at the tip. */
export function ReplicationChart({ runs }: { runs: Partitioning[] }) {
  const ordered = [...runs].reverse();
  return (
    <ChartCard
      title="Copies of each vertex, by partitioning strategy"
      subtitle={`The same ${runs[0].partitions} partitions; fewer copies = less memory and less vertex data shipped per superstep`}
      height={240}
      option={{
        grid: { left: 8, right: 60, top: 8, bottom: 8, containLabel: true },
        xAxis: { type: "value", min: 0, axisLabel: { formatter: (v: number) => fixed(v, 0) } },
        yAxis: { type: "category", data: ordered.map((r) => r.strategy) },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const r = ordered[p.dataIndex];
            return (
              tooltipRow(SERIES[0], escapeHtml(r.strategy), `${fixed(r.replicationFactor, 2)} copies`) +
              `<div style="color:${TEXT.secondary}">largest partition: ${fixed(r.edgeImbalance, 2)}× the mean</div>`
            );
          },
        },
        series: [
          {
            type: "bar",
            data: ordered.map((r) => r.replicationFactor),
            itemStyle: { color: SERIES[0], ...BAR_END_H },
            label: { show: true, position: "right", color: TEXT.primary, formatter: (p: { value: number }) => fixed(p.value, 1) },
          },
        ],
      }}
      table={{
        columns: ["Strategy", "Copies per vertex", "Edge imbalance", "Largest partition"],
        numeric: [false, true, true, true],
        rows: runs.map((r) => [r.strategy, fixed(r.replicationFactor, 2), `${fixed(r.edgeImbalance, 2)}×`, `${int(r.maxEdgesPerPartition)} edges`]),
      }}
    />
  );
}

/** A distribution: stations per bin (one series). */
export function HistogramChart({
  title,
  subtitle,
  bins,
}: {
  title: string;
  subtitle: string;
  /** Bins already labelled on the server (a function cannot cross into a client component). */
  bins: { name: string; stations: number }[];
}) {
  const names = bins.map((b) => b.name);
  return (
    <ChartCard
      title={title}
      subtitle={subtitle}
      height={260}
      option={{
        grid: { left: 8, right: 16, top: 16, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: names, axisLabel: { rotate: names.length > 12 ? 40 : 0 } },
        yAxis: { type: "value", axisLabel: { formatter: (v: number) => int(v) } },
        tooltip: { trigger: "item", formatter: (p: { name: string; value: number }) => tooltipRow(SERIES[0], escapeHtml(p.name), `${int(p.value)} stations`) },
        series: [{ type: "bar", data: bins.map((b) => b.stations), barMaxWidth: 24, barCategoryGap: "8%", itemStyle: { color: SERIES[0], borderRadius: [4, 4, 0, 0] } }],
      }}
      table={{ columns: ["Range", "Stations"], numeric: [false, true], rows: bins.map((b, i) => [names[i], int(b.stations)]) }}
    />
  );
}

/** Wall-clock time of each GraphX step of the job. */
export function TimingsChart({ timings }: { timings: [string, number][] }) {
  const ordered = [...timings].reverse();
  return (
    <ChartCard
      title="Where the GraphX job spends its time"
      subtitle="Host local[20], all of Jun–Sep 2026 (graph-metrics job report)"
      height={420}
      option={{
        grid: { left: 8, right: 64, top: 8, bottom: 8, containLabel: true },
        xAxis: { type: "value", axisLabel: { formatter: (v: number) => `${v} s` } },
        yAxis: { type: "category", data: ordered.map(([k]) => k) },
        tooltip: { trigger: "item", formatter: (p: { name: string; value: number }) => tooltipRow(SERIES[0], escapeHtml(p.name), duration(p.value * 1000)) },
        series: [
          {
            type: "bar",
            data: ordered.map(([, ms]) => Math.round(ms / 100) / 10),
            itemStyle: { color: SERIES[0], ...BAR_END_H },
            label: { show: true, position: "right", color: TEXT.secondary, formatter: (p: { value: number }) => `${p.value} s` },
          },
        ],
      }}
      table={{ columns: ["Step", "Time"], numeric: [false, true], rows: timings.map(([k, ms]) => [k, duration(ms)]) }}
    />
  );
}

