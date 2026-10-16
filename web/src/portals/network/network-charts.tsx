"use client";

import { ChartCard } from "@/components/charts/chart-card";
import { BAR_END_H, SERIES, TEXT, tooltipRow } from "@/components/charts/theme";
import { escapeHtml, fixed, int, share } from "@/lib/format";

/** Modularity of the three partitions (one measure, nominal categories: slot 1, labelled at the tip). */
export function ModularityChart({ rows }: { rows: { method: string; q: number; communities: number }[] }) {
  const ordered = [...rows].reverse();
  return (
    <ChartCard
      title="Modularity Q of each partition"
      subtitle="Higher = more trip weight inside communities than a random graph with the same degrees would put there"
      height={220}
      option={{
        grid: { left: 8, right: 60, top: 8, bottom: 8, containLabel: true },
        xAxis: { type: "value", min: 0, max: 1, axisLabel: { formatter: (v: number) => fixed(v, 1) } },
        yAxis: { type: "category", data: ordered.map((r) => r.method) },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const r = ordered[p.dataIndex];
            return tooltipRow(SERIES[0], escapeHtml(`${r.method} · ${r.communities} communities`), `Q = ${fixed(r.q, 3)}`);
          },
        },
        series: [
          {
            type: "bar",
            data: ordered.map((r) => r.q),
            itemStyle: { color: SERIES[0], ...BAR_END_H },
            label: { show: true, position: "right", color: TEXT.primary, formatter: (p: { value: number }) => fixed(p.value, 3) },
          },
        ],
      }}
      table={{
        columns: ["Method", "Communities (≥ 3 stations)", "Modularity Q"],
        numeric: [false, true, true],
        rows: rows.map((r) => [r.method, r.communities, fixed(r.q, 3)]),
      }}
    />
  );
}

/** How many stations changed label in each superstep of label propagation. */
export function LabelChangesChart({ changes }: { changes: number[] }) {
  return (
    <ChartCard
      title="Label changes per superstep"
      subtitle="Stations take turns by id parity, so odd and even supersteps alternate; the residue is the classic synchronous-LPA flip-flop"
      height={240}
      option={{
        grid: { left: 8, right: 16, top: 16, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: changes.map((_, i) => i + 1), name: "superstep", nameLocation: "middle", nameGap: 26 },
        yAxis: { type: "value", axisLabel: { formatter: (v: number) => int(v) } },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number; value: number }) =>
            tooltipRow(SERIES[0], `superstep ${p.dataIndex + 1}`, `${int(p.value)} stations changed label`),
        },
        series: [{ type: "bar", data: changes, barMaxWidth: 14, itemStyle: { color: SERIES[0], borderRadius: [3, 3, 0, 0] } }],
      }}
      table={{
        columns: ["Superstep", "Stations that changed label"],
        numeric: [true, true],
        rows: changes.map((c, i) => [i + 1, int(c)]),
      }}
    />
  );
}

/** Where morning and evening riders end up, per district: two series, so a legend and labels. */
export function DistrictChart({ rows, sample }: { rows: { district: string; morningShare: number; eveningShare: number }[]; sample: number }) {
  return (
    <ChartCard
      title={`Share of the ${sample} highest-PageRank stations inside each district`}
      subtitle="PageRank on working-day 07–10 trips (morning) and 16–19 trips (evening)"
      height={260}
      option={{
        legend: { top: 0, left: 0, data: ["morning", "evening"] },
        grid: { left: 8, right: 16, top: 40, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: rows.map((r) => r.district) },
        yAxis: { type: "value", min: 0, max: 1, axisLabel: { formatter: (v: number) => share(v) } },
        tooltip: {
          trigger: "axis",
          axisPointer: { type: "shadow" },
          formatter: (items: { dataIndex: number }[]) => {
            const r = rows[items[0].dataIndex];
            return (
              `<div style="color:${TEXT.secondary};margin-bottom:4px">${escapeHtml(r.district)}</div>` +
              tooltipRow(SERIES[1], "morning", share(r.morningShare)) +
              tooltipRow(SERIES[0], "evening", share(r.eveningShare))
            );
          },
        },
        series: [
          {
            name: "morning",
            type: "bar",
            data: rows.map((r) => r.morningShare),
            itemStyle: { color: SERIES[1] },
            label: { show: true, position: "top", color: TEXT.primary, formatter: (p: { value: number }) => share(p.value) },
          },
          {
            name: "evening",
            type: "bar",
            data: rows.map((r) => r.eveningShare),
            itemStyle: { color: SERIES[0] },
            label: { show: true, position: "top", color: TEXT.secondary, formatter: (p: { value: number }) => share(p.value) },
          },
        ],
      }}
      table={{
        columns: ["District", "Morning share", "Evening share"],
        numeric: [false, true, true],
        rows: rows.map((r) => [r.district, share(r.morningShare), share(r.eveningShare)]),
      }}
    />
  );
}

/** Hops along strong ties to the nearest top-10 hub. */
export function HopsChart({ bins, unreachable }: { bins: { hops: number; stations: number }[]; unreachable: number }) {
  return (
    <ChartCard
      title="Hops to the nearest hub, along strong ties"
      subtitle={`GraphX ShortestPaths to the top-10 PageRank hubs; ${int(unreachable)} stations cannot reach one`}
      height={240}
      option={{
        grid: { left: 8, right: 16, top: 24, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: bins.map((b) => `${b.hops} hop${b.hops === 1 ? "" : "s"}`) },
        yAxis: { type: "value", axisLabel: { formatter: (v: number) => int(v) } },
        tooltip: { trigger: "item", formatter: (p: { name: string; value: number }) => tooltipRow(SERIES[0], p.name, `${int(p.value)} stations`) },
        series: [
          {
            type: "bar",
            data: bins.map((b) => b.stations),
            itemStyle: { color: SERIES[0], borderRadius: [4, 4, 0, 0] },
            label: { show: true, position: "top", color: TEXT.secondary, formatter: (p: { value: number }) => int(p.value) },
          },
        ],
      }}
      table={{
        columns: ["Hops", "Stations"],
        numeric: [true, true],
        rows: [...bins.map((b) => [b.hops, int(b.stations)] as (string | number)[]), ["unreachable", int(unreachable)]],
      }}
    />
  );
}
