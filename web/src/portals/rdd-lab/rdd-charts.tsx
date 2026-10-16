"use client";

import { BAR_END_H, SERIES, TEXT, tooltipRow } from "@/components/charts/theme";
import { ChartCard } from "@/components/charts/chart-card";
import { bytes, duration, escapeHtml, int } from "@/lib/format";

type Shuffle = { operation: string; millis: number; shuffleBytes: number; result: number };

/** Bytes written to the shuffle by each operation (nominal categories, one series: slot 1, no legend). */
export function ShuffleChart({ runs, title, subtitle }: { runs: Shuffle[]; title: string; subtitle: string }) {
  const ordered = [...runs].reverse();
  return (
    <ChartCard
      title={title}
      subtitle={subtitle}
      height={60 + runs.length * 52}
      option={{
        grid: { left: 8, right: 86, top: 4, bottom: 8, containLabel: true },
        xAxis: { type: "value", axisLabel: { formatter: (v: number) => bytes(v) } },
        yAxis: { type: "category", data: ordered.map((r) => r.operation) },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const r = ordered[p.dataIndex];
            return (
              tooltipRow(SERIES[0], escapeHtml(r.operation), bytes(r.shuffleBytes)) +
              `<div style="color:${TEXT.secondary}">${duration(r.millis)} · ${int(r.result)} results</div>`
            );
          },
        },
        series: [
          {
            type: "bar",
            data: ordered.map((r) => r.shuffleBytes),
            itemStyle: { color: SERIES[0], ...BAR_END_H },
            label: {
              show: true,
              position: "right",
              color: TEXT.secondary,
              formatter: (p: { value: number }) => bytes(p.value),
            },
          },
        ],
      }}
      table={{
        columns: ["Operation", "Shuffle written", "Time", "Result size"],
        numeric: [false, true, true, true],
        rows: runs.map((r) => [r.operation, bytes(r.shuffleBytes), duration(r.millis), int(r.result)]),
      }}
    />
  );
}

type Persist = { level: string; firstCountMs: number; secondCountMs: number; memoryBytes: number; diskBytes: number };

/** First vs second count per storage level: two series, so a legend plus labels on the second count. */
export function PersistenceChart({ runs }: { runs: Persist[] }) {
  return (
    <ChartCard
      title="Count it twice: what caching buys"
      subtitle="Each storage level gets a fresh lineage; the second count shows what reuse costs"
      height={320}
      option={{
        legend: { top: 0, left: 0, data: ["first count (computes, and caches)", "second count (reuses)"] },
        grid: { left: 8, right: 16, top: 40, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: runs.map((r) => r.level) },
        yAxis: { type: "value", axisLabel: { formatter: (v: number) => duration(v) } },
        tooltip: {
          trigger: "axis",
          axisPointer: { type: "shadow" },
          formatter: (items: { dataIndex: number }[]) => {
            const r = runs[items[0].dataIndex];
            return (
              `<div style="color:${TEXT.secondary};margin-bottom:4px">${escapeHtml(r.level)}</div>` +
              tooltipRow(SERIES[0], "first count", duration(r.firstCountMs)) +
              tooltipRow(SERIES[1], "second count", duration(r.secondCountMs)) +
              `<div style="color:${TEXT.secondary};margin-top:4px">stored: ${bytes(r.memoryBytes)} in memory, ${bytes(r.diskBytes)} on disk</div>`
            );
          },
        },
        series: [
          {
            name: "first count (computes, and caches)",
            type: "bar",
            data: runs.map((r) => r.firstCountMs),
            itemStyle: { color: SERIES[0] },
          },
          {
            name: "second count (reuses)",
            type: "bar",
            data: runs.map((r) => r.secondCountMs),
            itemStyle: { color: SERIES[1] },
            label: {
              show: true,
              position: "top",
              color: TEXT.primary,
              fontWeight: 600,
              formatter: (p: { value: number }) => duration(p.value),
            },
          },
        ],
        barGap: "12%",
      }}
      table={{
        columns: ["Level", "First count", "Second count", "In memory", "On disk"],
        numeric: [false, true, true, true, true],
        rows: runs.map((r) => [
          r.level,
          duration(r.firstCountMs),
          duration(r.secondCountMs),
          bytes(r.memoryBytes),
          bytes(r.diskBytes),
        ]),
      }}
    />
  );
}

/** Trips per start hour from the RDD job (one series, line with the peak labelled). */
export function HourlyChart({ hours }: { hours: { hour: number; trips: number }[] }) {
  const peak = hours.reduce((a, b) => (b.trips > a.trips ? b : a));
  return (
    <ChartCard
      title="Trips by start hour, September 2026"
      subtitle="map + reduceByKey on the cached RDD of parsed trips"
      height={280}
      option={{
        grid: { left: 8, right: 24, top: 28, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: hours.map((h) => `${h.hour}:00`), boundaryGap: false },
        yAxis: { type: "value", axisLabel: { formatter: (v: number) => int(v) } },
        tooltip: {
          trigger: "axis",
          formatter: (items: { dataIndex: number }[]) => {
            const h = hours[items[0].dataIndex];
            return tooltipRow(SERIES[0], `${h.hour}:00 – ${h.hour}:59`, `${int(h.trips)} trips`);
          },
        },
        series: [
          {
            type: "line",
            data: hours.map((h) => h.trips),
            lineStyle: { color: SERIES[0], width: 2 },
            itemStyle: { color: SERIES[0], borderColor: "#1c1938", borderWidth: 2 },
            areaStyle: { color: "rgba(57, 135, 229, 0.10)" },
            showSymbol: false,
            markPoint: {
              symbol: "circle",
              symbolSize: 10,
              itemStyle: { color: SERIES[0], borderColor: "#1c1938", borderWidth: 2 },
              label: { show: true, position: "top", color: TEXT.primary, formatter: `peak ${peak.hour}:00 · ${int(peak.trips)}` },
              data: [{ coord: [`${peak.hour}:00`, peak.trips] }],
            },
          },
        ],
      }}
      table={{
        columns: ["Hour", "Trips"],
        numeric: [false, true],
        rows: hours.map((h) => [`${h.hour}:00`, int(h.trips)]),
      }}
    />
  );
}
