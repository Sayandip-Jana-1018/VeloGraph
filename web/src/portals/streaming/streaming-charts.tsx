"use client";

import { ChartCard } from "@/components/charts/chart-card";
import { CONTEXT_SERIES, HIGHLIGHT, SERIES, TEXT, tooltipRow } from "@/components/charts/theme";
import { fixed, int } from "@/lib/format";
import type { DrillSample, ProducerStats, ProgressEntry, ReplayAnalytics } from "@/lib/schemas";

const axisTime = (iso: string) => iso.slice(11, 16);

/** Keyed by station: how the station-status messages spread over the topic's six partitions. */
export function PartitionsChart({ byPartition, topic }: { byPartition: ProducerStats["data"]["byPartition"]; topic: string }) {
  const rows = byPartition.filter((p) => p.topic === topic).sort((a, b) => a.partition - b.partition);
  const total = rows.reduce((s, r) => s + r.messages, 0);
  return (
    <ChartCard
      title="Messages per partition"
      subtitle="The key is the station, so a station's updates always land in the same partition, in order"
      height={240}
      option={{
        grid: { left: 8, right: 16, top: 24, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: rows.map((r) => `p${r.partition}`) },
        yAxis: { type: "value", axisLabel: { formatter: (v: number) => int(v) } },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) =>
            tooltipRow(SERIES[0], `partition ${rows[p.dataIndex].partition}`, `${int(rows[p.dataIndex].messages)} messages`),
        },
        series: [
          {
            type: "bar",
            data: rows.map((r) => r.messages),
            itemStyle: { color: SERIES[0] },
            label: { show: true, position: "top", color: TEXT.secondary, formatter: (p: { value: number }) => `${fixed((p.value / total) * 100, 1)} %` },
          },
        ],
      }}
      table={{
        columns: ["Partition", "Messages", "Share"],
        numeric: [true, true, true],
        rows: rows.map((r) => [r.partition, int(r.messages), `${fixed((r.messages / total) * 100, 1)} %`]),
      }}
    />
  );
}

/** Rows each micro-batch took in, and how long the broker -> snapshot trip of its oldest row took. */
export function BatchCharts({ batches, limitS }: { batches: ProgressEntry[]; limitS: number }) {
  const scored = batches.filter((b) => b.scoring);
  const withRows = scored.filter((b) => b.inputRows > 0);
  const latencies = withRows.map((b) => b.scoring!.latencyS).sort((a, b) => a - b);
  const p95 = latencies.length ? latencies[Math.min(latencies.length - 1, Math.floor(0.95 * latencies.length))] : 0;
  return (
    <div className="grid gap-5 lg:grid-cols-2">
      <ChartCard
        title="Rows per micro-batch"
        subtitle="Changed stations since the last trigger; an empty batch still rescores, since the hour moves on"
        height={260}
        option={{
          grid: { left: 8, right: 16, top: 16, bottom: 8, containLabel: true },
          xAxis: { type: "category", data: scored.map((b) => b.batchId), name: "batch", nameLocation: "middle", nameGap: 26 },
          yAxis: { type: "value" },
          tooltip: {
            trigger: "item",
            formatter: (p: { dataIndex: number }) => {
              const b = scored[p.dataIndex];
              return tooltipRow(SERIES[0], `batch ${b.batchId} · ${axisTime(b.timestamp)} UTC`, `${int(b.inputRows)} rows`);
            },
          },
          series: [{ type: "bar", data: scored.map((b) => b.inputRows), itemStyle: { color: SERIES[0] } }],
        }}
        table={{
          columns: ["Batch", "Trigger (UTC)", "Rows", "Scored in"],
          numeric: [true, false, true, true],
          rows: scored.map((b) => [b.batchId, axisTime(b.timestamp), int(b.inputRows), `${fixed(b.scoring!.processingMs / 1000, 1)} s`]),
        }}
      />
      <ChartCard
        title="Broker → snapshot, per micro-batch"
        subtitle={`From the oldest row's arrival in Kafka to the snapshot on disk; p95 ${fixed(p95, 1)} s against the ${limitS} s gate`}
        height={260}
        option={{
          grid: { left: 8, right: 24, top: 16, bottom: 8, containLabel: true },
          xAxis: { type: "category", data: withRows.map((b) => b.batchId), name: "batch", nameLocation: "middle", nameGap: 26 },
          yAxis: { type: "value", max: Math.max(limitS + 10, ...latencies), axisLabel: { formatter: (v: number) => `${v} s` } },
          tooltip: {
            trigger: "item",
            formatter: (p: { dataIndex: number }) => {
              const b = withRows[p.dataIndex];
              return (
                tooltipRow(SERIES[0], `batch ${b.batchId}`, `${fixed(b.scoring!.latencyS, 1)} s`) +
                `<div style="color:${TEXT.secondary}">scoring ${fixed(b.scoring!.processingMs / 1000, 1)} s · ${int(b.inputRows)} rows</div>`
              );
            },
          },
          series: [
            {
              type: "bar",
              data: withRows.map((b) => b.scoring!.latencyS),
              itemStyle: { color: SERIES[0] },
              markLine: {
                symbol: "none",
                silent: true,
                lineStyle: { color: HIGHLIGHT, type: "dashed", width: 1.5 },
                label: { color: TEXT.secondary, formatter: `gate B: ${limitS} s`, position: "insideEndTop" },
                data: [{ yAxis: limitS }],
              },
            },
          ],
        }}
        table={{
          columns: ["Batch", "Rows", "Broker → snapshot", "Scoring"],
          numeric: [true, true, true, true],
          rows: withRows.map((b) => [b.batchId, int(b.inputRows), `${fixed(b.scoring!.latencyS, 1)} s`, `${fixed(b.scoring!.processingMs / 1000, 1)} s`]),
        }}
      />
    </div>
  );
}

/** Event time against the watermark: Spark keeps state only for what is younger than the watermark. */
export function WatermarkChart({ batches, delay }: { batches: ProgressEntry[]; delay: string }) {
  const t = (iso: string) => new Date(iso).getTime();
  // Until the first rows arrive, Spark reports the watermark as 1970-01-01: leave those batches out.
  const rows = batches.filter((b) => b.watermark && b.maxEventTime && t(b.watermark) > 0);
  const ny = (ms: number) =>
    new Date(ms).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", second: "2-digit", timeZone: "America/New_York" });
  return (
    <ChartCard
      title="Event time and the watermark"
      subtitle={`New York time. The watermark trails the newest station report by ${delay}; reports older than it are dropped as late`}
      height={260}
      option={{
        grid: { left: 8, right: 16, top: 32, bottom: 8, containLabel: true },
        legend: { top: 0, data: ["newest report (event time)", "watermark"] },
        xAxis: { type: "category", data: rows.map((b) => b.batchId) },
        yAxis: { type: "time", scale: true, axisLabel: { formatter: (v: number) => ny(v).slice(0, 5) } },
        tooltip: { trigger: "axis", valueFormatter: (v: number) => `${ny(v)} New York` },
        series: [
          { name: "newest report (event time)", type: "line", data: rows.map((b) => t(b.maxEventTime!)), itemStyle: { color: SERIES[0] }, lineStyle: { color: SERIES[0] } },
          { name: "watermark", type: "line", data: rows.map((b) => t(b.watermark!)), itemStyle: { color: SERIES[1] }, lineStyle: { color: SERIES[1], type: "dashed" } },
        ],
      }}
      table={{
        columns: ["Batch", "Newest report", "Watermark", "State rows", "Dropped as late"],
        numeric: [true, false, false, true, true],
        rows: rows.map((b) => [b.batchId, ny(t(b.maxEventTime!)), ny(t(b.watermark!)), int(b.stateRows), int(b.droppedByWatermark)]),
      }}
    />
  );
}

type Replay = ReplayAnalytics["data"];

/** Update mode: a sliding window is rewritten every trigger that brings it new trips. */
export function UpdateWindowsChart({ system }: { system: Replay["system"] }) {
  return (
    <ChartCard
      title="Update mode · trips in sliding 60-minute windows"
      subtitle="Each window was emitted again whenever a trigger changed it; the label counts how often"
      height={280}
      option={{
        grid: { left: 8, right: 16, top: 24, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: system.map((w) => w.start.slice(11)), axisLabel: { interval: 0, rotate: 40 } },
        yAxis: { type: "value", axisLabel: { formatter: (v: number) => int(v) } },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const w = system[p.dataIndex];
            return (
              tooltipRow(SERIES[0], `${w.start.slice(11)}–${w.end.slice(11)}`, `${int(w.trips)} trips`) +
              `<div style="color:${TEXT.secondary}">${int(w.member)} member · ${int(w.casual)} casual · ${int(w.ebike)} e-bike · updated ${w.updates}×</div>`
            );
          },
        },
        series: [
          {
            type: "bar",
            data: system.map((w) => w.trips),
            itemStyle: { color: SERIES[0] },
            label: { show: true, position: "top", color: TEXT.secondary, formatter: (p: { dataIndex: number }) => `${system[p.dataIndex].updates}×` },
          },
        ],
      }}
      table={{
        columns: ["Window", "Trips", "Member", "Casual", "E-bike", "Late in it", "Updates"],
        numeric: [false, true, true, true, true, true, true],
        rows: system.map((w) => [`${w.start.slice(11)}–${w.end.slice(11)}`, int(w.trips), int(w.member), int(w.casual), int(w.ebike), int(w.late), w.updates]),
      }}
    />
  );
}

/** Append mode: a tumbling window is written once, when the watermark passes its end. */
export function AppendWindowsChart({ stations }: { stations: Replay["stations"] }) {
  return (
    <ChartCard
      title="Append mode · departures in closed 15-minute windows"
      subtitle="Written once, after the watermark passed the window's end; the late trips it still took are stacked on top"
      height={280}
      option={{
        grid: { left: 8, right: 16, top: 32, bottom: 8, containLabel: true },
        legend: { top: 0, data: ["on time", "late, still counted"] },
        xAxis: { type: "category", data: stations.map((w) => w.start.slice(11)), axisLabel: { interval: 0, rotate: 40 } },
        yAxis: { type: "value", axisLabel: { formatter: (v: number) => int(v) } },
        tooltip: { trigger: "axis" },
        series: [
          { name: "on time", type: "bar", stack: "d", data: stations.map((w) => w.departures - w.late), itemStyle: { color: SERIES[0], borderRadius: 0 } },
          { name: "late, still counted", type: "bar", stack: "d", data: stations.map((w) => w.late), itemStyle: { color: SERIES[1] } },
        ],
      }}
      table={{
        columns: ["Window", "Stations", "Departures", "Late, still counted", "Busiest station"],
        numeric: [false, true, true, true, false],
        rows: stations.map((w) => [
          `${w.start.slice(11)}–${w.end.slice(11)}`,
          int(w.stations),
          int(w.departures),
          int(w.late),
          w.top[0] ? `${w.top[0].name} (${w.top[0].departures})` : "",
        ]),
      }}
    />
  );
}

/** The broker drill: partitions fully replicated (3 in-sync replicas) or running on two, second by second. */
export function BrokerDrillChart({ samples, downS }: { samples: DrillSample[]; downS: number }) {
  const rows = samples.filter((s) => s.isr);
  const at = (s: DrillSample, n: string) => s.isr?.[n] ?? 0;
  return (
    <ChartCard
      title="One broker down: in-sync replicas over time"
      subtitle={`Broker stopped at 0 s and started at ${downS} s; acks=all still succeeds with 2 of 3 in sync`}
      height={260}
      option={{
        grid: { left: 8, right: 16, top: 32, bottom: 8, containLabel: true },
        legend: { top: 0, data: ["3 in sync", "2 in sync"] },
        xAxis: { type: "category", data: rows.map((s) => `${Math.round(s.t)} s`) },
        yAxis: { type: "value", name: "partitions", minInterval: 1 },
        tooltip: { trigger: "axis" },
        series: [
          { name: "3 in sync", type: "bar", stack: "p", data: rows.map((s) => at(s, "3")), itemStyle: { color: SERIES[0], borderRadius: 0 } },
          { name: "2 in sync", type: "bar", stack: "p", data: rows.map((s) => at(s, "2")), itemStyle: { color: CONTEXT_SERIES } },
        ],
      }}
      table={{
        columns: ["Time", "Brokers", "ISR 3", "ISR 2", "Leaders (broker: n)", "Delivered", "Failed", "Last batch"],
        numeric: [false, false, true, true, false, true, true, true],
        rows: rows.map((s) => [
          `${fixed(s.t, 0)} s`,
          (s.brokers ?? []).join(", "),
          at(s, "3"),
          at(s, "2"),
          Object.entries(s.leaders ?? {})
            .map(([b, n]) => `${b}: ${n}`)
            .join(" · "),
          s.delivered === null ? "" : int(s.delivered),
          s.failed === null ? "" : int(s.failed),
          s.lastBatch ?? "",
        ]),
      }}
    />
  );
}
