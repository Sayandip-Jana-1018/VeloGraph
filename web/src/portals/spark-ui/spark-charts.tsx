"use client";

import { ChartCard } from "@/components/charts/chart-card";
import { BAR_END_H, CONTEXT_SERIES, SERIES, TEXT, tooltipRow } from "@/components/charts/theme";
import { duration, escapeHtml, int } from "@/lib/format";
import type { SparkUi } from "@/lib/schemas";

type Data = SparkUi["data"];

/** The history server's application list, as a chart: one bar per completed application (pipeline order). */
export function AppsChart({ apps }: { apps: Data["applications"] }) {
  const ordered = [...apps].reverse();
  return (
    <ChartCard
      title="Completed applications"
      subtitle="Spark history server · /api/v1/applications, latest attempt of each pipeline job"
      height={380}
      option={{
        grid: { left: 8, right: 70, top: 8, bottom: 8, containLabel: true },
        xAxis: { type: "value", axisLabel: { formatter: (v: number) => `${v} s` } },
        yAxis: { type: "category", data: ordered.map((a) => a.name) },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const a = ordered[p.dataIndex];
            return (
              tooltipRow(SERIES[0], escapeHtml(a.name), duration(a.durationMs)) +
              `<div style="color:${TEXT.secondary}">${escapeHtml(a.id)} · Spark ${escapeHtml(a.sparkVersion)}</div>`
            );
          },
        },
        series: [
          {
            type: "bar",
            data: ordered.map((a) => Math.round(a.durationMs / 100) / 10),
            itemStyle: { color: SERIES[0], ...BAR_END_H },
            label: { show: true, position: "right", color: TEXT.secondary, formatter: (p: { value: number }) => `${p.value} s` },
          },
        ],
      }}
      table={{
        columns: ["Application", "App id", "Duration"],
        numeric: [false, false, true],
        rows: apps.map((a) => [a.name, a.id, duration(a.durationMs)]),
      }}
    />
  );
}

const GROUPS = ["groupByKey", "reduceByKey", "join", "broadcast"];

/** The RDD application's jobs on a timeline (a Gantt drawn as stacked bars: invisible offset + duration). */
export function JobTimelineChart({ jobs }: { jobs: Data["rdd"]["jobs"] }) {
  const ordered = [...jobs].reverse(); // job 0 at the top
  const groupOf = (g: string | null) => (g && GROUPS.includes(g) ? g : "other jobs");
  const names = [...GROUPS, "other jobs"];
  return (
    <ChartCard
      title="Every job of the RDD application"
      subtitle="Job timeline from the history server; the four measured shuffle experiments are coloured"
      height={560}
      option={{
        legend: { top: 0, left: 0, data: names },
        grid: { left: 8, right: 24, top: 36, bottom: 8, containLabel: true },
        xAxis: { type: "value", axisLabel: { formatter: (v: number) => `${v} s` } },
        yAxis: { type: "category", data: ordered.map((j) => `job ${j.jobId}`), axisLabel: { fontSize: 10 } },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const j = ordered[p.dataIndex];
            return (
              `<div style="color:${TEXT.secondary};margin-bottom:4px">job ${j.jobId} · ${escapeHtml(j.name)}</div>` +
              tooltipRow(SERIES[0], escapeHtml(groupOf(j.group)), duration(j.durationMs)) +
              `<div style="color:${TEXT.secondary}">${int(j.tasks)} tasks · ${j.stages} stages run, ${j.skippedStages} skipped</div>`
            );
          },
        },
        series: [
          {
            name: "offset",
            type: "bar",
            stack: "t",
            silent: true,
            itemStyle: { color: "transparent" },
            data: ordered.map((j) => j.startMs / 1000),
          },
          ...names.map((g, i) => ({
            name: g,
            type: "bar",
            stack: "t",
            barMaxWidth: 12,
            itemStyle: { color: i < GROUPS.length ? SERIES[i] : CONTEXT_SERIES, borderRadius: [0, 3, 3, 0] },
            data: ordered.map((j) => (groupOf(j.group) === g ? j.durationMs / 1000 : null)),
          })),
        ],
      }}
      table={{
        columns: ["Job", "Description", "Group", "Starts at", "Duration", "Tasks"],
        numeric: [true, false, false, true, true, true],
        rows: jobs.map((j) => [j.jobId, j.name, groupOf(j.group), duration(j.startMs), duration(j.durationMs), int(j.tasks)]),
      }}
    />
  );
}

/** Task run-time spread per stage (min, quartiles, max from the stage task summary). */
export function TaskBoxplot({ stages }: { stages: { label: string; quantiles: number[] }[] }) {
  return (
    <ChartCard
      title="How long the tasks of each stage ran"
      subtitle="taskSummary quantiles (min, 25 %, median, 75 %, max) of each task's executor run time"
      height={300}
      option={{
        grid: { left: 8, right: 16, top: 16, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: stages.map((s) => s.label), axisLabel: { interval: 0, fontSize: 10 } },
        yAxis: { type: "value", axisLabel: { formatter: (v: number) => duration(v) } },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const s = stages[p.dataIndex];
            const [min, q1, med, q3, max] = s.quantiles;
            return (
              `<div style="color:${TEXT.secondary};margin-bottom:4px">${escapeHtml(s.label)}</div>` +
              tooltipRow(SERIES[0], "median", duration(med)) +
              `<div style="color:${TEXT.secondary}">quartiles ${duration(q1)} – ${duration(q3)} · range ${duration(min)} – ${duration(max)}</div>`
            );
          },
        },
        series: [
          {
            type: "boxplot",
            data: stages.map((s) => s.quantiles),
            boxWidth: [10, 28],
            itemStyle: { color: "rgba(57, 135, 229, 0.18)", borderColor: SERIES[0], borderWidth: 2 },
          },
        ],
      }}
      table={{
        columns: ["Stage", "Min", "25 %", "Median", "75 %", "Max"],
        numeric: [false, true, true, true, true, true],
        rows: stages.map((s) => [s.label, ...s.quantiles.map((q) => duration(q))]),
      }}
    />
  );
}

/** GraphX in the Spark UI: one Spark job per Pregel superstep (the first PageRank run). */
export function SuperstepJobsChart({ steps }: { steps: Data["pregel"]["firstRun"] }) {
  return (
    <ChartCard
      title="One Spark job per Pregel superstep"
      subtitle="Weighted PageRank's supersteps as the history server recorded them (isEmpty at Pregel.scala)"
      height={280}
      option={{
        grid: { left: 8, right: 16, top: 16, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: steps.map((_, i) => i), name: "superstep", nameLocation: "middle", nameGap: 26 },
        yAxis: { type: "value", axisLabel: { formatter: (v: number) => duration(v) } },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const s = steps[p.dataIndex];
            return (
              tooltipRow(SERIES[0], `superstep ${p.dataIndex} · job ${s.jobId}`, duration(s.durationMs)) +
              `<div style="color:${TEXT.secondary}">${s.skippedStages} stages skipped (cached lineage reused)</div>`
            );
          },
        },
        series: [{ type: "bar", data: steps.map((s) => s.durationMs), barMaxWidth: 12, itemStyle: { color: SERIES[0], borderRadius: [3, 3, 0, 0] } }],
      }}
      table={{
        columns: ["Superstep", "Job", "Duration", "Skipped stages"],
        numeric: [true, true, true, true],
        rows: steps.map((s, i) => [i, s.jobId, duration(s.durationMs), s.skippedStages]),
      }}
    />
  );
}
