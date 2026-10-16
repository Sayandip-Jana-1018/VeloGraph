"use client";

import { useState } from "react";

import { ChartCard } from "@/components/charts/chart-card";
import { BAR_END_H, BLUE_RAMP, CONTEXT_SERIES, DIVERGING, SERIES, SURFACE, TEXT, tooltipRow } from "@/components/charts/theme";
import { ClaySegmented } from "@/components/inkglass/clay";
import { escapeHtml, fixed, int, pct } from "@/lib/format";
import type { Archetypes, ClassMetrics, MlAblation, MlCorrelation, RegMetrics, RegressionTarget } from "@/lib/schemas";

import { modelLabel } from "./labels";

type Archetype = Archetypes["data"]["archetypes"][number];

/** Archetype colours: identity in fixed order (archetype 0 = slot 1), never cycled. */
export const archetypeColor = (id: number) => (id >= 0 && id < SERIES.length ? SERIES[id] : CONTEXT_SERIES);
const HOURS = Array.from({ length: 24 }, (_, h) => h);

// ---- P4.1 archetypes ------------------------------------------------------------------------------------------

/** Silhouette for each k: the chosen k in slot 1, the others recessive. */
export function SilhouetteChart({ selection, k, minShare }: { selection: Archetypes["data"]["selection"]; k: number; minShare: number }) {
  return (
    <ChartCard
      title="How many kinds of station? Silhouette for each k"
      subtitle={`Best of 5 KMeans runs per k. A k whose smallest cluster holds under ${pct(minShare * 100, 0)} of stations is not eligible.`}
      height={260}
      option={{
        grid: { left: 8, right: 16, top: 16, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: selection.map((s) => `k = ${s.k}`) },
        yAxis: { type: "value", min: 0, axisLabel: { formatter: (v: number) => fixed(v, 2) } },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const s = selection[p.dataIndex];
            return (
              tooltipRow(s.k === k ? SERIES[0] : CONTEXT_SERIES, `k = ${s.k}`, `silhouette ${fixed(s.silhouette, 3)}`) +
              `<div style="color:${TEXT.secondary}">smallest cluster ${pct(s.smallestShare * 100)}${s.smallestShare < minShare ? " · not eligible" : ""}</div>`
            );
          },
        },
        series: [
          {
            type: "bar",
            data: selection.map((s) => ({ value: s.silhouette, itemStyle: { color: s.k === k ? SERIES[0] : CONTEXT_SERIES } })),
            label: { show: true, position: "top", color: TEXT.secondary, formatter: (p: { value: number }) => fixed(p.value, 3) },
          },
        ],
      }}
      table={{
        columns: ["k", "Silhouette", "Smallest cluster", "Eligible"],
        numeric: [true, true, true, false],
        rows: selection.map((s) => [s.k, fixed(s.silhouette, 4), pct(s.smallestShare * 100), s.smallestShare >= minShare ? "yes" : "no"]),
      }}
    />
  );
}

/** One small chart per archetype: departures (solid) and arrivals (dashed) through a working day or a day off. */
export function ArchetypeCurves({ archetypes }: { archetypes: Archetype[] }) {
  const [day, setDay] = useState<"work" | "off">("work");
  return (
    <div className="space-y-4">
      <div className="flex justify-center">
        <ClaySegmented
          label="Day type"
          value={day}
          onChange={setDay}
          options={[
            { value: "work", label: "Working day" },
            { value: "off", label: "Day off" },
          ]}
        />
      </div>
      <div className="grid gap-5 lg:grid-cols-3">
        {archetypes.map((a) => {
          const dep = day === "work" ? a.curves.workDep : a.curves.offDep;
          const arr = day === "work" ? a.curves.workArr : a.curves.offArr;
          const color = archetypeColor(a.id);
          return (
            <ChartCard
              key={a.id}
              title={a.name}
              subtitle={`${int(a.stations)} stations · share of the weekly profile per hour`}
              height={210}
              option={{
                legend: { top: 0, right: 0, data: ["departures (solid)", "arrivals (dashed)"] },
                grid: { left: 8, right: 12, top: 30, bottom: 8, containLabel: true },
                xAxis: { type: "category", data: HOURS, axisLabel: { interval: 5 } },
                yAxis: { type: "value", axisLabel: { formatter: (v: number) => `${fixed(v, 1)} %` } },
                tooltip: {
                  trigger: "axis",
                  formatter: (ps: { dataIndex: number }[]) => {
                    const h = ps[0].dataIndex;
                    return (
                      `<div style="color:${TEXT.secondary};margin-bottom:4px">${h}:00 · ${day === "work" ? "working day" : "day off"}</div>` +
                      tooltipRow(color, "departures", `${fixed(dep[h], 2)} %`) +
                      tooltipRow(color, "arrivals", `${fixed(arr[h], 2)} %`)
                    );
                  },
                },
                series: [
                  { name: "departures (solid)", type: "line", data: dep, showSymbol: false, lineStyle: { color, width: 2 }, itemStyle: { color } },
                  { name: "arrivals (dashed)", type: "line", data: arr, showSymbol: false, lineStyle: { color, width: 2, type: "dashed" }, itemStyle: { color } },
                ],
              }}
              table={{
                columns: ["Hour", "Departures %", "Arrivals %"],
                numeric: [true, true, true],
                rows: HOURS.map((h) => [h, fixed(dep[h], 3), fixed(arr[h], 3)]),
              }}
            />
          );
        })}
      </div>
    </div>
  );
}

/** Every station in the plane of the first two principal components, coloured by archetype. */
export function ArchetypeScatter({ data }: { data: Archetypes["data"] }) {
  const names = new Map(data.archetypes.map((a) => [a.id, a.name]));
  const ids = data.archetypes.map((a) => a.id);
  return (
    <ChartCard
      title="Stations by the shape of their day (PCA)"
      subtitle={`First two principal components of the profiles: ${pct(data.explainedVariance[0] * 100, 0)} + ${pct(data.explainedVariance[1] * 100, 0)} of the variance`}
      height={420}
      option={{
        legend: { top: 0, left: "center", data: ids.map((id) => names.get(id)) },
        grid: { left: 8, right: 16, top: 36, bottom: 8, containLabel: true },
        xAxis: { type: "value", name: "PC 1", nameLocation: "middle", nameGap: 24, axisLabel: { formatter: (v: number) => fixed(v, 2) } },
        yAxis: { type: "value", name: "PC 2", nameLocation: "middle", nameGap: 36, axisLabel: { formatter: (v: number) => fixed(v, 2) } },
        tooltip: {
          trigger: "item",
          formatter: (p: { data: [number, number, number, string, number] }) =>
            tooltipRow(archetypeColor(p.data[2]), escapeHtml(names.get(p.data[2]) ?? ""), escapeHtml(p.data[3])) +
            `<div style="color:${TEXT.secondary}">${int(p.data[4])} trips in the profile window</div>`,
        },
        series: ids.map((id) => ({
          name: names.get(id),
          type: "scatter",
          symbolSize: 5,
          itemStyle: { color: archetypeColor(id), opacity: 0.75, borderColor: SURFACE, borderWidth: 0.5 },
          data: data.points.filter((p) => p.archetype === id).map((p) => [p.x, p.y, p.archetype, p.name, p.trips]),
        })),
      }}
      table={{
        columns: ["Archetype", "Stations", "PC 1 mean", "PC 2 mean"],
        numeric: [false, true, true, true],
        rows: ids.map((id) => {
          const pts = data.points.filter((p) => p.archetype === id);
          const mean = (f: (p: (typeof pts)[number]) => number) => pts.reduce((s, p) => s + f(p), 0) / Math.max(1, pts.length);
          return [names.get(id) ?? String(id), int(pts.length), fixed(mean((p) => p.x), 4), fixed(mean((p) => p.y), 4)];
        }),
      }}
    />
  );
}

// ---- P4.3 exploration -----------------------------------------------------------------------------------------

/** The correlation matrix as a heat map on the diverging scale (blue negative, red positive, dark = none). */
export function CorrelationHeatmap({ data }: { data: MlCorrelation["data"] }) {
  const [method, setMethod] = useState<"spearman" | "pearson">("spearman");
  const m = data[method];
  const cells = data.columns.flatMap((_, y) => data.columns.map((__, x) => [x, y, m[y][x]]));
  return (
    <div className="space-y-4">
      <div className="flex justify-center">
        <ClaySegmented
          label="Correlation"
          value={method}
          onChange={setMethod}
          options={[
            { value: "spearman", label: "Spearman (ranks)" },
            { value: "pearson", label: "Pearson (linear)" },
          ]}
        />
      </div>
      <ChartCard
        title={`${method === "spearman" ? "Spearman" : "Pearson"} correlation between features and targets`}
        subtitle={`${int(data.sampleRows)} fit rows (a ${pct(data.sampleFraction * 100, 0)} sample) · ${data.constant.length ? `constant in August, left out: ${data.constant.join(", ")}` : "no constant columns"}`}
        height={620}
        option={{
          grid: { left: 8, right: 16, top: 8, bottom: 64, containLabel: true },
          xAxis: { type: "category", data: data.columns, axisLabel: { rotate: 60, fontSize: 9, interval: 0 }, splitArea: { show: false } },
          yAxis: { type: "category", data: data.columns, inverse: true, axisLabel: { fontSize: 9, interval: 0 }, splitArea: { show: false } },
          visualMap: {
            min: -1,
            max: 1,
            orient: "horizontal",
            left: "center",
            bottom: 0,
            itemHeight: 180,
            textStyle: { color: TEXT.secondary },
            inRange: { color: [DIVERGING.negative, DIVERGING.neutral, DIVERGING.positive] },
          },
          tooltip: {
            trigger: "item",
            formatter: (p: { data: [number, number, number] }) =>
              `<div style="color:${TEXT.secondary};margin-bottom:4px">${escapeHtml(data.columns[p.data[1]])} × ${escapeHtml(data.columns[p.data[0]])}</div>` +
              `<b style="color:${TEXT.primary}">${fixed(p.data[2], 3)}</b>`,
          },
          series: [{ type: "heatmap", data: cells, itemStyle: { borderColor: SURFACE, borderWidth: 1 } }],
        }}
        table={{
          columns: ["Feature", `${method} with departures`],
          numeric: [false, true],
          rows: data.withDepartures.map((r) => [r.feature, fixed(r[method], 3)]),
        }}
      />
    </div>
  );
}

/** The features that move most with departures (Spearman), as diverging bars. */
export function TopCorrelations({ rows }: { rows: MlCorrelation["data"]["withDepartures"] }) {
  const top = rows.slice(0, 12).reverse();
  return (
    <ChartCard
      title="What moves with departures"
      subtitle="Spearman correlation with departures per station-hour, the twelve strongest"
      height={340}
      option={{
        grid: { left: 8, right: 48, top: 8, bottom: 8, containLabel: true },
        xAxis: { type: "value", min: -1, max: 1, axisLabel: { formatter: (v: number) => fixed(v, 1) } },
        yAxis: { type: "category", data: top.map((r) => r.feature) },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const r = top[p.dataIndex];
            return tooltipRow(r.spearman >= 0 ? DIVERGING.positive : DIVERGING.negative, escapeHtml(r.feature), `ρ ${fixed(r.spearman, 3)} · r ${fixed(r.pearson, 3)}`);
          },
        },
        series: [
          {
            type: "bar",
            data: top.map((r) => ({ value: r.spearman, itemStyle: { color: r.spearman >= 0 ? DIVERGING.positive : DIVERGING.negative, ...BAR_END_H } })),
            label: { show: true, position: "right", color: TEXT.secondary, formatter: (p: { value: number }) => fixed(p.value, 2) },
          },
        ],
      }}
      table={{ columns: ["Feature", "Spearman", "Pearson"], numeric: [false, true, true], rows: rows.map((r) => [r.feature, fixed(r.spearman, 3), fixed(r.pearson, 3)]) }}
    />
  );
}

// ---- P4.4 regression ------------------------------------------------------------------------------------------

/** Test MAE of each model; the champion (chosen on validation) in slot 1, the baselines as reference lines. */
export function RegressionLeaderboard({ target, data }: { target: string; data: RegressionTarget }) {
  const rows = [...data.leaderboard].sort((a, b) => b.test.mae - a.test.mae);
  const naive = data.baselines["seasonal-naive"].test.mae;
  const metricRow = (m: RegMetrics) => [fixed(m.mae, 3), fixed(m.rmse, 3), fixed(m.r2, 3)];
  return (
    <ChartCard
      title={`${target}: error in September (MAE)`}
      subtitle={`Champion: ${modelLabel(data.champion)} (chosen on the validation week) · ${data.improvementVsNaive >= 0 ? `${fixed(data.improvementVsNaive, 1)} % below` : `${fixed(-data.improvementVsNaive, 1)} % above`} the seasonal naive forecast (dashed); the global mean is in the table`}
      height={260}
      option={{
        grid: { left: 8, right: 56, top: 26, bottom: 8, containLabel: true },
        xAxis: { type: "value", min: 0, axisLabel: { formatter: (v: number) => fixed(v, 1) } },
        yAxis: { type: "category", data: rows.map((r) => modelLabel(r.model)) },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const r = rows[p.dataIndex];
            return (
              tooltipRow(r.model === data.champion ? SERIES[0] : CONTEXT_SERIES, modelLabel(r.model), `MAE ${fixed(r.test.mae, 3)}`) +
              `<div style="color:${TEXT.secondary}">validation MAE ${fixed(r.validation.mae, 3)} · test RMSE ${fixed(r.test.rmse, 2)} · R² ${fixed(r.test.r2, 3)}</div>`
            );
          },
        },
        series: [
          {
            type: "bar",
            data: rows.map((r) => ({ value: r.test.mae, itemStyle: { color: r.model === data.champion ? SERIES[0] : CONTEXT_SERIES, ...BAR_END_H } })),
            label: { show: true, position: "right", color: TEXT.secondary, formatter: (p: { value: number }) => fixed(p.value, 3) },
            markLine: {
              silent: true,
              symbol: "none",
              lineStyle: { color: TEXT.muted, type: "dashed" },
              label: { color: TEXT.secondary, fontSize: 10, formatter: (p: { name: string }) => p.name },
              data: [{ name: `seasonal naive ${fixed(naive, 2)}`, xAxis: naive }],
            },
          },
        ],
      }}
      table={{
        columns: ["Model", "Validation MAE", "Test MAE", "Test RMSE", "Test R²"],
        numeric: [false, true, true, true, true],
        rows: [
          ...data.leaderboard.map((r) => [modelLabel(r.model) + (r.model === data.champion ? " (champion)" : ""), fixed(r.validation.mae, 3), ...metricRow(r.test)]),
          ...Object.entries(data.baselines).map(([name, b]) => [modelLabel(name), fixed(b.validation.mae, 3), ...metricRow(b.test)]),
        ],
      }}
    />
  );
}

/** MAE through the day: the champion against the seasonal naive forecast. */
export function HourErrorChart({ target, data }: { target: string; data: RegressionTarget }) {
  return (
    <ChartCard
      title={`${target}: error by hour of day`}
      subtitle="Mean absolute error per station-hour in September"
      height={250}
      option={{
        legend: { top: 0, right: 0, data: [modelLabel(data.champion), "Seasonal naive"] },
        grid: { left: 8, right: 12, top: 30, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: data.byHour.map((h) => h.hour), axisLabel: { interval: 2 } },
        yAxis: { type: "value", min: 0, axisLabel: { formatter: (v: number) => fixed(v, 1) } },
        tooltip: {
          trigger: "axis",
          formatter: (ps: { dataIndex: number }[]) => {
            const h = data.byHour[ps[0].dataIndex];
            return (
              `<div style="color:${TEXT.secondary};margin-bottom:4px">${h.hour}:00</div>` +
              tooltipRow(SERIES[0], modelLabel(data.champion), fixed(h.champion, 3)) +
              tooltipRow(CONTEXT_SERIES, "Seasonal naive", fixed(h.naive, 3))
            );
          },
        },
        series: [
          { name: modelLabel(data.champion), type: "line", data: data.byHour.map((h) => h.champion), showSymbol: false, lineStyle: { color: SERIES[0], width: 2 }, itemStyle: { color: SERIES[0] } },
          { name: "Seasonal naive", type: "line", data: data.byHour.map((h) => h.naive), showSymbol: false, lineStyle: { color: CONTEXT_SERIES, width: 2, type: "dashed" }, itemStyle: { color: CONTEXT_SERIES } },
        ],
      }}
      table={{ columns: ["Hour", "Champion MAE", "Seasonal naive MAE"], numeric: [true, true, true], rows: data.byHour.map((h) => [h.hour, fixed(h.champion, 3), fixed(h.naive, 3)]) }}
    />
  );
}

/** For each actual count, the mean prediction: on the dashed diagonal is a perfect forecast. */
export function CalibrationChart({ target, data }: { target: string; data: RegressionTarget }) {
  const pts = data.calibration;
  const max = Math.max(...pts.map((p) => p.actual));
  return (
    <ChartCard
      title={`${target}: predicted against actual`}
      subtitle="Mean prediction for every actual count (30 = 30 or more); the dashed diagonal is a perfect forecast"
      height={250}
      option={{
        grid: { left: 8, right: 16, top: 16, bottom: 8, containLabel: true },
        xAxis: { type: "value", min: 0, max, name: "actual", nameLocation: "middle", nameGap: 24 },
        yAxis: { type: "value", min: 0, max, name: "predicted (mean)", nameLocation: "middle", nameGap: 36 },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number; seriesIndex: number }) => {
            if (p.seriesIndex !== 0) return "";
            const c = pts[p.dataIndex];
            return tooltipRow(SERIES[0], `actual ${c.actual}${c.actual === max ? "+" : ""}`, `predicted ${fixed(c.meanPrediction, 2)}`) + `<div style="color:${TEXT.secondary}">${int(c.rows)} station-hours</div>`;
          },
        },
        series: [
          { type: "line", data: pts.map((p) => [p.actual, p.meanPrediction]), symbolSize: 6, lineStyle: { color: SERIES[0], width: 2 }, itemStyle: { color: SERIES[0] } },
          { type: "line", data: [[0, 0], [max, max]], silent: true, showSymbol: false, lineStyle: { color: TEXT.muted, type: "dashed", width: 1 } },
        ],
      }}
      table={{ columns: ["Actual", "Mean prediction", "Station-hours"], numeric: [true, true, true], rows: pts.map((p) => [p.actual, fixed(p.meanPrediction, 3), int(p.rows)]) }}
    />
  );
}

/** How far off the champion is, hour by hour: residuals (prediction - actual), rounded and capped at ±10. */
export function ResidualChart({ target, data }: { target: string; data: RegressionTarget }) {
  const bins = data.residuals;
  const label = (b: number) => (b <= -10 ? "≤ −10" : b >= 10 ? "≥ 10" : String(b));
  return (
    <ChartCard
      title={`${target}: residuals`}
      subtitle="Prediction minus actual, per station-hour in September, rounded to whole trips"
      height={250}
      option={{
        grid: { left: 8, right: 16, top: 16, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: bins.map((b) => label(b.bin)), axisLabel: { interval: 1 } },
        yAxis: { type: "value", axisLabel: { formatter: (v: number) => `${fixed(v * 100, 0)} %` } },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const b = bins[p.dataIndex];
            return tooltipRow(SERIES[0], `off by ${label(b.bin)}`, pct(b.share * 100));
          },
        },
        series: [{ type: "bar", data: bins.map((b) => b.share), barCategoryGap: "12%", itemStyle: { color: SERIES[0], borderRadius: [4, 4, 0, 0] } }],
      }}
      table={{ columns: ["Prediction − actual", "Share of station-hours"], numeric: [true, true], rows: bins.map((b) => [label(b.bin), pct(b.share * 100, 2)]) }}
    />
  );
}

/** What the champion's trees split on most (gain-based importance). */
export function ImportanceChart({ target, data }: { target: string; data: RegressionTarget }) {
  const top = data.importances.slice(0, 12).reverse();
  if (top.length === 0) return null;
  return (
    <ChartCard
      title={`${target}: what the champion relies on`}
      subtitle="Feature importance of the trees (share of the total reduction in error), the twelve largest"
      height={320}
      option={{
        grid: { left: 8, right: 56, top: 8, bottom: 8, containLabel: true },
        xAxis: { type: "value", min: 0, axisLabel: { formatter: (v: number) => `${fixed(v * 100, 0)} %` } },
        yAxis: { type: "category", data: top.map((r) => r.feature) },
        tooltip: { trigger: "item", formatter: (p: { dataIndex: number }) => tooltipRow(SERIES[0], escapeHtml(top[p.dataIndex].feature), pct(top[p.dataIndex].importance * 100)) },
        series: [
          {
            type: "bar",
            data: top.map((r) => r.importance),
            itemStyle: { color: SERIES[0], ...BAR_END_H },
            label: { show: true, position: "right", color: TEXT.secondary, formatter: (p: { value: number }) => pct(p.value * 100) },
          },
        ],
      }}
      table={{ columns: ["Feature", "Importance"], numeric: [false, true], rows: data.importances.map((r) => [r.feature, pct(r.importance * 100, 2)]) }}
    />
  );
}

// ---- P4.5 classification --------------------------------------------------------------------------------------

/** Test macro-F1 of each classifier; the champion in slot 1, the baselines as reference lines. */
export function ClassificationLeaderboard({
  leaderboard,
  baselines,
  champion,
}: {
  leaderboard: { model: string; validation: ClassMetrics; test: ClassMetrics }[];
  baselines: Record<string, { validation: ClassMetrics; test: ClassMetrics }>;
  champion: string;
}) {
  const rows = [...leaderboard].sort((a, b) => a.test.macroF1 - b.test.macroF1);
  return (
    <ChartCard
      title="Regime classifiers: macro-F1 in September"
      subtitle={`Champion ${modelLabel(champion)}, chosen on the validation week's macro-F1`}
      height={270}
      option={{
        grid: { left: 8, right: 56, top: 26, bottom: 8, containLabel: true },
        xAxis: { type: "value", min: 0, max: 1, axisLabel: { formatter: (v: number) => fixed(v, 1) } },
        yAxis: { type: "category", data: rows.map((r) => modelLabel(r.model)) },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const r = rows[p.dataIndex];
            return (
              tooltipRow(r.model === champion ? SERIES[0] : CONTEXT_SERIES, modelLabel(r.model), `macro-F1 ${fixed(r.test.macroF1, 3)}`) +
              `<div style="color:${TEXT.secondary}">accuracy ${pct(r.test.accuracy * 100)} · validation macro-F1 ${fixed(r.validation.macroF1, 3)}</div>`
            );
          },
        },
        series: [
          {
            type: "bar",
            data: rows.map((r) => ({ value: r.test.macroF1, itemStyle: { color: r.model === champion ? SERIES[0] : CONTEXT_SERIES, ...BAR_END_H } })),
            label: { show: true, position: "right", color: TEXT.secondary, formatter: (p: { value: number }) => fixed(p.value, 3) },
            markLine: {
              silent: true,
              symbol: "none",
              lineStyle: { color: TEXT.muted, type: "dashed" },
              label: { color: TEXT.secondary, fontSize: 10, formatter: (p: { name: string }) => p.name },
              data: Object.entries(baselines).map(([name, b]) => ({ name: `${modelLabel(name).toLowerCase()} ${fixed(b.test.macroF1, 2)}`, xAxis: b.test.macroF1 })),
            },
          },
        ],
      }}
      table={{
        columns: ["Model", "Validation macro-F1", "Test macro-F1", "Test accuracy"],
        numeric: [false, true, true, true],
        rows: [
          ...leaderboard.map((r) => [modelLabel(r.model) + (r.model === champion ? " (champion)" : ""), fixed(r.validation.macroF1, 3), fixed(r.test.macroF1, 3), pct(r.test.accuracy * 100)]),
          ...Object.entries(baselines).map(([name, b]) => [modelLabel(name), fixed(b.validation.macroF1, 3), fixed(b.test.macroF1, 3), pct(b.test.accuracy * 100)]),
        ],
      }}
    />
  );
}

/** The confusion matrix, each row shaded by its share (recall on the diagonal), counts in the cells. */
export function ConfusionMatrix({ metrics, names, title }: { metrics: ClassMetrics; names: string[]; title: string }) {
  const m = metrics.confusion;
  const cells = m.flatMap((row, a) => row.map((n, p) => [p, a, n / Math.max(1, row.reduce((s, x) => s + x, 0)), n]));
  return (
    <ChartCard
      title={title}
      subtitle="Rows: what happened · columns: what the model said · shading: the share of each row (the diagonal is recall)"
      height={300}
      option={{
        grid: { left: 8, right: 16, top: 8, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: names.map((n) => `said ${n}`), splitArea: { show: false } },
        yAxis: { type: "category", data: names, inverse: true, splitArea: { show: false } },
        // the share (dimension 2) drives the shading: one blue hue, more = lighter, from a mid step of the ramp so
        // dark labels keep their contrast on every cell
        visualMap: { show: false, dimension: 2, min: 0, max: 1, inRange: { color: [BLUE_RAMP[4], BLUE_RAMP[BLUE_RAMP.length - 1]] } },
        tooltip: {
          trigger: "item",
          formatter: (p: { data: [number, number, number, number] }) =>
            `<div style="color:${TEXT.secondary};margin-bottom:4px">actual ${escapeHtml(names[p.data[1]])} → predicted ${escapeHtml(names[p.data[0]])}</div>` +
            `<b style="color:${TEXT.primary}">${int(p.data[3])}</b> <span style="color:${TEXT.secondary}">station-hours · ${pct(p.data[2] * 100)} of the row</span>`,
        },
        series: [
          {
            type: "heatmap",
            data: cells,
            itemStyle: { borderColor: SURFACE, borderWidth: 2 },
            label: {
              show: true,
              fontSize: 11,
              fontWeight: 700,
              lineHeight: 15,
              color: SURFACE,
              formatter: (p: { data: [number, number, number, number] }) => `${int(p.data[3])}\n${pct(p.data[2] * 100, 0)}`,
            },
          },
        ],
      }}
      table={{
        columns: ["Actual \\ predicted", ...names],
        numeric: [false, ...names.map(() => true)],
        rows: m.map((row, a) => [names[a], ...row.map((n) => int(n))]),
      }}
    />
  );
}

// ---- P4.6 ablation --------------------------------------------------------------------------------------------

/** September MAE of the champion with feature groups removed; the full model in slot 1. */
export function AblationChart({ data }: { data: MlAblation["data"] }) {
  const rows = [...data.variants].reverse();
  return (
    <ChartCard
      title={`What each feature group adds: ${modelLabel(data.model)}, ${data.target}`}
      subtitle="The champion refit on August without whole groups, scored on September; the dashed line is the seasonal naive forecast"
      height={280}
      option={{
        grid: { left: 8, right: 64, top: 26, bottom: 8, containLabel: true },
        xAxis: { type: "value", min: 0, axisLabel: { formatter: (v: number) => fixed(v, 1) } },
        yAxis: { type: "category", data: rows.map((v) => v.name) },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number }) => {
            const v = rows[p.dataIndex];
            return (
              tooltipRow(v.dropped.length === 0 ? SERIES[0] : CONTEXT_SERIES, escapeHtml(v.name), `MAE ${fixed(v.test.mae, 3)}`) +
              `<div style="color:${TEXT.secondary}">${v.features} features · ${v.deltaMaePct >= 0 ? "+" : ""}${fixed(v.deltaMaePct, 1)} % against all features · R² ${fixed(v.test.r2, 3)}</div>`
            );
          },
        },
        series: [
          {
            type: "bar",
            data: rows.map((v) => ({ value: v.test.mae, itemStyle: { color: v.dropped.length === 0 ? SERIES[0] : CONTEXT_SERIES, ...BAR_END_H } })),
            label: {
              show: true,
              position: "right",
              color: TEXT.secondary,
              formatter: (p: { dataIndex: number; value: number }) => {
                const v = rows[p.dataIndex];
                return v.dropped.length === 0 ? fixed(p.value, 3) : `${fixed(p.value, 3)} (${v.deltaMaePct >= 0 ? "+" : ""}${fixed(v.deltaMaePct, 1)} %)`;
              },
            },
            markLine: {
              silent: true,
              symbol: "none",
              lineStyle: { color: TEXT.muted, type: "dashed" },
              label: { color: TEXT.secondary, fontSize: 10, formatter: () => `seasonal naive ${fixed(data.naive.mae, 2)}` },
              data: [{ xAxis: data.naive.mae }],
            },
          },
        ],
      }}
      table={{
        columns: ["Variant", "Features", "Test MAE", "Δ MAE", "Test R²"],
        numeric: [false, true, true, true, true],
        rows: data.variants.map((v) => [v.name, v.features, fixed(v.test.mae, 3), `${v.deltaMaePct >= 0 ? "+" : ""}${fixed(v.deltaMaePct, 1)} %`, fixed(v.test.r2, 3)]),
      }}
    />
  );
}
