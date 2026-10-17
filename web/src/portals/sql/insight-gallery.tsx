"use client";

import type { EChartsCoreOption } from "echarts/core";
import { useMemo, useSyncExternalStore } from "react";

import { ChartCard } from "@/components/charts/chart-card";
import { SERIES, SURFACE, TEXT, tooltipRow } from "@/components/charts/theme";
import { Glass } from "@/components/inkglass/surfaces";
import { cn } from "@/lib/cn";
import { duration, escapeHtml, fixed, int } from "@/lib/format";
import type { ChartHint } from "@/lib/schemas";

type Cell = string | number | boolean | null;
export type InsightView = {
  id: string;
  title: string;
  question: string;
  chart: ChartHint;
  columns: { name: string; dataType: string }[];
  rows: Record<string, Cell>[];
  sqlHtml: string;
  finding: string;
  elapsedMs: number;
};

const isNumeric = (dataType: string) => /int|double|long|decimal|float/.test(dataType);
const show = (value: Cell, dataType: string): string =>
  value === null ? "–" : isNumeric(dataType) ? (Number.isInteger(value) ? int(Number(value)) : fixed(Number(value), 2)) : String(value);

/**
 * Builds a chart from the chart hint the SQL job wrote next to each result: line, bar or horizontal bar, with an
 * optional series column. One series uses slot 1 and no legend; several get slots in fixed order and a legend.
 */
function insightOption(insight: InsightView): EChartsCoreOption | null {
  const { chart, rows } = insight;
  if (chart.type === "table" || !chart.x || !chart.y) return null;
  const horizontal = chart.type === "barh";
  const categoryKey = horizontal ? chart.y : chart.x;
  const valueKey = horizontal ? chart.x : chart.y;
  const categories = [...new Set(rows.map((r) => String(r[categoryKey])))];
  if (horizontal) categories.reverse(); // the first (largest) row at the top
  const groups = chart.series ? [...new Set(rows.map((r) => String(r[chart.series!])))] : [valueKey];
  const valueOf = (group: string, category: string) => {
    const row = rows.find((r) => String(r[categoryKey]) === category && (!chart.series || String(r[chart.series]) === group));
    return row ? Number(row[valueKey]) : null;
  };
  const isLine = chart.type === "line";
  const series = groups.map((group, i) => ({
    name: group,
    type: isLine ? "line" : "bar",
    stack: horizontal && chart.series ? "one" : undefined, // one bar per category even when coloured by series
    data: categories.map((c) => valueOf(group, c)),
    itemStyle: {
      color: SERIES[i],
      borderRadius: horizontal ? [0, 4, 4, 0] : [4, 4, 0, 0],
      ...(isLine ? { borderColor: SURFACE, borderWidth: 2 } : {}),
    },
    lineStyle: isLine ? { color: SERIES[i], width: 2 } : undefined,
    symbol: "circle",
    symbolSize: 8,
    showSymbol: isLine && categories.length <= 24,
    barMaxWidth: 24,
  }));
  const valueAxis = { type: "value", axisLabel: { formatter: (v: number) => int(v) } };
  const categoryAxis = { type: "category", data: categories, axisLabel: { color: TEXT.muted, width: 180, overflow: "truncate" } };
  return {
    legend: groups.length > 1 ? { top: 0, left: 0 } : undefined,
    grid: { left: 8, right: 24, top: groups.length > 1 ? 36 : 12, bottom: 8, containLabel: true },
    xAxis: horizontal ? valueAxis : categoryAxis,
    yAxis: horizontal ? categoryAxis : valueAxis,
    tooltip: {
      trigger: isLine ? "axis" : "item",
      formatter: (p: { dataIndex: number; seriesIndex?: number } | { dataIndex: number }[]) => {
        const items = Array.isArray(p) ? p : [p];
        const category = categories[items[0].dataIndex];
        const lines = groups
          .map((g, i) => ({ g, i, v: valueOf(g, category) }))
          .filter((x) => x.v !== null && (Array.isArray(p) || (p as { seriesIndex?: number }).seriesIndex === x.i))
          .map((x) => tooltipRow(SERIES[x.i], escapeHtml(groups.length > 1 ? x.g : valueKey), fixed(x.v!, Number.isInteger(x.v) ? 0 : 1)));
        return `<div style="color:${TEXT.secondary};margin-bottom:4px">${escapeHtml(category)}</div>${lines.join("")}`;
      },
    },
    series,
  };
}

// The selected insight lives in the URL hash (#rain-impact), so the report and the slides can deep-link to it.
const onHashChange = (notify: () => void) => {
  window.addEventListener("hashchange", notify);
  return () => window.removeEventListener("hashchange", notify);
};
const readHash = () => window.location.hash.slice(1);

/** The Spark SQL gallery: pick an insight, see its chart (or table), the finding and the exact query. */
export function InsightGallery({ insights }: { insights: InsightView[] }) {
  const hash = useSyncExternalStore(onHashChange, readHash, () => "");
  const selected = insights.some((i) => i.id === hash) ? hash : insights[0].id;

  const choose = (id: string) => {
    window.history.replaceState(null, "", `#${id}`);
    window.dispatchEvent(new HashChangeEvent("hashchange")); // replaceState itself does not announce the change
  };

  const insight = insights.find((i) => i.id === selected)!;
  const option = useMemo(() => insightOption(insight), [insight]);
  const table = {
    columns: insight.columns.map((c) => c.name),
    numeric: insight.columns.map((c) => isNumeric(c.dataType)),
    rows: insight.rows.map((r) => insight.columns.map((c) => show(r[c.name], c.dataType))),
  };

  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-[19rem_minmax(0,1fr)]">
      <nav aria-label="SQL insights" className="flex max-h-[44rem] flex-col gap-2 overflow-y-auto pr-1">
        {insights.map((i, n) => (
          <button
            key={i.id}
            type="button"
            onClick={() => choose(i.id)}
            aria-current={i.id === selected}
            className={cn(
              "rounded-2xl px-4 py-3 text-left transition-[box-shadow,color] duration-200",
              i.id === selected ? "clay-pressed" : "clay hover:text-marker",
            )}
          >
            <span className="font-mono text-[0.68rem] text-fg-3">{String(n + 1).padStart(2, "0")} · {i.chart.type}</span>
            <span className={cn("block font-display text-[0.95rem] leading-snug font-bold", i.id === selected ? "text-marker" : "text-fg")}>
              {i.title}
            </span>
          </button>
        ))}
      </nav>

      <div className="min-w-0 space-y-5">
        <div>
          <p className="font-mono text-xs text-fg-3">insights/{insight.id}.json · ran in {duration(insight.elapsedMs)}</p>
          <h3 className="font-display text-3xl font-extrabold tracking-tight text-fg">{insight.title}</h3>
          <p className="mt-1 text-fg-2">{insight.question}</p>
          <p className="mt-3 inline-block -rotate-1 rounded-lg bg-marker/12 px-3 py-1 font-hand text-2xl leading-tight text-marker">
            {insight.finding}
          </p>
        </div>
        {option ? (
          <ChartCard key={insight.id} title={insight.title} subtitle={insight.question} option={option} table={table} height={340} />
        ) : (
          <Glass variant="inset" className="overflow-auto p-2">
            <table className="w-full text-left text-sm">
              <thead>
                <tr>
                  {table.columns.map((c, i) => (
                    <th key={c} className={cn("px-3 py-2 text-xs font-semibold text-fg-3", table.numeric[i] && "text-right")}>
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {table.rows.map((row, r) => (
                  <tr key={r} className="border-t border-white/5">
                    {row.map((cell, i) => (
                      <td key={i} className={cn("px-3 py-1.5 text-fg-2", table.numeric[i] && "text-right tabular-nums")}>
                        {cell}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </Glass>
        )}
        <Glass variant="inset" className="code-panel overflow-hidden">
          <p className="border-b border-white/8 px-5 py-3 font-display text-sm font-bold text-fg">
            <span className="mr-2 font-hand text-lg font-normal text-marker">View SQL</span>
            resources/sql/insights/{insight.id}
          </p>
          <div dangerouslySetInnerHTML={{ __html: insight.sqlHtml }} />
        </Glass>
      </div>
    </div>
  );
}
