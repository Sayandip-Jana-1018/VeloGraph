"use client";

import type { EChartsCoreOption } from "echarts/core";
import { useState } from "react";

import { ClaySegmented } from "@/components/inkglass/clay";
import { Glass } from "@/components/inkglass/surfaces";
import { cn } from "@/lib/cn";

import { EChart } from "./echart";

export type TableView = { columns: string[]; rows: (string | number)[][]; numeric?: boolean[] };

/**
 * A chart with its table twin: every value is reachable without hovering (the dataviz accessibility rule).
 * The toggle is a clay control; the chart and the table sit on the validated chart surface.
 */
export function ChartCard({
  title,
  subtitle,
  option,
  table,
  height = 320,
  footnote,
  className,
}: {
  title: string;
  subtitle?: string;
  option: EChartsCoreOption;
  table: TableView;
  height?: number;
  footnote?: React.ReactNode;
  className?: string;
}) {
  const [view, setView] = useState<"chart" | "table">("chart");
  return (
    <Glass variant="inset" className={cn("p-5", className)}>
      <div className="mb-3 flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="font-display text-base font-bold text-fg">{title}</h3>
          {subtitle && <p className="mt-0.5 text-xs text-fg-3">{subtitle}</p>}
        </div>
        <ClaySegmented
          label={`${title}: view`}
          value={view}
          onChange={setView}
          options={[
            { value: "chart", label: "Chart" },
            { value: "table", label: "Table" },
          ]}
        />
      </div>
      {view === "chart" ? (
        <EChart option={option} height={height} label={title} />
      ) : (
        <div className="overflow-auto" style={{ maxHeight: height }}>
          <table className="w-full text-left text-sm">
            <thead className="sticky top-0 bg-ink-800">
              <tr>
                {table.columns.map((c, i) => (
                  <th
                    key={c}
                    className={cn("px-3 py-2 text-xs font-semibold text-fg-3", table.numeric?.[i] && "text-right")}
                  >
                    {c}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {table.rows.map((row, r) => (
                <tr key={r} className="border-t border-white/5">
                  {row.map((cell, i) => (
                    <td key={i} className={cn("px-3 py-1.5 text-fg-2", table.numeric?.[i] && "text-right tabular-nums")}>
                      {cell}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {footnote && <div className="mt-3 text-xs text-fg-3">{footnote}</div>}
    </Glass>
  );
}
