"use client";

import { ChartCard } from "@/components/charts/chart-card";
import { SERIES, TEXT, tooltipRow } from "@/components/charts/theme";
import { int } from "@/lib/format";

/** Vertices updated in each superstep of the full-graph run: the vote to halt, at scale. */
export function ActiveVerticesChart({ active, vertices }: { active: number[]; vertices: number }) {
  const lastIndex = active.length - 1;
  return (
    <ChartCard
      title="Vertices still updating, per superstep"
      subtitle={`Weighted PageRank on all ${int(vertices)} stations; a vertex drops out once its rank change falls below the tolerance`}
      height={280}
      option={{
        grid: { left: 8, right: 24, top: 28, bottom: 8, containLabel: true },
        xAxis: { type: "category", data: active.map((_, i) => i), name: "superstep", nameLocation: "middle", nameGap: 26 },
        yAxis: { type: "value", min: 0, axisLabel: { formatter: (v: number) => int(v) } },
        tooltip: {
          trigger: "item",
          formatter: (p: { dataIndex: number; value: number }) =>
            tooltipRow(SERIES[0], `superstep ${p.dataIndex}`, `${int(p.value)} vertices`),
        },
        series: [
          {
            type: "bar",
            data: active,
            barMaxWidth: 14,
            itemStyle: { color: SERIES[0], borderRadius: [3, 3, 0, 0] },
            label: {
              show: true,
              position: "top",
              color: TEXT.primary,
              formatter: (p: { dataIndex: number; value: number }) => (p.dataIndex >= lastIndex - 1 ? int(p.value) : ""),
            },
          },
        ],
      }}
      table={{
        columns: ["Superstep", "Vertices updated"],
        numeric: [true, true],
        rows: active.map((v, i) => [i, int(v)]),
      }}
    />
  );
}
