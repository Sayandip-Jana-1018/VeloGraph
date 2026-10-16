"use client";

import { BarChart, BoxplotChart, HeatmapChart, LineChart, ScatterChart } from "echarts/charts";
import {
  DatasetComponent,
  GridComponent,
  LegendComponent,
  MarkLineComponent,
  MarkPointComponent,
  TooltipComponent,
  VisualMapComponent,
} from "echarts/components";
import * as echarts from "echarts/core";
import type { EChartsCoreOption } from "echarts/core";
import { SVGRenderer } from "echarts/renderers";
import { useEffect, useRef } from "react";

import { INKGLASS_THEME } from "./theme";

echarts.use([
  BarChart,
  BoxplotChart,
  LineChart,
  ScatterChart,
  HeatmapChart,
  DatasetComponent,
  GridComponent,
  LegendComponent,
  MarkLineComponent,
  MarkPointComponent,
  TooltipComponent,
  VisualMapComponent,
  SVGRenderer,
]);
echarts.registerTheme("inkglass", INKGLASS_THEME);

function apply(chart: echarts.ECharts, option: EChartsCoreOption) {
  const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  chart.setOption({ animation: !reduce, ...option }, { notMerge: true });
}

/**
 * ECharts on the SVG renderer (crisp at any zoom, prints well), themed with the Inkglass chart tokens. A chart is
 * created only when it comes within a screen of the viewport, so opening a portal with a dozen charts does not
 * build them all at once. It resizes with its box and is disposed with the component; reduced motion turns
 * animation off.
 */
export function EChart({
  option,
  height = 320,
  label,
  className,
}: {
  option: EChartsCoreOption;
  height?: number;
  label: string;
  className?: string;
}) {
  const box = useRef<HTMLDivElement>(null);
  const chart = useRef<echarts.ECharts | null>(null);
  const latest = useRef(option);

  useEffect(() => {
    const el = box.current;
    if (!el) return;
    let resize: ResizeObserver | undefined;
    const near = new IntersectionObserver(
      (entries) => {
        if (!entries.some((e) => e.isIntersecting)) return;
        near.disconnect();
        const instance = echarts.init(el, "inkglass", { renderer: "svg" });
        chart.current = instance;
        apply(instance, latest.current);
        resize = new ResizeObserver(() => instance.resize());
        resize.observe(el);
      },
      { rootMargin: "100% 0px" },
    );
    near.observe(el);
    return () => {
      near.disconnect();
      resize?.disconnect();
      chart.current?.dispose();
      chart.current = null;
    };
  }, []);

  useEffect(() => {
    latest.current = option;
    if (chart.current) apply(chart.current, option);
  }, [option]);

  return <div ref={box} role="img" aria-label={label} className={className} style={{ height }} />;
}
