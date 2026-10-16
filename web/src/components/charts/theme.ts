// Chart tokens: the dataviz method's parameters filled with Inkglass values. Series colours are the validated
// categorical palette (dark steps, fixed order) on the ink-800 chart surface; text never wears a series colour.
export const SURFACE = "#1c1938";
export const TEXT = { primary: "#f5f2ff", secondary: "#cbc5ef", muted: "#9b95c9" };
export const GRID = "#2c2950";
export const AXIS = "#403b75";

export const SERIES = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"];

/** Emphasis pattern: the series the story is about in slot 1, every other one in this recessive context colour. */
export const CONTEXT_SERIES = "#5f5a8c";

/** Marker yellow, for a "you are here" rule on a chart (chrome, not data). */
export const HIGHLIGHT = "#ffe066";

/**
 * Sequential (magnitude): one hue, the documented blue ramp from step 600 to 150. On a dark surface the anchor
 * flips, so more is lighter; step 600 is the darkest that still clears 2:1 on the surface.
 */
export const BLUE_RAMP = ["#184f95", "#1c5cab", "#256abf", "#2a78d6", "#3987e5", "#5598e7", "#6da7ec", "#86b6ef", "#9ec5f4", "#b7d3f6"];

/** Diverging (polarity): blue and red poles around the documented dark neutral, which reads as "nothing". */
export const DIVERGING = { negative: "#3987e5", neutral: "#383835", positive: "#e66767" };

const FONT = "Inter, 'Segoe UI', system-ui, sans-serif";

export const INKGLASS_THEME = {
  color: SERIES,
  backgroundColor: "transparent",
  textStyle: { fontFamily: FONT, color: TEXT.secondary },
  grid: { left: 12, right: 24, top: 28, bottom: 12, containLabel: true },
  categoryAxis: {
    axisLine: { show: true, lineStyle: { color: AXIS } },
    axisTick: { show: false },
    axisLabel: { color: TEXT.muted, fontSize: 11 },
    splitLine: { show: false },
  },
  valueAxis: {
    axisLine: { show: false },
    axisTick: { show: false },
    axisLabel: { color: TEXT.muted, fontSize: 11 },
    splitLine: { show: true, lineStyle: { color: GRID, width: 1, type: "solid" } },
  },
  legend: { textStyle: { color: TEXT.secondary, fontSize: 12 }, itemWidth: 14, itemHeight: 3, icon: "roundRect" },
  tooltip: {
    backgroundColor: "rgba(22, 19, 58, 0.96)",
    borderColor: "rgba(255, 255, 255, 0.14)",
    borderWidth: 1,
    padding: [8, 12],
    textStyle: { color: TEXT.primary, fontFamily: FONT, fontSize: 12 },
    extraCssText: "border-radius: 12px; box-shadow: 0 12px 30px -10px rgba(0,0,0,0.7); backdrop-filter: blur(8px);",
  },
  line: { symbol: "circle", symbolSize: 8, lineStyle: { width: 2 }, smooth: false },
  bar: { barMaxWidth: 24, itemStyle: { borderRadius: [4, 4, 0, 0] } },
};

/** Rounded data end for horizontal bars (the baseline end stays square). */
export const BAR_END_H = { borderRadius: [0, 4, 4, 0] };

/** A tooltip row: line key in the series colour, the value strong, the name secondary. */
export function tooltipRow(color: string, name: string, value: string): string {
  return (
    `<div style="display:flex;align-items:center;gap:8px;margin:2px 0">` +
    `<span style="width:12px;height:3px;border-radius:2px;background:${color}"></span>` +
    `<b style="color:${TEXT.primary}">${value}</b><span style="color:${TEXT.secondary}">${name}</span></div>`
  );
}
