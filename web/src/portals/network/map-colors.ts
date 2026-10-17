// Map colour encodings for the network explorer, each doing one dataviz job:
//   magnitude  -> the documented blue ramp (sequential; on the dark map, more = lighter)
//   polarity   -> blue / red poles around the documented dark neutral (diverging)
//   identity   -> emphasis: the chosen community in slot 1, every other one recessive (no 15-hue palette)
//   bands      -> a five-step ordinal blue ramp, validated with the dataviz script on the dark surface
import { BLUE_RAMP, DIVERGING, SERIES } from "@/components/charts/theme";

export type RGBA = [number, number, number, number];

const rgb = (hex: string): [number, number, number] => {
  const n = Number.parseInt(hex.slice(1), 16);
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
};
const mix = (a: string, b: string, t: number): [number, number, number] => {
  const [x, y] = [rgb(a), rgb(b)];
  return [0, 1, 2].map((i) => Math.round(x[i] + (y[i] - x[i]) * t)) as [number, number, number];
};

/** Position in [0, 1] -> a step of the blue ramp. */
export function sequential(position: number, alpha = 230): RGBA {
  const i = Math.max(0, Math.min(BLUE_RAMP.length - 1, Math.round(position * (BLUE_RAMP.length - 1))));
  return [...rgb(BLUE_RAMP[i]), alpha];
}

/** Value in [-1, 1] -> evening (blue) ... neutral ... morning (red). */
export function diverging(value: number, alpha = 235): RGBA {
  const t = Math.max(-1, Math.min(1, value));
  const pole = t < 0 ? DIVERGING.negative : DIVERGING.positive;
  // A small floor keeps near-neutral stations visible as dots on the dark basemap.
  return [...mix(DIVERGING.neutral, pole, Math.min(1, Math.abs(t) * 1.25)), alpha];
}

export const EMPHASIS: RGBA = [...rgb(SERIES[0]), 245];
export const CONTEXT: RGBA = [95, 90, 140, 120];
export const NONE: RGBA = [60, 58, 80, 110];

/** Travel-time bands: upper bounds in minutes and their colours (validated ordinal ramp, near = light). */
export const TRAVEL_BANDS: { max: number; label: string; color: string }[] = [
  { max: 10, label: "≤ 10 min", color: "#b7d3f6" },
  { max: 15, label: "≤ 15 min", color: "#86b6ef" },
  { max: 20, label: "≤ 20 min", color: "#5598e7" },
  { max: 30, label: "≤ 30 min", color: "#2a78d6" },
  { max: Infinity, label: "> 30 min", color: "#184f95" },
];

export function travelColor(seconds: number | null): RGBA {
  if (seconds === null) return NONE;
  const band = TRAVEL_BANDS.find((b) => seconds / 60 <= b.max)!;
  return [...rgb(band.color), 235];
}

export const hex = rgb;
