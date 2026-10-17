// Live map encodings (dataviz method). A station's problem is a polarity: it drains (no bikes left) or fills (no
// free docks), so the two families take the validated diverging poles, red for empty and blue for full. "Now"
// and "within the hour" share a hue and differ in shape: a solid dot is the feed, a ring is the forecast. Every
// state also has a label and a glyph, so colour never carries it alone. Suggested moves are an action, not data,
// and wear the marker yellow of the chrome.
import { DIVERGING, HIGHLIGHT } from "@/components/charts/theme";
import type { Risk } from "@/lib/schemas";

export type RGBA = [number, number, number, number];

const rgb = (hex: string): [number, number, number] => {
  const n = parseInt(hex.slice(1), 16);
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
};

export const EMPTY_HEX = DIVERGING.positive;
export const FULL_HEX = DIVERGING.negative;
export const MOVE_HEX = HIGHLIGHT;

export const STATE: Record<Risk, { label: string; short: string; glyph: string; hex: string; ring: boolean }> = {
  EMPTY: { label: "Empty now", short: "empty", glyph: "●", hex: EMPTY_HEX, ring: false },
  EMPTY_RISK: { label: "Empty within 60 min", short: "empties soon", glyph: "◯", hex: EMPTY_HEX, ring: true },
  FULL: { label: "Full now", short: "full", glyph: "●", hex: FULL_HEX, ring: false },
  FULL_RISK: { label: "Full within 60 min", short: "fills soon", glyph: "◯", hex: FULL_HEX, ring: true },
  OK: { label: "Fine", short: "fine", glyph: "·", hex: "#8f8ab8", ring: false },
  OFFLINE: { label: "Offline", short: "offline", glyph: "×", hex: "#4a4766", ring: false },
};

/** Problem states first: the legend and the ticker read in this order. */
export const STATE_ORDER: Risk[] = ["EMPTY_RISK", "EMPTY", "FULL_RISK", "FULL", "OK", "OFFLINE"];

export function fill(risk: Risk): RGBA {
  const s = STATE[risk];
  if (risk === "OK") return [...rgb(s.hex), 150];
  if (risk === "OFFLINE") return [...rgb(s.hex), 120];
  return s.ring ? [...rgb(s.hex), 45] : [...rgb(s.hex), 235];
}

export function stroke(risk: Risk): RGBA {
  const s = STATE[risk];
  return s.ring ? [...rgb(s.hex), 255] : [11, 10, 29, 200];
}

export const MOVE: RGBA = [...rgb(MOVE_HEX), 230];
