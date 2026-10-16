import rough from "roughjs";
import type { Drawable, Options } from "roughjs/bin/core";
import { getStroke } from "perfect-freehand";

/**
 * Hand-drawn geometry, computed (not drawn) so it can run on the server. rough.js and perfect-freehand both
 * return path data; a fixed seed makes every render identical, so server and client markup always match.
 */
const generator = rough.generator();

export type InkPath = { d: string; stroke: string; strokeWidth: number; fill?: string };

export function toInk(drawable: Drawable): InkPath[] {
  return generator.toPaths(drawable).map((p) => ({
    d: p.d,
    stroke: p.stroke,
    strokeWidth: p.strokeWidth,
    fill: p.fill && p.fill !== "none" ? p.fill : undefined,
  }));
}

export const sketch = {
  line: (x1: number, y1: number, x2: number, y2: number, o: Options) => toInk(generator.line(x1, y1, x2, y2, o)),
  rect: (x: number, y: number, w: number, h: number, o: Options) => toInk(generator.rectangle(x, y, w, h, o)),
  ellipse: (x: number, y: number, w: number, h: number, o: Options) => toInk(generator.ellipse(x, y, w, h, o)),
  curve: (points: [number, number][], o: Options) => toInk(generator.curve(points, o)),
  polygon: (points: [number, number][], o: Options) => toInk(generator.polygon(points, o)),
};

/** perfect-freehand outline -> SVG path (closed, filled). */
export function freehand(points: [number, number][], size = 3.2, taperEnd = 24): string {
  const outline = getStroke(points, {
    size,
    thinning: 0.62,
    smoothing: 0.6,
    streamline: 0.45,
    simulatePressure: true,
    start: { taper: 6 },
    end: { taper: taperEnd },
  });
  if (outline.length < 2) return "";
  const [first, ...rest] = outline;
  const segments = rest.map(([x, y], i) => {
    const [px, py] = outline[i];
    return `Q${px.toFixed(2)},${py.toFixed(2)} ${((px + x) / 2).toFixed(2)},${((py + y) / 2).toFixed(2)}`;
  });
  return `M${first[0].toFixed(2)},${first[1].toFixed(2)} ${segments.join(" ")} Z`;
}

/** A gently curved hand-drawn arrow from `a` to `b`; `bend` bows it sideways (fraction of its length). */
export function arrowPaths(a: [number, number], b: [number, number], bend = 0.25): string[] {
  const [dx, dy] = [b[0] - a[0], b[1] - a[1]];
  const control: [number, number] = [a[0] + dx / 2 - dy * bend, a[1] + dy / 2 + dx * bend];
  const shaft = Array.from({ length: 28 }, (_, i): [number, number] => {
    const t = i / 27;
    return [
      (1 - t) ** 2 * a[0] + 2 * (1 - t) * t * control[0] + t ** 2 * b[0],
      (1 - t) ** 2 * a[1] + 2 * (1 - t) * t * control[1] + t ** 2 * b[1],
    ];
  });
  // The head follows the curve's final direction (tangent at t = 1 points from the control point to b).
  const angle = Math.atan2(b[1] - control[1], b[0] - control[0]);
  const barb = (turn: number): [number, number][] => {
    const len = 13;
    const end: [number, number] = [b[0] - len * Math.cos(angle + turn), b[1] - len * Math.sin(angle + turn)];
    return [end, [(end[0] + b[0]) / 2, (end[1] + b[1]) / 2], b];
  };
  return [freehand(shaft), freehand(barb(0.5), 2.8, 4), freehand(barb(-0.5), 2.8, 4)];
}
