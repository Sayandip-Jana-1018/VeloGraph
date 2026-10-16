"use client";

import { useEffect, useRef } from "react";

import { watchPortal } from "@/components/portal/portal-state";

import { THEME_EVENT } from "./themes";

type Point = [number, number, number]; // lon, lat, pagerank; stations arrive in PageRank order
type Flow = [number, number, number]; // from index, to index, trips

/** Map annotations, not data: where each borough's name is written. */
const PLACES: [string, number, number][] = [
  ["Manhattan", -73.9705, 40.7795],
  ["Brooklyn", -73.9445, 40.6655],
  ["Queens", -73.8935, 40.7555],
  ["The Bronx", -73.8885, 40.8515],
  ["Jersey City", -74.0625, 40.7015],
];
const HUBS = 40;
const TAIL = 4;

/**
 * The home page's city of lights, north up: every station is a crisp dot sized by PageRank (the 40 biggest hubs
 * in the highlight colour), the summer's 500 busiest flows are faint arcs, and comets ride them, busier routes
 * carrying more. Everything static is drawn once into an offscreen layer; a frame only adds the comets, in a
 * handful of batched draw calls. The loop runs only while the map is on screen and no portal covers it.
 * Colours follow the active preset; reduced motion draws one still frame.
 */
export function CityLights({ points, flows, className }: { points: Point[]; flows: Flow[]; className?: string }) {
  const ref = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = ref.current;
    const ctx = canvas?.getContext("2d");
    if (!canvas || !ctx) return;
    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    // Equirectangular projection: at New York's latitude a degree of longitude is cos(lat) of a degree of latitude.
    const kx = Math.cos(((points.reduce((s, p) => s + p[1], 0) / points.length) * Math.PI) / 180);
    const projected = points.map(([lon, lat]) => [lon * kx, -lat] as const);
    const xs = projected.map((p) => p[0]);
    const ys = projected.map((p) => p[1]);
    const [x0, x1, y0, y1] = [Math.min(...xs), Math.max(...xs), Math.min(...ys), Math.max(...ys)];
    const maxPr = points[0]?.[2] ?? 1;
    const maxTrips = Math.max(...flows.map((f) => f[2]));

    let width = 0;
    let height = 0;
    let project = (lon: number, lat: number): [number, number] => [lon, lat];
    let curves: { a: [number, number]; c: [number, number]; b: [number, number]; w: number }[] = [];
    let base: HTMLCanvasElement | null = null;
    let colors = { arc: "#7c5cff", station: "#14b8a6", hub: "#ffe066", label: "#9b95c9", font: "cursive" };

    const readColors = () => {
      const css = getComputedStyle(document.documentElement);
      const v = (name: string, fallback: string) => css.getPropertyValue(name).trim() || fallback;
      colors = {
        arc: v("--color-glow-a", colors.arc),
        station: v("--color-glow-b", colors.station),
        hub: v("--color-marker", colors.hub),
        label: v("--color-fg-2", colors.label),
        font: v("--font-caveat", colors.font),
      };
    };

    const layout = () => {
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      width = canvas.clientWidth;
      height = canvas.clientHeight;
      canvas.width = Math.round(width * dpr);
      canvas.height = Math.round(height * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

      const scale = Math.min((width * 0.88) / (x1 - x0), (height * 0.88) / (y1 - y0));
      const [ox, oy] = [(width - (x1 - x0) * scale) / 2, (height - (y1 - y0) * scale) / 2];
      project = (lon, lat) => [ox + (lon * kx - x0) * scale, oy + (-lat - y0) * scale];
      const screen = points.map(([lon, lat]) => project(lon, lat));
      curves = flows.map(([i, j, trips]) => {
        const [a, b] = [screen[i], screen[j]];
        const [dx, dy] = [b[0] - a[0], b[1] - a[1]];
        return { a, b, c: [(a[0] + b[0]) / 2 - dy * 0.2, (a[1] + b[1]) / 2 + dx * 0.2], w: trips / maxTrips };
      });

      base = document.createElement("canvas");
      base.width = canvas.width;
      base.height = canvas.height;
      const g = base.getContext("2d")!;
      g.setTransform(dpr, 0, 0, dpr, 0, 0);
      // Arcs are painted normally, not added up, so the dense Midtown core keeps its colour instead of burning white.
      g.strokeStyle = colors.arc;
      g.lineCap = "round";
      for (const k of curves) {
        g.globalAlpha = 0.07 + 0.3 * k.w;
        g.lineWidth = 0.5 + 1.5 * k.w;
        g.beginPath();
        g.moveTo(k.a[0], k.a[1]);
        g.quadraticCurveTo(k.c[0], k.c[1], k.b[0], k.b[1]);
        g.stroke();
      }
      // Stations, smallest first so hubs sit on top; crisp circles, no glow sprites.
      for (let i = points.length - 1; i >= 0; i--) {
        const [x, y] = screen[i];
        const t = Math.sqrt(points[i][2] / maxPr);
        const hub = i < HUBS;
        g.globalAlpha = hub ? 1 : 0.5 + 0.45 * t;
        g.fillStyle = hub ? colors.hub : colors.station;
        g.beginPath();
        g.arc(x, y, hub ? 2.2 + 2.6 * t : 0.9 + 1.9 * t, 0, Math.PI * 2);
        g.fill();
        if (hub) {
          g.globalAlpha = 0.35;
          g.strokeStyle = colors.hub;
          g.lineWidth = 1;
          g.beginPath();
          g.arc(x, y, 5.5 + 3 * t, 0, Math.PI * 2);
          g.stroke();
        }
      }
      g.globalAlpha = 0.8;
      g.fillStyle = colors.label;
      g.font = `600 ${Math.round(Math.max(17, Math.min(24, width / 26)))}px ${colors.font}`;
      g.textAlign = "center";
      for (const [name, lon, lat] of PLACES) {
        const [x, y] = project(lon, lat);
        g.fillText(name, x, y);
      }
      g.globalAlpha = 1;
    };

    // One comet per route, two on the busiest fifth; each has its own phase and speed.
    const comets = flows.flatMap((f, i) =>
      Array.from({ length: f[2] / maxTrips > 0.45 ? 2 : 1 }, (_, k) => ({
        curve: i,
        phase: (i * 0.618 + k * 0.5) % 1,
        speed: 0.00005 + 0.00004 * ((i * 7) % 5) * 0.25,
      })),
    );
    const at = (k: (typeof curves)[number], u: number): [number, number] => [
      (1 - u) ** 2 * k.a[0] + 2 * (1 - u) * u * k.c[0] + u ** 2 * k.b[0],
      (1 - u) ** 2 * k.a[1] + 2 * (1 - u) * u * k.c[1] + u ** 2 * k.b[1],
    ];

    const draw = (time: number) => {
      ctx.clearRect(0, 0, width, height);
      if (base) ctx.drawImage(base, 0, 0, width, height);
      const heads = comets.map((m) => ({ k: curves[m.curve], u: (m.phase + time * m.speed) % 1 })).filter((h) => h.k);
      ctx.strokeStyle = colors.hub;
      ctx.fillStyle = colors.hub;
      ctx.lineCap = "round";
      // The tail: TAIL short segments, each one batched across all comets and fainter than the one before.
      for (let s = 0; s < TAIL; s++) {
        ctx.globalAlpha = 0.55 - s * 0.12;
        ctx.lineWidth = 1.8 - s * 0.35;
        ctx.beginPath();
        for (const { k, u } of heads) {
          const [from, to] = [u - s * 0.012, u - (s + 1) * 0.012];
          if (to < 0) continue;
          const [p, q] = [at(k, from), at(k, to)];
          ctx.moveTo(p[0], p[1]);
          ctx.lineTo(q[0], q[1]);
        }
        ctx.stroke();
      }
      for (const [radius, alpha] of [
        [3.6, 0.22],
        [1.6, 1],
      ]) {
        ctx.globalAlpha = alpha;
        ctx.beginPath();
        for (const { k, u } of heads) {
          const [x, y] = at(k, u);
          ctx.moveTo(x + radius, y);
          ctx.arc(x, y, radius, 0, Math.PI * 2);
        }
        ctx.fill();
      }
      ctx.globalAlpha = 1;
    };

    let frame = 0;
    let visible = false;
    let covered = false;
    const loop = (time: number) => {
      draw(time);
      frame = requestAnimationFrame(loop);
    };
    const sync = () => {
      const run = visible && !covered && !reduce;
      if (run && !frame) frame = requestAnimationFrame(loop);
      if (!run && frame) {
        cancelAnimationFrame(frame);
        frame = 0;
      }
    };
    const repaint = () => {
      readColors();
      layout();
      draw(reduce ? 0 : performance.now());
    };

    // Preset colours cross-fade for about a second (registered CSS properties), so repaint once they have settled.
    let settle: ReturnType<typeof setTimeout> | undefined;
    const onTheme = () => {
      clearTimeout(settle);
      settle = setTimeout(repaint, 1300);
    };
    let live = true;
    repaint();
    void document.fonts.ready.then(() => live && repaint()); // the borough names need the hand font
    const resize = new ResizeObserver(repaint);
    resize.observe(canvas);
    const seen = new IntersectionObserver(([entry]) => {
      visible = entry.isIntersecting;
      sync();
    });
    seen.observe(canvas);
    const unwatch = watchPortal((open) => {
      covered = open;
      sync();
    });
    window.addEventListener(THEME_EVENT, onTheme);
    return () => {
      live = false;
      cancelAnimationFrame(frame);
      clearTimeout(settle);
      resize.disconnect();
      seen.disconnect();
      unwatch();
      window.removeEventListener(THEME_EVENT, onTheme);
    };
  }, [points, flows]);

  return <canvas ref={ref} aria-hidden className={className} />;
}
