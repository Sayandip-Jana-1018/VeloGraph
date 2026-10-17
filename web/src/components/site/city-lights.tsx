"use client";

import { useEffect, useRef } from "react";

import { watchPortal } from "@/components/portal/portal-state";

import { THEME_EVENT } from "./themes";

type Point = [number, number, number]; // lon, lat, pagerank; stations arrive in PageRank order
type Flow = [number, number, number]; // from index, to index, trips

/** serving/hero-trips.json (HeroTripsJob): one real evening, one trip in `sampleEvery`. */
type Replay = {
  window: { date: string; from: string; until: string; sampleEvery: number };
  totalTrips: number;
  stations: [number, number][]; // lon, lat
  trips: [number, number, number, number][]; // start station, end station, start second, duration
  underWay: { minute: number; underWay: number }[];
};

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
/** The replay runs at 120× real time: three hours of an evening in 90 seconds. */
const SPEED = 120;

function isReplay(x: unknown): x is Replay {
  const r = x as Replay;
  return Array.isArray(r?.stations) && Array.isArray(r?.trips) && Array.isArray(r?.underWay) && !!r?.window;
}

/**
 * The home page's city of lights, north up: every station is a crisp dot sized by PageRank (the 40 biggest hubs
 * in the highlight colour) and the summer's 500 busiest flows are faint arcs. Over them runs one real evening:
 * each comet is a trip of `replay`, leaving its station at its real time, 120 times faster. A clock reads the
 * evening's time and how many rides were under way then. Without the replay (offline, say) the comets ride the
 * busiest flows instead.
 *
 * Everything static is drawn once into an offscreen layer; a frame only adds the comets, in a handful of batched
 * draw calls. The loop runs only while the map is on screen and no portal covers it. Colours follow the active
 * preset; reduced motion draws one still frame at the evening's busiest minute.
 */
export function CityLights({
  points,
  flows,
  replay: replayUrl,
  label,
  className,
  clockClassName,
}: {
  points: Point[];
  flows: Flow[];
  replay?: string;
  label: string;
  className?: string;
  clockClassName?: string;
}) {
  const ref = useRef<HTMLCanvasElement>(null);
  const clock = useRef<HTMLParagraphElement>(null);

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

    // The replay, once loaded: per trip its curve (a, control, b) in screen space, start and duration.
    let replay: Replay | null = null;
    let flowGeo = new Float32Array(0);
    let geo = new Float32Array(0);
    let starts = new Int32Array(0);
    let lengths = new Int32Array(0);
    let windowSeconds = 1;
    let peakSecond = 0;
    let shownMinute = -1;

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

    const bend = (a: [number, number], b: [number, number], k: number): [number, number] => {
      const [dx, dy] = [b[0] - a[0], b[1] - a[1]];
      return [(a[0] + b[0]) / 2 - dy * k, (a[1] + b[1]) / 2 + dx * k];
    };

    const layoutReplay = () => {
      if (!replay) return;
      const at = replay.stations.map(([lon, lat]) => project(lon, lat));
      const n = replay.trips.length;
      geo = new Float32Array(n * 6);
      starts = new Int32Array(n);
      lengths = new Int32Array(n);
      replay.trips.forEach(([s, e, t, d], i) => {
        const [a, b] = [at[s], at[e]];
        const c = bend(a, b, 0.12);
        geo.set([a[0], a[1], c[0], c[1], b[0], b[1]], i * 6);
        starts[i] = t;
        lengths[i] = d;
      });
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
        return { a, b, c: bend(a, b, 0.2), w: trips / maxTrips };
      });
      flowGeo = new Float32Array(curves.length * 6);
      curves.forEach((k, i) => flowGeo.set([k.a[0], k.a[1], k.c[0], k.c[1], k.b[0], k.b[1]], i * 6));
      layoutReplay();

      base = document.createElement("canvas");
      base.width = canvas.width;
      base.height = canvas.height;
      const g = base.getContext("2d")!;
      g.setTransform(dpr, 0, 0, dpr, 0, 0);
      // Arcs are painted normally, not added up, so the dense Midtown core keeps its colour instead of burning white.
      g.strokeStyle = colors.arc;
      g.lineCap = "round";
      for (const k of curves) {
        g.globalAlpha = (replay ? 0.04 : 0.07) + (replay ? 0.18 : 0.3) * k.w;
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

    // Without a replay: one comet per route, two on the busiest fifth; each has its own phase and speed.
    const comets = flows.flatMap((f, i) =>
      Array.from({ length: f[2] / maxTrips > 0.45 ? 2 : 1 }, (_, k) => ({
        curve: i,
        phase: (i * 0.618 + k * 0.5) % 1,
        speed: 0.00005 + 0.00004 * ((i * 7) % 5) * 0.25,
      })),
    );
    // The comets of this frame: which curve (6 floats in `source`) and how far along it, in reused buffers, so
    // a frame allocates nothing even with a thousand trips under way.
    let index = new Int32Array(Math.max(comets.length, 1));
    let along = new Float32Array(Math.max(comets.length, 1));
    let count = 0;
    let source = flowGeo;
    const collect = (time: number) => {
      count = 0;
      if (!replay) {
        source = flowGeo;
        for (const m of comets) {
          index[count] = m.curve;
          along[count++] = (m.phase + time * m.speed) % 1;
        }
        return;
      }
      if (index.length < starts.length) {
        index = new Int32Array(starts.length);
        along = new Float32Array(starts.length);
      }
      source = geo;
      const T = replaySecond(time);
      for (let i = 0; i < starts.length; i++) {
        const dt = T - starts[i];
        if (dt < 0 || dt >= lengths[i]) continue;
        index[count] = i;
        along[count++] = dt / lengths[i];
      }
    };
    // A point on comet k's quadratic curve at u.
    const qx = (o: number, u: number) => (1 - u) ** 2 * source[o] + 2 * (1 - u) * u * source[o + 2] + u ** 2 * source[o + 4];
    const qy = (o: number, u: number) =>
      (1 - u) ** 2 * source[o + 1] + 2 * (1 - u) * u * source[o + 3] + u ** 2 * source[o + 5];

    // Reduced motion shows the busiest minute; otherwise the loop starts an hour in, near the evening rush.
    const replaySecond = (time: number) => (reduce ? peakSecond : (3600 + (time / 1000) * SPEED) % windowSeconds);

    const tellTime = (time: number) => {
      if (!replay || !clock.current) return;
      const minute = Math.floor(replaySecond(time) / 60);
      if (minute === shownMinute) return;
      shownMinute = minute;
      const [h, m] = replay.window.from.split(":").map(Number);
      const total = h * 60 + m + minute;
      const hhmm = `${String(Math.floor(total / 60)).padStart(2, "0")}:${String(total % 60).padStart(2, "0")}`;
      const under = replay.underWay[minute]?.underWay ?? 0;
      const day = new Date(`${replay.window.date}T12:00:00Z`).toLocaleDateString("en-GB", {
        weekday: "short",
        day: "numeric",
        month: "short",
        timeZone: "UTC",
      });
      // Two lines (the clock is `white-space: pre-line`), so it clears the station count on a narrow map.
      clock.current.textContent = `${day} · ${hhmm}
${under.toLocaleString("en-US")} rides under way`;
    };

    const draw = (time: number) => {
      ctx.clearRect(0, 0, width, height);
      if (base) ctx.drawImage(base, 0, 0, width, height);
      collect(time);
      const step = replay ? 0.035 : 0.012;
      ctx.strokeStyle = colors.hub;
      ctx.fillStyle = colors.hub;
      ctx.lineCap = "round";
      // The tail: short segments, each one batched across all comets and fainter than the one before. A thousand
      // trips under way at once get one segment fewer and no halo, which keeps a frame inside the budget.
      const tail = replay ? TAIL - 1 : TAIL;
      for (let s = 0; s < tail; s++) {
        ctx.globalAlpha = 0.55 - s * 0.12;
        ctx.lineWidth = 1.8 - s * 0.35;
        ctx.beginPath();
        for (let k = 0; k < count; k++) {
          const o = index[k] * 6;
          const from = along[k] - s * step;
          const to = along[k] - (s + 1) * step;
          if (to < 0) continue;
          ctx.moveTo(qx(o, from), qy(o, from));
          ctx.lineTo(qx(o, to), qy(o, to));
        }
        ctx.stroke();
      }
      for (const [radius, alpha] of replay
        ? [[1.4, 1]]
        : [
            [3.2, 0.2],
            [1.4, 1],
          ]) {
        ctx.globalAlpha = alpha;
        ctx.beginPath();
        for (let k = 0; k < count; k++) {
          const o = index[k] * 6;
          const x = qx(o, along[k]);
          const y = qy(o, along[k]);
          ctx.moveTo(x + radius, y);
          ctx.arc(x, y, radius, 0, Math.PI * 2);
        }
        ctx.fill();
      }
      ctx.globalAlpha = 1;
      tellTime(time);
    };

    let frame = 0;
    let visible = false;
    let covered = false;
    let live = true;
    let requested = false;
    // 30 frames a second: a comet crosses its ride in seconds, so every other display frame is enough, and the
    // main thread keeps the rest for scrolling.
    let drawn = 0;
    const loop = (time: number) => {
      if (time - drawn >= 31) {
        draw(time);
        drawn = time;
      }
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

    // The evening is fetched once the map is first on screen, so a visitor who never scrolls there pays nothing.
    const loadReplay = () => {
      if (!replayUrl || requested) return;
      requested = true;
      fetch(replayUrl)
        .then((response) => (response.ok ? response.json() : Promise.reject(new Error(`HTTP ${response.status}`))))
        .then((json: { data?: unknown }) => {
          if (!live || !isReplay(json.data)) return;
          replay = json.data;
          windowSeconds = Math.max(60, replay.underWay.length * 60);
          const peak = replay.underWay.reduce((best, m) => (m.underWay > best.underWay ? m : best), replay.underWay[0]);
          peakSecond = (peak?.minute ?? 0) * 60 + 30;
          repaint();
        })
        .catch(() => {}); // the busiest-flow comets stay: the hero never depends on the replay
    };

    // Preset colours cross-fade for about a second (registered CSS properties), so repaint once they have settled.
    let settle: ReturnType<typeof setTimeout> | undefined;
    const onTheme = () => {
      clearTimeout(settle);
      settle = setTimeout(repaint, 1300);
    };
    repaint();
    void document.fonts.ready.then(() => live && repaint()); // the borough names need the hand font
    const resize = new ResizeObserver(repaint);
    resize.observe(canvas);
    const seen = new IntersectionObserver(([entry]) => {
      visible = entry.isIntersecting;
      if (visible) loadReplay();
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
  }, [points, flows, replayUrl]);

  return (
    <>
      <canvas ref={ref} role="img" aria-label={label} className={className} />
      {replayUrl && <p ref={clock} aria-hidden className={clockClassName} />}
    </>
  );
}
