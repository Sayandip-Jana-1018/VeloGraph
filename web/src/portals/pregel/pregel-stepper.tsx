"use client";

import { useEffect, useMemo, useState } from "react";

import { EChart } from "@/components/charts/echart";
import { CONTEXT_SERIES, HIGHLIGHT, SERIES, TEXT, tooltipRow } from "@/components/charts/theme";
import { ClayButton, ClaySlider } from "@/components/inkglass/clay";
import { Glass } from "@/components/inkglass/surfaces";
import { cn } from "@/lib/cn";
import { escapeHtml, fixed } from "@/lib/format";
import type { NetworkPregel } from "@/lib/schemas";

type Data = NetworkPregel["data"];
const W = 620;
const H = 470;
const PAD = 70;

const short = (name: string) => name.split(" & ")[0];

/**
 * Weighted PageRank on a real neighbourhood, superstep by superstep. The states and messages come from the job's
 * driver-side replay of the Pregel program, which is checked against GraphX's own run (network-pregel.json).
 */
export function PregelStepper({ data }: { data: Data }) {
  const last = data.steps.length - 1;
  const [step, setStep] = useState(0);
  const [playing, setPlaying] = useState(false);
  const [focus, setFocus] = useState(0);
  const alpha = data.resetProb;
  const running = playing && step < last; // playback stops by itself on the final superstep

  useEffect(() => {
    if (!running) return;
    const timer = setTimeout(() => setStep((s) => Math.min(s + 1, last)), 1100);
    return () => clearTimeout(timer);
  }, [running, step, last]);

  // Moving by hand pauses playback; Play on the final superstep starts over.
  const goTo = (s: number) => {
    setPlaying(false);
    setStep(Math.max(0, Math.min(last, s)));
  };
  const togglePlay = () => {
    if (running) return setPlaying(false);
    if (step >= last) setStep(0);
    setPlaying(true);
  };

  // Real coordinates, scaled to the box (longitude shrunk by cos(latitude) to keep the shape).
  const xy = useMemo(() => {
    const lats = data.stations.map((s) => s.lat);
    const lons = data.stations.map((s) => s.lon);
    const kx = Math.cos((lats.reduce((a, b) => a + b, 0) / lats.length) * (Math.PI / 180));
    const [x0, x1, y0, y1] = [Math.min(...lons) * kx, Math.max(...lons) * kx, Math.min(...lats), Math.max(...lats)];
    const scale = Math.min((W - PAD * 2) / (x1 - x0), (H - PAD * 2) / (y1 - y0));
    const ox = (W - (x1 - x0) * scale) / 2;
    const oy = (H - (y1 - y0) * scale) / 2;
    return data.stations.map((s) => [ox + (s.lon * kx - x0) * scale, oy + (y1 - s.lat) * scale] as const);
  }, [data.stations]);

  // Each direction of a pair bends to its own side, so the two arcs never overlap.
  const arc = (from: number, to: number) => {
    const [a, b] = [xy[from], xy[to]];
    const [mx, my] = [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2];
    const [dx, dy] = [b[0] - a[0], b[1] - a[1]];
    return `M${a[0]},${a[1]} Q${mx - dy * 0.14},${my + dx * 0.14} ${b[0]},${b[1]}`;
  };

  const now = data.steps[step];
  const sent = now.messages;
  const received = step > 0 ? data.steps[step - 1].messages.filter((m) => m.to === focus) : [];
  const incoming = received.reduce((s, m) => s + m.mass, 0);
  const prevRank = step > 0 ? data.steps[step - 1].ranks[focus] : 0;
  const updated = now.updated.filter(Boolean).length;
  const maxRank = Math.max(...data.steps[last].ranks);
  const radius = (rank: number) => 9 + 20 * Math.sqrt(rank / maxRank);
  const maxMass = Math.max(...data.steps.flatMap((s) => s.messages.map((m) => m.mass)), 1e-9);

  const chart = useMemo(
    () => ({
      grid: { left: 8, right: 120, top: 16, bottom: 8, containLabel: true },
      xAxis: { type: "category", data: data.steps.map((s) => s.superstep), name: "superstep", nameLocation: "middle", nameGap: 26 },
      yAxis: { type: "value", axisLabel: { formatter: (v: number) => fixed(v, 1) } },
      tooltip: {
        trigger: "axis",
        formatter: (items: { dataIndex: number }[]) => {
          const s = data.steps[items[0].dataIndex];
          return (
            `<div style="color:${TEXT.secondary};margin-bottom:4px">superstep ${s.superstep}</div>` +
            data.stations
              .map((st, i) => tooltipRow(i === focus ? SERIES[0] : CONTEXT_SERIES, escapeHtml(short(st.name)), fixed(s.ranks[i], 3)))
              .join("")
          );
        },
      },
      series: data.stations.map((st, i) => ({
        name: st.name,
        type: "line",
        data: data.steps.map((s) => s.ranks[i]),
        showSymbol: false,
        z: i === focus ? 3 : 1,
        lineStyle: { color: i === focus ? SERIES[0] : CONTEXT_SERIES, width: i === focus ? 2.5 : 1.2, opacity: i === focus ? 1 : 0.6 },
        endLabel: { show: i === focus, color: TEXT.primary, formatter: short(st.name) },
        markLine:
          i === focus
            ? { symbol: "none", silent: true, label: { show: false }, lineStyle: { color: HIGHLIGHT, width: 1 }, data: [{ xAxis: step }] }
            : undefined,
      })),
    }),
    [data, focus, step],
  );

  return (
    <div className="space-y-5">
      <div className="grid gap-5 xl:grid-cols-[minmax(0,1.25fr)_minmax(0,1fr)]">
        <Glass variant="inset" className="relative overflow-hidden p-2">
          <svg viewBox={`0 0 ${W} ${H}`} className="h-auto w-full" role="img" aria-label={`Superstep ${now.superstep} of weighted PageRank on ${data.stations.length} stations`}>
            <defs>
              <marker id="tip" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
                <path d="M0,0 L10,5 L0,10 z" fill="var(--color-marker)" />
              </marker>
            </defs>
            {data.edges.map((e) => {
              const active = sent.some((m) => m.from === e.from && m.to === e.to);
              return (
                <path
                  key={`${e.from}-${e.to}`}
                  d={arc(e.from, e.to)}
                  fill="none"
                  stroke={active ? "var(--color-marker)" : "var(--color-chalk)"}
                  strokeOpacity={active ? 0.55 : 0.08}
                  strokeWidth={0.6 + 3 * e.probability}
                  markerEnd={active ? "url(#tip)" : undefined}
                />
              );
            })}
            {sent.map((m, i) => (
              <circle key={`${step}-${i}`} r={2 + 7 * Math.sqrt(m.mass / maxMass)} fill="var(--color-marker)">
                <animateMotion dur="1s" repeatCount="1" fill="freeze" path={arc(m.from, m.to)} />
              </circle>
            ))}
            {data.stations.map((s, i) => {
              const [x, y] = xy[i];
              const r = radius(now.ranks[i]);
              const halted = step > 0 && !now.updated[i];
              return (
                <g key={s.id} onClick={() => setFocus(i)} className="cursor-pointer" role="button" aria-label={`Focus ${s.name}`}>
                  <circle cx={x} cy={y} r={r + 7} fill="transparent" />
                  <circle
                    cx={x}
                    cy={y}
                    r={r}
                    fill={halted ? "var(--color-ink-700)" : "var(--color-series-1)"}
                    fillOpacity={i === focus || halted ? 1 : 0.7}
                    stroke={i === focus ? "var(--color-marker)" : now.updated[i] ? "var(--color-chalk)" : "var(--color-fg-3)"}
                    strokeWidth={i === focus ? 3 : 1.5}
                    strokeDasharray={halted ? "3 3" : undefined}
                  />
                  <text x={x} y={y + 4} textAnchor="middle" fontSize={11} fontWeight={700} fill="var(--color-fg)">
                    {fixed(now.ranks[i], 2)}
                  </text>
                  <text x={x} y={y + r + 18} textAnchor="middle" className="font-hand" fontSize={17} fill="var(--color-chalk)" stroke="var(--color-ink-950)" strokeWidth={4} paintOrder="stroke">
                    {short(s.name)}
                  </text>
                </g>
              );
            })}
          </svg>
          <p className="absolute top-3 left-4 font-hand text-lg text-marker">Williamsburg, real positions · click a station</p>
        </Glass>

        <div className="space-y-4">
          <Glass variant="inset" className="p-5">
            <p className="text-xs font-semibold tracking-[0.14em] text-fg-3 uppercase">Superstep</p>
            <p className="text-5xl font-semibold text-fg">
              {now.superstep}
              <span className="text-xl text-fg-3"> / {last}</span>
            </p>
            <p className="mt-2 text-sm text-fg-2">
              {updated} of {data.stations.length} vertices updated · {sent.length} messages go out next
            </p>
            <p className="mt-3 text-sm leading-relaxed text-fg-2">
              {step === 0
                ? `Superstep 0: Pregel runs vprog on every vertex with the initial message α/(1−α), so every rank starts at α = ${alpha}.`
                : sent.length === 0
                  ? "No vertex changed by more than the tolerance, so nothing is sent: every vertex has voted to halt and Pregel stops."
                  : "Each vertex updated in this superstep sends its rank change × the route's probability (sendMsg); messages to one station are summed (mergeMsg); next superstep each receiver adds 0.85 × the sum (vprog)."}
            </p>
          </Glass>
          <Glass variant="inset" className="p-5">
            <p className="font-hand text-xl text-marker">{data.stations[focus].name}</p>
            <dl className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
              <dt className="text-fg-3">rank now</dt>
              <dd className="text-right font-semibold text-fg tabular-nums">{fixed(now.ranks[focus], 4)}</dd>
              <dt className="text-fg-3">received this superstep</dt>
              <dd className="text-right text-fg tabular-nums">
                {step === 0 ? "initial message" : `${received.length} messages, Σ = ${fixed(incoming, 4)}`}
              </dd>
              <dt className="text-fg-3">change (delta)</dt>
              <dd className="text-right text-fg tabular-nums">{fixed(now.deltas[focus], 4)}</dd>
            </dl>
            {step > 0 && received.length > 0 && (
              <p className="mt-3 rounded-lg bg-white/5 px-3 py-2 font-mono text-xs text-fg-2">
                vprog: {fixed(prevRank, 4)} + 0.85 × {fixed(incoming, 4)} = {fixed(prevRank + (1 - alpha) * incoming, 4)}
              </p>
            )}
            <p className={cn("mt-2 text-xs", now.deltas[focus] <= data.tol ? "text-coral" : "text-fg-3")}>
              {now.deltas[focus] <= data.tol
                ? `change ≤ tol (${data.tol}): this vertex sends nothing next — it votes to halt`
                : `change > tol (${data.tol}): it keeps sending`}
            </p>
          </Glass>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <ClayButton onClick={() => goTo(0)} aria-label="First superstep">⏮</ClayButton>
        <ClayButton onClick={() => goTo(step - 1)} aria-label="Previous superstep">◀</ClayButton>
        <ClayButton pressed={running} onClick={togglePlay} className="min-w-28">
          {running ? "❚❚ Pause" : "▶ Play"}
        </ClayButton>
        <ClayButton onClick={() => goTo(step + 1)} aria-label="Next superstep">▶</ClayButton>
        <ClayButton onClick={() => goTo(last)} aria-label="Last superstep">⏭</ClayButton>
        <ClaySlider label="Superstep" value={step} min={0} max={last} onChange={goTo} className="min-w-[14rem] flex-1" />
      </div>

      <Glass variant="inset" className="p-5">
        <h3 className="font-display text-base font-bold text-fg">Every station&apos;s rank, superstep by superstep</h3>
        <p className="text-xs text-fg-3">
          Un-normalized ranks as Pregel holds them; the focused station in blue. Normalized, the final shares are{" "}
          {data.stations.map((s, i) => `${short(s.name)} ${fixed(data.finalRanks[i] * 100, 1)} %`).join(" · ")}.
        </p>
        <EChart option={chart} height={280} label="Rank of each station by superstep" />
      </Glass>
    </div>
  );
}

