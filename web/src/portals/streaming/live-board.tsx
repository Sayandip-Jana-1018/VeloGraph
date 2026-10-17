"use client";

import dynamic from "next/dynamic";
import { useMemo, useState } from "react";

import { Glass, KpiPuck } from "@/components/inkglass/surfaces";
import { cn } from "@/lib/cn";
import { fixed, int } from "@/lib/format";
import type { LiveAlert, LiveMove, LiveSnapshot, LiveStation } from "@/lib/schemas";

import { STATE, STATE_ORDER } from "./live-colors";
import { type Connection, GATEWAY, nyDate, nyTime, useLive } from "./use-live";

const LiveMap = dynamic(() => import("./live-map"), {
  ssr: false,
  loading: () => <div className="grid h-full place-items-center text-sm text-fg-3">Loading the map…</div>,
});

type Snapshot = LiveSnapshot["data"];

function StatusPill({ connection, snapshot }: { connection: Connection; snapshot: Snapshot }) {
  const replay = snapshot.source === "replay";
  const tone =
    connection !== "live"
      ? "border-white/15 text-fg-2"
      : replay
        ? "border-amber/60 text-amber"
        : "border-mint/60 text-mint";
  const label = connection === "connecting" ? "Connecting" : connection === "offline" ? "Recorded" : replay ? "Replay" : "Live";
  return (
    <span
      className={cn("inline-flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-semibold", tone)}
      title={connection === "offline" ? `The gateway at ${GATEWAY} is not answering: this is the last recorded snapshot.` : undefined}
    >
      <span className={cn("size-2 rounded-full bg-current", connection === "live" && "animate-pulse")} aria-hidden />
      {label} · batch {snapshot.batchId} · {nyDate(snapshot.asOf)} {nyTime(snapshot.asOf)} New York
    </span>
  );
}

function Legend({ counts }: { counts: Record<string, number> }) {
  return (
    <ul className="glass-inset absolute bottom-3 left-3 z-10 space-y-1 rounded-2xl px-3 py-2.5 text-xs">
      {STATE_ORDER.map((risk) => (
        <li key={risk} className="flex items-center gap-2">
          <span aria-hidden className="w-3 text-center text-sm leading-none" style={{ color: STATE[risk].hex }}>
            {STATE[risk].glyph}
          </span>
          <span className="text-fg-2">{STATE[risk].label}</span>
          <span className="ml-auto pl-3 text-fg tabular-nums">{int(counts[risk] ?? 0)}</span>
        </li>
      ))}
      <li className="flex items-center gap-2">
        <span aria-hidden className="w-3 text-center text-marker">⌒</span>
        <span className="text-fg-2">Suggested move</span>
      </li>
    </ul>
  );
}

function StationDrawer({ station, asOf, onClose }: { station: LiveStation; asOf: number; onClose: () => void }) {
  const s = STATE[station.risk];
  const bar = (value: number) => `${Math.max(0, Math.min(100, (value / Math.max(1, station.capacity)) * 100))}%`;
  const ago = Math.max(0, Math.round(asOf - station.reported)); // before this snapshot, not before now
  return (
    <Glass variant="panel" className="absolute top-3 left-3 z-20 w-[19rem] p-4">
      <div className="flex items-start justify-between gap-2">
        <div>
          <p className="font-display text-base leading-tight font-bold text-fg">{station.name}</p>
          <p className="mt-0.5 font-mono text-[0.7rem] text-fg-3">station {station.id}</p>
        </div>
        <button type="button" onClick={onClose} aria-label="Close" className="clay grid size-8 shrink-0 place-items-center rounded-full text-fg-2 hover:text-marker">
          ×
        </button>
      </div>
      <p className="mt-2 inline-flex items-center gap-1.5 text-sm font-semibold" style={{ color: s.hex }}>
        <span aria-hidden>{s.glyph}</span> {s.label}
      </p>
      <dl className="mt-3 space-y-2 text-xs">
        <div>
          <dt className="flex justify-between text-fg-3">
            <span>Bikes now → in 60 min</span>
            <span className="text-fg tabular-nums">
              {station.bikes} → {fixed(station.projected, 1)} of {station.capacity}
            </span>
          </dt>
          <dd className="relative mt-1 h-2 rounded-full bg-white/8">
            <span className="absolute inset-y-0 left-0 rounded-full bg-white/35" style={{ width: bar(station.bikes) }} />
            <span className="absolute inset-y-[-3px] w-0.5 rounded bg-marker" style={{ left: bar(station.projected) }} />
          </dd>
        </div>
        {(
          [
            ["Riders leaving, next 60 min", fixed(station.dep60, 1)],
            ["Riders arriving, next 60 min", fixed(station.arr60, 1)],
            ["This hour / next hour (out)", `${fixed(station.dep0, 1)} / ${fixed(station.dep1, 1)}`],
            ["This hour / next hour (in)", `${fixed(station.arr0, 1)} / ${fixed(station.arr1, 1)}`],
            ["E-bikes · free docks", `${station.ebikes} · ${station.docks}`],
            ["Archetype", station.archetype],
            ["District (GraphX community)", station.community > 0 ? `#${station.community}` : "none"],
            ["PageRank", fixed(station.pagerank * 1000, 3) + " ‰"],
            ["Last report", ago < 120 ? `${ago} s before` : `${Math.round(ago / 60)} min before`],
          ] as const
        ).map(([k, v]) => (
          <div key={k} className="flex justify-between gap-3">
            <dt className="text-fg-3">{k}</dt>
            <dd className="text-right text-fg-2 tabular-nums">{v}</dd>
          </div>
        ))}
      </dl>
    </Glass>
  );
}

function AlertTicker({ alerts, onPick }: { alerts: LiveAlert[]; onPick: (id: string) => void }) {
  return (
    <Glass variant="inset" className="p-4">
      <h3 className="font-display text-sm font-bold text-fg">Alerts</h3>
      <p className="text-xs text-fg-3">A station entering empty or full, as Spark sends it to Kafka</p>
      {alerts.length === 0 ? (
        <p className="mt-4 text-sm text-fg-3">No alerts in this window.</p>
      ) : (
        <ul className="mt-3 max-h-[19rem] space-y-1.5 overflow-auto pr-1">
          {alerts.map((a) => (
            <li key={`${a.at}-${a.stationId}`}>
              <button type="button" onClick={() => onPick(a.stationId)} className="flex w-full items-start gap-2 rounded-xl px-2 py-1.5 text-left hover:bg-white/5">
                <span aria-hidden className="mt-0.5 w-3 text-center" style={{ color: STATE[a.risk].hex }}>
                  {STATE[a.risk].glyph}
                </span>
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm text-fg">{a.name}</span>
                  <span className="block text-[0.7rem] text-fg-3">
                    {STATE[a.risk].short} · {fixed(a.projected, 1)} bikes projected · was {a.previous.toLowerCase().replace("_", " ")}
                  </span>
                </span>
                <span className="shrink-0 font-mono text-[0.68rem] text-fg-3">{nyTime(a.at).slice(0, 5)}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </Glass>
  );
}

function MoveList({ moves, onPick }: { moves: LiveMove[]; onPick: (id: string) => void }) {
  return (
    <Glass variant="inset" className="p-4">
      <h3 className="font-display text-sm font-bold text-fg">Suggested moves</h3>
      <p className="text-xs text-fg-3">Greedy matching inside a district, ≤ 2.5 km, ≤ 12 bikes a trip</p>
      <ol className="mt-3 max-h-[15rem] space-y-1.5 overflow-auto pr-1">
        {moves.slice(0, 12).map((m) => (
          <li key={`${m.from}-${m.to}`}>
            <button type="button" onClick={() => onPick(m.to)} className="flex w-full items-center gap-2 rounded-xl px-2 py-1.5 text-left text-xs hover:bg-white/5">
              <span className="w-8 shrink-0 text-right font-semibold text-marker tabular-nums">{m.bikes}</span>
              <span className="min-w-0 flex-1 truncate text-fg-2">
                {m.fromName} <span className="text-fg-3">→</span> <span className="text-fg">{m.toName}</span>
              </span>
              <span className="shrink-0 text-fg-3 tabular-nums">{fixed(m.km, 1)} km</span>
            </button>
          </li>
        ))}
      </ol>
    </Glass>
  );
}

/**
 * The live operations view: the risk map, alerts and suggested moves of the latest micro-batch. `connect` false
 * shows the recorded snapshot only (inside a portal, or when the site is served without a backend).
 */
export function LiveBoard({ initial, connect = true }: { initial: Snapshot; connect?: boolean }) {
  const { snapshot, alerts, connection } = useLive(initial, connect);
  const [selected, setSelected] = useState<string | null>(null);
  const byId = useMemo(() => new Map(snapshot.stations.map((s) => [s.id, s])), [snapshot.stations]);
  const counts = useMemo(() => {
    const c: Record<string, number> = {};
    for (const s of snapshot.stations) c[s.risk] = (c[s.risk] ?? 0) + 1;
    return c;
  }, [snapshot.stations]);
  const k = snapshot.kpis;
  const station = selected ? byId.get(selected) : undefined;

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-center gap-3">
        <StatusPill connection={connection} snapshot={snapshot} />
        <span className="rounded-full border border-white/15 px-3 py-1 text-xs text-fg-2">
          {fixed(snapshot.weather.tempC, 1)} °C · {snapshot.weather.raining ? `rain ${fixed(snapshot.weather.precipMm, 1)} mm` : "dry"} ·
          wind {fixed(snapshot.weather.windKmh, 0)} km/h
        </span>
        <span className="rounded-full border border-white/15 px-3 py-1 text-xs text-fg-2">
          broker → snapshot {fixed(snapshot.latency.brokerToSnapshotS, 1)} s · scored in {fixed(snapshot.latency.processingMs / 1000, 1)} s
        </span>
      </div>
      <div className="flex flex-wrap justify-center gap-4">
        <KpiPuck label="Empty within 60 min" value={int(k.emptyRisk)} note={`and ${int(k.empty)} empty now`} />
        <KpiPuck label="Full within 60 min" value={int(k.fullRisk)} note={`and ${int(k.full)} full now`} />
        <KpiPuck label="Bikes docked" value={int(k.bikes)} note={`${int(k.ebikes)} of them e-bikes`} />
        <KpiPuck label="Suggested moves" value={int(k.moves)} note={`${int(k.bikesToMove)} bikes in all`} />
        <KpiPuck label="Stations reporting" value={int(k.reporting)} note={`${int(k.offline)} offline`} />
      </div>
      <div className="grid gap-5 lg:grid-cols-[minmax(0,1fr)_22rem]">
        <figure className="map-surface relative h-[36rem] overflow-hidden rounded-[var(--radius-card)]">
          <LiveMap stations={snapshot.stations} moves={snapshot.moves} selected={selected} onSelect={setSelected} />
          <Legend counts={counts} />
          {station && <StationDrawer station={station} asOf={snapshot.asOfEpoch} onClose={() => setSelected(null)} />}
          <figcaption className="sr-only">
            Map of {snapshot.stations.length} stations by state: {STATE_ORDER.map((r) => `${counts[r] ?? 0} ${STATE[r].label}`).join(", ")}.
          </figcaption>
        </figure>
        <div className="space-y-5">
          <AlertTicker alerts={alerts} onPick={setSelected} />
          <MoveList moves={snapshot.moves} onPick={setSelected} />
        </div>
      </div>
    </div>
  );
}
