"use client";

import type { PickingInfo } from "@deck.gl/core";
import dynamic from "next/dynamic";
import { useMemo, useState } from "react";

import { DIVERGING } from "@/components/charts/theme";
import { ClayButton, ClaySegmented, ClaySlider } from "@/components/inkglass/clay";
import { Glass } from "@/components/inkglass/surfaces";
import { cn } from "@/lib/cn";
import { fixed, int, pct } from "@/lib/format";
import { NetworkFlows, NetworkNodes, NetworkPaths, type NodeRow } from "@/lib/schemas";
import { useServing } from "@/lib/use-serving";

import { CONTEXT, EMPHASIS, NONE, TRAVEL_BANDS, diverging, sequential, travelColor } from "./map-colors";
import type { MapFlow, MapStation } from "./network-map";

// deck.gl and MapLibre need WebGL and the window, so the map only ever renders in the browser.
const NetworkMap = dynamic(() => import("./network-map"), {
  ssr: false,
  loading: () => <MapMessage text="warming up WebGL…" />,
});

type Mode = "pagerank" | "shift" | "communities" | "travel" | "ppr";
type Community = { id: number; size: number; topStations: string[]; radiusKm: number };

const MODES: { value: Mode; label: string }[] = [
  { value: "pagerank", label: "PageRank" },
  { value: "shift", label: "AM ↔ PM" },
  { value: "communities", label: "Communities" },
  { value: "travel", label: "Travel time" },
  { value: "ppr", label: "Where riders go" },
];

function MapMessage({ text, tone = "info" }: { text: string; tone?: "info" | "error" }) {
  return (
    <div className="absolute inset-0 grid place-items-center">
      <p className={cn("font-hand text-2xl", tone === "error" ? "text-coral" : "text-marker")}>{text}</p>
    </div>
  );
}

export function NetworkExplorer({ communities }: { communities: Community[] }) {
  const nodes = useServing("network-nodes.json", NetworkNodes);
  const flows = useServing("network-flows.json", NetworkFlows);
  const paths = useServing("network-paths.json", NetworkPaths);

  const [mode, setMode] = useState<Mode>("pagerank");
  const [morph, setMorph] = useState(0);
  const [community, setCommunity] = useState(communities[0]?.id ?? 1);
  const [landmark, setLandmark] = useState(0);
  const [animate, setAnimate] = useState(true);
  const [hover, setHover] = useState<{ x: number; y: number; node: NodeRow } | null>(null);

  const ready = nodes.state === "ready" && flows.state === "ready" && paths.state === "ready";
  // Memoised so the derived maps below are rebuilt only when a file finishes loading, not on every render.
  const nodeList = useMemo(() => (nodes.state === "ready" ? nodes.data.data : []), [nodes]);
  const byId = useMemo(() => new Map(nodeList.map((n) => [n.id, n])), [nodeList]);
  const landmarks = useMemo(() => (paths.state === "ready" ? paths.data.data.landmarks : []), [paths]);
  const stationIndex = useMemo(
    () => new Map((paths.state === "ready" ? paths.data.data.stationIds : []).map((id, i) => [id, i])),
    [paths],
  );

  const stations: MapStation[] = useMemo(() => {
    if (!ready) return [];
    const n = nodeList.length;
    const maxPr = Math.max(...nodeList.map((d) => d.pagerank));
    const maxPeak = Math.max(...nodeList.map((d) => Math.max(d.prAm, d.prPm)));
    const lm = landmarks[landmark];
    const ppr = new Map(lm?.destinations.map((d) => [d.id, d.value]) ?? []);
    const maxPpr = Math.max(1e-9, ...ppr.values());
    return nodeList.map((d) => {
      const base = { id: d.id, position: [d.lon, d.lat] as [number, number], ring: false };
      const prRadius = 1.6 + 8.5 * Math.sqrt(d.pagerank / maxPr);
      switch (mode) {
        case "pagerank":
          return { ...base, radius: prRadius, color: sequential(1 - (d.pagerankRank - 1) / (n - 1)) };
        case "shift": {
          const value = d.prAm + (d.prPm - d.prAm) * morph;
          return { ...base, radius: 1.6 + 8.5 * Math.sqrt(value / maxPeak), color: diverging(d.prShift) };
        }
        case "communities":
          return {
            ...base,
            radius: prRadius,
            color: d.community === community ? EMPHASIS : d.community > 0 ? CONTEXT : NONE,
          };
        case "travel": {
          const seconds = lm?.travelSeconds[stationIndex.get(d.id) ?? -1] ?? null;
          return { ...base, radius: d.id === lm?.id ? 9 : 2.6, color: travelColor(seconds), ring: d.id === lm?.id };
        }
        case "ppr": {
          const v = ppr.get(d.id);
          return {
            ...base,
            radius: d.id === lm?.id ? 9 : v ? 2.5 + 8 * Math.sqrt(v / maxPpr) : 1.6,
            color: v ? sequential(0.25 + 0.75 * (v / maxPpr)) : NONE,
            ring: d.id === lm?.id,
          };
        }
      }
    });
  }, [ready, nodeList, landmarks, landmark, mode, morph, community, stationIndex]);

  const mapFlows: MapFlow[] = useMemo(() => {
    if (!ready || flows.state !== "ready") return [];
    if (mode === "travel" || mode === "ppr") return [];
    const all = flows.data.data;
    const weightOf = (f: (typeof all)[number]) =>
      mode === "shift" ? (morph < 0.5 ? f.amTrips : f.pmTrips) : f.trips;
    return [...all]
      .sort((a, b) => weightOf(b) - weightOf(a))
      .slice(0, 450)
      .flatMap((f) => {
        const [a, b] = [byId.get(f.from), byId.get(f.to)];
        return a && b ? [{ from: [a.lon, a.lat] as [number, number], to: [b.lon, b.lat] as [number, number], weight: weightOf(f) }] : [];
      });
  }, [ready, flows, mode, morph, byId]);

  const onHover = (info: PickingInfo<MapStation>) =>
    setHover(info.object ? { x: info.x, y: info.y, node: byId.get(info.object.id)! } : null);

  const lm = landmarks[landmark];
  const hoverLine = (d: NodeRow): string => {
    switch (mode) {
      case "pagerank":
        return `PageRank #${d.pagerankRank} · ${fixed(d.pagerank * 100, 3)} % of the random rider's time`;
      case "shift":
        return d.prShift >= 0 ? `morning magnet, shift +${fixed(d.prShift, 2)}` : `evening magnet, shift ${fixed(d.prShift, 2)}`;
      case "communities":
        return d.community > 0 ? `community ${d.community} · ${int(d.communitySize)} stations` : "no community (no strong ties)";
      case "travel": {
        const s = lm?.travelSeconds[stationIndex.get(d.id) ?? -1];
        return s == null ? `not reachable from ${lm?.name}` : `${fixed(s / 60, 1)} min from ${lm?.name}`;
      }
      case "ppr": {
        const v = lm?.destinations.find((x) => x.id === d.id)?.value;
        return v ? `${fixed(v * 100, 2)} % of riders from ${lm?.name} end up here` : "not among the top destinations";
      }
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-center gap-3">
        <ClaySegmented label="Map mode" value={mode} options={MODES} onChange={setMode} />
        {(mode === "pagerank" || mode === "shift" || mode === "communities") && (
          <ClayButton pressed={animate} onClick={() => setAnimate((a) => !a)}>
            {animate ? "◉ Flows animated" : "○ Animate flows"}
          </ClayButton>
        )}
      </div>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_20rem]">
        <Glass variant="inset" className="relative h-[34rem] overflow-hidden">
          {nodes.state === "error" || flows.state === "error" || paths.state === "error" ? (
            <MapMessage tone="error" text="the network exports could not be loaded" />
          ) : !ready ? (
            <MapMessage text={`loading ${nodes.state === "ready" ? int(nodeList.length) : "2,490"} stations…`} />
          ) : (
            <NetworkMap stations={stations} flows={mapFlows} animate={animate && mapFlows.length > 0} encodingKey={`${mode}-${morph}-${community}-${landmark}`} onHover={onHover} />
          )}
          {hover && (
            <div
              className="pointer-events-none absolute z-10 max-w-64 rounded-xl border border-white/15 bg-ink-850/95 px-3 py-2 text-xs shadow-xl"
              style={{
                left: hover.x + 14,
                top: hover.y + 14,
                transform: hover.x > 560 ? "translateX(calc(-100% - 28px))" : undefined,
              }}
            >
              <p className="font-semibold text-fg">{hover.node.name}</p>
              <p className="mt-0.5 text-fg-2">{hoverLine(hover.node)}</p>
            </div>
          )}
          <p className="absolute right-3 bottom-2 text-[0.65rem] text-fg-3">© CARTO © OpenStreetMap contributors</p>
        </Glass>

        <Glass variant="inset" className="flex flex-col gap-4 p-5 text-sm">
          {mode === "pagerank" && (
            <>
              <p className="font-hand text-xl text-marker">brighter and bigger = more central</p>
              <p className="text-fg-2">
                Weighted PageRank: the share of time a random rider, choosing routes in proportion to their trips, spends
                at each station. Arcs show the 450 busiest flows, blue where they start and yellow where they end.
              </p>
              <ol className="space-y-1">
                {nodeList.slice(0, 8).map((d) => (
                  <li key={d.id} className="flex justify-between gap-3 text-fg-2">
                    <span className="truncate">
                      <span className="text-fg-3">#{d.pagerankRank}</span> {d.name}
                    </span>
                    <span className="text-fg tabular-nums">{fixed(d.pagerank * 100, 3)} %</span>
                  </li>
                ))}
              </ol>
            </>
          )}
          {mode === "shift" && (
            <>
              <p className="font-hand text-xl text-marker">slide from morning to evening</p>
              <ClaySlider label="Morning to evening" value={morph} min={0} max={1} step={0.02} onChange={setMorph} />
              <div className="flex justify-between text-xs text-fg-3">
                <span>07–10 working days</span>
                <span>16–19 working days</span>
              </div>
              <div
                className="h-2.5 rounded-full"
                style={{ background: `linear-gradient(90deg, ${DIVERGING.negative}, ${DIVERGING.neutral}, ${DIVERGING.positive})` }}
              />
              <div className="flex justify-between text-xs text-fg-2">
                <span>evening magnet (home)</span>
                <span>morning magnet (office)</span>
              </div>
              <p className="text-fg-2">
                Station size is PageRank on {morph < 0.5 ? "morning" : "evening"} trips; colour is the shift between the two,
                (am − pm) / (am + pm). Arcs follow the busiest {morph < 0.5 ? "morning" : "evening"} flows.
              </p>
            </>
          )}
          {mode === "communities" && (
            <>
              <p className="font-hand text-xl text-marker">pick a community</p>
              <div className="flex max-h-[24rem] flex-col gap-1.5 overflow-y-auto pr-1">
                {communities.map((c) => (
                  <button
                    key={c.id}
                    type="button"
                    onClick={() => setCommunity(c.id)}
                    className={cn("rounded-xl px-3 py-2 text-left", c.id === community ? "clay-pressed text-marker" : "clay text-fg-2 hover:text-fg")}
                  >
                    <span className="block text-xs text-fg-3">
                      #{c.id} · {int(c.size)} stations · {fixed(c.radiusKm, 1)} km radius
                    </span>
                    <span className="block truncate font-semibold">{c.topStations[0]}</span>
                  </button>
                ))}
              </div>
            </>
          )}
          {(mode === "travel" || mode === "ppr") && lm && (
            <>
              <label className="text-xs font-semibold tracking-[0.14em] text-fg-3 uppercase" htmlFor="landmark">
                From landmark
              </label>
              <select
                id="landmark"
                value={landmark}
                onChange={(e) => setLandmark(Number(e.target.value))}
                className="clay-well rounded-xl px-3 py-2 text-fg outline-none"
              >
                {landmarks.map((l, i) => (
                  <option key={l.id} value={i} className="bg-ink-900">
                    {l.name}
                  </option>
                ))}
              </select>
              {mode === "travel" ? (
                <>
                  <ul className="space-y-1">
                    {TRAVEL_BANDS.map((b) => (
                      <li key={b.label} className="flex items-center gap-2 text-fg-2">
                        <span className="size-3 rounded-full" style={{ background: b.color }} />
                        {b.label}
                      </li>
                    ))}
                  </ul>
                  <p className="text-fg-2">
                    {int(lm.within15Min)} stations within 15 min, {int(lm.within30Min)} within 30 min, chaining real rides
                    by their median time (Pregel shortest paths).
                  </p>
                </>
              ) : (
                <>
                  <p className="text-fg-2">Personalized PageRank: riders who start here, and where they end up.</p>
                  <ol className="space-y-1">
                    {lm.destinations.slice(0, 7).map((d) => (
                      <li key={d.id} className="flex justify-between gap-3 text-fg-2">
                        <span className="truncate">{d.name}</span>
                        <span className="text-fg tabular-nums">{pct(d.value * 100, 2)}</span>
                      </li>
                    ))}
                  </ol>
                </>
              )}
            </>
          )}
        </Glass>
      </div>
    </div>
  );
}
