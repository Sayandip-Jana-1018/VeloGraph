"use client";

import type { PickingInfo } from "@deck.gl/core";
import { ArcLayer, ScatterplotLayer } from "@deck.gl/layers";
import DeckGL from "@deck.gl/react";
import { setWorkerUrl } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Map } from "react-map-gl/maplibre";

import type { LiveMove, LiveStation } from "@/lib/schemas";

import { fill, MOVE, stroke } from "./live-colors";

setWorkerUrl("/maplibre/maplibre-gl-worker.mjs");

const BASEMAP = "https://basemaps.cartocdn.com/gl/dark-matter-nolabels-gl-style/style.json";
const OFFLINE_STYLE = {
  version: 8 as const,
  sources: {},
  layers: [{ id: "background", type: "background" as const, paint: { "background-color": "#0b0a1d" } }],
};
const VIEW = { longitude: -73.955, latitude: 40.735, zoom: 11.1, pitch: 0, bearing: 0 };

const problem = (s: LiveStation) => s.risk !== "OK" && s.risk !== "OFFLINE";
const forecast = (s: LiveStation) => s.risk === "EMPTY_RISK" || s.risk === "FULL_RISK";

/**
 * The live risk map: every station as a dot (solid = the feed says so now, ring = the forecast says so within the
 * hour), the forecast rings pulsing, and the suggested moves as arcs from the station with bikes to spare to the one
 * running out. The pulse runs only while the map is on screen and motion is allowed.
 */
export default function LiveMap({
  stations,
  moves,
  selected,
  onSelect,
}: {
  stations: LiveStation[];
  moves: LiveMove[];
  selected: string | null;
  onSelect: (id: string | null) => void;
}) {
  const box = useRef<HTMLDivElement>(null);
  const [onScreen, setOnScreen] = useState(false);
  const [style, setStyle] = useState<string | typeof OFFLINE_STYLE>(BASEMAP);
  const [pulse, setPulse] = useState(0);
  const [view, setView] = useState(VIEW);
  const zoom = useCallback((by: number) => setView((v) => ({ ...v, zoom: Math.min(16, Math.max(9, v.zoom + by)) })), []);

  useEffect(() => {
    const el = box.current;
    if (!el) return;
    const observer = new IntersectionObserver(([entry]) => setOnScreen(entry.isIntersecting));
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (!onScreen || reduce) return;
    let frame = 0;
    let last = 0;
    const tick = (now: number) => {
      // About 20 frames a second is plenty for a slow pulse, and leaves the main thread alone.
      if (now - last > 50) {
        setPulse((now / 1400) % 1);
        last = now;
      }
      frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [onScreen]);

  // Quiet stations underneath, problems on top, forecast rings last so their halo is never covered.
  const ordered = useMemo(
    () => [...stations].sort((a, b) => Number(problem(a)) - Number(problem(b)) || Number(forecast(a)) - Number(forecast(b))),
    [stations],
  );
  const rings = useMemo(() => stations.filter(forecast), [stations]);
  const maxBikes = Math.max(1, ...moves.map((m) => m.bikes));

  const onClick = useCallback((info: PickingInfo<LiveStation>) => onSelect(info.object?.id ?? null), [onSelect]);

  const dots = useMemo(
    () =>
      new ScatterplotLayer<LiveStation>({
        id: "stations",
        data: ordered,
        getPosition: (d) => [d.lon, d.lat],
        // The forecast is what the models add, so its rings are the largest marks; "now" dots stay small.
        getRadius: (d) => (forecast(d) ? 5.5 : problem(d) ? 3.2 : 2.2) + (d.id === selected ? 4 : 0),
        radiusUnits: "pixels",
        getFillColor: (d) => fill(d.risk),
        stroked: true,
        getLineColor: (d) => (d.id === selected ? [255, 224, 102, 255] : stroke(d.risk)),
        getLineWidth: (d) => (d.id === selected ? 3 : forecast(d) ? 2.5 : 0.5),
        lineWidthUnits: "pixels",
        pickable: true,
        autoHighlight: true,
        highlightColor: [255, 224, 102, 200],
        onClick,
        updateTriggers: { getRadius: selected, getLineColor: selected, getLineWidth: selected },
        transitions: { getFillColor: 600 },
      }),
    [ordered, selected, onClick],
  );
  const halo = new ScatterplotLayer<LiveStation>({
    id: "halo",
    data: rings,
    getPosition: (d) => [d.lon, d.lat],
    getRadius: 6 + 14 * pulse,
    radiusUnits: "pixels",
    filled: false,
    stroked: true,
    getLineColor: (d) => {
      const [r, g, b] = stroke(d.risk);
      return [r, g, b, Math.round(200 * (1 - pulse))];
    },
    getLineWidth: 1.5,
    lineWidthUnits: "pixels",
    updateTriggers: { getRadius: pulse, getLineColor: pulse },
  });
  const arcs = useMemo(
    () =>
      new ArcLayer<LiveMove>({
        id: "moves",
        data: moves,
        getSourcePosition: (d) => [d.fromLon, d.fromLat],
        getTargetPosition: (d) => [d.toLon, d.toLat],
        getSourceColor: [MOVE[0], MOVE[1], MOVE[2], 70],
        getTargetColor: MOVE,
        getWidth: (d) => 1 + 2 * (d.bikes / maxBikes),
        widthUnits: "pixels",
        getHeight: 0.35,
        greatCircle: false,
      }),
    [moves, maxBikes],
  );

  return (
    <>
      <div ref={box} aria-hidden className="pointer-events-none absolute inset-0" />
      <DeckGL
        viewState={view}
        onViewStateChange={({ viewState }) => setView(viewState as typeof VIEW)}
        controller={{ dragRotate: false, touchRotate: false, scrollZoom: false }}
        layers={[dots, halo, arcs]}
        style={{ position: "absolute", inset: "0" }}
        getCursor={({ isHovering }) => (isHovering ? "pointer" : "grab")}
        getTooltip={({ object }) => {
          if (!object || !("risk" in object)) return null;
          const s = object as LiveStation;
          return {
            text: `${s.name}\n${s.bikes} bikes · ${s.docks} free docks`,
            style: {
              background: "rgba(22, 19, 58, 0.96)",
              color: "#f5f2ff",
              border: "1px solid rgba(255, 255, 255, 0.14)",
              borderRadius: "12px",
              padding: "8px 12px",
              fontSize: "12px",
              fontFamily: "Inter, system-ui, sans-serif",
            },
          };
        }}
      >
        <Map mapStyle={style} attributionControl={false} onError={() => setStyle(OFFLINE_STYLE)} />
      </DeckGL>
      <div className="absolute top-3 right-3 z-10 flex flex-col gap-2">
        <button type="button" onClick={() => zoom(0.6)} aria-label="Zoom in" className="clay grid size-10 place-items-center rounded-full text-lg text-fg hover:text-marker">
          +
        </button>
        <button type="button" onClick={() => zoom(-0.6)} aria-label="Zoom out" className="clay grid size-10 place-items-center rounded-full text-lg text-fg hover:text-marker">
          −
        </button>
        <button type="button" onClick={() => setView(VIEW)} aria-label="Reset view" className="clay grid size-10 place-items-center rounded-full text-sm text-fg hover:text-marker">
          ⟲
        </button>
      </div>
    </>
  );
}
