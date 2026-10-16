"use client";

import type { PickingInfo } from "@deck.gl/core";
import { TripsLayer } from "@deck.gl/geo-layers";
import { ArcLayer, ScatterplotLayer } from "@deck.gl/layers";
import DeckGL from "@deck.gl/react";
import { setWorkerUrl } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Map } from "react-map-gl/maplibre";

import type { RGBA } from "./map-colors";

// Copied there by `pnpm sync`: MapLibre cannot find its worker inside a bundle on its own.
setWorkerUrl("/maplibre/maplibre-gl-worker.mjs");

export type MapStation = {
  id: string;
  position: [number, number];
  radius: number;
  color: RGBA;
  ring: boolean;
};
export type MapFlow = { from: [number, number]; to: [number, number]; weight: number };

// CARTO's dark basemap needs the network; offline the map falls back to a plain canvas and the stations still draw.
const BASEMAP = "https://basemaps.cartocdn.com/gl/dark-matter-nolabels-gl-style/style.json";
const OFFLINE_STYLE = {
  version: 8 as const,
  sources: {},
  layers: [{ id: "background", type: "background" as const, paint: { "background-color": "#0b0a1d" } }],
};
const VIEW = { longitude: -73.952, latitude: 40.742, zoom: 11.05, pitch: 0, bearing: 0 };
const LOOP = 1200; // animation loop length, in TripsLayer time units

/**
 * The deck.gl map: stations as a scatterplot (radius and colour animate between modes), flows as arcs coloured
 * from start (blue) to end (yellow), and optional "comets" that ride the arcs to make the busiest flows visible.
 * The comets only run while the map is on screen, and only their layer is rebuilt per frame.
 */
export default function NetworkMap({
  stations,
  flows,
  animate,
  encodingKey,
  onHover,
}: {
  stations: MapStation[];
  flows: MapFlow[];
  animate: boolean;
  encodingKey: string;
  onHover: (info: PickingInfo<MapStation>) => void;
}) {
  const box = useRef<HTMLDivElement>(null);
  const [onScreen, setOnScreen] = useState(false);
  const [style, setStyle] = useState<string | typeof OFFLINE_STYLE>(BASEMAP);
  const [time, setTime] = useState(0);
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
    if (!animate || !onScreen || reduce) return;
    let frame = 0;
    const tick = (now: number) => {
      setTime((now / 12) % LOOP);
      frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(frame);
  }, [animate, onScreen]);

  // Two comets per flow, half a loop apart, each taking a time proportional to the flow's length.
  const comets = useMemo(
    () =>
      flows.flatMap((f, i) => {
        const length = Math.hypot(f.to[0] - f.from[0], f.to[1] - f.from[1]);
        const duration = Math.max(40, length * 9000);
        const start = (i * 137) % LOOP;
        return [0, LOOP / 2].map((shift) => ({
          path: [f.from, f.to],
          timestamps: [(start + shift) % LOOP, ((start + shift) % LOOP) + duration],
          weight: f.weight,
        }));
      }),
    [flows],
  );

  const maxWeight = Math.max(1, ...flows.map((f) => f.weight));
  const arcs = useMemo(
    () =>
      new ArcLayer<MapFlow>({
        id: "flows",
        data: flows,
        getSourcePosition: (d) => d.from,
        getTargetPosition: (d) => d.to,
        getSourceColor: (d) => [57, 135, 229, 40 + 150 * (d.weight / maxWeight)],
        getTargetColor: (d) => [255, 224, 102, 40 + 170 * (d.weight / maxWeight)],
        getWidth: (d) => 0.6 + 3.2 * Math.sqrt(d.weight / maxWeight),
        widthUnits: "pixels",
        getHeight: 0.35,
        updateTriggers: { getSourceColor: encodingKey, getTargetColor: encodingKey, getWidth: encodingKey },
      }),
    [flows, maxWeight, encodingKey],
  );
  const dots = useMemo(
    () =>
      new ScatterplotLayer<MapStation>({
        id: "stations",
        data: stations,
        getPosition: (d) => d.position,
        getRadius: (d) => d.radius,
        radiusUnits: "pixels",
        getFillColor: (d) => d.color,
        stroked: true,
        getLineColor: (d) => (d.ring ? [255, 224, 102, 255] : [11, 10, 29, 180]),
        getLineWidth: (d) => (d.ring ? 3 : 0.6),
        lineWidthUnits: "pixels",
        pickable: true,
        autoHighlight: true,
        highlightColor: [255, 224, 102, 200],
        onHover,
        transitions: { getRadius: 700, getFillColor: 700 },
        updateTriggers: {
          getRadius: encodingKey,
          getFillColor: encodingKey,
          getLineColor: encodingKey,
          getLineWidth: encodingKey,
        },
      }),
    [stations, encodingKey, onHover],
  );
  const layers = [
    arcs,
    animate &&
      new TripsLayer<(typeof comets)[number]>({
        id: "comets",
        data: comets,
        getPath: (d) => d.path,
        getTimestamps: (d) => d.timestamps,
        getColor: [255, 236, 160],
        getWidth: (d) => 1.5 + 3 * Math.sqrt(d.weight / maxWeight),
        widthUnits: "pixels",
        trailLength: 120,
        currentTime: time,
        capRounded: true,
        jointRounded: true,
        fadeTrail: true,
      }),
    dots,
  ];

  return (
    <>
      <div ref={box} aria-hidden className="pointer-events-none absolute inset-0" />
      <DeckGL
        viewState={view}
        onViewStateChange={({ viewState }) => setView(viewState as typeof VIEW)}
        // The wheel stays with the page (no scroll-zoom trap); zoom with the buttons, a double-click or a pinch.
        controller={{ dragRotate: false, touchRotate: false, scrollZoom: false }}
        layers={layers}
        style={{ position: "absolute", inset: "0" }}
        getCursor={({ isHovering }) => (isHovering ? "pointer" : "grab")}
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
