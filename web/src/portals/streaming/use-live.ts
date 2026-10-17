"use client";

import { useEffect, useState } from "react";

import { LiveSnapshot, type LiveAlert, type Risk } from "@/lib/schemas";

type Snapshot = LiveSnapshot["data"];
export type Connection = "connecting" | "live" | "offline";

/** Where the FastAPI gateway listens (python/src/velograph/api); the site works without it. */
export const GATEWAY = process.env.NEXT_PUBLIC_GATEWAY_URL ?? "http://localhost:8000";

type AlertMessage = {
  as_of: string;
  stationId: string;
  name: string;
  lat: number;
  lon: number;
  risk: Risk;
  previous: string;
  projectedBikes: number;
  capacity: number;
};

/**
 * The live snapshot: the one the last run left in the static export first, then the gateway's as it changes.
 * A `batch` event from the server-sent stream means a new snapshot exists, an `alert` event is shown at once,
 * and a poll every 30 s covers a blocked stream. Without a gateway the page stays on the recorded snapshot,
 * marked offline.
 */
export function useLive(initial: Snapshot, connect = true) {
  const [snapshot, setSnapshot] = useState(initial);
  const [alerts, setAlerts] = useState<LiveAlert[]>(initial.alerts);
  const [connection, setConnection] = useState<Connection>(connect ? "connecting" : "offline");

  useEffect(() => {
    if (!connect) return;
    let alive = true;
    const refresh = async () => {
      try {
        const response = await fetch(`${GATEWAY}/api/live/snapshot`, { cache: "no-store" });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const parsed = LiveSnapshot.safeParse(await response.json());
        if (!parsed.success) throw new Error("the snapshot does not match its contract");
        if (!alive) return;
        setSnapshot(parsed.data.data);
        setAlerts(parsed.data.data.alerts);
        setConnection("live");
      } catch {
        if (alive) setConnection("offline");
      }
    };
    void refresh();
    const timer = window.setInterval(refresh, 30_000);
    const stream = new EventSource(`${GATEWAY}/api/live/stream`);
    stream.addEventListener("batch", () => void refresh());
    stream.addEventListener("alert", (event) => {
      const a = JSON.parse((event as MessageEvent<string>).data) as AlertMessage;
      const alert: LiveAlert = {
        at: a.as_of,
        stationId: a.stationId,
        name: a.name,
        risk: a.risk,
        previous: a.previous,
        projected: a.projectedBikes,
        capacity: a.capacity,
        lat: a.lat,
        lon: a.lon,
      };
      setAlerts((previous) =>
        [alert, ...previous.filter((p) => !(p.stationId === alert.stationId && p.at === alert.at))].slice(0, 50),
      );
    });
    stream.onerror = () => setConnection((c) => (c === "live" ? c : "offline")); // EventSource retries by itself
    return () => {
      alive = false;
      window.clearInterval(timer);
      stream.close();
    };
  }, [connect]);

  return { snapshot, alerts, connection };
}

const NY = "America/New_York";

/** "2026-10-16T21:21:01.079-04:00" -> "21:21:01", on New York's clock whatever the viewer's. */
export const nyTime = (iso: string) =>
  new Date(iso).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", second: "2-digit", timeZone: NY });

export const nyDate = (iso: string) =>
  new Date(iso).toLocaleDateString("en-GB", { weekday: "short", day: "numeric", month: "short", timeZone: NY });
