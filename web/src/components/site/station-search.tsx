"use client";

import Link from "next/link";
import { Dialog } from "radix-ui";
import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";

import { cn } from "@/lib/cn";
import { fixed, int } from "@/lib/format";
import type { LiveStation } from "@/lib/schemas";
import { STATE } from "@/portals/streaming/live-colors";
import { GATEWAY } from "@/portals/streaming/use-live";

/** public/data/station-index.json (written by `pnpm sync` from network-nodes.json and archetypes.json). */
type Row = [string, string, number, number, number, number, number, number, number, number, number];
type Index = { archetypes: string[]; stations: Row[] };
type Station = {
  id: string;
  name: string;
  capacity: number;
  rank: number;
  district: number;
  archetype: string | null;
  out: number;
  in: number;
  shift: number;
  haystack: string;
};

let indexRequest: Promise<{ stations: Station[]; total: number }> | null = null;

function loadIndex() {
  indexRequest ??= fetch("/data/station-index.json")
    .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
    .then(({ data }: { data: Index }) => ({
      total: data.stations.length,
      stations: data.stations.map(([id, name, , , capacity, rank, district, archetype, out, inn, shift]) => ({
        id,
        name,
        capacity,
        rank,
        district,
        archetype: archetype >= 0 ? data.archetypes[archetype] : null,
        out,
        in: inn,
        shift,
        haystack: normalize(`${name} ${id}`),
      })),
    }))
    .catch((error: unknown) => {
      indexRequest = null; // try again next time
      throw error;
    });
  return indexRequest;
}

const normalize = (text: string) =>
  text
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // accents, split off by NFD
    .replace(/[^a-z0-9.]+/g, " ")
    .trim();

/** Every word must appear. Words that start a word of the name rank first, then by PageRank: busiest first. */
function search(stations: Station[], query: string, limit = 8): Station[] {
  const words = normalize(query).split(" ").filter(Boolean);
  if (words.length === 0) return [];
  const score = (s: Station) => {
    if (!words.every((w) => s.haystack.includes(w))) return -1;
    return words.every((w) => s.haystack.startsWith(w) || s.haystack.includes(` ${w}`)) ? 0 : 1;
  };
  return stations
    .map((s) => ({ s, score: score(s) }))
    .filter((m) => m.score >= 0)
    .sort((a, b) => a.score - b.score || a.s.rank - b.s.rank)
    .slice(0, limit)
    .map((m) => m.s);
}

/** A station's live state: the gateway's snapshot if it answers, else the one the last run recorded. */
async function liveState(id: string): Promise<{ station: LiveStation; asOf: string; live: boolean } | null> {
  for (const [url, live] of [
    [`${GATEWAY}/api/live/snapshot`, true],
    ["/data/live/snapshot.json", false],
  ] as const) {
    try {
      // The recorded snapshot does not change while the page is open; the gateway's does.
      const response = await fetch(url, { cache: live ? "no-store" : "force-cache" });
      if (!response.ok) continue;
      const { data } = (await response.json()) as { data: { asOf: string; stations: LiveStation[] } };
      const station = data.stations.find((s) => s.id === id);
      return station ? { station, asOf: data.asOf, live } : null;
    } catch {
      // no gateway: fall through to the recorded snapshot
    }
  }
  return null;
}

const isMac = () => /Mac|iPhone|iPad/.test(navigator.platform);
const shortcut = () => (isMac() ? "⌘K" : "Ctrl K");
const noop = () => () => {};

function Detail({
  station,
  total,
  onBack,
  onLeave,
}: {
  station: Station;
  total: number;
  onBack: () => void;
  onLeave: () => void;
}) {
  const [state, setState] = useState<Awaited<ReturnType<typeof liveState>> | "loading">("loading");
  useEffect(() => {
    let alive = true;
    void liveState(station.id).then((s) => alive && setState(s));
    return () => {
      alive = false;
    };
  }, [station.id]);
  const live = state !== "loading" ? state : null;
  const s = live?.station;
  return (
    <div className="space-y-4 p-5">
      <button type="button" onClick={onBack} className="text-xs font-semibold text-fg-3 hover:text-marker">
        ← back to results
      </button>
      <div>
        <p className="font-display text-xl leading-tight font-bold text-fg">{station.name}</p>
        <p className="mt-0.5 font-mono text-xs text-fg-3">station {station.id}</p>
      </div>
      <div className="flex flex-wrap gap-2 text-xs">
        <span className="rounded-full border border-white/15 px-2.5 py-0.5 text-fg-2">
          PageRank #{int(station.rank)} of {int(total)}
        </span>
        {/* District 0 holds the stations in none of GraphX's districts (too few strong ties). */}
        {station.district > 0 && (
          <span className="rounded-full border border-white/15 px-2.5 py-0.5 text-fg-2">district {station.district}</span>
        )}
        {station.archetype && (
          <span className="rounded-full border border-white/15 px-2.5 py-0.5 text-fg-2">{station.archetype}</span>
        )}
      </div>
      <dl className="grid grid-cols-3 gap-3 text-center">
        {(
          [
            ["docks", int(station.capacity)],
            ["trips out", int(station.out)],
            ["trips in", int(station.in)],
          ] as const
        ).map(([k, v]) => (
          <div key={k} className="clay flex flex-col-reverse rounded-2xl px-2 py-2.5">
            <dt className="text-[0.65rem] tracking-[0.12em] text-fg-3 uppercase">{k}</dt>
            <dd className="text-lg font-semibold text-fg tabular-nums">{v}</dd>
          </div>
        ))}
      </dl>
      <p className="text-xs text-fg-3">
        Jun–Sep 2026. Morning against evening PageRank: {station.shift > 0 ? "+" : ""}
        {fixed(station.shift, 2)} ({station.shift > 0.15 ? "busier in the morning" : station.shift < -0.15 ? "busier in the evening" : "even through the day"}).
      </p>
      <div className="glass-inset rounded-2xl p-3.5 text-sm">
        {state === "loading" ? (
          <p className="text-fg-3">Reading the live snapshot…</p>
        ) : s ? (
          <p className="flex flex-wrap items-center gap-x-2 gap-y-1 text-fg-2">
            <span className="font-semibold" style={{ color: STATE[s.risk].hex }}>
              {STATE[s.risk].glyph} {STATE[s.risk].label}
            </span>
            <span>
              · {s.bikes} bikes, {s.docks} free docks · {fixed(s.projected, 1)} bikes in 60 min
            </span>
            <span className="text-xs text-fg-3">{live?.live ? "live" : "recorded"}</span>
          </p>
        ) : (
          <p className="text-fg-3">Not in the live feed (closed, or not yet in GBFS).</p>
        )}
      </div>
      <div className="flex flex-wrap gap-2">
        <Link
          href={`/live?station=${encodeURIComponent(station.id)}`}
          onClick={onLeave}
          className="clay rounded-full px-4 py-2 text-sm font-semibold text-marker"
        >
          Show on the live map
        </Link>
        <Link href="/portal/network" onClick={onLeave} className="clay rounded-full px-4 py-2 text-sm font-semibold text-fg hover:text-marker">
          The network portal
        </Link>
      </div>
    </div>
  );
}

/**
 * ⌘K (Ctrl K) station search, from any page: type part of a station's name or its id, pick one with the arrow
 * keys, and see its place in the network, its archetype and its live state, with a way onto the live map.
 */
export function StationSearch() {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const [picked, setPicked] = useState<Station | null>(null);
  const [index, setIndex] = useState<{ stations: Station[]; total: number } | null>(null);
  const [failed, setFailed] = useState(false);
  const input = useRef<HTMLInputElement>(null);
  const keys = useSyncExternalStore(noop, shortcut, () => "⌘K");

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() === "k" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        setOpen((o) => !o);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const onOpenChange = useCallback((next: boolean) => {
    setOpen(next);
    if (!next) {
      setPicked(null);
      setQuery("");
      setActive(0);
    }
  }, []);

  useEffect(() => {
    if (!open || index) return;
    let alive = true;
    loadIndex()
      .then((i) => alive && setIndex(i))
      .catch(() => alive && setFailed(true));
    return () => {
      alive = false;
    };
  }, [open, index]);

  const results = useMemo(() => (index ? search(index.stations, query) : []), [index, query]);
  const choose = (s: Station) => setPicked(s);

  const onInputKey = (event: React.KeyboardEvent) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActive((a) => Math.min(results.length - 1, a + 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setActive((a) => Math.max(0, a - 1));
    } else if (event.key === "Enter" && results[active]) {
      event.preventDefault();
      choose(results[active]);
    }
  };

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Trigger asChild>
        <button
          type="button"
          className="clay inline-flex items-center gap-2 rounded-full px-3.5 py-1.5 text-xs font-semibold text-fg-2 hover:text-marker"
          aria-label={`Search stations ${keys}`}
        >
          <svg aria-hidden viewBox="0 0 16 16" className="size-3.5" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round">
            <circle cx="7" cy="7" r="4.5" />
            <path d="M10.5 10.5 14 14" />
          </svg>
          <span className="hidden xl:inline">Stations</span>{" "}
          <kbd className="hidden rounded-md border border-white/15 px-1.5 py-px font-mono text-[0.65rem] whitespace-nowrap text-fg-3 sm:inline-block">
            {keys}
          </kbd>
        </button>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-50 bg-ink-950/80" />
        <Dialog.Content
          className="glass fixed top-[12vh] left-1/2 z-50 w-[min(36rem,calc(100vw-2rem))] -translate-x-1/2 overflow-hidden rounded-[var(--radius-card)] shadow-2xl"
          onOpenAutoFocus={(event) => {
            event.preventDefault();
            input.current?.focus();
          }}
        >
          <Dialog.Title className="sr-only">Search stations</Dialog.Title>
          <Dialog.Description className="sr-only">
            Type part of a station&apos;s name or its id, then choose a result with the arrow keys and Enter.
          </Dialog.Description>
          {picked && index ? (
            <Detail
              key={picked.id}
              station={picked}
              total={index.total}
              onBack={() => setPicked(null)}
              // The header (and so this dialog) stays mounted across navigations: close it when leaving.
              onLeave={() => onOpenChange(false)}
            />
          ) : (
            <>
              <div className="flex items-center gap-3 border-b border-white/10 px-5 py-3.5">
                <span aria-hidden className="text-lg text-fg-3">
                  ⌕
                </span>
                <input
                  ref={input}
                  value={query}
                  onChange={(e) => {
                    setQuery(e.target.value);
                    setActive(0);
                  }}
                  onKeyDown={onInputKey}
                  placeholder="Search 2,490 stations by name or id"
                  aria-label="Station name or id"
                  aria-controls="station-results"
                  aria-activedescendant={results[active] ? `station-${results[active].id}` : undefined}
                  role="combobox"
                  aria-expanded={results.length > 0}
                  aria-autocomplete="list"
                  className="w-full bg-transparent text-base text-fg outline-none placeholder:text-fg-3"
                />
                <kbd className="rounded-md border border-white/15 px-1.5 py-px font-mono text-[0.65rem] text-fg-3">Esc</kbd>
              </div>
              <ul id="station-results" role="listbox" aria-label="Stations" className="max-h-[22rem] overflow-auto p-2">
                {failed && <li className="px-3 py-6 text-center text-sm text-fg-3">The station list could not be loaded.</li>}
                {!failed && !index && <li className="px-3 py-6 text-center text-sm text-fg-3">Loading the stations…</li>}
                {index && query.trim() === "" && (
                  <li className="px-3 py-6 text-center text-sm text-fg-3">Try &ldquo;Broadway&rdquo;, &ldquo;Bedford&rdquo; or a station id.</li>
                )}
                {index && query.trim() !== "" && results.length === 0 && (
                  <li className="px-3 py-6 text-center text-sm text-fg-3">No station matches &ldquo;{query}&rdquo;.</li>
                )}
                {results.map((s, i) => (
                  <li
                    key={s.id}
                    id={`station-${s.id}`}
                    role="option"
                    aria-selected={i === active}
                    onMouseEnter={() => setActive(i)}
                    onClick={() => choose(s)}
                    className={cn(
                      "flex cursor-pointer items-center justify-between gap-3 rounded-xl px-3 py-2.5",
                      i === active ? "bg-white/8" : "hover:bg-white/5",
                    )}
                  >
                    <span className="min-w-0">
                      <span className="block truncate text-sm font-semibold text-fg">{s.name}</span>
                      <span className="block font-mono text-[0.68rem] text-fg-3">
                        {s.id}
                        {s.archetype ? ` · ${s.archetype}` : ""}
                      </span>
                    </span>
                    <span className="shrink-0 text-xs text-fg-3 tabular-nums">PageRank #{int(s.rank)}</span>
                  </li>
                ))}
              </ul>
            </>
          )}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
