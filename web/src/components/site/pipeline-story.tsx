import Link from "next/link";

import { InkRing } from "@/components/inkglass/ink";
import { Reveal } from "@/components/inkglass/motion";
import { Chip } from "@/components/inkglass/surfaces";
import { cn } from "@/lib/cn";
import { portalById, type PortalId } from "@/portals/registry";

import { StoryScroller } from "./story-scroller";

export type Stop = {
  /** Anchor of the chapter (`#stop-<id>`). */
  id: string;
  /** The stop's name on the line, and the Spark API it uses. */
  line: string;
  api: string;
  kicker: string;
  title: string;
  body: string;
  /** Measured numbers, already formatted from the exports. */
  facts: { value: string; label: string }[];
  portals: PortalId[];
  /** A page to visit as well, e.g. the live map. */
  page?: { href: string; label: string };
};

/** One stop of the line: a fixed height, so the travelled part of the track can be a plain scale. */
const STOP_H = "h-16";

/**
 * The pipeline as a subway line, told in chapters: the line stays in view (on wide screens) while the chapters
 * scroll past, and lights each stop as its chapter reaches the middle of the screen. Every chapter opens the
 * portals that prove it.
 */
export function PipelineStory({ stops }: { stops: Stop[] }) {
  return (
    <section id="pipeline" aria-labelledby="pipeline-title" className="scroll-mt-28 pt-28">
      <Reveal className="mx-auto mb-14 max-w-2xl text-center">
        <p className="font-hand text-2xl text-marker">ride the line</p>
        <h2 id="pipeline-title" className="font-display text-5xl font-extrabold tracking-tight text-fg sm:text-7xl">
          The pipeline, stop by stop
        </h2>
        <p className="mt-4 text-fg-2">
          {stops.length} stops from raw files to the live map. Each one is a Spark module with its own portal: scroll
          along the line, or jump straight to a stop.
        </p>
      </Reveal>

      <StoryScroller className="grid grid-cols-1 items-start gap-x-14 gap-y-8 lg:grid-cols-[minmax(0,0.78fr)_minmax(0,1.22fr)]">
        <nav aria-label="Pipeline stops" className="sticky top-28 hidden lg:block">
          <div className="glass rounded-[var(--radius-card)] p-7">
            <div className="flex items-center gap-3">
              <span
                aria-hidden
                className="grid size-10 place-items-center rounded-full bg-marker font-display text-xl font-extrabold text-ink-950"
              >
                V
              </span>
              <div>
                <p className="font-display text-lg leading-tight font-bold text-fg">The VeloGraph line</p>
                <p className="text-xs text-fg-3">raw files → live map · {stops.length} stops</p>
              </div>
            </div>
            <ol className="relative mt-6">
              {/* The track from the first stop's centre to the last one's, then the part travelled so far. */}
              <span aria-hidden className="absolute top-8 bottom-8 left-[0.6875rem] w-1.5 -translate-x-1/2 rounded-full bg-white/10" />
              <span
                aria-hidden
                data-progress
                style={{ scale: "1 0" }}
                className="absolute top-8 bottom-8 left-[0.6875rem] w-1.5 origin-top -translate-x-1/2 rounded-full bg-marker transition-[scale] duration-700 ease-out motion-reduce:transition-none"
              />
              {stops.map((s, i) => (
                <li key={s.id} data-node={i} data-current={i === 0 ? "" : undefined} className={cn("group relative flex items-center", STOP_H)}>
                  <a
                    href={`#stop-${s.id}`}
                    aria-current={i === 0 ? "step" : undefined}
                    className="flex w-full items-center gap-4 rounded-xl py-1 pr-2"
                  >
                    <span
                      aria-hidden
                      className="relative size-[1.375rem] shrink-0 rounded-full border-4 border-ink-850 bg-ink-600 transition-[background-color,box-shadow] duration-500 group-data-current:bg-marker group-data-current:shadow-[0_0_0_5px_color-mix(in_oklab,var(--color-marker)_28%,transparent)] group-data-passed:bg-marker motion-reduce:transition-none"
                    />
                    <span className="min-w-0">
                      <span className="block font-display text-lg leading-tight font-bold text-fg-2 transition-colors group-hover:text-marker group-data-current:text-fg">
                        {s.line}
                      </span>
                      <span className="block truncate text-xs text-fg-3">{s.api}</span>
                    </span>
                    <span
                      aria-hidden
                      className="ml-auto font-hand text-lg whitespace-nowrap text-marker opacity-0 transition-opacity duration-500 group-data-current:opacity-100 motion-reduce:transition-none"
                    >
                      ← you are here
                    </span>
                  </a>
                </li>
              ))}
            </ol>
          </div>
        </nav>

        <ol className="space-y-6">
          {stops.map((s, i) => (
            <Chapter key={s.id} stop={s} n={i + 1} />
          ))}
        </ol>
      </StoryScroller>
    </section>
  );
}

function Chapter({ stop: s, n }: { stop: Stop; n: number }) {
  const portals = s.portals.map((id) => portalById(id)!);
  const units = [...new Set(portals.flatMap((p) => p.units))];
  const outcomes = [...new Set(portals.flatMap((p) => p.outcomes))].sort((a, b) => a.localeCompare(b));
  return (
    // Jumping to a stop puts its chapter across the middle of the screen, where the line reads its position.
    <li id={`stop-${s.id}`} data-stop={n - 1} className="scroll-mt-[calc(50vh-7rem)]">
      <Reveal>
        <article className="glass glow-ring relative flex flex-col items-center rounded-[var(--radius-card)] p-7 text-center sm:p-9 lg:items-start lg:text-left">
          <div className="flex flex-wrap items-center justify-center gap-3 lg:justify-start">
            <InkRing size={46} seed={n + 4} color="var(--color-marker)">
              <span className="font-display text-lg font-extrabold text-marker">{n}</span>
            </InkRing>
            <p className="font-hand text-xl text-marker">{s.kicker}</p>
          </div>
          <h3 className="mt-3 font-display text-3xl font-extrabold tracking-tight text-balance text-fg sm:text-4xl">
            {s.title}
          </h3>
          <div className="mt-3 flex flex-wrap justify-center gap-2 lg:justify-start">
            {units.map((u) => (
              <Chip key={u} tone="unit">
                {u}
              </Chip>
            ))}
            {outcomes.map((o) => (
              <Chip key={o} tone="co">
                {o}
              </Chip>
            ))}
          </div>
          <p className="mt-4 max-w-xl leading-relaxed text-pretty text-fg-2">{s.body}</p>
          <dl className="mt-6 flex flex-wrap justify-center gap-x-9 gap-y-4 lg:justify-start">
            {s.facts.map((f) => (
              <div key={f.label} className="flex flex-col-reverse">
                <dt className="max-w-[11rem] text-xs leading-snug text-fg-3">{f.label}</dt>
                <dd className="text-2xl font-semibold text-fg tabular-nums">{f.value}</dd>
              </div>
            ))}
          </dl>
          <div className="mt-7 flex flex-wrap justify-center gap-3 lg:justify-start">
            {portals.map((p) => (
              <Link
                key={p.id}
                href={`/portal/${p.id}`}
                scroll={false}
                aria-label={`Open portal: ${p.title}`}
                className="clay rounded-full px-5 py-2 text-sm font-semibold whitespace-nowrap text-fg transition-colors hover:text-marker"
              >
                {p.title} ↗
              </Link>
            ))}
            {s.page && (
              <Link
                href={s.page.href}
                className="clay rounded-full px-5 py-2 text-sm font-semibold whitespace-nowrap text-marker transition-colors hover:text-fg"
              >
                {s.page.label} →
              </Link>
            )}
          </div>
        </article>
      </Reveal>
    </li>
  );
}
