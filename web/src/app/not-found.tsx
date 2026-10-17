import type { Metadata } from "next";
import Link from "next/link";

import { Doodle } from "@/components/inkglass/ink";
import { Sticker } from "@/components/inkglass/paper";

export const metadata: Metadata = { title: "Not on the line" };

const WAYS = [
  { href: "/", label: "Home" },
  { href: "/#pipeline", label: "The pipeline" },
  { href: "/#portals", label: "All portals" },
  { href: "/live", label: "Live map" },
];

// Unknown URLs and unknown portal ids (`notFound()` in PortalView) both end here.
export default function NotFound() {
  return (
    <div className="mx-auto flex max-w-2xl flex-col items-center px-5 pt-24 pb-8 text-center sm:pt-32">
      <Sticker accent="coral" tilt={-4}>
        404 · no such stop
      </Sticker>
      <h1 className="mt-8 font-display text-[clamp(3.2rem,9vw,6.5rem)] leading-[0.9] font-extrabold tracking-[-0.035em] text-balance text-fg">
        Not on <span className="text-gleam">the line.</span>
      </h1>
      <p className="mt-6 max-w-md text-lg leading-relaxed text-pretty text-fg-2">
        No page or portal lives at this address. Every stop of the pipeline is one click away from here.
      </p>
      <nav aria-label="Ways back" className="mt-9 flex flex-wrap justify-center gap-3">
        {WAYS.map((w, i) => (
          <Link
            key={w.href}
            href={w.href}
            className={`clay rounded-full px-5 py-2.5 text-sm font-semibold transition-colors ${i === 0 ? "text-marker hover:text-fg" : "text-fg hover:text-marker"}`}
          >
            {w.label}
          </Link>
        ))}
      </nav>
      <p className="mt-10 flex items-center gap-2 font-hand text-xl text-fg-2">
        <Doodle kind="spark" size={22} />
        looking for a station? press Ctrl K (⌘K on a Mac)
      </p>
    </div>
  );
}
