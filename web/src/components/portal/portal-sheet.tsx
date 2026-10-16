import Link from "next/link";

import { InkUnderline } from "@/components/inkglass/ink";
import { Sticker } from "@/components/inkglass/paper";
import { Chip, Glass } from "@/components/inkglass/surfaces";
import type { Meta } from "@/lib/schemas";
import { stamp } from "@/lib/format";
import type { PortalMeta } from "@/portals/registry";

import { PortalCloseButton } from "./portal-modal";

/**
 * The portal frame shared by the overlay and the full page: title, syllabus chips, the export it reads and when
 * that was generated, then the portal's acts.
 */
export function PortalSheet({
  meta,
  mode,
  source,
  children,
}: {
  meta: PortalMeta;
  mode: "modal" | "page";
  source?: Meta;
  children: React.ReactNode;
}) {
  return (
    <Glass as="article" variant="sheet" className="glow-ring relative overflow-hidden">
      <header className="relative flex flex-wrap items-start gap-x-10 gap-y-6 border-b border-white/10 px-6 pt-9 pb-8 sm:px-12">
        <div className="min-w-0 flex-1">
          <div className="mb-3 flex flex-wrap items-center gap-2">
            {meta.units.map((u) => (
              <Chip key={u} tone="unit">
                {u}
              </Chip>
            ))}
            {meta.outcomes.map((o) => (
              <Chip key={o} tone="co">
                {o}
              </Chip>
            ))}
            <span className="font-hand text-lg text-marker">{meta.kicker}</span>
          </div>
          <h1 className="font-display text-4xl leading-[0.95] font-extrabold tracking-tight text-fg sm:text-6xl">
            {meta.title}
          </h1>
          <InkUnderline width={220} seed={meta.title.length} className="mt-1" />
          <p className="mt-3 max-w-2xl text-base text-fg-2">{meta.hook}</p>
        </div>
        <div className="flex items-start gap-4">
          <div className="flex flex-col items-end gap-2 pt-1 text-right">
            <Sticker accent={meta.accent} tilt={3}>
              Real data
            </Sticker>
            {source && (
              <p className="max-w-[16rem] text-xs leading-relaxed text-fg-3">
                <span className="font-mono text-fg-2">{meta.sources.join(" · ")}</span>
                <br />
                written by <span className="font-mono">{source.job}</span>, {stamp(source.generatedAt)}
                {source.source && (
                  <>
                    <br />
                    trips {source.source.from} → {source.source.to}
                  </>
                )}
              </p>
            )}
          </div>
          {mode === "modal" ? (
            <PortalCloseButton />
          ) : (
            <Link
              href="/#portals"
              className="clay rounded-full px-4 py-2.5 text-sm font-semibold whitespace-nowrap text-fg hover:text-marker"
            >
              ← All portals
            </Link>
          )}
        </div>
      </header>
      <div className="relative space-y-24 px-6 py-14 sm:px-12">{children}</div>
    </Glass>
  );
}
