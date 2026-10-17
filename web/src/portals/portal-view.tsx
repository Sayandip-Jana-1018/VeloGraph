import { notFound } from "next/navigation";

import { Glass } from "@/components/inkglass/surfaces";
import { PortalSheet } from "@/components/portal/portal-sheet";

import { PortalBody, portalSource } from "./body";
import { portalById } from "./registry";

/**
 * Resolves the route's `params` inside a Suspense boundary (the page passes the promise down), so opening a
 * portal shows the skeleton at once instead of blocking the navigation.
 */
export async function PortalView({ params, mode }: { params: Promise<{ id: string }>; mode: "modal" | "page" }) {
  const meta = portalById((await params).id);
  if (!meta) notFound();
  const source = await portalSource(meta.id);
  return (
    <PortalSheet meta={meta} mode={mode} source={source}>
      <PortalBody id={meta.id} />
    </PortalSheet>
  );
}

export function PortalSkeleton() {
  return (
    <Glass variant="sheet" className="min-h-[70vh] px-10 py-9" aria-busy>
      <p className="font-hand text-2xl text-marker">opening the portal…</p>
      <div className="mt-5 h-14 w-2/3 animate-pulse rounded-2xl bg-white/8" />
      <div className="mt-4 h-4 w-1/2 animate-pulse rounded-full bg-white/6" />
      <div className="mt-12 grid grid-cols-1 gap-4 md:grid-cols-3">
        {[0, 1, 2].map((i) => (
          <div key={i} className="h-40 animate-pulse rounded-3xl bg-white/5" />
        ))}
      </div>
    </Glass>
  );
}
