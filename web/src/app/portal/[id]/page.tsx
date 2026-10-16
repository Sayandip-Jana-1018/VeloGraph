import type { Metadata } from "next";
import { Suspense } from "react";

import { PortalSkeleton, PortalView } from "@/portals/portal-view";
import { PORTALS, portalById } from "@/portals/registry";

// Direct visits and refreshes of /portal/<id> render the portal as a full page.
export function generateStaticParams() {
  return PORTALS.map((p) => ({ id: p.id }));
}

export async function generateMetadata({ params }: PageProps<"/portal/[id]">): Promise<Metadata> {
  const meta = portalById((await params).id);
  return meta ? { title: meta.title, description: meta.hook } : {};
}

export default function PortalPage({ params }: PageProps<"/portal/[id]">) {
  return (
    <div className="mx-auto max-w-[1280px] px-3 pt-10 sm:px-6">
      <Suspense fallback={<PortalSkeleton />}>
        <PortalView params={params} mode="page" />
      </Suspense>
    </div>
  );
}
