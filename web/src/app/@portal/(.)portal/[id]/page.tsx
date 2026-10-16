import { Suspense } from "react";

import { PortalModal } from "@/components/portal/portal-modal";
import { PortalSkeleton, PortalView } from "@/portals/portal-view";
import { PORTALS } from "@/portals/registry";

// Clicking a portal link inside the app intercepts /portal/<id> and opens it over the current page.
export function generateStaticParams() {
  return PORTALS.map((p) => ({ id: p.id }));
}

export default function PortalOverlay({ params }: PageProps<"/portal/[id]">) {
  return (
    <PortalModal>
      <Suspense fallback={<PortalSkeleton />}>
        <PortalView params={params} mode="modal" />
      </Suspense>
    </PortalModal>
  );
}
