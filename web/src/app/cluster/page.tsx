import type { Metadata } from "next";
import { Suspense } from "react";

import { PortalSkeleton, PortalView } from "@/portals/portal-view";

export const metadata: Metadata = {
  title: "Cluster",
  description: "The Spark standalone cluster and the three-broker Kafka cluster, live, and the whole batch pipeline timed end to end.",
};

/** /cluster: the door to the cluster portal (ARCHITECTURE §11), rendered as a full page. */
export default function ClusterPage() {
  return (
    <div className="mx-auto max-w-[1280px] px-3 pt-10 sm:px-6">
      <Suspense fallback={<PortalSkeleton />}>
        <PortalView params={Promise.resolve({ id: "cluster" })} mode="page" />
      </Suspense>
    </div>
  );
}
