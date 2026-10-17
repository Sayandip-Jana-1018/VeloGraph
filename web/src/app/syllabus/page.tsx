import type { Metadata } from "next";
import { Suspense } from "react";

import { PortalSkeleton, PortalView } from "@/portals/portal-view";

export const metadata: Metadata = {
  title: "Syllabus",
  description: "Every unit of INT315 and CO4 to CO6, each with what VeloGraph built, one measured number, and the portals that prove it.",
};

/** /syllabus: the door to the syllabus portal (ARCHITECTURE §11), rendered as a full page. */
export default function SyllabusPage() {
  return (
    <div className="mx-auto max-w-[1280px] px-3 pt-10 sm:px-6">
      <Suspense fallback={<PortalSkeleton />}>
        <PortalView params={Promise.resolve({ id: "syllabus" })} mode="page" />
      </Suspense>
    </div>
  );
}
