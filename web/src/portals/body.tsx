import {
  getClusterSnapshot,
  getDqReport,
  getInsightIndex,
  getLiveSnapshot,
  getMlFeatures,
  getNetworkPregel,
  getNetworkSummary,
  getRddReport,
  getSparkUi,
} from "@/lib/data";
import type { Meta } from "@/lib/schemas";

import { ClusterPortal } from "./cluster/cluster-portal";
import { GraphOpsPortal } from "./graph-ops/graph-ops-portal";
import { LakePortal } from "./lake/lake-portal";
import { ModelsPortal } from "./models/models-portal";
import { NetworkPortal } from "./network/network-portal";
import { PregelPortal } from "./pregel/pregel-portal";
import { RddLabPortal } from "./rdd-lab/rdd-lab-portal";
import type { PortalId } from "./registry";
import { SparkUiPortal } from "./spark-ui/spark-ui-portal";
import { SqlPortal } from "./sql/sql-portal";
import { StreamingPortal } from "./streaming/streaming-portal";
import { SyllabusPortal } from "./syllabus/syllabus-portal";

/** The body of each portal; the overlay and the full page render the same thing. */
export function PortalBody({ id }: { id: PortalId }) {
  switch (id) {
    case "lake":
      return <LakePortal />;
    case "rdd-lab":
      return <RddLabPortal />;
    case "sql":
      return <SqlPortal />;
    case "spark-ui":
      return <SparkUiPortal />;
    case "pregel":
      return <PregelPortal />;
    case "network":
      return <NetworkPortal />;
    case "graph-ops":
      return <GraphOpsPortal />;
    case "models":
      return <ModelsPortal />;
    case "streaming":
      return <StreamingPortal />;
    case "cluster":
      return <ClusterPortal />;
    case "syllabus":
      return <SyllabusPortal />;
  }
}

/** The export whose `meta` the portal header shows (which job wrote it, when, over which trips). */
export async function portalSource(id: PortalId): Promise<Meta | undefined> {
  switch (id) {
    case "lake":
      return (await getDqReport()).meta;
    case "rdd-lab":
      return (await getRddReport()).meta;
    case "sql":
      return (await getInsightIndex()).meta;
    case "pregel":
      return (await getNetworkPregel()).meta;
    case "network":
    case "graph-ops":
      return (await getNetworkSummary()).meta;
    case "spark-ui":
      return (await getSparkUi()).meta;
    case "models":
      return (await getMlFeatures()).meta;
    case "streaming":
      return (await getLiveSnapshot()).meta;
    case "cluster":
      return (await getClusterSnapshot()).meta;
    case "syllabus":
      return undefined; // it reads every export: no single source to name
  }
}
