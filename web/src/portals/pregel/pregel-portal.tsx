import { Notate } from "@/components/inkglass/ink-client";
import { KpiPuck } from "@/components/inkglass/surfaces";
import { Act, CodePanel, Ledger, VivaNote, WithNote } from "@/components/portal/blocks";
import { getNetworkPregel, getNetworkSummary } from "@/lib/data";
import { duration, int } from "@/lib/format";

import { ActiveVerticesChart } from "./pregel-charts";
import { PregelStepper } from "./pregel-stepper";

export async function PregelPortal() {
  const [{ data: pregel }, { data: net }] = await Promise.all([getNetworkPregel(), getNetworkSummary()]);
  const pr = net.pagerank.weighted;
  const hub = pregel.stations[0];

  return (
    <>
      <Act
        n={1}
        title="Watch PageRank converge"
        lede={
          <>
            The top PageRank hub, {hub.name}, and its {pregel.stations.length - 1} strongest partners, with every route
            between them. Step through the {pregel.steps.length} supersteps: watch the messages, and the moment each
            station{" "}
            <Notate type="underline" color="var(--color-coral)">
              votes to halt
            </Notate>
            .
          </>
        }
      >
        <PregelStepper data={pregel} />
      </Act>

      <Act
        n={2}
        title={`The real run: ${int(net.graph.vertices)} vertices, ${pr.supersteps} supersteps`}
        lede={`On the whole network the same program needed ${pr.supersteps} supersteps (${duration(pr.millis)}). Nearly every station keeps updating almost to the end, then drops out together as the changes shrink below the tolerance.`}
      >
        <ActiveVerticesChart active={pr.activeVertices} vertices={net.graph.vertices} />
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Weighted PageRank" value={`${pr.supersteps} steps`} note={duration(pr.millis)} />
          <KpiPuck label="Morning trips only" value={`${net.shift.am.supersteps} steps`} note={duration(net.shift.am.millis)} />
          <KpiPuck label="Evening trips only" value={`${net.shift.pm.supersteps} steps`} note={duration(net.shift.pm.millis)} />
          <KpiPuck label="GraphX built-in" value={duration(net.pagerank.builtinMillis)} note="unweighted, same tolerance" />
        </div>
      </Act>

      <Act n={3} title="The program" lede="Three small functions and an initial message are the whole algorithm; Pregel runs them superstep after superstep.">
        <WithNote
          note={
            <VivaNote>
              Pregel works in supersteps. A vertex that got messages runs vprog; then sendMsg runs on its edges; mergeMsg
              adds up messages for the same vertex. A vertex whose rank changed by less than the tolerance sends
              nothing, it votes to halt, and Pregel stops when no messages are left: here after {pr.supersteps} supersteps.
            </VivaNote>
          }
        >
          <CodePanel id="pregel-program" caption="vprog, sendMsg and mergeMsg of weighted PageRank" />
        </WithNote>
      </Act>

      <Ledger
        rows={[
          { label: "Explainer stations", value: String(pregel.stations.length), path: "network-pregel.json · data.stations" },
          { label: "Explainer supersteps", value: String(pregel.convergedAfter), path: "network-pregel.json · data.convergedAfter" },
          { label: "Full-graph supersteps", value: String(pr.supersteps), path: "network-summary.json · pagerank.weighted.supersteps" },
          { label: "Full-graph run time", value: duration(pr.millis), path: "network-summary.json · pagerank.weighted.millis" },
          { label: "Reset probability α", value: String(pregel.resetProb), path: "network-pregel.json · data.resetProb" },
          { label: "Tolerance", value: String(pregel.tol), path: "network-pregel.json · data.tol" },
        ]}
      />
    </>
  );
}
