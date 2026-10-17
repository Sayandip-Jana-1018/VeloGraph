import { Notate } from "@/components/inkglass/ink-client";
import { Glass, KpiPuck } from "@/components/inkglass/surfaces";
import { Act, CodePanel, Ledger, VivaNote, WithNote } from "@/components/portal/blocks";
import { getNetworkSummary } from "@/lib/data";
import { compact, duration, fixed, int, share } from "@/lib/format";

import { HistogramChart, ReplicationChart, TimingsChart } from "./graph-ops-charts";

/**
 * Why EdgePartition2D bounds the copies: partitions form a √P × √P grid; an edge goes to the cell (row of its
 * source's block, column of its destination's block), so a vertex's edges stay inside one row and one column.
 */
function GridDiagram({ side }: { side: number }) {
  const row = 1;
  const col = 2;
  return (
    <figure className="mx-auto w-fit text-center">
      <div className="grid gap-1.5" style={{ gridTemplateColumns: `repeat(${side}, 3.2rem)` }}>
        {Array.from({ length: side * side }, (_, i) => {
          const [r, c] = [Math.floor(i / side), i % side];
          const hit = r === row || c === col;
          return (
            <div
              key={i}
              className={`grid h-12 place-items-center rounded-lg text-xs font-semibold ${hit ? "clay text-marker" : "clay-well text-fg-3"}`}
            >
              {r * side + c}
            </div>
          );
        })}
      </div>
      <figcaption className="mt-4 max-w-xs font-hand text-xl leading-snug text-marker">
        vertex v lives in row {row} and column {col}: at most 2·{side} − 1 = {2 * side - 1} partitions
      </figcaption>
    </figure>
  );
}

export async function GraphOpsPortal() {
  const { data: net } = await getNetworkSummary();
  const g = net.graph;
  const twoD = net.partitioning.find((p) => p.strategy === "EdgePartition2D")!;
  const worst = net.partitioning.reduce((a, b) => (b.replicationFactor > a.replicationFactor ? b : a));
  const side = Math.round(Math.sqrt(twoD.partitions));
  const timings = Object.entries(net.timingsMs).sort((a, b) => b[1] - a[1]);

  return (
    <>
      <Act
        n={1}
        title="The property graph"
        lede="Stations are vertices, routes are directed edges, round trips are self-loops. Every attribute travels with them through every operator."
      >
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Vertices" value={int(g.vertices)} note="stations" />
          <KpiPuck label="Edges" value={compact(g.edges)} note={`+ ${int(g.selfLoops)} self-loops`} />
          <KpiPuck label="Density" value={share(g.density)} note="of all station pairs" />
          <KpiPuck label="Reciprocity" value={share(g.reciprocity)} note="routes ridden both ways" />
          <KpiPuck label="Out-degree" value={int(Math.round(g.avgOutDegree))} note="destinations per station, on average" />
          <KpiPuck label="Strong ties" value={int(g.strongUndirectedEdges)} note="≥ 20 trips, top-10" />
        </div>
        <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
          <HistogramChart
            title="Out-degree distribution"
            subtitle="How many different stations riders leave each station for"
            bins={net.outDegreeHistogram.map((b) => ({ name: `${b.from}–${b.to - 1}`, stations: b.stations }))}
          />
          <HistogramChart
            title="Strength distribution (log bins)"
            subtitle="Trips through each station, in quarter-decade bins"
            bins={net.strengthHistogram.map((b) => ({ name: compact(b.from), stations: b.stations }))}
          />
        </div>
      </Act>

      <Act
        n={2}
        title="Vertex-cut partitioning, measured"
        lede={
          <>
            GraphX splits the <em>edges</em> across partitions and copies each vertex to every partition that holds one of
            its edges. On {twoD.partitions} partitions, EdgePartition2D needs{" "}
            <Notate type="circle" color="var(--color-marker)" padding={6}>
              {fixed(twoD.replicationFactor, 1)} copies
            </Notate>{" "}
            per vertex; the others need up to {fixed(worst.replicationFactor, 1)}.
          </>
        }
      >
        <div className="grid grid-cols-1 items-center gap-8 lg:grid-cols-[minmax(0,1fr)_auto]">
          <ReplicationChart runs={net.partitioning} />
          <GridDiagram side={side} />
        </div>
        <CodePanel id="graphops-partitioning" caption="counting vertex copies per partition" />
      </Act>

      <Act
        n={3}
        title="Thirteen operators, each checked"
        lede="Run once on the real graph by OperatorTour; every claim below is asserted in the job before it is written."
      >
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3">
          {net.operators.map((o) => (
            <Glass key={o.operator} className="p-5 text-center">
              <p className="font-mono text-sm font-bold text-marker">.{o.operator}</p>
              <p className="mt-1 text-xs text-fg-3">{o.purpose}</p>
              <p className="mt-3 text-sm leading-snug text-fg">{o.result}</p>
            </Glass>
          ))}
        </div>
        <WithNote
          note={
            <VivaNote>
              groupEdges only merges parallel edges that sit in the same partition, so I call partitionBy first: it sends
              every edge with the same source and destination to the same partition.
            </VivaNote>
          }
        >
          <CodePanel id="graphops-group-edges" caption="a multigraph, partitioned, merged back" />
        </WithNote>
      </Act>

      <Act n={4} title="The cost of each algorithm" lede={`The whole suite runs in ${duration(timings.reduce((s, [, ms]) => s + ms, 0))} of step time.`}>
        <TimingsChart timings={timings} />
      </Act>

      <Ledger
        rows={[
          { label: "Vertices / edges", value: `${int(g.vertices)} / ${int(g.edges)}`, path: "network-summary.json · graph" },
          { label: "EdgePartition2D copies per vertex", value: fixed(twoD.replicationFactor, 2), path: "network-summary.json · partitioning" },
          { label: "Worst strategy", value: `${worst.strategy}, ${fixed(worst.replicationFactor, 2)}`, path: "network-summary.json · partitioning" },
          { label: "Operators demonstrated", value: String(net.operators.length), path: "network-summary.json · operators" },
          { label: "Dangling stations", value: int(g.danglingVertices), path: "network-summary.json · graph.danglingVertices" },
        ]}
      />
    </>
  );
}
