import { Notate } from "@/components/inkglass/ink-client";
import { Glass, KpiPuck } from "@/components/inkglass/surfaces";
import { Act, CodePanel, Ledger, VivaNote, WithNote } from "@/components/portal/blocks";
import { getNetworkCommunities, getNetworkSummary } from "@/lib/data";
import { fixed, int, share } from "@/lib/format";
import type { StationValue } from "@/lib/schemas";

import { DistrictChart, HopsChart, LabelChangesChart, ModularityChart } from "./network-charts";
import { NetworkExplorer } from "./network-explorer";

function RankList({ title, rows, value }: { title: string; rows: StationValue[]; value: (v: number) => string }) {
  return (
    <Glass variant="inset" className="p-5">
      <h3 className="mb-3 text-center font-display text-base font-bold text-fg">{title}</h3>
      <ol className="space-y-1.5 text-sm">
        {rows.map((r, i) => (
          <li key={r.id} className="flex items-baseline justify-between gap-3">
            <span className="truncate text-fg-2">
              <span className="mr-1.5 text-fg-3 tabular-nums">{i + 1}.</span>
              {r.name}
            </span>
            <span className="text-fg tabular-nums">{value(r.value)}</span>
          </li>
        ))}
      </ol>
    </Glass>
  );
}

export async function NetworkPortal() {
  const [{ data: net }, { data: com }] = await Promise.all([getNetworkSummary(), getNetworkCommunities()]);
  const pr = net.pagerank;
  const office = net.shift.districts.at(-1)!;
  const tri = net.triangles;
  const comp = net.components;
  const jerseyLike = comp.outsideGiantScc.filter((s) => /^(JC|HB)/.test(s.id)).length;
  const communities = [...com.communityList].sort((a, b) => b.size - a.size);

  return (
    <>
      <Act
        n={1}
        title="2,490 stations, five ways to look at them"
        lede="Every dot is a station at its real position, every arc a real flow. Switch the map between the GraphX results; sizes and colours animate between them."
      >
        <NetworkExplorer
          communities={communities.map((c) => ({ id: c.id, size: c.size, topStations: c.topStations, radiusKm: c.radiusKm }))}
        />
      </Act>

      <Act
        n={2}
        title="Why weights matter"
        lede={
          <>
            GraphX&apos;s built-in PageRank splits a station&apos;s rank equally over its routes; ours splits it by trips.
            The two agree broadly (Spearman ρ ={" "}
            <Notate type="circle" color="var(--color-marker)" padding={5}>
              {fixed(pr.spearman, 2)}
            </Notate>
            ), but only {pr.topOverlap} of the top {pr.topWeighted.length} are the same.
          </>
        }
      >
        <div className="grid gap-5 md:grid-cols-2">
          <RankList title="Weighted (ours, Pregel)" rows={pr.topWeighted.slice(0, 10)} value={(v) => `${fixed(v * 100, 3)} %`} />
          <RankList title="Unweighted (GraphX built-in)" rows={pr.topUnweighted.slice(0, 10)} value={(v) => `${fixed(v * 100, 3)} %`} />
        </div>
        <div className="grid gap-5 md:grid-cols-2">
          <Glass variant="inset" className="p-5 text-sm">
            <h3 className="mb-2 text-center font-display text-base font-bold text-fg">Rise with weights</h3>
            {pr.risers.map((m) => (
              <p key={m.id} className="flex justify-between gap-3 text-fg-2">
                <span className="truncate">{m.name}</span>
                <span className="text-fg tabular-nums">
                  #{m.unweightedRank} → #{m.weightedRank}
                </span>
              </p>
            ))}
          </Glass>
          <Glass variant="inset" className="p-5 text-sm">
            <h3 className="mb-2 text-center font-display text-base font-bold text-fg">Fall with weights</h3>
            {pr.fallers.map((m) => (
              <p key={m.id} className="flex justify-between gap-3 text-fg-2">
                <span className="truncate">{m.name}</span>
                <span className="text-fg tabular-nums">
                  #{m.unweightedRank} → #{m.weightedRank}
                </span>
              </p>
            ))}
          </Glass>
        </div>
      </Act>

      <Act
        n={3}
        title="Mornings end in offices, evenings at home"
        lede={`${share(office.morningShare)} of the ${net.shift.sample} stations where morning riders end up are in Midtown or Lower Manhattan; in the evening it is ${share(office.eveningShare)}.`}
      >
        <DistrictChart rows={net.shift.districts} sample={net.shift.sample} />
        <div className="grid gap-5 md:grid-cols-2">
          <RankList title="Morning magnets" rows={net.shift.morningMagnets.slice(0, 8)} value={(v) => `+${fixed(v, 2)}`} />
          <RankList title="Evening magnets" rows={net.shift.eveningMagnets.slice(0, 8)} value={(v) => fixed(v, 2)} />
        </div>
      </Act>

      <Act
        n={4}
        title={`${com.communities} rebalancing districts`}
        lede={`Weighted label propagation found ${com.lpaCommunities} communities (Q = ${fixed(com.lpaModularity, 3)}), below GraphX's built-in (${fixed(com.builtinModularity, 3)}). A greedy modularity merge on top joined them into ${com.communities} (Q = ${fixed(com.modularity, 3)}).`}
      >
        <ModularityChart
          rows={[
            { method: "Weighted LPA + modularity merge", q: com.modularity, communities: com.communities },
            { method: "GraphX LabelPropagation", q: com.builtinModularity, communities: com.builtinCommunities },
            { method: "Weighted LPA (ours, Pregel)", q: com.lpaModularity, communities: com.lpaCommunities },
          ]}
        />
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Median distance to centre" value={`${fixed(com.medianRadiusKm, 1)} km`} note="communities are compact" />
          <KpiPuck label="Same sizes, shuffled" value={`${fixed(com.randomMedianRadiusKm, 1)} km`} note="random baseline" />
          <KpiPuck label="Stations covered" value={share(com.coverage)} note="the rest have no strong tie" />
        </div>
        <LabelChangesChart changes={com.labelChanges} />
        <WithNote
          note={
            <VivaNote>
              Label propagation: every station starts with its own label and keeps adopting the label with the most trip
              weight among its neighbours. Modularity Q measures how much more weight stays inside communities than
              chance would put there; we got {fixed(com.modularity, 3)}.
            </VivaNote>
          }
        >
          <CodePanel id="network-lpa" caption="weighted label propagation in Pregel" />
        </WithNote>
      </Act>

      <Act
        n={5}
        title="Structure: components, triangles, strength"
        lede="Connected components ignore direction; strongly connected ones need a way there and back. Triangles measure how local the strong ties are."
      >
        <div className="flex flex-wrap justify-center gap-4">
          <KpiPuck label="Weakly connected" value={int(comp.weak.count)} note={`component of ${int(comp.weak.largest)}`} />
          <KpiPuck label="Strongly connected" value={int(comp.strong.count)} note={`giant: ${int(comp.strong.largest)} stations`} />
          <KpiPuck label="Arrival-only stations" value={int(comp.outsideGiantScc.length)} note={`${jerseyLike} in Jersey City / Hoboken`} />
          <KpiPuck label="Triangles" value={int(tri.triangles)} note="of strong ties" />
          <KpiPuck label="Clustering" value={fixed(tri.averageClustering, 2)} note={`random graph: ${fixed(tri.randomGraphClustering, 3)}`} />
        </div>
        <div className="grid gap-5 md:grid-cols-2">
          <RankList title="Where bikes pile up (net arrivals)" rows={net.strength.sinks.slice(0, 6)} value={(v) => `+${int(v)}`} />
          <RankList title="Where bikes run out (net departures)" rows={net.strength.sources.slice(0, 6)} value={(v) => int(v)} />
        </div>
        <CodePanel id="network-strength" caption="strength in one aggregateMessages pass" />
      </Act>

      <Act
        n={6}
        title="Paths"
        lede={`Hop counts use GraphX ShortestPaths; travel times use our multi-source Pregel shortest paths over ${int(net.paths.travelEdges)} routes, weighted by median ride time. From the median landmark, ${int(net.paths.medianReachableWithin15Min)} stations are within 15 minutes.`}
      >
        <HopsChart bins={net.paths.hopHistogram} unreachable={net.paths.unreachable} />
      </Act>

      <Ledger
        rows={[
          { label: "Spearman ρ (weighted vs built-in)", value: fixed(pr.spearman, 3), path: "network-summary.json · pagerank.spearman" },
          { label: "Morning share in office districts", value: share(office.morningShare), path: "network-summary.json · shift.districts" },
          { label: "Modularity Q (final)", value: fixed(com.modularity, 3), path: "network-communities.json · modularity" },
          { label: "Communities", value: String(com.communities), path: "network-communities.json · communities" },
          { label: "Strongly connected components", value: int(comp.strong.count), path: "network-summary.json · components.strong.count" },
          { label: "Triangles", value: int(tri.triangles), path: "network-summary.json · triangles.triangles" },
        ]}
      />
    </>
  );
}
