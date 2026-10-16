import type { SparkUi } from "@/lib/schemas";

type Plan = SparkUi["data"]["sql"];
type PlanNode = Plan["nodes"][number];

const NODE_W = 178;
const NODE_H = 56;
const GAP_X = 30;
const GAP_Y = 38;
const PAD = 30;

/** Task-level metrics arrive as "total (min, med, max (…))\n2 ms (0 ms, 0 ms, 2 ms (…))"; the total is what we show. */
const total = (value: string) => (value.includes("\n") ? value.split("\n")[1].split(" (")[0] : value);

const HEADLINE = ["number of output rows", "size of files read", "shuffle records written"];
function headline(n: PlanNode) {
  const m = HEADLINE.map((name) => n.metrics.find((x) => x.name === name)).find(Boolean) ?? n.metrics[0];
  return m && `${m.name.replace("number of ", "")}: ${total(m.value)}`;
}

/**
 * Layered layout of the physical plan, data flowing from the scans (top) to the result (bottom), as in the Spark
 * UI. A node's layer is the longest path from a scan to it; within a layer, nodes follow the mean position of
 * their inputs, which keeps every branch in its own column. WholeStageCodegen entries are not operators but
 * clusters: they become the tinted boxes around the operators compiled together.
 */
function layout(nodes: PlanNode[], edges: Plan["edges"]) {
  const inputs = new Map<number, number[]>(nodes.map((n) => [n.id, []]));
  for (const e of edges) inputs.get(e.to)?.push(e.from);
  const depth = new Map<number, number>();
  const depthOf = (id: number): number => {
    const known = depth.get(id);
    if (known !== undefined) return known;
    depth.set(id, 0); // a physical plan is a DAG; this only stops a malformed export from recursing forever
    const d = Math.max(-1, ...(inputs.get(id) ?? []).map(depthOf)) + 1;
    depth.set(id, d);
    return d;
  };
  nodes.forEach((n) => depthOf(n.id));
  const layers: number[][] = [];
  nodes.forEach((n) => (layers[depth.get(n.id)!] ??= []).push(n.id));

  const slot = new Map<number, number>();
  layers.forEach((ids) => {
    const mean = (id: number) => {
      const xs = (inputs.get(id) ?? []).map((p) => slot.get(p) ?? 0);
      return xs.length ? xs.reduce((s, v) => s + v, 0) / xs.length : 0;
    };
    [...ids].sort((a, b) => mean(a) - mean(b) || a - b).forEach((id, i) => slot.set(id, i));
  });
  const widest = Math.max(...layers.map((l) => l.length));
  const pos = new Map<number, [number, number]>();
  layers.forEach((ids, d) => {
    // a layer narrower than the widest one is centred (in a plan, that is the Union under its branches)
    const xs = ids.map((id) => slot.get(id)!);
    const centre = ids.length === widest ? 0 : ((widest - 1) / 2 - (Math.min(...xs) + Math.max(...xs)) / 2) * (NODE_W + GAP_X);
    ids.forEach((id) => pos.set(id, [PAD + centre + slot.get(id)! * (NODE_W + GAP_X), PAD + 14 + d * (NODE_H + GAP_Y)]));
  });
  return {
    pos,
    width: PAD * 2 + widest * (NODE_W + GAP_X) - GAP_X,
    height: PAD * 2 + 14 + layers.length * (NODE_H + GAP_Y) - GAP_Y,
  };
}

export function SqlPlan({ plan }: { plan: Plan }) {
  const isCluster = (n: PlanNode) => n.name.startsWith("WholeStageCodegen");
  const operators = plan.nodes.filter((n) => !isCluster(n));
  const clusterTime = new Map(
    plan.nodes.filter(isCluster).map((n) => [Number(/\((\d+)\)/.exec(n.name)?.[1]), total(n.metrics[0]?.value ?? "")]),
  );
  const { pos, width, height } = layout(operators, plan.edges);
  const clusters = new Map<number, [number, number][]>();
  for (const n of operators) if (n.codegen !== null) clusters.set(n.codegen, [...(clusters.get(n.codegen) ?? []), pos.get(n.id)!]);

  return (
    <div className="glass-inset overflow-x-auto rounded-2xl">
      <svg
        width={width}
        height={height}
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label={`Physical plan of SQL query ${plan.executionId}: ${operators.length} operators, ${plan.exchanges} exchanges`}
        className="mx-auto block"
      >
        <defs>
          <marker id="plan-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M0,0 L10,5 L0,10 z" fill="var(--color-fg-3)" />
          </marker>
        </defs>
        {[...clusters.entries()].map(([id, pts]) => {
          const xs = pts.map((p) => p[0]);
          const ys = pts.map((p) => p[1]);
          const [x0, y0] = [Math.min(...xs) - 9, Math.min(...ys) - 22];
          const [x1, y1] = [Math.max(...xs) + NODE_W + 9, Math.max(...ys) + NODE_H + 9];
          return (
            <g key={`cg${id}`}>
              <rect x={x0} y={y0} width={x1 - x0} height={y1 - y0} rx={14} fill="var(--color-series-1)" fillOpacity={0.09} stroke="var(--color-series-1)" strokeOpacity={0.4} />
              <text x={x0 + 10} y={y0 + 14} fontSize={10} fontWeight={600} fill="var(--color-fg-2)">
                {`WholeStageCodegen (${id})${clusterTime.get(id) ? ` · ${clusterTime.get(id)}` : ""}`}
              </text>
            </g>
          );
        })}
        {plan.edges.map((e) => {
          const [a, b] = [pos.get(e.from), pos.get(e.to)];
          if (!a || !b) return null;
          const [x1, y1, x2, y2] = [a[0] + NODE_W / 2, a[1] + NODE_H, b[0] + NODE_W / 2, b[1] - 2];
          return (
            <path
              key={`${e.from}-${e.to}`}
              d={`M${x1},${y1} C${x1},${y1 + GAP_Y * 0.7} ${x2},${y2 - GAP_Y * 0.7} ${x2},${y2}`}
              fill="none"
              stroke="var(--color-fg-3)"
              strokeOpacity={0.7}
              strokeWidth={1.4}
              markerEnd="url(#plan-arrow)"
            />
          );
        })}
        {operators.map((n) => {
          const [x, y] = pos.get(n.id)!;
          const shuffle = n.name.startsWith("Exchange");
          const line = headline(n);
          return (
            <g key={n.id}>
              <title>{[n.name, ...n.metrics.map((m) => `${m.name}: ${total(m.value)}`)].join("\n")}</title>
              <rect
                x={x}
                y={y}
                width={NODE_W}
                height={NODE_H}
                rx={12}
                fill="var(--color-ink-850)"
                stroke={shuffle ? "var(--color-coral)" : "var(--color-fg-3)"}
                strokeOpacity={shuffle ? 1 : 0.5}
                strokeWidth={shuffle ? 2 : 1}
              />
              <text x={x + NODE_W / 2} y={y + (line ? 23 : 33)} textAnchor="middle" fontSize={12.5} fontWeight={700} fill={shuffle ? "var(--color-coral)" : "var(--color-fg)"}>
                {n.name}
              </text>
              {line && (
                <text x={x + NODE_W / 2} y={y + 41} textAnchor="middle" fontSize={10.5} fill="var(--color-fg-3)">
                  {line.length > 30 ? `${line.slice(0, 29)}…` : line}
                </text>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}
