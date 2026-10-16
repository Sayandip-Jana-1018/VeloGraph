import { sketch } from "@/components/inkglass/rough";
import { getNetworkNodes } from "@/lib/data";

type XY = [number, number];

const W = 1000;
const PAD = 40;

/** Turns the station cloud about its principal axis, so the city's long side lies across the card. */
function principalTurn(points: XY[]): XY[] {
  const cx = points.reduce((s, p) => s + p[0], 0) / points.length;
  const cy = points.reduce((s, p) => s + p[1], 0) / points.length;
  let [sxx, syy, sxy] = [0, 0, 0];
  for (const [x, y] of points) {
    sxx += (x - cx) ** 2;
    syy += (y - cy) ** 2;
    sxy += (x - cx) * (y - cy);
  }
  const theta = -0.5 * Math.atan2(2 * sxy, sxx - syy);
  const [cos, sin] = [Math.cos(theta), Math.sin(theta)];
  return points.map(([x, y]) => [(x - cx) * cos - (y - cy) * sin, (x - cx) * sin + (y - cy) * cos]);
}

/** Convex hull (Andrew's monotone chain), counter-clockwise. */
function hull(points: XY[]): XY[] {
  const p = [...points].sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  if (p.length < 3) return p;
  const cross = (o: XY, a: XY, b: XY) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]);
  const half = (pts: XY[]) => {
    const out: XY[] = [];
    for (const q of pts) {
      while (out.length >= 2 && cross(out[out.length - 2], out[out.length - 1], q) <= 0) out.pop();
      out.push(q);
    }
    return out.slice(0, -1);
  };
  return [...half(p), ...half([...p].reverse())];
}

/**
 * The network card's figure: every station as a dot (bigger for higher PageRank) and each of the 15 districts that
 * GraphX found drawn as a hand-sketched outline around its stations. The outline skips a district's farthest 15 %
 * of stations, so one far-flung member does not stretch it over its neighbours. Server-rendered SVG: no
 * JavaScript, crisp at any size. Decorative here (the portal has the full, labelled explorer).
 */
export async function DistrictSketch({ className }: { className?: string }) {
  const { data: nodes } = await getNetworkNodes();
  const kx = Math.cos(((nodes.reduce((s, n) => s + n.lat, 0) / nodes.length) * Math.PI) / 180);
  const turned = principalTurn(nodes.map((n) => [n.lon * kx, -n.lat]));
  const xs = turned.map((p) => p[0]);
  const ys = turned.map((p) => p[1]);
  const [x0, y0] = [Math.min(...xs), Math.min(...ys)];
  const scale = (W - 2 * PAD) / (Math.max(...xs) - x0);
  const H = Math.round((Math.max(...ys) - y0) * scale + 2 * PAD);
  const pts: XY[] = turned.map(([x, y]) => [PAD + (x - x0) * scale, PAD + (y - y0) * scale]);

  const maxPr = Math.max(...nodes.map((n) => n.pagerank));
  const tier = (pr: number) => (Math.sqrt(pr / maxPr) < 0.3 ? 0 : Math.sqrt(pr / maxPr) < 0.55 ? 1 : 2);
  const dots = [0, 1, 2].map((t) =>
    pts
      .filter((_, i) => tier(nodes[i].pagerank) === t)
      .map(([x, y]) => `M${Math.round(x)} ${Math.round(y)}h0`)
      .join(""),
  );

  const members = new Map<number, XY[]>(); // community 0 = stations without strong ties, drawn but not outlined
  nodes.forEach((n, i) => {
    if (n.community <= 0) return;
    const own = members.get(n.community);
    if (own) own.push(pts[i]);
    else members.set(n.community, [pts[i]]);
  });
  const districts = [...members.entries()].map(([id, own]) => {
    const c: XY = [own.reduce((s, p) => s + p[0], 0) / own.length, own.reduce((s, p) => s + p[1], 0) / own.length];
    const core = [...own].sort((a, b) => Math.hypot(a[0] - c[0], a[1] - c[1]) - Math.hypot(b[0] - c[0], b[1] - c[1]));
    const outline = hull(core.slice(0, Math.max(3, Math.ceil(core.length * 0.85)))).map(([x, y]): XY => {
      const d = Math.hypot(x - c[0], y - c[1]) || 1;
      return [x + ((x - c[0]) / d) * 12, y + ((y - c[1]) / d) * 12]; // a little air between the dots and the line
    });
    return { id, outline, ink: sketch.polygon(outline, { stroke: "#000", strokeWidth: 2, roughness: 0.7, bowing: 0.5, seed: id * 13, disableMultiStroke: true }) };
  });

  return (
    <svg viewBox={`0 0 ${W} ${H}`} aria-hidden className={className}>
      {districts.map((d) => (
        <path key={`fill${d.id}`} d={`M${d.outline.map((p) => p.map(Math.round).join(" ")).join("L")}Z`} fill="var(--color-marker)" fillOpacity={0.05} />
      ))}
      {dots.map((d, t) => (
        <path key={t} d={d} stroke="var(--color-glow-b)" strokeOpacity={[0.55, 0.8, 1][t]} strokeWidth={[5, 8, 12][t]} strokeLinecap="round" />
      ))}
      {districts.flatMap((d) =>
        d.ink.map((p, i) => <path key={`ink${d.id}-${i}`} d={p.d} fill="none" stroke="var(--color-marker)" strokeOpacity={0.8} strokeWidth={2.2} strokeLinecap="round" />),
      )}
    </svg>
  );
}
