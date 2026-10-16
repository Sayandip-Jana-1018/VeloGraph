import type { MlFeatures } from "@/lib/schemas";

const W = 1000;
const LABEL = 250;
const ROW = 44;
const TOP = 34;

const day = (iso: string) => Date.parse(`${iso}T00:00:00Z`) / 86_400_000;
const short = (iso: string) => new Date(`${iso}T00:00:00Z`).toLocaleDateString("en-GB", { day: "numeric", month: "short", timeZone: "UTC" });

/**
 * The leakage-safe layout of ARCHITECTURE §8.2 on one date axis: every split's rows, and the profile window its
 * features come from, which always ends before the rows begin. Server-rendered SVG with text labels on the bars,
 * so no colour has to be decoded.
 */
export function WindowsTimeline({ data }: { data: MlFeatures["data"] }) {
  const fit = data.windows.find((w) => w.split === "fit")!;
  const test = data.windows.find((w) => w.split === "test")!;
  const trainEnd = new Date(Date.parse(`${data.validation.from}T00:00:00Z`) - 86_400_000).toISOString().slice(0, 10);
  const rows = [
    { label: "fit features: profile A", from: fit.profile.from, to: fit.profile.to, color: "var(--color-series-3)" },
    { label: "fit rows: training", from: fit.rows.from, to: trainEnd, color: "var(--color-series-1)" },
    { label: "fit rows: validation week", from: data.validation.from, to: data.validation.to, color: "var(--color-series-4)" },
    { label: "test features: profile B", from: test.profile.from, to: test.profile.to, color: "var(--color-series-3)" },
    { label: "test rows: September", from: test.rows.from, to: test.rows.to, color: "var(--color-series-2)" },
  ];
  const start = day(`${fit.profile.from.slice(0, 7)}-01`);
  const end = day(test.rows.to) + 1;
  const x = (iso: string, inclusiveEnd = false) => LABEL + ((day(iso) + (inclusiveEnd ? 1 : 0) - start) / (end - start)) * (W - LABEL - 10);
  const months = ["06", "07", "08", "09"].map((m) => `${fit.profile.from.slice(0, 4)}-${m}-01`);
  const height = TOP + rows.length * ROW + 10;

  return (
    <div className="glass-inset overflow-x-auto rounded-2xl p-4">
      <svg viewBox={`0 0 ${W} ${height}`} role="img" aria-label="Feature windows and the rows they feed" className="block min-w-[44rem]">
        {months.map((m) => (
          <g key={m}>
            <line x1={x(m)} x2={x(m)} y1={TOP - 8} y2={height} stroke="var(--color-fg-3)" strokeOpacity={0.25} />
            <text x={x(m) + 4} y={TOP - 14} fontSize={12} fill="var(--color-fg-3)">
              {new Date(`${m}T00:00:00Z`).toLocaleDateString("en-GB", { month: "long", timeZone: "UTC" })}
            </text>
          </g>
        ))}
        {rows.map((r, i) => {
          const y = TOP + i * ROW;
          const [x0, x1] = [x(r.from), x(r.to, true)];
          return (
            <g key={r.label}>
              <text x={LABEL - 14} y={y + 21} textAnchor="end" fontSize={13} fontWeight={600} fill="var(--color-fg-2)">
                {r.label}
              </text>
              <rect x={x0} y={y + 6} width={x1 - x0} height={24} rx={8} fill={r.color} fillOpacity={0.85} />
              {x1 - x0 >= 120 ? (
                <text x={(x0 + x1) / 2} y={y + 22} textAnchor="middle" fontSize={11} fontWeight={700} fill="var(--color-ink-950)">
                  {short(r.from)} – {short(r.to)}
                </text>
              ) : (
                // too narrow for its dates: the label sits beside the bar, on the side with room
                <text
                  x={x1 + 8 + 110 <= W ? x1 + 8 : x0 - 8}
                  y={y + 22}
                  textAnchor={x1 + 8 + 110 <= W ? "start" : "end"}
                  fontSize={11}
                  fontWeight={700}
                  fill="var(--color-fg-2)"
                >
                  {short(r.from)} – {short(r.to)}
                </text>
              )}
            </g>
          );
        })}
        {[fit, test].map((w) => (
          <line
            key={w.split}
            x1={x(w.rows.from)}
            x2={x(w.rows.from)}
            y1={TOP - 4}
            y2={height - 4}
            stroke="var(--color-marker)"
            strokeWidth={1.5}
            strokeDasharray="4 4"
          />
        ))}
      </svg>
      <p className="mt-2 text-center font-hand text-lg text-marker">
        the dashed lines: every feature window ends before the rows it feeds begin
      </p>
    </div>
  );
}
