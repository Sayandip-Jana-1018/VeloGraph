import { cn } from "@/lib/cn";

import { arrowPaths, sketch } from "./rough";

/**
 * Server-side ink: shapes with a known size, drawn once with a fixed seed. Everything here is decorative
 * (aria-hidden); the fact it points at always exists as text too.
 */

type Point = [number, number];

export function InkArrow({
  width,
  height,
  from,
  to,
  bend = 0.25,
  color = "var(--color-chalk)",
  className,
}: {
  width: number;
  height: number;
  from: Point;
  to: Point;
  bend?: number;
  color?: string;
  className?: string;
}) {
  return (
    <svg aria-hidden width={width} height={height} viewBox={`0 0 ${width} ${height}`} className={className}>
      {arrowPaths(from, to, bend).map((d, i) => (
        <path key={i} d={d} fill={color} />
      ))}
    </svg>
  );
}

/** A sketchy underline of a fixed width, to sit under a headline word. */
export function InkUnderline({
  width,
  color = "var(--color-marker)",
  seed = 3,
  className,
}: {
  width: number;
  color?: string;
  seed?: number;
  className?: string;
}) {
  const paths = sketch.curve(
    [
      [4, 12],
      [width * 0.3, 7],
      [width * 0.62, 11],
      [width - 4, 5],
    ],
    { stroke: "#000", strokeWidth: 3.2, roughness: 1.4, bowing: 1, seed },
  );
  return (
    <svg aria-hidden width={width} height={18} viewBox={`0 0 ${width} 18`} className={className}>
      {paths.map((p, i) => (
        <path key={i} d={p.d} fill="none" stroke={color} strokeWidth={3.2} strokeLinecap="round" />
      ))}
    </svg>
  );
}

/** A hand-drawn ring, e.g. around a step number. */
export function InkRing({
  size,
  color = "var(--color-chalk)",
  seed = 5,
  className,
  children,
}: {
  size: number;
  color?: string;
  seed?: number;
  className?: string;
  children?: React.ReactNode;
}) {
  const paths = sketch.ellipse(size / 2, size / 2, size - 6, size - 8, {
    stroke: "#000",
    strokeWidth: 1.8,
    roughness: 1.6,
    seed,
  });
  return (
    <span className={cn("relative inline-grid place-items-center", className)} style={{ width: size, height: size }}>
      <svg aria-hidden className="absolute inset-0" viewBox={`0 0 ${size} ${size}`}>
        {paths.map((p, i) => (
          <path key={i} d={p.d} fill="none" stroke={color} strokeWidth={1.8} />
        ))}
      </svg>
      <span className="relative">{children}</span>
    </span>
  );
}

/** Small decorative doodles for the maximalist layer. */
export function Doodle({
  kind,
  size = 28,
  color = "var(--color-marker)",
  seed = 9,
  className,
}: {
  kind: "star" | "spark" | "squiggle" | "dot";
  size?: number;
  color?: string;
  seed?: number;
  className?: string;
}) {
  const o = { stroke: "#000", strokeWidth: 1.8, roughness: 1.3, seed };
  const c = size / 2;
  const paths =
    kind === "star"
      ? sketch.polygon(
          Array.from({ length: 10 }, (_, i): Point => {
            const r = i % 2 === 0 ? c - 2 : c * 0.42;
            const a = (Math.PI / 5) * i - Math.PI / 2;
            return [c + r * Math.cos(a), c + r * Math.sin(a)];
          }),
          o,
        )
      : kind === "spark"
        ? [
            ...sketch.line(c, 2, c, size - 2, o),
            ...sketch.line(2, c, size - 2, c, o),
            ...sketch.line(c * 0.45, c * 0.45, c * 1.55, c * 1.55, o),
          ]
        : kind === "squiggle"
          ? sketch.curve(
              Array.from({ length: 7 }, (_, i): Point => [2 + (i * (size - 4)) / 6, c + (i % 2 ? -c * 0.55 : c * 0.55)]),
              o,
            )
          : sketch.ellipse(c, c, size * 0.5, size * 0.5, { ...o, fill: "#000", fillStyle: "solid" });
  return (
    <svg aria-hidden width={size} height={size} viewBox={`0 0 ${size} ${size}`} className={className}>
      {paths.map((p, i) => (
        <path key={i} d={p.d} fill={p.fill ? color : "none"} stroke={color} strokeWidth={1.8} strokeLinecap="round" />
      ))}
    </svg>
  );
}
