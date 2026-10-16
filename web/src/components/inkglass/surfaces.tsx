import { cn } from "@/lib/cn";

import { CountUp, type CountFormat } from "./motion";

/** Glass panel. `sheet` is the denser portal surface, `inset` the chart and code surface inside it. */
export function Glass({
  as: Tag = "div",
  variant = "panel",
  className,
  children,
  ...rest
}: {
  as?: "div" | "section" | "article" | "aside" | "header";
  variant?: "panel" | "sheet" | "inset";
  className?: string;
  children: React.ReactNode;
} & React.HTMLAttributes<HTMLElement>) {
  const surface = { panel: "glass rounded-[var(--radius-card)]", sheet: "glass-sheet rounded-[var(--radius-sheet)]", inset: "glass-inset rounded-2xl" }[variant];
  return (
    <Tag className={cn(surface, className)} {...rest}>
      {children}
    </Tag>
  );
}

/**
 * A neumorphic stat tile: the label names it, the value is the hero (proportional figures, UI sans). Give `count`
 * to have the number count up when it scrolls into view.
 */
export function KpiPuck({
  label,
  value,
  count,
  note,
  className,
}: {
  label: string;
  value?: string;
  count?: { to: number; format?: CountFormat; prefix?: string; suffix?: string };
  note?: string;
  className?: string;
}) {
  return (
    <div className={cn("clay flex min-w-[10.5rem] flex-col items-center gap-1.5 rounded-[1.75rem] px-6 py-5 text-center", className)}>
      <span className="text-[0.68rem] font-semibold tracking-[0.16em] text-fg-3 uppercase">{label}</span>
      <span className="text-[2rem] leading-none font-semibold text-fg">{count ? <CountUp {...count} /> : value}</span>
      {note && <span className="max-w-[12rem] text-xs leading-snug text-fg-2">{note}</span>}
    </div>
  );
}

/** A slow ticker of facts; pauses on hover and stands still for reduced motion. */
export function Marquee({ items, className }: { items: string[]; className?: string }) {
  const row = items.map((item, i) => (
    <span key={i} className="flex items-center gap-6 pr-6">
      <span className="font-display text-lg font-bold whitespace-nowrap text-fg">{item}</span>
      <span aria-hidden className="text-marker">
        ✦
      </span>
    </span>
  ));
  return (
    <div className={cn("marquee overflow-hidden", className)}>
      <div className="marquee-track flex w-max">
        <div className="flex">{row}</div>
        <div aria-hidden className="flex">
          {row}
        </div>
      </div>
    </div>
  );
}

/** Unit / CO / phase chip. */
export function Chip({
  children,
  tone = "default",
  className,
}: {
  children: React.ReactNode;
  tone?: "default" | "unit" | "co";
  className?: string;
}) {
  const tones = {
    default: "border-white/15 text-fg-2",
    unit: "border-teal/50 text-teal",
    co: "border-violet/60 text-violet",
  };
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full border px-2.5 py-0.5 text-[0.7rem] font-semibold tracking-wide whitespace-nowrap",
        tones[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}
