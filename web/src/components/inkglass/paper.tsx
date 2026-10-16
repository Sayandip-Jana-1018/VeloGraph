import { cn } from "@/lib/cn";

const ACCENTS = {
  teal: "bg-teal text-ink-950",
  coral: "bg-coral text-ink-950",
  amber: "bg-amber text-ink-950",
  violet: "bg-violet text-ink-950",
  mint: "bg-mint text-ink-950",
  pink: "bg-pink text-ink-950",
  marker: "bg-marker text-ink-950",
} as const;
export type Accent = keyof typeof ACCENTS;

/** A die-cut sticker: thick white edge, slight tilt, loud colour. Chrome only, never data. */
export function Sticker({
  children,
  accent = "marker",
  tilt = -4,
  className,
}: {
  children: React.ReactNode;
  accent?: Accent;
  tilt?: number;
  className?: string;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border-[3px] border-white px-3.5 py-1 font-display text-[0.78rem] font-extrabold tracking-wide uppercase shadow-[0_6px_14px_-6px_rgb(0_0_0/0.7)]",
        ACCENTS[accent],
        className,
      )}
      style={{ rotate: `${tilt}deg` }}
    >
      {children}
    </span>
  );
}

/** A strip of washi tape, e.g. holding a screenshot to the wall. */
export function Tape({ className, tilt = -6 }: { className?: string; tilt?: number }) {
  return (
    <span
      aria-hidden
      className={cn("absolute h-6 w-24 bg-amber/70 shadow-sm backdrop-blur-[1px]", className)}
      style={{
        rotate: `${tilt}deg`,
        clipPath:
          "polygon(2% 10%, 8% 0, 14% 12%, 22% 2%, 30% 10%, 40% 0, 50% 10%, 60% 2%, 70% 12%, 80% 0, 90% 10%, 98% 2%, 100% 90%, 92% 100%, 84% 88%, 74% 98%, 64% 90%, 54% 100%, 44% 88%, 34% 98%, 24% 90%, 14% 100%, 6% 90%, 0 98%)",
      }}
    />
  );
}

/** A sticky note on paper, in handwriting: where the viva lines and margin notes live. */
export function StickyNote({
  title,
  children,
  tilt = -1.5,
  className,
}: {
  title?: string;
  children: React.ReactNode;
  tilt?: number;
  className?: string;
}) {
  return (
    <aside
      className={cn(
        "relative rounded-[4px] bg-paper px-6 pt-7 pb-6 text-paper-ink shadow-[0_18px_30px_-18px_rgb(0_0_0/0.85),0_2px_0_rgb(0_0_0/0.15)]",
        className,
      )}
      style={{ rotate: `${tilt}deg` }}
    >
      <Tape className="-top-3 left-1/2 -translate-x-1/2" tilt={tilt * 2} />
      {title && <p className="mb-2 font-display text-xs font-extrabold tracking-[0.18em] uppercase opacity-70">{title}</p>}
      <div className="font-hand text-[1.32rem] leading-snug">{children}</div>
    </aside>
  );
}
