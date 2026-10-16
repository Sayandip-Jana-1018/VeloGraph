"use client";

import { animate, motion, useInView, useReducedMotion } from "motion/react";
import { useEffect, useRef, useState } from "react";

import { cn } from "@/lib/cn";
import { compact, fixed, int } from "@/lib/format";

/**
 * Fades and lifts its children into place the first time they scroll into view. Only opacity and transform
 * animate, so the browser runs it on the compositor (a blur filter here made every reveal repaint).
 */
export function Reveal({
  children,
  delay = 0,
  className,
}: {
  children: React.ReactNode;
  delay?: number;
  className?: string;
}) {
  const reduce = useReducedMotion();
  return (
    <motion.div
      className={className}
      initial={reduce ? false : { opacity: 0, y: 24 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true, amount: 0.12 }}
      transition={{ duration: 0.6, delay, ease: [0.22, 1, 0.36, 1] }}
    >
      {children}
    </motion.div>
  );
}

export type CountFormat = "int" | "compact" | "fixed1" | "fixed2" | "fixed3";
const FORMAT: Record<CountFormat, (n: number) => string> = {
  int: (n) => int(Math.round(n)),
  compact,
  fixed1: (n) => fixed(n, 1),
  fixed2: (n) => fixed(n, 2),
  fixed3: (n) => fixed(n, 3),
};

/** Counts up to `to` once it is visible; the final value is the real one (and what screen readers get). */
export function CountUp({ to, format = "int", prefix = "", suffix = "" }: { to: number; format?: CountFormat; prefix?: string; suffix?: string }) {
  const ref = useRef<HTMLSpanElement>(null);
  const inView = useInView(ref, { once: true, amount: 0.6 });
  const reduce = useReducedMotion();
  const [shown, setShown] = useState(reduce ? to : 0);

  useEffect(() => {
    if (!inView || reduce) return;
    const controls = animate(0, to, { duration: 1.6, ease: [0.16, 1, 0.3, 1], onUpdate: setShown });
    return () => controls.stop();
  }, [inView, reduce, to]);

  const text = `${prefix}${FORMAT[format](to)}${suffix}`;
  return (
    <span ref={ref} aria-label={text}>
      <span aria-hidden>
        {prefix}
        {FORMAT[format](shown)}
        {suffix}
      </span>
    </span>
  );
}

/**
 * A card whose highlight follows the pointer (a soft spotlight in the preset's highlight colour). Pointer events
 * can outpace the display, so the position is written at most once per frame.
 */
export function SpotlightCard({ children, className }: { children: React.ReactNode; className?: string }) {
  const ref = useRef<HTMLDivElement>(null);
  const frame = useRef(0);
  useEffect(() => () => cancelAnimationFrame(frame.current), []);
  const onMove = (e: React.PointerEvent) => {
    const { clientX, clientY } = e;
    cancelAnimationFrame(frame.current);
    frame.current = requestAnimationFrame(() => {
      const el = ref.current;
      if (!el) return;
      const r = el.getBoundingClientRect();
      el.style.setProperty("--mx", `${clientX - r.left}px`);
      el.style.setProperty("--my", `${clientY - r.top}px`);
    });
  };
  return (
    <div ref={ref} onPointerMove={onMove} className={cn("spotlight", className)}>
      {children}
    </div>
  );
}
