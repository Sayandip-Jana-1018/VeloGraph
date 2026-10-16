"use client";

import { useEffect, useRef, useState } from "react";
import { annotate } from "rough-notation";
import type { RoughAnnotationType } from "rough-notation/lib/model";

import { cn } from "@/lib/cn";

import { sketch, type InkPath } from "./rough";

function prefersReducedMotion(): boolean {
  return typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

/**
 * Draws a hand-drawn rectangle exactly around its children, re-sketched when they resize. The seed keeps the
 * wobble stable between renders, so the frame does not "boil".
 */
export function SketchFrame({
  children,
  className,
  color = "var(--color-chalk)",
  seed = 1,
  inset = 4,
  roughness = 1.3,
}: {
  children: React.ReactNode;
  className?: string;
  color?: string;
  seed?: number;
  inset?: number;
  roughness?: number;
}) {
  const box = useRef<HTMLDivElement>(null);
  const [paths, setPaths] = useState<{ w: number; h: number; ink: InkPath[] } | null>(null);

  useEffect(() => {
    const el = box.current;
    if (!el) return;
    const draw = () => {
      const { width: w, height: h } = el.getBoundingClientRect();
      if (w < 8 || h < 8) return;
      setPaths({
        w,
        h,
        ink: sketch.rect(inset, inset, w - inset * 2, h - inset * 2, {
          stroke: "#000",
          strokeWidth: 1.6,
          roughness,
          bowing: 0.8,
          seed,
        }),
      });
    };
    draw();
    const observer = new ResizeObserver(draw);
    observer.observe(el);
    return () => observer.disconnect();
  }, [inset, roughness, seed]);

  return (
    <div ref={box} className={cn("relative", className)}>
      {paths && (
        <svg
          aria-hidden
          className="pointer-events-none absolute inset-0 overflow-visible"
          width={paths.w}
          height={paths.h}
          viewBox={`0 0 ${paths.w} ${paths.h}`}
        >
          {paths.ink.map((p, i) => (
            <path key={i} d={p.d} fill="none" stroke={color} strokeWidth={1.6} strokeLinecap="round" />
          ))}
        </svg>
      )}
      {children}
    </div>
  );
}

/**
 * A rough-notation annotation (circle, underline, highlight, box, bracket) that draws itself when it scrolls into
 * view. The annotated text stays ordinary text; only the stroke is decoration.
 *
 * rough-notation places its SVG at the corner of the nearest positioned (or transformed) ancestor and measures
 * the stroke from there. Inside a Reveal that ancestor is the animating wrapper until its transform is cleared,
 * after which the SVG jumped to the page's corner and left the stroke floating above the hero. The `relative`
 * span pins the SVG to the annotated text itself.
 */
export function Notate({
  children,
  type,
  color = "var(--color-marker)",
  strokeWidth = 2,
  padding = 4,
  iterations = 2,
  multiline = true,
  delay = 0,
  className,
}: {
  children: React.ReactNode;
  type: RoughAnnotationType;
  color?: string;
  strokeWidth?: number;
  padding?: number;
  iterations?: number;
  multiline?: boolean;
  delay?: number;
  className?: string;
}) {
  const span = useRef<HTMLSpanElement>(null);

  useEffect(() => {
    const el = span.current;
    if (!el) return;
    // rough-notation writes the colour into SVG attributes, so CSS variables are resolved here first.
    const resolved = color.startsWith("var(")
      ? getComputedStyle(el).getPropertyValue(color.slice(4, -1)).trim() || "#ffe066"
      : color;
    const annotation = annotate(el, {
      type,
      color: type === "highlight" ? `${resolved}55` : resolved,
      strokeWidth,
      padding,
      iterations,
      multiline,
      animate: !prefersReducedMotion(),
      animationDuration: 900,
    });
    let timer: ReturnType<typeof setTimeout> | undefined;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          // Measure only after the web fonts have swapped in, or the stroke lands where the fallback text was.
          void document.fonts.ready.then(() => {
            timer = setTimeout(() => annotation.show(), delay);
          });
          observer.disconnect();
        }
      },
      { threshold: 0.6 },
    );
    observer.observe(el);
    return () => {
      observer.disconnect();
      if (timer) clearTimeout(timer);
      annotation.remove();
    };
  }, [color, delay, iterations, multiline, padding, strokeWidth, type]);

  return (
    <span className="relative">
      <span ref={span} className={className}>
        {children}
      </span>
    </span>
  );
}
