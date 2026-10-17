"use client";

import { useEffect, useRef } from "react";

/**
 * Lights the stop whose chapter crosses the middle of the screen. An IntersectionObserver reports the crossing, so
 * nothing runs on scroll itself, and the change is a few attributes plus one `scale`, which the compositor animates.
 * Markup: chapters carry `data-stop="<i>"`, stops `data-node="<i>"`, the travelled track `data-progress`.
 */
export function StoryScroller({ className, children }: { className?: string; children: React.ReactNode }) {
  const root = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = root.current;
    if (!el) return;
    const nodes = [...el.querySelectorAll<HTMLElement>("[data-node]")];
    const track = el.querySelector<HTMLElement>("[data-progress]");
    const light = (current: number) => {
      nodes.forEach((node, i) => {
        node.toggleAttribute("data-current", i === current);
        node.toggleAttribute("data-passed", i < current);
        const link = node.querySelector("a");
        if (i === current) link?.setAttribute("aria-current", "step");
        else link?.removeAttribute("aria-current");
      });
      if (track) track.style.scale = `1 ${current / Math.max(1, nodes.length - 1)}`;
    };
    // A thin band just above the middle of the viewport: the chapter in it is the current stop.
    const observer = new IntersectionObserver(
      (entries) => {
        for (const e of entries) if (e.isIntersecting) light(Number((e.target as HTMLElement).dataset.stop));
      },
      { rootMargin: "-45% 0px -50% 0px" },
    );
    el.querySelectorAll("[data-stop]").forEach((chapter) => observer.observe(chapter));
    return () => observer.disconnect();
  }, []);

  return (
    <div ref={root} className={className}>
      {children}
    </div>
  );
}
