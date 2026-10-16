import { cacheLife } from "next/cache";
import { codeToHtml } from "shiki";

import { InkRing } from "@/components/inkglass/ink";
import { StickyNote } from "@/components/inkglass/paper";
import { Reveal } from "@/components/inkglass/motion";
import { Glass } from "@/components/inkglass/surfaces";
import snippets from "@/generated/snippets.json";
import { cn } from "@/lib/cn";

/**
 * One step of a portal's story: a numbered ring, a centred title and lede, then the evidence, revealed on scroll.
 * Acts far off screen skip rendering (cv-auto), which keeps long portals light to scroll.
 */
export function Act({
  n,
  title,
  lede,
  children,
  className,
}: {
  n: number;
  title: string;
  lede?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <section className={cn("cv-auto space-y-9", className)}>
      <Reveal className="mx-auto flex max-w-3xl flex-col items-center text-center">
        <InkRing size={56} seed={n * 7} className="font-display text-xl font-extrabold text-marker">
          {n}
        </InkRing>
        <h2 className="mt-4 font-display text-3xl font-bold tracking-tight text-balance text-fg sm:text-4xl">{title}</h2>
        {lede && <p className="mt-3 text-base leading-relaxed text-pretty text-fg-2">{lede}</p>}
      </Reveal>
      <Reveal delay={0.08} className="space-y-6">
        {children}
      </Reveal>
    </section>
  );
}

export type SnippetId = keyof typeof snippets;

/** Syntax highlighting, done once at build time (shiki reads the clock, so it must be cached to prerender). */
async function highlight(code: string): Promise<string> {
  "use cache";
  cacheLife("max");
  return codeToHtml(code, { lang: "scala", theme: "tokyo-night" });
}

/** "How Spark did it": a real excerpt of the Scala code, extracted at sync time, highlighted on the server. */
export async function CodePanel({ id, caption }: { id: SnippetId; caption: string }) {
  const snippet = snippets[id];
  const html = await highlight(snippet.code);
  return (
    <Glass variant="inset" className="code-panel overflow-hidden">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-white/8 px-5 py-3">
        <p className="font-display text-sm font-bold text-fg">
          <span className="mr-2 font-hand text-lg font-normal text-marker">How Spark did it</span>
          {caption}
        </p>
        <p className="font-mono text-xs text-fg-3">
          {snippet.file.replace("spark/src/main/scala/", "")}:{snippet.startLine}–{snippet.endLine}
        </p>
      </div>
      <div
        style={{ "--first-line": snippet.startLine - 1 } as React.CSSProperties}
        dangerouslySetInnerHTML={{ __html: html }}
      />
    </Glass>
  );
}

/** "Say it in the viva": two or three plain sentences to say out loud, from docs/viva/qa-bank.md. */
export function VivaNote({ children, tilt = -1.2, className }: { children: React.ReactNode; tilt?: number; className?: string }) {
  return (
    <StickyNote title="Say it in the viva" tilt={tilt} className={cn("max-w-md", className)}>
      {children}
    </StickyNote>
  );
}

/** The numbers on a portal, each with the export field it was read from. */
export function Ledger({ rows }: { rows: { label: string; value: string; path: string }[] }) {
  return (
    <Glass variant="inset" className="overflow-hidden">
      <p className="border-b border-white/8 px-5 py-4 text-center font-display text-sm font-bold text-fg">
        Numbers ledger <span className="ml-2 font-sans text-xs font-normal text-fg-3">every figure above, and where it comes from</span>
      </p>
      <table className="w-full text-left text-sm">
        <tbody>
          {rows.map((r) => (
            <tr key={r.path} className="border-b border-white/5 last:border-0">
              <td className="px-5 py-2 text-fg-2">{r.label}</td>
              <td className="px-5 py-2 text-right font-semibold text-fg tabular-nums">{r.value}</td>
              <td className="hidden px-5 py-2 font-mono text-xs text-fg-3 md:table-cell">{r.path}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Glass>
  );
}

/** A two-column band: evidence on the left, the sticky note (or any aside) on the right. */
export function WithNote({ children, note }: { children: React.ReactNode; note: React.ReactNode }) {
  return (
    <div className="grid items-start gap-8 lg:grid-cols-[minmax(0,1fr)_22rem]">
      <div className="min-w-0">{children}</div>
      <div className="lg:pt-6">{note}</div>
    </div>
  );
}
