"use client";

import Link from "next/link";
import { useEffect } from "react";

import { Sticker } from "@/components/inkglass/paper";

/**
 * The error state of every page. The data is static and validated at build time, so what can fail here is the
 * browser side: a chart, a map or a live view. The header stays, so the rest of the site remains one click away.
 */
export default function PageError({ error, retry }: { error: unknown; retry: () => void }) {
  useEffect(() => {
    console.error(error);
  }, [error]);
  const digest = error instanceof Error && "digest" in error ? String(error.digest) : null;

  return (
    <div role="alert" className="mx-auto flex max-w-2xl flex-col items-center px-5 pt-24 pb-8 text-center sm:pt-32">
      <Sticker accent="amber" tilt={3}>
        something broke
      </Sticker>
      <h1 className="mt-8 font-display text-[clamp(2.8rem,7vw,5rem)] leading-[0.92] font-extrabold tracking-[-0.03em] text-fg">
        This view hit a snag.
      </h1>
      <p className="mt-6 max-w-md text-lg leading-relaxed text-pretty text-fg-2">
        The page could not finish drawing in your browser. The numbers behind it are safe: trying again usually
        clears it.
      </p>
      <div className="mt-9 flex flex-wrap justify-center gap-3">
        <button type="button" onClick={retry} className="clay rounded-full px-5 py-2.5 text-sm font-semibold text-marker hover:text-fg">
          Try again
        </button>
        <Link href="/" className="clay rounded-full px-5 py-2.5 text-sm font-semibold text-fg hover:text-marker">
          Home
        </Link>
      </div>
      {digest && <p className="mt-8 font-mono text-xs text-fg-3">reference {digest}</p>}
    </div>
  );
}
