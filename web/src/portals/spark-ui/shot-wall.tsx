"use client";

import Image from "next/image";
import { Dialog } from "radix-ui";
import { useState } from "react";

import { Tape } from "@/components/inkglass/paper";
import { Chip } from "@/components/inkglass/surfaces";

export type Shot = { file: string; title: string; caption: string; look: string; unit: string };

/** Screenshots pinned to the wall with tape; click one to see it full size. */
export function ShotWall({ shots }: { shots: Shot[] }) {
  const [open, setOpen] = useState<Shot | null>(null);
  return (
    <>
      <div className="grid grid-cols-1 gap-x-8 gap-y-12 md:grid-cols-2">
        {shots.map((s, i) => (
          <figure key={s.file} className="relative" style={{ rotate: `${(i % 2 ? 1 : -1) * (0.6 + (i % 3) * 0.35)}deg` }}>
            <Tape className="-top-3 left-8 z-10" tilt={-8 + i * 3} />
            <Tape className="-top-3 right-8 z-10" tilt={6 - i * 2} />
            <button
              type="button"
              onClick={() => setOpen(s)}
              className="block w-full overflow-hidden rounded-lg border-[6px] border-white bg-white shadow-[0_24px_40px_-24px_rgb(0_0_0/0.9)] transition-transform duration-300 hover:scale-[1.015]"
              aria-label={`Enlarge: ${s.title}`}
            >
              <Image
                src={`/spark-ui/${s.file}`}
                alt={`${s.title}. ${s.caption}`}
                width={1440}
                height={960}
                className="h-auto w-full"
                sizes="(min-width: 768px) 600px, 100vw"
              />
            </button>
            <figcaption className="mt-4 px-1">
              <div className="flex items-center gap-2">
                <Chip tone="unit">{s.unit}</Chip>
                <span className="font-display text-lg font-bold text-fg">{s.title}</span>
              </div>
              <p className="mt-1 text-sm leading-relaxed text-fg-2">{s.caption}</p>
              <p className="mt-1 font-hand text-xl leading-snug text-marker">look: {s.look}</p>
            </figcaption>
          </figure>
        ))}
      </div>

      <Dialog.Root open={open !== null} onOpenChange={(o) => !o && setOpen(null)}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-ink-950/85 backdrop-blur-sm" />
          <Dialog.Content
            aria-describedby={undefined}
            className="fixed inset-4 z-[70] flex flex-col items-center justify-center gap-3 outline-none"
            onClick={() => setOpen(null)}
          >
            {open && (
              <>
                <Dialog.Title className="font-display text-xl font-bold text-fg">{open.title}</Dialog.Title>
                <Image
                  src={`/spark-ui/${open.file}`}
                  alt={`${open.title}. ${open.caption}`}
                  width={1440}
                  height={960}
                  className="max-h-[82vh] w-auto rounded-lg border-[6px] border-white object-contain"
                />
                <p className="font-hand text-xl text-marker">click anywhere or press Esc to close</p>
              </>
            )}
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </>
  );
}
