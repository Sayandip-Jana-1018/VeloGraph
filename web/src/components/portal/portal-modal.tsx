"use client";

import { motion, useReducedMotion } from "motion/react";
import { useRouter } from "next/navigation";
import { Dialog } from "radix-ui";
import { useEffect } from "react";

import { PORTAL_OPEN_ATTR } from "./portal-state";

/**
 * The overlay version of a portal (the intercepted `/portal/<id>` route). Radix renders it through a React
 * portal into document.body, traps focus and closes on Escape; closing goes back in history, so the URL and
 * the Back button always agree. The overlay dims the page without blurring it: a full-screen backdrop blur had
 * to be recomputed on every scrolled frame.
 */
export function PortalModal({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const reduce = useReducedMotion();

  useEffect(() => {
    const root = document.documentElement;
    root.setAttribute(PORTAL_OPEN_ATTR, "");
    return () => root.removeAttribute(PORTAL_OPEN_ATTR);
  }, []);

  return (
    <Dialog.Root open onOpenChange={(open) => !open && router.back()}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-40 bg-ink-950/85" />
        <Dialog.Content
          aria-describedby={undefined}
          className="fixed inset-x-0 top-0 bottom-0 z-50 flex items-start justify-center overflow-y-auto px-3 py-4 outline-none sm:px-6 sm:py-6"
          // The content layer spans the viewport; a click on its empty margin (not on the sheet) closes the portal.
          onClick={(e) => e.target === e.currentTarget && router.back()}
        >
          {/* The portal's own h1 names it; this title only satisfies the dialog's accessible name while it streams. */}
          <Dialog.Title className="sr-only">Evidence portal</Dialog.Title>
          <motion.div
            className="h-fit w-full max-w-[1280px]"
            initial={reduce ? false : { opacity: 0, y: 28, scale: 0.97 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            transition={{ type: "spring", stiffness: 260, damping: 28 }}
          >
            {children}
          </motion.div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

/** Close control inside the modal; on the full page the sheet links home instead. */
export function PortalCloseButton() {
  const router = useRouter();
  return (
    <button
      type="button"
      onClick={() => router.back()}
      aria-label="Close portal"
      className="clay grid size-11 place-items-center rounded-full text-lg text-fg transition-colors hover:text-marker"
    >
      ✕
    </button>
  );
}
