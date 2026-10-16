"use client";

import { AnimatePresence, motion } from "motion/react";
import { Popover } from "radix-ui";
import { useState, useSyncExternalStore } from "react";

import { cn } from "@/lib/cn";

import { DEFAULT_THEME, THEME_EVENT, THEME_KEY, THEMES, type ThemeId } from "./themes";

// The active preset is data-theme on <html> (set before hydration by the boot script, then by this switcher).
const onThemeChange = (notify: () => void) => {
  window.addEventListener(THEME_EVENT, notify);
  return () => window.removeEventListener(THEME_EVENT, notify);
};
const readTheme = (): ThemeId => {
  const id = document.documentElement.getAttribute("data-theme");
  return THEMES.find((t) => t.id === id)?.id ?? DEFAULT_THEME;
};

/**
 * Picks a colour preset for the whole site: it sets data-theme on <html> (every token follows), remembers the
 * choice, and tells canvas-drawn views to repaint with the new colours.
 */
export function ThemeSwitcher() {
  const theme = useSyncExternalStore(onThemeChange, readTheme, () => DEFAULT_THEME);
  const [open, setOpen] = useState(false);

  const choose = (id: ThemeId) => {
    document.documentElement.setAttribute("data-theme", id);
    try {
      localStorage.setItem(THEME_KEY, id);
    } catch {
      // Private windows may refuse storage; the preset still applies for this visit.
    }
    window.dispatchEvent(new CustomEvent(THEME_EVENT, { detail: id }));
  };

  const current = THEMES.find((t) => t.id === theme)!;
  return (
    <Popover.Root open={open} onOpenChange={setOpen}>
      <Popover.Trigger
        className="clay flex items-center gap-2 rounded-full py-2 pr-4 pl-2.5 text-sm font-semibold text-fg hover:text-marker"
        aria-label={`Colour theme: ${current.name}`}
      >
        <span className="flex -space-x-1.5">
          {current.swatch.map((c) => (
            <span key={c} className="size-4 rounded-full ring-2 ring-ink-900" style={{ background: c }} />
          ))}
        </span>
        {current.name}
      </Popover.Trigger>
      <AnimatePresence>
        {open && (
          <Popover.Portal forceMount>
            <Popover.Content asChild sideOffset={10} align="end" forceMount>
              <motion.div
                initial={{ opacity: 0, y: -8, scale: 0.96 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: -8, scale: 0.96 }}
                transition={{ type: "spring", stiffness: 380, damping: 30 }}
                className="glass z-[80] w-72 rounded-3xl p-3"
              >
                <p className="px-2 pt-1 pb-2 text-xs font-semibold tracking-[0.16em] text-fg-3 uppercase">Colour theme</p>
                <div className="grid grid-cols-2 gap-2">
                  {THEMES.map((t) => (
                    <button
                      key={t.id}
                      type="button"
                      onClick={() => choose(t.id)}
                      aria-pressed={t.id === theme}
                      className={cn(
                        "flex flex-col items-center gap-2 rounded-2xl px-3 py-3 text-sm font-semibold transition-colors",
                        t.id === theme ? "clay-pressed text-marker" : "clay text-fg-2 hover:text-fg",
                      )}
                    >
                      <span className="flex -space-x-2">
                        {t.swatch.map((c) => (
                          <span key={c} className="size-6 rounded-full ring-2 ring-ink-900" style={{ background: c }} />
                        ))}
                      </span>
                      {t.name}
                    </button>
                  ))}
                </div>
              </motion.div>
            </Popover.Content>
          </Popover.Portal>
        )}
      </AnimatePresence>
    </Popover.Root>
  );
}
