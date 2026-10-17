// Number and date formatting for the whole site: one locale, so a figure reads the same everywhere.
const LOCALE = "en-US";

const intFmt = new Intl.NumberFormat(LOCALE, { maximumFractionDigits: 0 });
const compactFmt = new Intl.NumberFormat(LOCALE, { notation: "compact", maximumFractionDigits: 1 });

export const int = (n: number): string => intFmt.format(n);

/** 20,528,598 -> "20.5M"; 1,284 -> "1.3K". */
export const compact = (n: number): string => compactFmt.format(n);

export const fixed = (n: number, digits = 1): string =>
  new Intl.NumberFormat(LOCALE, { minimumFractionDigits: digits, maximumFractionDigits: digits }).format(n);

/** A share already in percent (0.379 means 0.379 %). */
export const pct = (n: number, digits = 1): string => `${fixed(n, digits)} %`;

/** A ratio in [0, 1] shown as a percentage. */
export const share = (ratio: number, digits = 0): string => pct(ratio * 100, digits);

/** Signed percent change, e.g. -69.0 -> "−69 %". */
export const delta = (n: number, digits = 0): string =>
  `${n > 0 ? "+" : n < 0 ? "−" : "±"}${fixed(Math.abs(n), digits)} %`;

export function bytes(n: number): string {
  if (n === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log10(n) / 3));
  return `${fixed(n / 1000 ** i, i === 0 ? 0 : 1)} ${units[i]}`;
}

export function duration(ms: number): string {
  if (ms < 1000) return `${int(ms)} ms`;
  if (ms < 60_000) return `${fixed(ms / 1000, 1)} s`;
  return `${fixed(ms / 60_000, 1)} min`;
}

/** "2026-10-15T16:01:44Z" -> "15 Oct 2026, 16:01 UTC" (exports are stamped in UTC). */
export function stamp(iso: string): string {
  const d = new Date(iso);
  const date = d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
  const time = d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", timeZone: "UTC" });
  return `${date}, ${time} UTC`;
}

/** Escapes text placed into HTML strings (chart tooltips): station names contain "&". */
export function escapeHtml(text: string): string {
  return text.replace(/[&<>"']/g, (c) => `&#${c.codePointAt(0)};`);
}
