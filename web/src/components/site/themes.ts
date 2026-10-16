/** Colour presets (data-theme on <html>); the CSS for each lives in globals.css. */
export const THEMES = [
  { id: "nebula", name: "Nebula", swatch: ["#7c5cff", "#14b8a6", "#ffe066"] },
  { id: "aurora", name: "Aurora", swatch: ["#10b981", "#22d3ee", "#7af5d6"] },
  { id: "ember", name: "Ember", swatch: ["#ff6a3d", "#ff3d6e", "#ffc061"] },
  { id: "rose", name: "Rosé", swatch: ["#ff4fa3", "#b56cff", "#ffb3d4"] },
  { id: "midnight", name: "Midnight", swatch: ["#3b82f6", "#6366f1", "#7dd3fc"] },
  { id: "noir", name: "Noir", swatch: ["#c9a96e", "#6d6d7d", "#e8c27a"] },
] as const;

export type ThemeId = (typeof THEMES)[number]["id"];
export const DEFAULT_THEME: ThemeId = "nebula";
export const THEME_KEY = "velograph-theme";
export const THEME_EVENT = "velograph:theme";

/** Runs in <head> before the first paint, so a saved preset never flashes the default first. */
export const THEME_BOOT_SCRIPT = `(function(){try{var t=localStorage.getItem("${THEME_KEY}");if(t)document.documentElement.setAttribute("data-theme",t)}catch(e){}})()`;
