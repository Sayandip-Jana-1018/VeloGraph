/**
 * `<html data-portal-open>` is set while a portal overlay is open. The page underneath is hidden then, so CSS
 * pauses its animations (globals.css) and canvas loops pause through `watchPortal`.
 */
export const PORTAL_OPEN_ATTR = "data-portal-open";

/** Calls `onChange` now and whenever an overlay opens or closes; returns the unsubscribe function. */
export function watchPortal(onChange: (open: boolean) => void): () => void {
  const root = document.documentElement;
  const observer = new MutationObserver(() => onChange(root.hasAttribute(PORTAL_OPEN_ATTR)));
  observer.observe(root, { attributes: true, attributeFilter: [PORTAL_OPEN_ATTR] });
  onChange(root.hasAttribute(PORTAL_OPEN_ATTR));
  return () => observer.disconnect();
}
