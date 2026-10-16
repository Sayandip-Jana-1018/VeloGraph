/**
 * The stage behind every page: a top spotlight in the preset's main colour, three slowly drifting light orbs, a fine
 * grid that fades out from the top, turning light beams, twinkling star dust, film grain and a vignette. Fixed and
 * decorative, so it never scrolls or takes focus; it stands still for reduced motion.
 */
export function Canvas() {
  return (
    <div aria-hidden className="stage pointer-events-none">
      <div className="beams" />
      <div className="orb orb-a" />
      <div className="orb orb-b" />
      <div className="orb orb-c" />
      <div className="grid-veil" />
      <div className="dust" />
      <div className="grain" />
      <div className="vignette" />
    </div>
  );
}
