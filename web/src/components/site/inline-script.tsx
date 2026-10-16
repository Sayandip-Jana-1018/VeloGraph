/**
 * A script that runs while the HTML is parsed, before the first paint (Next.js "preventing flash" guide). On the
 * client it is inert text, so React neither re-runs it nor warns about a script inside a component.
 */
export function InlineScript({ html }: { html: string }) {
  return (
    <script
      type={typeof window === "undefined" ? "text/javascript" : "text/plain"}
      suppressHydrationWarning
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}
