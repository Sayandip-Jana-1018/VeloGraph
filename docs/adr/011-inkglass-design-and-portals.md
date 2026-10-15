# ADR 011 — "Inkglass" design language and evidence portals

- **Status:** accepted, 15 Oct 2026. Replaces the "Nocturne" direction in ARCHITECTURE §11.

## Context

You asked for two things for the web app: every part of the project should be showcased in the UI, through modals
or portals, and the look should combine **maximalism, neumorphism, glassmorphism and hand-drawn** styles.

These have to work alongside constraints that still hold:

- Charts must stay exact (numbers come from `data/serving/*.json`).
- Text must meet WCAG AA contrast, and reduced-motion settings must be respected.
- The site must run offline from static JSON.
- A laptop GPU has to draw a deck.gl map under glass.
- In the viva, the examiner names a unit and the matching evidence should be on screen within two clicks.

## Decision

### 1. Evidence portals

Every module of the project gets a **portal**: a full-screen glass sheet that opens over any page.

- **Routing.** It is built with a Next.js parallel route (`@portal`) and an intercepting route (`(.)portal/[id]`), so
  each portal has its own URL. Clicking opens it as an overlay, refreshing shows it as a full page, and Back closes
  it.
- **Accessibility.** It renders through Radix Dialog, which is a React portal into `document.body`: focus is trapped
  and Esc closes it.
- **Links.** The syllabus page, the slides and the report link straight to portals.

Every portal uses the same layout:

| Part | Content |
|---|---|
| Header | title, syllabus-unit and CO chips, the source file and its `generatedAt`, a "real data" sticker |
| Hero | the one visual that tells the story |
| Acts | 2–4 steps, each with one concept, one exact chart and one handwritten takeaway |
| How Spark did it | the real code excerpt from the repository, with file and line |
| Say it in the viva | a sticky note with 2–3 sentences, taken from `docs/viva/qa-bank.md` |
| Numbers ledger | every figure on the portal, with its JSON path |

Initial catalogue:

| Portal | Phase | Reads |
|---|---|---|
| `lake` — medallion lake, data-quality funnel | P1 | `dq-report.json`, `jobs/*.json` |
| `rdd-lab` — shuffle duel, persistence race, accumulator trap, lineage, RDD → DataFrame | P2 | `rdd-fundamentals.json` |
| `sql` — the 14 insights, each with chart + SQL (one sub-portal per insight) | P2 | `insights/*.json` |
| `spark-ui` — history-server screenshots pinned with tape, hand-drawn callouts | P2 | `docs/assets/spark-ui/` |
| `pregel` — PageRank superstep by superstep on a real hub neighbourhood | P3 | `network-pregel.json` |
| `network` — PageRank map, AM ↔ PM morph, communities, components, triangles, isochrones, "where riders go" | P3 | `network-*.json` |
| `graph-ops` — GraphX operator cards, vertex-cut partitioning comparison | P3 | `network-summary.json` |
| `models` — leaderboards vs baselines, confusion matrix, ablation, archetypes | P4 | `ml-*.json`, `archetypes.json` |
| `streaming` — Kafka topology, broker-failover drill, watermarks | P5 | gateway + `live-replay/` |
| `cluster` — Spark master/workers, brokers, job timeline | P5 | `/api/cluster`, `jobs/*.json` |
| `syllabus` — unit / CO → feature → evidence; every row opens its portal | P6 | all of the above |

### 2. Design language "Inkglass": one job per style

Each of the four styles has its own layer and job. That separation is what makes a maximalist mix read as
intentional rather than noisy.

| Layer | Style | Used for | Rules |
|---|---|---|---|
| **Canvas** | maximalism | page backgrounds, heroes, section openers | deep ink-indigo base, slowly drifting aurora mesh gradient, paper-grain noise, faint hand-drawn street grid; oversized display type, stickers, washi tape, marquee tickers. Dense and layered, but on a strict 12-column grid with one focal point per view |
| **Glass** | glassmorphism | portals, map overlays, floating panels | translucent fill, `backdrop-filter: blur(16–24px) saturate(160%)`, 1 px light edge, inner highlight, soft shadow; at most two glass layers stacked; solid fallback when `backdrop-filter` is unsupported or reduced transparency is requested |
| **Clay** | neumorphism | controls only: toggles, segmented controls (AM/PM), sliders and knobs (superstep scrubber), play/step buttons, KPI pucks | extruded with two soft shadows on a mid-tone clay surface; pressed means inset. Never holds body text; every control keeps a text label and a visible focus ring, because soft shadows alone fail contrast |
| **Ink** | hand-drawn | annotations that explain | rough.js sketchy frames, underlines and hachure highlights; rough-notation circles, underlines and brackets that draw themselves on scroll; perfect-freehand arrows with handwritten notes ("17× less shuffle!") pointing at the key number; sticky notes on paper. Decorative ink is `aria-hidden`, and the same fact always exists as text |

**Data marks stay precise.** ECharts and deck.gl draw the data; ink only annotates it, never replaces it. Sketchy
chart libraries (roughViz, chart.xkcd) were rejected for data marks because they blur values.

**Type.** All fonts are self-hosted through `next/font`, so they work offline.

| Use | Font |
|---|---|
| Display | Bricolage Grotesque (variable) |
| UI and body | Inter |
| Annotations | Caveat |
| Code and tabular numbers | JetBrains Mono |

**Colour.** Colours are CSS-variable tokens only:

- surfaces: ink canvas, clay surface, paper for sticky notes;
- one accent per meaning: supply, empty risk, full risk, graph, healthy;
- chalk-white ink for annotations.

The palette is validated with the `dataviz` skill (contrast and colour-blind safety) before any chart is written.

**Motion.**

- Springs of 200–400 ms.
- A portal opens with a shared-element zoom from the card that was clicked.
- Ink draws itself in (stroke-dashoffset).
- Map animation pauses while a portal is open.
- With `prefers-reduced-motion`, states change instantly and nothing draws.

**New libraries for P6.1**, pinned at install: `roughjs`, `rough-notation`, `perfect-freehand`.

## Consequences

**Gains**

- Every module can be demonstrated from one place, and the viva can follow unit → portal.
- The batch jobs already write portal-ready exports. For example, P3 writes `network-pregel.json` for the
  step-through and `network-summary.json` for the operator cards.

**Costs and how they are handled**

- **More web work.** Mitigated by one shared portal layout and component kit. The portals for P1–P3 can be built
  before P4/P5 land. If time runs short, the 14 SQL sub-portals collapse into one gallery (ROADMAP §6).
- **Blur cost.** Blur over a moving map is expensive. Glass over maps uses a lower blur radius, and map animation
  pauses under an open portal.
- **Accessibility risks of neumorphism and glass.** Handled by the rules above, and checked in P6.8 (contrast,
  Lighthouse, reduced motion and transparency).

## Alternatives considered

- **Keep "Nocturne"** (minimal, dark). Rejected because you asked for maximalism.
- **Neumorphism everywhere.** Rejected because of poor contrast for text and data.
- **Hand-drawn charts.** Rejected because values become imprecise; hand-drawn style is used for annotation only.
- **Plain modals without URLs.** Rejected because the slides, the report and the syllabus table could not link to
  them.
