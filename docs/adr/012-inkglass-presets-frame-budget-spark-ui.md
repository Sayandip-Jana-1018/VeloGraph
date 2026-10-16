# ADR 012 — Inkglass in practice: colour presets, a frame budget, Spark's dashboards from its REST API

- **Status:** accepted, 16 Oct 2026. Amends [ADR 011](011-inkglass-design-and-portals.md) (the glass rule and the
  `spark-ui` portal).

## Context

Your design feedback on the first build of the web app (16 Oct) asked for:

- the messy hand-drawn hub map on the home page to be replaced by something clear and eye-catching;
- content centred everywhere, except two-column bands, which may align left and right;
- a more premium background, plus colour presets that the whole site follows;
- the charts the real Spark UI shows, rebuilt on our pages, not just screenshots of them.

Your second round of feedback asked for:

- the two-column hero back, with the text on the left and a map on the right;
- a crisp map instead of a hazy one, and no cards overlapping text;
- an end to the lag when scrolling the portals.

## Decision

### 1. Six colour presets

- **Presets:** Nebula (default), Aurora, Ember, Rose, Midnight and Noir.
- **Mechanism:** a preset is `data-theme` on `<html>`, and every surface, text, glow and accent token is
  redefined per preset in `globals.css`.
- **Cross-fade:** the main colours are `@property`-registered, so switching presets cross-fades the whole site
  instead of snapping.
- **No flash:** a tiny inline boot script applies the stored choice before the first paint.
- **Data marks:** they keep the validated dataviz palette on every preset. Presets recolour chrome, never data.

### 2. Glass without live blur: a frame budget

ADR 011 gave glass panels `backdrop-filter: blur(16–24px)`. That cost more than it showed. The stage behind the
panels is soft gradients, so blurring it changed little on screen, but every panel was re-blurred on every frame of
scrolling and of the backdrop's animations.

`velograph.ops.scroll_bench` measures this. It scrolls one page step per animation frame in headless Edge, where
there is no vsync, so the interval between frames is the real cost of a frame. Portals are opened as overlays.

| View | Mean frame before → after | Frames > 33 ms before → after | Live backdrop blurs before → after |
|---|---|---|---|
| Home | 25.5 → 6.2 ms | 14.6 % → 0 % | 9 → 1 |
| `rdd-lab` portal | 26.6 → 6.9 ms | 19.0 % → 0.9 % | 21 → 3 |
| `sql` portal | 29.9 → 7.5 ms | 36.4 % → 0 % | 12 → 2 |
| `network` portal | 25.7 → 6.3 ms | 13.2 % → 0 % | 12 → 2 |

The rules now are:

- **Glass is translucency plus a lit edge.** Only the sticky header, which has page content sliding under it, keeps a
  real blur (`glass-frost`). The portal overlay dims the page without blurring it.
- **The backdrop animates only compositor properties.** The light orbs are radial gradients moved by `transform`.
  No blur filters or blend modes sit on moving layers, and the grain has no blend mode.
- **Reveals animate only opacity and transform.** No `filter: blur()`.
- **Hidden animation stops.** While a portal is open, `<html data-portal-open>` pauses the page's CSS animations,
  and the home map's canvas loop pauses through `watchPortal`. Canvas and WebGL loops run only while on screen.
  Only the comet layer of the network map is rebuilt per frame.
- **Work is deferred until needed.** Charts are created when they come within a screen of the viewport. Portal acts
  far off screen skip layout and paint (`content-visibility: auto`).

### 3. Spark's own dashboards, redrawn from its REST API

`velograph.ops.spark_ui_export` reads the history server's `/api/v1`: applications, jobs, stages (with task-time
quantiles), executors, and SQL executions with their plan graph. It writes `serving/spark-ui.json` (one envelope,
like every export).

The `spark-ui` portal redraws, from that file:

- the application list;
- the RDD lab's job timeline;
- each shuffle experiment as a stage DAG, labelled with the bytes that crossed the shuffle;
- task-time box plots;
- Catalyst's physical plan, with WholeStageCodegen clusters and exchanges;
- one job per Pregel superstep.

The screenshots stay below them as the original pages.

**Why:** a screenshot cannot be hovered, read as a table or recoloured, and the REST API is the same data the Spark
UI draws.

### 4. Home and layout

- **Hero:** two columns, with the claim on the left and a north-up "city of lights" canvas on the right. The canvas
  shows:
  - stations as crisp dots sized by PageRank;
  - the 40 top hubs in the highlight colour;
  - the 500 busiest routes as arcs, with comets riding them.

  It replaces the hand-drawn hub sketch.
- **Network card:** a server-rendered SVG of the 15 GraphX districts. Each district is outlined around its core
  stations: its farthest 15 % are left out, so a single outlier does not stretch the outline.
- **Alignment:** content is centred, and two-column bands align their columns left and right.

## Consequences

- The site keeps a full frame well under 16.7 ms while scrolling. Any new effect has to stay inside that budget, and
  `scroll_bench` is the check.
- Glass looks slightly less "frosted" over the backdrop's grid lines. That was accepted for smoothness.
- `spark-ui.json` must be re-exported when the pipeline is re-run. The history server has to be up for the export,
  but the site itself never needs it.
