# ADR 015 — Home: one real evening on the city canvas, the pipeline as a subway line, a static station index

- **Status:** accepted, 17 Oct 2026. Amends ARCHITECTURE §11, whose home page planned a deck.gl TripsLayer.

## Context

P6.4 asks for three things on the home page:
- a night-time replay of one real evening's trips;
- a scroll story of the pipeline whose chapters open the portals;
- a ⌘K station search.

The approved two-column hero (16 Oct) already draws the "city of lights" on a 2D canvas inside the ADR 012 frame
budget. The site must also work offline and with the gateway down.

## Decision

### 1. The replay runs on the existing canvas, not on deck.gl's TripsLayer

`HeroTripsJob` exports one evening, Thursday 17 September 2026, 16:30–19:30 New York time
(`velograph.hero` in `application.conf`), as `serving/hero-trips.json`:
- **Trips:** 53,887 trips with a start and an end station, round trips left out because they would not move.
- **Sample:** one in five, chosen by `xxhash64(ride_id)`, so a re-run picks the same 10,694 trips.
- **Stations:** the 1,929 that those trips use.
- **Counter:** rides under way in each minute, counted from every trip, not just the sample. The peak is 4,779 at
  17:46.

The canvas replays it at 120×, so the evening loops every 90 s. Each trip is a comet along a gently bent line
between its two stations, leaving at its real start time. A clock and the counter are written into the corner.

Why not TripsLayer:
- **WebGL:** it would add a second WebGL context to the landing page.
- **Weight:** about a megabyte more JavaScript before the first paint.
- **Frame budget:** it still has to fit. The canvas draws only the trips under way, from typed arrays, capped at
  30 fps. Scrolling the home page costs 6.6 ms a frame at p95.

### 2. The pipeline as a subway line

A section between the KPI band and the portal grid tells the pipeline in eight stops: lake, RDDs, Spark SQL,
GraphX, MLlib, streaming, the cluster and the syllabus.
- **The line:** on wide screens it stays in view while the chapters scroll.
- **Current stop:** an IntersectionObserver lights the stop whose chapter crosses the middle of the screen, so
  nothing runs on scroll. The travelled part of the track is a `scale`, which the compositor animates.
- **Chapters:** each holds the numbers that stop produced, read from the exports, and buttons to its portals.

This is the demo's "architecture in one breath" beat.

### 3. The station search reads a static index

`pnpm sync` derives `public/data/station-index.json` from `network-nodes.json` and `archetypes.json`: 2,490
stations, 207 KB, against 1.7 MB for the network file.

The dialog loads the index the first time it opens. Every word of the query must match a station's name. Names
whose words start with the query rank first, then by PageRank.

A station's details come from the exports:
- docks, PageRank rank, district, archetype, trips out and in, and the AM/PM shift;
- its live state, from the gateway when it is up and from the recorded snapshot when it is not.

No server is involved, so the search works offline.

## Consequences

- ARCHITECTURE §10 and §11 describe the canvas replay, the pipeline story and the station index. The planned
  `ServingExportJob` and `overview.json` are dropped: every job writes its own exports, and the home page reads
  them directly.
- The replay is labelled honestly: "a real trip, 1 in 5, at 120× speed". The counter counts every trip.
- With reduced motion the hero shows the busiest minute as a still. If the export cannot be loaded, the hero falls
  back to comets on the 500 busiest routes.
