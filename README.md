# VeloGraph

Real-time bike-share intelligence on Apache Spark: forecast next-hour demand at every Citi Bike station in New York,
fuse it with the live GBFS feed to flag stations about to run empty or full, and explain the network with GraphX.

INT315 Cluster Computing · CA project · 2026

> Work in progress. The full quick start arrives with roadmap task P7.3.

| Document | What it covers |
|---|---|
| [docs/ROADMAP.md](docs/ROADMAP.md) | Phases, tasks, acceptance criteria and schedule |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design, data model, jobs, streaming, UI |
| [CLAUDE.md](CLAUDE.md) | Working agreement and conventions for this repository |

Data: Citi Bike trip data and GBFS feeds © NYC Bike Share, LLC, used under its Data License Agreement. Weather data by
Open-Meteo.com (CC BY 4.0). This is an independent student project, not affiliated with Citi Bike or Lyft.
