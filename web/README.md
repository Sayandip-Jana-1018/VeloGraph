# VeloGraph · web

The evidence site. It has one portal per module of the project, and every portal reads the batch jobs' exports
(`data/serving/*.json`). The design language is "Inkglass", described in [ADR 011](../docs/adr/011-inkglass-design-and-portals.md)
and [ADR 012](../docs/adr/012-inkglass-presets-frame-budget-spark-ui.md).

## Run

```powershell
pnpm install
pnpm sync    # data/serving -> public/data, plus Spark UI screenshots, MapLibre's worker and the quoted code snippets
pnpm dev     # http://localhost:3000
```

## Check

```powershell
pnpm typecheck
pnpm lint
pnpm build
cd ../python; uv run python -m velograph.ops.scroll_bench   # the scroll frame budget, with a server running
```

## Layout

| Path | What lives there |
|---|---|
| `src/app/` | routes: `/` (home), `portal/[id]` (a portal as a page) and `@portal/(.)portal/[id]` (the same portal as an overlay) |
| `src/portals/` | one folder per portal, plus `registry.ts` (titles, units, sources) and `body.tsx` (id → portal) |
| `src/components/inkglass/` | the design kit: backdrop, glass surfaces, clay controls, ink (rough.js, rough-notation) |
| `src/components/portal/` | portal sheet, overlay, acts, code panel, viva note, numbers ledger |
| `src/components/charts/` | ECharts on SVG, the validated chart palette, chart/table cards |
| `src/components/site/` | header and footer, colour presets, the city of lights, the district sketch |
| `src/lib/` | zod contracts (one per export), cached server loaders, number formatting |
