# ADR 010 — Kafka topic names use hyphens, never underscores

- **Status:** accepted, 15 Oct 2026

## Context

Creating `velograph.gbfs.station_status.v1` made Kafka warn: *"topics with a period ('.') or underscore ('_') could
collide"*. Kafka metric names replace `.` with `_`, so `a.b_c` and `a_b.c` would report into the same metrics.

## Decision

Topic names keep the `velograph.<domain>.<entity>.v<N>` shape with `.` between segments and kebab-case inside
them: `velograph.gbfs.station-status.v1`, `velograph.trip-metrics.v1`. The two topics created with underscores
were deleted (they were empty) and re-created.

## Consequences

No metric collisions are possible, and the names stay readable. CLAUDE.md, ARCHITECTURE §9.2 and
`infra/kafka/create-topics.sh` all use the new names.
