# ADR 009 — Size the Docker cluster for a 6 GB VM

- **Status:** accepted, 15 Oct 2026
- **Supersedes:** the ≈ 7 GB budget in the original ARCHITECTURE §3

## Context

The plan assumed Docker Desktop's WSL2 default of half the host RAM (≈ 7.8 GB). On this machine
`%USERPROFILE%\.wslconfig` sets `memory=6GB` and `swap=8GB`, and `docker info` reports 5.8 GB. Changing that file
is a machine-wide setting (it also restarts WSL), so it stays the user's decision.

## Decision

Fit the cluster into 6 GB instead of asking for more:

- Kafka: three nodes with 256 MB heaps (`mem_limit` 640 MB each); Kafka UI heap ≤ 224 MB.
- Spark: daemons at 256 MB; one 768 MB executor (2 cores) per worker; client-mode drivers at 640 MB.
- Health checks are port/HTTP probes, not CLI tools that start an extra JVM per check.
- Heavy batch work (bronze/silver ETL over ~4 GB of CSV) stays on the host in `local[20]`, as planned.

## Consequences

- The cluster fits with margin at idle; executors are small, so cluster runs use the compact gold tables.
- If the cluster runs tight during the demo, raising `memory=` to 8 GB in `.wslconfig` is a one-line fix — but only
  with the user's approval.
