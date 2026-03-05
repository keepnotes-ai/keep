# Discriminator Precompute Spec (Minimal)

Date: 2026-03-05
Status: Draft (Canonical)
Related:
- `later/continuation-decision-support-contract.md`
- `later/continue-wire-contract-v1.md`

## 1) Goal

Keep precompute tiny, generic, and rebuildable.

Precompute only stable priors reused across many queries:
1. `fanout`
2. `selectivity`
3. `cardinality`

Everything query-specific stays runtime-computed per tick.

## 2) Storage Boundary

Two-store boundary:
1. `documents.db`
- canonical notes/tags/edges
- trigger-written `planner_outbox`

2. `planner_stats.db`
- materialized priors only
- safe to wipe/rebuild

Semantics:
- mutation + outbox enqueue are atomic in `documents.db`
- stats materialization is async/eventual
- recompute is idempotent

## 3) Generic Keying

Use logical `scope_key` (project/namespace boundary), not embedding provider IDs.

No duplication across model/provider swaps.

## 4) Update Model

1. triggers enqueue compact deltas to outbox
2. worker drains in bounded batches
3. full rebuild regenerates priors from canonical data

No cross-db distributed transaction.

## 5) Continuation Integration

Continuation reads priors best-effort and publishes:
- `frame.views.discriminators.planner_priors`
- `frame.views.discriminators.staleness`

If stale/missing:
- set `fallback_mode=true`
- continue with query-time signals

## 6) Semantics Policy

Priors are key-agnostic statistics.

They MUST NOT encode semantic meaning of specific tags.
Any behavior difference by key must come from metaschema structure (for example edge/facet class), not hard-coded key names.

## 7) Phase Boundary

In scope now:
- fanout/selectivity/cardinality
- bounded drain + full rebuild

Out of scope now:
- heavyweight bridgeiness metrics
- LLM-generated priors
