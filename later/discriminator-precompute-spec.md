# Discriminator Precompute (Minimal Spec)

Date: 2026-03-04
Status: Draft
Related:
- `later/continuation-api-spec.md`
- `later/continuation-decision-support-contract.md`

## 1) Goal

Keep precompute tiny and generic:
1. provide stable planner priors
2. keep continuation runtime simple
3. stay domain-agnostic

Rule:
- precompute only what is reused across many queries
- compute everything query-specific at tick time

## 2) Precomputed Priors (Only)

1. `fanout`
- expected expansion size for a candidate facet/edge.

2. `selectivity`
- fraction of nodes where that facet/edge is present and useful.

3. `cardinality`
- distinct-value and top-value counts for facets.

4. `bridgeiness` (optional in phase 1)
- lightweight cross-partition connectivity score.

No answer-level scoring, no query embeddings, no LLM outputs in precompute.

## 3) Storage and Semantics

Use two stores with a strict boundary:
1. `documents.db`
   - canonical notes/tags/edges
   - `planner_outbox` (trigger-written)
2. `planner_stats.db`
   - materialized priors only (rebuildable)

Semantics:
1. mutation + outbox enqueue are atomic (same `documents.db` transaction)
2. stats materialization is async/eventual
3. recompute is idempotent

## 4) Update Model

1. Triggers write compact outbox deltas on relevant table changes.
2. Worker drains outbox in bounded batches and upserts priors.
3. Full rebuild command can regenerate all priors from canonical data.

No long transactions, no cross-db distributed commit.

## 5) Generic Keying

Use logical scope keys, not embedding-provider collections.

`scope_key` identifies the logical dataset boundary (local project / hosted org+project / optional namespace).

This avoids duplicated priors across embedding model changes.

## 6) Continuation Integration

Continuation reads priors best-effort and publishes them under:
- `frame.views.discriminators.planner_priors`

If priors are missing/stale:
1. set fallback flag
2. continue with query-time stats only

No write dependency from continuation to stats maintenance.

## 7) Phase Plan

Phase 1:
1. outbox + drain loop
2. `fanout`, `selectivity`, `cardinality`

Phase 2:
1. optional `bridgeiness`
2. tuning thresholds based on observed gain

Design constraint:
- if a metric does not change strategy quality measurably, remove it.
