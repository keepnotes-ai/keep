# Discriminator Precompute Spec (Planner Priors for Continuation)

Date: 2026-03-04
Status: Draft (Proposed Canonical Addendum)
Related:
- `later/continuation-api-spec.md`
- `later/continue-wire-contract-v1.md`
- `later/continuation-machine-architecture.md`

## 1) Objective

Define how continuation runtime gets high-leverage planner signals for broad->refine query strategy:
1. edge/tag expansion fanout + selectivity priors
2. pivot quality priors
3. ambiguity discriminators for refine policy

Design rule:
- use a hybrid model:
  - precompute stable priors
  - compute query-specific discriminators on demand per tick

## 2) Scope and Non-Goals

In scope (v1):
1. precomputed priors for planning (`fanout`, `selectivity`, `bridgeiness`, `facet cardinality`)
2. incremental maintenance model
3. runtime shape for `frame.views.discriminators`

Out of scope (v1):
1. hard-coded domain tags (for example `conv`, `speaker`) in kernel logic
2. `eval`-like expression runtime
3. replacing query-time scoring entirely with precompute

## 3) What Should Be Precomputed

## 3.1 Metric Families

1. `expansion.fanout`
- expected fanout when following a relation/facet edge in a scope.
- planner usage: bound deep expansion cost.

2. `expansion.selectivity`
- fraction of source nodes where relation/facet exists and yields at least one target.
- planner usage: estimate likelihood of useful expansion.

3. `facet.cardinality`
- count distribution for facet key/value pairs in scope.
- planner usage: estimate filter narrowing power.

4. `node.bridgeiness`
- node-level score indicating cross-facet / cross-cluster connectivity.
- planner usage: identify bridge pivots for multi-hop refine.

## 3.2 What Stays Query-Time

1. query-to-pivot lexical/semantic alignment
2. ambiguity metrics on current evidence (lane entropy, top1-top2 margin)
3. final refine policy choice (`single-lane`, `top2+bridge`, `explore-more`)

## 4) Generic (Data-Driven) Definitions

No hard-coded tag or relation names in code.

Definitions live in system docs:
- `.planner/discriminator/*`

Each definition declares:
1. extractor: how candidates are derived (tag facets, inverse edges, ids, parts, versions)
2. scope dimensions: (global, collection, optional facet partitions)
3. metric families to materialize
4. refresh/update policy

Example (conceptual):

```yaml
id: .planner/discriminator/default
kind: planner_discriminator_profile
extractors:
  - id: tag_facets
    source: node.tags
    include_system: false
  - id: inverse_edges
    source: edge_index
metrics:
  - family: expansion.fanout
  - family: expansion.selectivity
  - family: facet.cardinality
  - family: node.bridgeiness
scopes:
  - collection
  - collection+facet_partition
refresh:
  mode: incremental
  full_rebuild_interval: P7D
```

## 5) Persistence Model

Use a dedicated local DB (`planner_stats.db`) to avoid coupling with flow state tables.

## 5.1 Tables

1. `planner_stat`
- `stat_id` TEXT PK (hash of normalized key)
- `metric_family` TEXT
- `scope_key` TEXT (opaque normalized scope id)
- `subject_key` TEXT (relation/facet descriptor)
- `value_json` TEXT (aggregates; schema by metric family)
- `sample_n` INTEGER
- `updated_at` TEXT
- index: `(metric_family, scope_key, subject_key)`

2. `planner_node_stat`
- `node_id` TEXT
- `scope_key` TEXT
- `metric_family` TEXT (`node.bridgeiness`, optional others)
- `value` REAL
- `updated_at` TEXT
- PK: `(node_id, scope_key, metric_family)`

3. `planner_pending`
- append-only delta tasks from mutations
- `pending_id`, `mutation_ref`, `payload_json`, `created_at`, `attempt`

4. `planner_watermark`
- per-source progress for rebuild/incremental maintenance
- `stream`, `offset`, `updated_at`

## 5.2 Value JSON Conventions

Examples:
1. `expansion.fanout`:
```json
{
  "mean": 3.7,
  "p50": 2.0,
  "p90": 9.0,
  "max": 41
}
```

2. `expansion.selectivity`:
```json
{
  "selectivity": 0.42,
  "sources_total": 12031,
  "sources_with_hits": 5053
}
```

3. `facet.cardinality`:
```json
{
  "distinct_values": 38,
  "top_values": [["v1", 412], ["v2", 288], ["v3", 211]],
  "entropy": 2.31
}
```

## 6) Update Semantics

## 6.1 Incremental Path (Default)

On each committed memory mutation:
1. write memory mutation atomically as today
2. enqueue minimal delta into `planner_pending` in same transaction
3. return request (no planner recompute on foreground path)

Background planner worker:
1. consume `planner_pending` in bounded batches
2. update affected `planner_stat` / `planner_node_stat`
3. commit per batch (small atomic increments)

No long-running transaction context across ticks.

## 6.2 Rebuild Path

1. full rebuild command scans memory store and recomputes all metric families
2. swap-in by watermark/version marker
3. old snapshot retained briefly for rollback

## 7) Runtime Integration (Continuation)

`_frame_evidence_for_request` remains query executor.
Planner priors are attached as discriminators in frame views:

```json
"frame": {
  "views": {
    "evidence": [...],
    "discriminators": {
      "planner_priors": {
        "candidate_pivots": [...],
        "expansion_estimates": [...],
        "ambiguity": {...}
      },
      "staleness": {
        "stats_age_s": 14,
        "fallback_mode": false
      }
    }
  }
}
```

Kernel decision policy:
1. use precomputed priors to rank candidate refine plans
2. compute query-time ambiguity metrics
3. if ambiguity above threshold, branch refine (`top2+bridge`) instead of single-lane refine
4. escalate to LLM discriminator only when planner confidence remains low

## 8) Candidate Pivot Scoring (Planner Formula)

No hard-coded facet names; pivot candidates come from active extractors.

For candidate pivot `p`:

```text
score(p) =
  w1 * query_alignment(p)
  + w2 * selectivity_gain(p)
  - w3 * expected_cost(p)
  + w4 * bridgeiness(p)
```

where:
1. `selectivity_gain` and `expected_cost` come from precomputed priors
2. `query_alignment` is query-time
3. `bridgeiness` comes from `planner_node_stat`

Weights can be profile-configured in `.planner/discriminator/*`.

## 9) Guardrails and Degradation

1. If stats are stale/unavailable, runtime falls back to query-time-only planning.
2. Hard bounds:
   - max candidate pivots per tick
   - max branch fanout for refine
   - max deep expansions per tick
3. If precompute worker is behind, do not block foreground queries.

## 10) Observability

Emit per-tick planner telemetry:
1. precompute hit/miss rate
2. estimated vs observed fanout
3. chosen refine policy and branch count
4. latency split: query execution vs discriminator assembly

Use this to tune:
1. metric families retained
2. extractor definitions
3. thresholds for LLM escalation

## 11) Phased Implementation

Phase 1:
1. `planner_stats.db` tables + incremental queue
2. precompute `expansion.fanout`, `expansion.selectivity`, `facet.cardinality`
3. expose `frame.views.discriminators.planner_priors` (read-only)

Phase 2:
1. add `node.bridgeiness`
2. add branch refine policy selection in continuation kernel
3. add ambiguity-triggered LLM discriminator handoff

Phase 3:
1. move metric/extractor definitions fully into mutable system docs
2. hosted deployment parity (same contract, different worker ownership)

## 12) Decision Summary

Not premature:
- precomputing the first planner priors is high-leverage and low risk.

Premature:
- precomputing query-specific discriminators or answer selection.

Canonical split:
1. precompute stable priors
2. compute contextual discriminators per query tick
3. escalate to LLM discriminator only when needed
