# Simplified Model and Continuation Design

Date: 2026-03-05
Status: Canonical
Related:
- `later/continue-wire-contract-v1.md`
- `later/continuation-decision-support-contract.md`

## 1) Core Model

Memory is:
- nodes
- facts about nodes

Everything is a node flavor:
- note
- version node
- part node
- tag doc
- prompt doc
- meta doc

Tags are facts. Some facts are directional edges when metaschema says so.

## 2) Three Relationship Channels

1. Structural lineage (built-in)
- version-string lineage
- part-string lineage
- treated in parallel by decision signals

2. Edge tags (metaschema-defined)
- directional links from `_inverse` on `.tag/<key>`
- semantics are user/domain-defined, not hard-coded

3. Non-edge tags (facets)
- grouping/scoping dimensions
- no directionality

## 3) Projection Semantics

Current frame request:
- seed: `id|query|similar_to`
- pipeline: `where|slice`
- options: `deep`, metadata level

Projection returns evidence rows plus metadata required for continuation decisions.

## 4) Decision Signals

Published per tick:
- planner priors (`fanout/selectivity/cardinality`)
- query-time statistics
- lineage signals (`version` and `part`, identical schema)
- tag profile (`edge` vs `facet` key sets)

These produce a policy hint strategy:
- `single_lane_refine`
- `top2_plus_bridge`
- `explore_more`

## 5) Strategy Consumption

`query.auto` consumes policy hints in bounded steps:
- single-lane: one refine frame
- top2+bridge: bounded branch plan then select
- explore: broaden then re-evaluate

No unbounded evaluator, no free-form code execution.

## 6) Design Rules

1. no key-name semantics in continuation logic
2. edge-vs-facet classification comes only from metaschema (`_inverse`)
3. lineage channels are structural and first-class
4. all steps are bounded by budget and termination rules
