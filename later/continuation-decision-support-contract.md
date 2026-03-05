# Continuation Decision-Support Contract

Date: 2026-03-05
Status: Draft (Canonical Addendum)
Related:
- `later/continue-wire-contract-v1.md`
- `later/discriminator-precompute-spec.md`

## 1) Goal

Publish the smallest actionable decision payload per tick, with bounded strategy behavior:
1. no task semantics hard-coded by tag key name
2. structural and statistical signals only
3. explicit strategy output for continuation consumers

## 2) Publish/Persist Locations

Per tick output:
- `frame.views.discriminators`

Minimal durable snapshot:
- `state.frontier.decision_support`

## 3) Frame Payload (Current)

```json
{
  "frame": {
    "views": {
      "discriminators": {
        "version": "ds.v1",
        "planner_priors": {
          "fanout": {},
          "selectivity": {},
          "cardinality": {}
        },
        "query_stats": {
          "lane_entropy": 0.0,
          "top1_top2_margin": 0.0,
          "pivot_coverage_topk": 0.0,
          "expansion_yield_prev_step": 0.0,
          "cost_per_gain_prev_step": 0.0,
          "temporal_alignment": 0.0
        },
        "lineage": {
          "version": {
            "coverage_topk": 0.0,
            "dominant_concentration_topk": 0.0,
            "dominant": "",
            "distinct_topk": 0
          },
          "part": {
            "coverage_topk": 0.0,
            "dominant_concentration_topk": 0.0,
            "dominant": "",
            "distinct_topk": 0
          }
        },
        "tag_profile": {
          "edge_key_count": 0,
          "facet_key_count": 0,
          "edge_keys": [],
          "facet_keys": []
        },
        "policy_hint": {
          "strategy": "single_lane_refine|top2_plus_bridge|explore_more",
          "reason_codes": []
        },
        "staleness": {
          "stats_age_s": null,
          "fallback_mode": false
        }
      }
    }
  }
}
```

## 4) Durable Snapshot (Current)

Persist only decision outcome summary, not full signal blob:

```json
{
  "state": {
    "frontier": {
      "decision_support": {
        "version": "ds.v1",
        "strategy_chosen": "single_lane_refine",
        "reason_codes": [],
        "pivot_ids": []
      }
    }
  }
}
```

## 5) Strategy Selection Rules

Base rules:
1. high margin + low entropy -> `single_lane_refine`
2. low margin or high entropy -> `top2_plus_bridge`
3. otherwise -> `explore_more`

Structural override:
- strong lineage concentration (`version` or `part`) may promote `single_lane_refine` when entropy is not high.

Thresholds are policy-driven (`decision_policy`), with runtime defaults as fallback.

## 6) Query.Auto Consumption

- `single_lane_refine`:
  - build one refine request (`where + slice`, deep=true) from dominant fact.

- `top2_plus_bridge`:
  - create bounded branch plan in state:
    - up to two pivot branches
    - one bridge branch
  - execute branches over subsequent ticks
  - compute branch utility
  - select best branch and schedule final refine frame

- `explore_more`:
  - bounded broaden/deepen pass, then re-evaluate.

## 7) Tag Semantics Rule

Decision support MUST treat tag keys generically.

Allowed classification only:
- edge vs facet from metaschema (`.tag/<key>` has `_inverse` => edge)

Not allowed:
- hard-coding meaning for specific keys (for example `speaker`, `informs`, benchmark-only keys).

## 8) Caller Override

Caller may set `decision_override.strategy` for one tick.
Override does not bypass bounds/termination rules.

## 9) Deferred Signals

Keep out unless they show measurable lift:
- contradiction/conflict synthesis
- heavyweight bridgeiness inference
- large model-generated discriminator fields
