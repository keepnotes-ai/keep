# Continuation Decision-Support (Minimal Contract)

Date: 2026-03-04
Status: Draft
Related:
- `later/continuation-api-spec.md`
- `later/discriminator-precompute-spec.md`

## 1) Goal

Define the smallest useful contract for broad->refine guidance:
1. publish decision-support once per tick
2. persist only minimal audit state
3. keep strategy composable and bounded

## 2) Publish Location

Per tick output:
- `frame.views.discriminators`

Minimal durable snapshot:
- `state.frontier.decision_support`

## 3) Frame Payload (Minimal)

```json
{
  "frame": {
    "views": {
      "discriminators": {
        "version": "ds.v1",
        "planner_priors": {
          "fanout": [],
          "selectivity": [],
          "cardinality": []
        },
        "query_stats": {
          "lane_entropy": 0.0,
          "top1_top2_margin": 0.0,
          "pivot_coverage_topk": 0.0,
          "expansion_yield_prev_step": 0.0,
          "cost_per_gain_prev_step": 0.0,
          "temporal_alignment": 0.0
        },
        "policy_hint": {
          "strategy": "single_lane_refine|top2_plus_bridge|explore_more",
          "reason_codes": []
        },
        "staleness": {
          "fallback_mode": false
        }
      }
    }
  }
}
```

Notes:
1. `planner_priors` may be empty.
2. `query_stats` are query-time and always preferred over stale priors.
3. `policy_hint` is advisory, never implicit mutation.

## 4) Durable Snapshot (Minimal)

Persist only:

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

No full discriminator blobs in state.

## 5) Consumption Rules

Kernel strategy selection:
1. high margin + low entropy -> `single_lane_refine`
2. low margin or high entropy -> `top2_plus_bridge`
3. otherwise -> `explore_more`

Thresholds come from profile/system-doc policy, not hard-coded constants.

## 6) Strategy Outputs

1. `single_lane_refine`
- one pivot filter, one refined query step

2. `top2_plus_bridge`
- at most 2 pivot branches plus 1 bridge branch

3. `explore_more`
- one bounded broaden step, then re-evaluate

All strategies obey existing fanout/termination limits.

## 7) Agent Override

Caller may override strategy explicitly:

```json
{
  "decision_override": {
    "strategy": "top2_plus_bridge",
    "reason": "cross-lane question"
  }
}
```

If absent, kernel policy applies.

## 8) Deferred Metrics

Explicitly parked:
1. contradiction/conflict score
2. advanced bridge-likelihood synthesis

These must prove clear lift before inclusion.
