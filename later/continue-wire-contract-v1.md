# Continue Wire Contract v1 (Normative)

Date: 2026-03-05
Status: Draft (Normative, Local Scope)
Related:
- `later/simplified-model-and-continuation-design.md`
- `later/continuation-decision-support-contract.md`
- `later/continuation-machine-architecture.md`

## 1) Purpose

Define one local runtime boundary:

```text
continue(input) -> output
```

The contract supports:
- read/explore ticks
- write/mutation ticks
- multi-tick continuation with durable state
- optional work delegation and reconcile

## 2) Normative Rules

1. New flow input MUST provide either:
- one or more top-level flow fields: `goal|profile|steps|frame_request`.
2. `flow_id + state_version` govern optimistic concurrency.
3. Mutations MUST use typed ops (`upsert_item|set_tags|set_summary`) only.
4. Runtime scope here is local single-tenant; hosted auth/rbac is out of scope.

## 3) ContinueInput (Normative Shape)

```json
{
  "request_id": "uuid",
  "idempotency_key": "optional-stable-retry-key",
  "flow_id": "optional-existing-flow",
  "state_version": 0,

  "goal": "optional",
  "profile": "optional",
  "params": {},
  "frame_request": {
    "seed": {"mode": "query|id|similar_to", "value": "..."},
    "pipeline": [
      {"op": "where", "args": {}},
      {"op": "slice", "args": {"limit": 10}}
    ],
    "budget": {"tokens": 2400, "max_nodes": 100},
    "options": {"deep": false, "metadata": "none|basic|rich"}
  },
  "steps": [],
  "decision_policy": {},
  "decision_override": {
    "strategy": "single_lane_refine|top2_plus_bridge|explore_more",
    "reason": "optional"
  },
  "feedback": {
    "work_results": []
  }
}
```

## 4) ContinueOutput (Normative Shape)

```json
{
  "request_id": "echo",
  "idempotency_key": "echo",
  "flow_id": "flow-id",
  "state_version": 12,
  "status": "waiting_work|paused|done|failed",

  "frame": {
    "slots": {},
    "views": {
      "task": "",
      "evidence": [],
      "hygiene": [],
      "discriminators": {}
    },
    "budget_used": {"tokens": 0, "nodes": 0},
    "status": "done"
  },

  "requests": {"work": []},
  "applied": {"work_ops": []},
  "state": {
    "cursor": {"step": 0, "stage": "tick", "phase": "tick"},
    "frontier": {},
    "pending": {"work_ids": []},
    "budget_used": {"tokens": 0, "nodes": 0},
    "termination": {"idle_ticks": 0, "max_idle_ticks": 3}
  },
  "next": {"recommended": "continue|stop", "reason": "..."},
  "errors": []
}
```

## 5) Frame Operators (Current v1)

Allowed operators today:
- `where`
- `slice`

Unsupported operators MUST return `invalid_frame_operator`.

## 6) State/Frontier Fields (Current v1)

Common fields:
- `frontier.evidence_ids`
- `frontier.decision_support`

`query.auto` fields:
- `frontier.auto_query_next_frame_request`
- `frontier.auto_query_branch_plan`
- `frontier.auto_query_selected_branch`
- `frontier.auto_query_refined`

These are runtime-managed fields, not caller-owned.

## 7) Decision Support Link

Per tick, runtime publishes `frame.views.discriminators` and persists minimal audit state in `frontier.decision_support`.

See: `later/continuation-decision-support-contract.md`

## 8) Strategy + Query.Auto Semantics

- `single_lane_refine`: one constrained refine request.
- `top2_plus_bridge`: bounded branch plan (up to 3: two pivots + one bridge), scored and selected.
- `explore_more`: bounded broaden then re-evaluate.

All are bounded by existing budget + termination rules.

## 9) Work + Mutation Contracts

Work request contains:
- `work_id`, `kind`, `executor_class`, `input`, `output_contract`, `quality_gates`, `escalate_if`.

Reconcile applies typed mutation ops only:
- `upsert_item`
- `set_tags`
- `set_summary`

No eval-like execution is allowed in mutation payloads.

## 10) Concurrency, Idempotency, Errors

1. `state_version` mismatch returns `state_conflict`.
2. Same `idempotency_key` with different payload is invalid.
3. Replayed idempotent call returns stored output envelope.

Canonical error codes:
- `invalid_input`
- `missing_required_field`
- `state_conflict`
- `unknown_profile`
- `invalid_frame_operator`
- `internal_error`

## 11) Compatibility Compilation (Implementation Intent)

- `get(id)` => single done tick with `seed=id`.
- `find(query)` => single done tick with `seed=query`.
- simple `put` => single done tick with inline `upsert_item`.
- ingest `put(file/url)` => multi-tick profile-driven flow.
