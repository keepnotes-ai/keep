# Continuation API Spec (Tiny Surface, Full Dynamics)

Date: 2026-03-03
Status: Draft (Design, Non-Normative)
Related:
- `later/continue-wire-contract-v1.md`
- `later/simplified-model-and-continuation-design.md`
- `later/continuation-machine-architecture.md`
- `later/use-case-simulations-with-continuations.md`
- `later/continuation-decision-support-contract.md`

## 1) Goal

Define the smallest useful API surface that still supports:
- whole-memory exploration
- continuation-based iterative reasoning
- dynamic delegation to different executors:
  - coordinating `agent`
  - scoped `sub_agent`
  - tool/provider executors

The API must treat callbacks and queued/background tasks as the same runtime primitive.

## 2) Minimal Surface

Single operation:

```text
continue(input) -> output
```

No separate start/step/callback/queue endpoints. A new flow starts when `flow_id` is absent.

## 2.1) Canonical Terms

- `Frame`: the bounded, current working view (derived each tick; cacheable).
- `State`: durable progress of the flow (must survive crashes, task switches, and harness restarts).

Mapping in this spec:
- request asks for a `frame_request`
- response returns a `frame`
- response returns updated `state`

## 3) Contract

## 3.1 Input

```json
{
  "flow_id": "optional",
  "goal": "Research and development objective from evidence",
  "profile": "optional-profile",
  "success_tests": [
    "at least 3 independent sources",
    "major contradictions resolved"
  ],
  "steps": [],
  "frame_request": {
    "seed": { "mode": "query|id|similar_to", "value": "rollout risk" },
    "pipeline": [
      { "op": "where", "args": { "facts": ["project=myapp"] } },
      { "op": "slice", "args": { "limit": 12 } }
    ],
    "budget": { "tokens": 2400, "max_nodes": 120 }
  },
  "feedback": {
    "work_results": []
  }
}
```

## 3.2 Output

```json
{
  "flow_id": "f_9f2a",
  "status": "running|waiting_work|paused|done|failed",
  "frame": {
    "slots": {
      "flow_id": "f_9f2a",
      "goal": "transform"
    },
    "views": {
      "task": "...",
      "evidence": [
        { "id": "note:123", "role": "support", "score": 0.84 }
      ],
      "hygiene": []
    }
  },
  "applied": {"work_ops": []},
  "requests": {
    "work": [
      {
        "work_id": "w_31",
        "kind": "transform.chunk",
        "executor_class": "provider",
        "suggested_executor_id": "small-ops",
        "input": {
          "nodes": ["note:111@P{1}", "note:111@P{2}"],
          "prompt_profile": "extract_findings"
        },
        "output_contract": {
          "must_return": ["claims", "citations"],
          "schema_version": "1.0"
        },
        "sla": { "deadline_s": 30 }
      }
    ]
  },
  "state": {
    "cursor": { "step": 4, "stage": "explore", "phase": "explore" },
    "frontier": {},
    "pending": {"work_ids": []}
  },
  "next": {
    "recommended": "continue",
    "reason": "Return completed work_results to continue"
  }
}
```

## 4) Unified Runtime Primitive: Work Contract

All deferred actions are represented as `work` items. This includes old-style callbacks.

```json
{
  "work_id": "w_x",
  "kind": "opaque string defined by profile/template",
  "executor_class": "executor hint (optional)",
  "input": {},
  "output_contract": {
    "must_return": [],
    "optional": [],
    "schema_version": "1.0"
  },
  "quality_gates": {
    "min_confidence": 0.8,
    "citation_required": true
  },
  "escalate_if": ["low_confidence", "conflict", "missing_citation"]
}
```

The caller may execute this work externally and return the result in the next `feedback.work_results`.

## 5) What Crosses the Interface

Only five things need to cross:
1. `goal/profile/steps`: what outcome is wanted and how process shape is defined.
2. `frame_request`: what slice/transformation of memory should be computed now.
3. `work requests`: structured tasks with executor hints.
4. `feedback`: completed work results.
5. `state`: enough cursor/frontier to resume deterministically.

Everything else stays internal to the continuation kernel.

## 6) Dynamics (Not Just Objects)

Runtime loop behind `continue`:
1. Resolve current stage from `state.cursor.stage` (or default for the profile).
2. Expand frame for this stage (subject to budget/policy).
3. Auto-apply lightweight housekeeping (tags/links).
4. Resolve effective plan (`steps` override or `profile`).
5. Evaluate preconditions and emit all eligible work requests for the tick.
6. Merge incoming `feedback.work_results`.
7. Advance stage and persist updated state atomically.
8. Stop when success/termination conditions are met.

Control model:
- Top-down: template/profile defines process skeleton (goal + stage intent).
- Bottom-up: kernel uses pressure/utility scoring *within a stage*.

This is not FIFO queue semantics.

## 6.1) Minimal Stage Semantics (v1)

Stage is explicit in state:

```json
"state": {
  "cursor": {
    "step": 4,
    "stage": "process stage name or coarse stage",
    "phase": "explore|waiting_work|reconcile|write|failed|tick"
  }
}
```

v1 constraints:
1. Stage names/transitions are kernel-controlled.
2. Template text does not execute process control code.
3. Conditionals are implemented as kernel-evaluated preconditions over goal/profile/frame, not eval-like template logic.

Phase-2 extension:
1. Stage skeleton can be declared in template/profile docs.
2. Kernel remains the only evaluator.

## 7) Executor Hints and Registry

Executor labels are data, not enums. Common labels are `agent`, `sub_agent`, and `provider`,
but the runtime treats both `work.kind` and `executor_class` as opaque strings.

The API does not expose internal model details. It exposes capability contracts and quality gates.

## 8) Example Executor Registry

```json
{
  "executor_registry": [
    {
      "executor_id": "small-ops.v1",
      "class": "provider",
      "accepts": ["summarize", "tag", "transform"],
      "io": {
        "max_input_tokens": 4000,
        "max_output_tokens": 700,
        "structured_output": true
      },
      "quality": {
        "expected_confidence": 0.78
      },
      "cost": { "tier": "low", "latency_ms_p50": 1200 }
    },
    {
      "executor_id": "scout.graph.v1",
      "class": "sub_agent",
      "accepts": ["explore", "compare", "probe"],
      "scope_limits": { "max_steps": 6, "max_nodes": 160 }
    },
    {
      "executor_id": "primary.agent",
      "class": "agent",
      "accepts": ["decision", "synthesis", "write_review"]
    }
  ]
}
```

## 9) Facade Dispatch Rules (Profile-Driven)

Dispatch is profile-driven, then score-based:

1. Start from work-provided hints (`executor_class`, `suggested_executor_id`, contract requirements).
2. Filter candidates by capability (`accepts`), limits, and policy.
3. Pick lowest-cost executor meeting quality gates.
4. If none qualify, escalate to configured fallback (typically coordinator agent).
5. If a result fails gates, reissue with adjusted executor hints.

Execution boundary:
1. Kernel computes frame/state and emits work contracts.
2. Executor layer fulfills work contracts by runner/provider registry.

## 10) Return Path and Agentic Re-entry

Every work completion returns through one normalized envelope:

```json
{
  "work_result": {
    "work_id": "w_31",
    "executor_id": "small-ops.v1",
    "status": "ok|failed|partial",
    "outputs": {
      "claims": [],
      "citations": [],
      "artifacts": []
    },
    "quality": {
      "confidence": 0.76,
      "passed_gates": false,
      "fail_reasons": ["citation_coverage_low"]
    },
    "provenance": {
      "input_hash": "sha256:...",
      "attempt": 1
    }
  }
}
```

The kernel then chooses:
- `accepted`: merge into state frontier.
- `revise`: resend to another machine/sub-agent with adjusted prompt/profile.
- `agentic_eval`: emit follow-up work for a higher-capability executor.

This is the key dynamic: machine outputs are provisional until they pass gates or are accepted by agentic evaluation.

## 11) First-Time User Instructions

1. Call `continue` without `flow_id` and provide top-level program fields (`goal/profile/steps`) plus an initial `frame_request`.
2. Read `frame.views.task` and `frame.views.evidence` for current progress.
3. If `requests.work` exists:
   - run them with available executors matching `executor_class`, then
   - return outputs in `feedback.work_results`.
4. Call `continue` again with `flow_id` until `status=done`.

## 11.1) Minimum-Viable Tick (Trivial Ops)

1. `get` and `find` compile to single-tick flows; expected outcome is `status=done` with no work.
2. Simple inline `put/write` may also complete in one tick (`status=done`) when no heavy transforms are needed.
3. Multi-tick behavior is only required when work is emitted.

## 12) Example: Callback vs Background Task (Unified)

Old callback:
- "Which Deborah entity?"

Now:
- emitted as `work.kind=resolve.reference`, `executor_class=agent`
- returned as a normal `work_result`

Old queue task:
- "summarize chunk 7"

Now:
- emitted as `work.kind=transform.chunk`, `executor_class=provider`
- returned as a normal `work_result`

Both share one path through the interface.

## 13) Simplification Rule

If a feature requires a second endpoint, first try expressing it as:
- a new `work.kind`
- or a new `frame_request.pipeline` operator
- or a new `feedback` payload type

Keep surface area constant; grow capability via data contracts.

## 13.1) Suitability Test: Add Classifier Step

Change request:
- prepend ingest with a classifier stage that tags `source_quality` and `tone`.

Fit in this model:
1. Add `work.kind=classify.document`.
2. Add a profile precondition:
   - if `params.run_classify=true`, emit `classify.document` before `transform.document`.
3. Apply classifier output as typed tag mutation.
4. Continue existing ingest stages.

No new endpoint is required.

## 14) State Persistence and Termination Rules

`State` is durable; `Frame` is derived.

- Persist atomically per tick:
  - updated `state`
  - newly emitted `work`
  - event record
- `Frame` persistence is optional cache only; losing it must not lose progress.

Simple termination/anti-accumulation rules:

1. `max_idle_age` for `waiting_work` -> transition to `paused`.
2. `max_attempts` per `work_id` -> transition to `failed`.
3. `max_revisions` (`revise -> revise` loops) -> escalate to `agent` once, then `failed`.
4. `max_fanout` per parent work item -> reject extra children.
5. `orphan_timeout` for child work whose parent is terminal -> garbage collect child.

These rules prevent silent dead-letter-style growth of unresolved flow state/work.

## 15) Execution-Boundary Profiles (Local, Hybrid, Remote)

`continue(input) -> output` stays constant across deployment modes; only execution ownership changes.

## `local_only`
- Foreground: local API/CLI (`Keeper`).
- Background: local daemon (`keep pending --daemon`).
- Authority: local `pending_summaries` queue is canonical for heavy work.

## `hybrid_delegate`
- Foreground: local API/CLI (`Keeper`).
- Background: local daemon still owns dequeue/retry/dead-letter semantics.
- Delegation: daemon may submit delegatable profile steps to hosted processors and poll for results.
- Fallback: stale/unreachable delegated tasks revert to local queue processing.

## `remote_only`
- Foreground: API-level delegation via `RemoteKeeper` (CLI/MCP calls hosted `/v1/*`).
- Background: hosted queue + hosted worker containers.
- Owner: hosted service is canonical for flow state and work lifecycle.
- Local pending queue/daemon are non-authoritative (or disabled) in this mode.

## Boundary Rules

1. Exactly one queue owner per task lineage.
2. API-level delegation (`remote_only`) and task-level delegation (`hybrid_delegate`) must not both own the same lineage.
3. Keep low-latency foreground path: enqueue/emit work quickly; run heavy transforms off-path.
4. Preserve existing pending queue split for current primitives; evolve internals behind the same `continue` interface.
5. For compatibility, a FIFO pending queue may back selected profiles/steps even while the logical continuation scheduler remains pressure-driven at the flow layer.

## 16) Gotchas for Self-Modifying Metaschema

If system-doc mutations are allowed, the main risks are semantic race conditions, not storage corruption.

1. Same-tick policy bleed:
   - Risk: tick reads old policy, writes new policy, then accidentally uses new policy later in same tick.
   - Rule: freeze policy snapshot at tick start; activate mutations next tick only.
2. Replay drift:
   - Risk: retry re-applies system-doc mutation and diverges.
   - Rule: idempotency key must return prior result without reapplying side effects.
3. Work-result policy mismatch:
   - Risk: queued work produced under old policy is accepted under new policy without re-gating.
   - Rule: stamp work with policy hash and gate on reconcile.
4. Cross-flow write skew:
   - Risk: two flows mutate same system doc and both think they won.
   - Rule: apply mutation preconditions (`if_version`) + conflict error/retry.
5. Cache incoherence:
   - Risk: template/tag parsers cache old policy indefinitely.
   - Rule: key caches by metaschema hash and invalidate on change.

These guardrails keep per-tick atomicity incremental and predictable, without a long-lived final-commit transaction model.

Hosted authorization specifics are intentionally out of scope in this local-first spec.
