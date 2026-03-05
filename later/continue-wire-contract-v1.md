# Continue Wire Contract v1 (Normative)

Date: 2026-03-03
Status: Draft (Normative)
Related:
- `later/continuation-api-spec.md`
- `later/template-frame-state-contract.md`
- `later/continuation-machine-architecture.md`

## 1) Purpose

Define a single normative wire contract for:

```text
continue(input) -> output
```

This contract unifies:
- objective-driven flows (`goal/profile/steps + frame_request`)
- template-driven flows (`template_ref + params`)

and adds required semantics for:
- idempotency
- state concurrency/versioning
- deterministic replay
- side-effect control

Current bounded scope for implementation:
- Normative runtime profile is `local_only` (single-tenant local client/service).
- Hosted RBAC and hosted work-result trust model are deferred to hosted implementation docs.

## 2) Normative Rules

1. `input.schema_version` MUST be present and equal to `"continue.v1"`.
2. Callers MUST provide either:
   - `template_ref`, or
   - one or more top-level program fields (`goal`, `profile`, `steps`, `frame_request`).
3. Callers MUST NOT send legacy continuation fields (`intent`, `work_plan`, `write`).
4. A caller MAY provide both `template_ref` and `frame_request`; explicit `frame_request` overlays template-derived frame bindings.
5. `state` is server-owned. Caller-provided `state` is advisory echo only and MUST be ignored for mutation control.
6. Progress control is governed by `flow_id + state_version`.
7. Mutations are applied only through typed mutation ops; no free-form evaluator is allowed.
8. Local profile is single-tenant; caller identity is out of scope for this contract.

## 3) ContinueInput (Normative Shape)

```json
{
  "schema_version": "continue.v1",
  "request_id": "uuid-string",
  "idempotency_key": "opaque-client-key",
  "flow_id": "optional-flow-id",
  "state_version": 0,

  "goal": "optional-goal-text",
  "profile": "optional-profile-name",
  "success_tests": ["optional tests"],
  "template_ref": ".prompt/agent/query",
  "steps": [],
  "params": {
    "text": "optional text",
    "id": "optional id",
    "since": "optional duration/date",
    "until": "optional duration/date",
    "tags": {"project": "myapp"}
  },

  "frame_request": {
    "seed": {"mode": "query|id|similar_to", "value": "..."},
    "pipeline": [
      {"op": "where", "args": {}},
      {"op": "slice", "args": {}}
    ],
    "budget": {"tokens": 2400, "max_nodes": 120}
  },

  "feedback": {
    "work_results": []
  }
}
```

### 3.1 Input Invariants

1. `request_id` MUST be unique per call.
2. `idempotency_key` SHOULD be stable for retries of the same logical call.
3. If `flow_id` is absent, server MUST start a new flow.
4. If `flow_id` is present, `state_version` MUST match current stored version, else `409 state_conflict`.
5. `frame_request.pipeline[*].op` MUST be in the allow-list: `where|slice`.

## 4) ContinueOutput (Normative Shape)

```json
{
  "schema_version": "continue.v1",
  "request_id": "echo",
  "idempotency_key": "echo",

  "flow_id": "flow-id",
  "state_version": 12,
  "status": "running|waiting_work|paused|done|failed",

  "frame": {
    "slots": {},
    "views": {
      "task": "",
      "evidence": [],
      "hygiene": []
    },
    "budget_used": {"tokens": 900, "nodes": 42}
  },

  "rendered": {
    "template_ref": ".prompt/agent/query",
    "template_hash": "sha256:...",
    "metaschema_hash": "sha256:...",
    "text": "optional rendered text"
  },

  "requests": {
    "work": []
  },

  "applied": {
    "work_ops": [],
    "mutation_txn_id": "optional"
  },

  "state": {
    "cursor": {"step": 0, "stage": "tick", "phase": "tick"},
    "frontier": {},
    "pending": {"work_ids": []},
    "termination": {
      "idle_ticks": 0,
      "max_idle_ticks": 3
    }
  },

  "errors": []
}
```

## 5) Canonical Subcontracts

## 5.1 Template Binding Contract

Template docs are declarative. Bindings MAY reference:
- `from: params.*`
- `frame_request: ...`

Template execution MUST NOT execute code, scripts, or eval-like expressions.

Allowed interpolation syntax in template source (phase-1 implementation):
- `{{slot_name}}`
- no function calls
- no loops/conditionals in v1 template text

## 5.2 Stage Model (Minimal v1)

`continue` executes one kernel tick. Runtime stage fields:

```json
"state": {
  "cursor": {
    "step": 4,
    "stage": "process stage name or coarse stage",
    "phase": "explore|waiting_work|reconcile|write|failed|tick"
  }
}
```

Stage ownership in v1:
1. Template docs describe retrieval/render structure.
2. Kernel controls stage transitions and work emission.
3. No template-level `if/else` evaluator exists.

This keeps v1 safe and bounded while still exposing process state to callers.

## 5.3 Work Contract

Each emitted work item MUST include:
- `work_id`
- `kind`
- `executor_class`
- `input`
- `output_contract.schema_version`
- `quality_gates`

`work_id` is globally unique within flow.

## 5.4 Mutation Contract

All side effects use typed mutation ops:

```json
{
  "op": "upsert_item|set_tags|set_summary",
  "target": "id",
  "preconditions": {"if_version": 3},
  "payload": {},
  "provenance": {"source": "housekeeping|agent|work_result", "evidence_ids": []}
}
```

No mutation op may contain executable code.

## 6) State, Concurrency, and Replay

1. Server persists `state + emitted work + event` atomically per accepted call.
2. `state_version` increments by 1 on each accepted transition.
3. Replays with same `idempotency_key` MUST return same `flow_id/state_version/output_hash` (or explicit replay marker).
4. If state conflict occurs, server returns `409` with latest `state_version` and optional merge hint.

## 7) Determinism Pins

Each tick MUST capture:
- `template_hash` (if template flow)
- `metaschema_hash` (resolved system-doc set hash)
- `planner_version`

This supports reproducible audits even when templates/metaschema evolve later.

## 8) Termination and Anti-Accumulation

Mandatory guardrails:

1. `max_idle_age` for `waiting_work` -> `paused`.
2. `max_attempts` per `work_id` -> `failed`.
3. `max_revisions` for revise loops -> escalate once -> `failed`.
4. `max_fanout` per parent work item.
5. `orphan_timeout` for child work whose parent is terminal.

## 9) Error Model

Canonical error codes:
- `invalid_input`
- `missing_required_field`
- `state_conflict`
- `forbidden`
- `unknown_template`
- `invalid_frame_operator`
- `budget_exceeded`
- `executor_unavailable`
- `mutation_precondition_failed`
- `internal_error`

Errors MUST include machine-readable `code` and stable `message`.

## 10) Compatibility Mapping to Existing keep Primitives

1. `keep prompt query`
   - input: `template_ref=.prompt/agent/query`, `params.text=...`
2. `keep now` read/write
   - profile-backed `frame_request` + typed `set_now` mutation op
3. `keep put` PDF ingest
   - continuation stages emit profile-defined work kinds (for example `extract`, `transform`, `index`) and reconcile through typed mutation ops

### 10.1 Concrete Compilation Rules (Minimum Viable Tick)

These define the lower bound of flow overhead.

1. `keep get <id>`
   - compile to one `continue` call:
   ```json
   {
     "schema_version": "continue.v1",
     "goal": "get",
     "profile": "read.get",
     "params": {"id": "<id>"},
     "frame_request": {"seed": {"mode": "id", "value": "<id>"}}
   }
   ```
   - expected result: no work emitted, `status=done`, one projected frame.

2. `keep find "<query>"`
   - compile to:
   ```json
   {
     "schema_version": "continue.v1",
     "goal": "find",
     "profile": "read.find",
     "params": {"text": "<query>"},
     "frame_request": {"seed": {"mode": "query", "value": "<query>"}}
   }
   ```
   - expected result: no work emitted, usually `status=done`.

3. `keep put "text"` (simple inline write)
   - compile to:
   ```json
   {
     "schema_version": "continue.v1",
     "goal": "write",
     "profile": "write.put",
     "params": {"id": "<id>", "content": "<text>", "tags": {}}
   }
   ```
   - expected result: typed write applied in same tick, no background work required for trivial case.

4. `keep put file://...` (ingest pipeline)
   - compile to `profile=ingest.put`; kernel may emit profile-defined work over multiple ticks.

Simple reads/writes are therefore not forced through long-running pipelines.

### 10.2 Frame->Work Bridging Rule (v1)

Work emission is kernel-strategy driven:
1. Evaluate current stage + goal/profile + frame evidence.
2. Resolve plan source:
   - top-level `steps` when provided
   - else `profile` -> `.profile/*` system doc
3. Emit all eligible typed work steps for the tick (not only one), skipping steps already emitted/completed.
4. Otherwise finish.

No free-form template evaluator is used.

## 11) Minimal Endpoints

Normative API surface:

1. `continue(input) -> output`

Optional operational endpoints:

1. `get_flow(flow_id)`
2. `cancel_flow(flow_id)`

No additional flow-control endpoint is required for v1 capability.

## 12) Pre-Implementation Checklist

Before coding full runtime, freeze:

1. JSON Schema for `continue.v1` input/output.
2. Template grammar and compiler allow-list.
3. Mutation op enum and precondition semantics.
4. State transition table (`status` transitions + error transitions).
5. Idempotency and replay conformance tests.
6. Local authorization policy behavior and conformance tests.

## 13) Metaschema Mutation Semantics (Normative)

Self-modification of system docs is allowed, but activation timing is strict.

1. Each tick runs on a single committed policy snapshot:
   - resolve `template_hash` and `metaschema_hash` at tick start.
2. `frame`, `rendered`, routing, and gates for that tick MUST use that start snapshot only.
3. If the tick emits mutations to system docs (`.prompt/*`, `.tag/*`, `.meta/*`, etc.), those mutations:
   - MUST be committed atomically with `state + emitted work + event`,
   - MUST NOT affect reads/rendering/planning inside the same tick.
4. Metaschema changes become visible on the next accepted tick for that flow (or any other flow that starts later).
5. Replays with same `idempotency_key` MUST NOT re-apply metaschema mutations.

Required observability fields per accepted tick:
- `metaschema_hash_before` (snapshot used to compute output)
- `metaschema_hash_after` (post-commit snapshot; may equal `before`)

Existing `rendered.metaschema_hash` SHOULD be interpreted as `metaschema_hash_before`.

Recommended execution detail:
- Stamp each emitted work item with `policy_hash = metaschema_hash_before`.
- On reconcile, re-run quality gates under current policy; do not blindly accept stale-policy outputs.

This preserves incremental atomic commits per tick and avoids accumulating open transaction context across multiple ticks.

## 14) Profile-Step Suitability Test

Example extension request: add a first-stage classifier that tags notes with
caller-defined keys (for example `source_quality` and `tone`).

This fits v1 by adding one new work kind and profile rule, no new endpoint:

1. Flow input:
```json
{
  "schema_version": "continue.v1",
  "goal": "transform",
  "profile": "ingest.put",
  "params": {
    "id": "note:123",
    "content": "...",
    "run_classify": true
  }
}
```
2. Kernel precondition emits `work.kind=classify.document`.
3. Returned `work_result.outputs.tags` is applied as lightweight housekeeping tag updates.
4. Flow continues to the next profile-defined work kind.

Creation model:
1. Local mode: agent may create/update profile/template system docs directly.
2. Hosted authorization is deferred to hosted docs.

Mutation classes:
1. `data mutations`:
   - regular notes/parts/versions/tags/edges in user space.
   - allowed in local profile.
2. `system-doc mutations`:
   - targets in metaschema/control namespace (for example `.prompt/*`, `.tag/*`, `.meta/*`, `.stop`, `.domains`, `.conversations`).
   - allowed in local profile.

Per-tick atomicity still applies:
1. Either all mutations in the transaction apply with state transition.
2. Or none apply and state remains at previous committed version.
