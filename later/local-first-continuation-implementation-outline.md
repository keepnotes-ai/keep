# Local-First Continuation Implementation Outline

Date: 2026-03-03
Status: Draft (Implementation Plan)
Related:
- `later/continue-wire-contract-v1.md`
- `later/continuation-api-spec.md`
- `later/template-frame-state-contract.md`
- `later/continuation-machine-architecture.md`

## Scope Boundary

Current implementation scope is local-only:
- single-tenant local store/runtime
- no hosted RBAC enforcement in this phase
- no external callback trust/HMAC requirements in this phase

Hosted trust and role controls are deferred.

## Delivery Strategy

Use 3 phases with API-first sequencing:
- publish a minimal but real `continue` boundary early,
- prove it end-to-end on one concrete profile-defined work plan,
- then retrofit existing primitives onto it.

## Phase 1: API-First Continuation Core (Current)

Goal:
- establish `continue(input)->output` as the canonical flow boundary without replacing existing commands.

Implemented:
1. Durable flow/work/idempotency/event persistence (`continuation.db`).
2. State/version conflict handling and idempotent replay.
3. End-to-end summarize work contract (`requests.work` + `work_results` reconcile).
4. Local preview commands:
   - `keep continue`
   - `keep continue-work`

Course-corrections applied:
1. Persist flow program in state (`goal/profile/template/frame_request/params`) so continuation does not depend on per-call restatement.
2. Frame generation now derives from the persisted program (not only ad-hoc payload fields).
3. Template-driven rendering path added (uses `.prompt/agent/*` docs), but retrieval remains frame-driven.
4. Frame pipeline operator allow-list validation added (`where|slice` in phase-1 runtime).
5. Query/reflect flows now stay in bounded tick/explore stages with no decision callback channel.
6. Minimal stage state exposed (`state.cursor.step/stage`) with kernel-managed transitions.
7. Simple inline write goal (`write/put`) can complete in one tick without background work.
8. Added custom profile-defined step lists (including tag-apply and summarize steps) to validate extensibility without reserved work kinds.

Acceptance criteria:
1. New flow can start from top-level program fields (`goal/profile/steps/frame_request`) or `template_ref`.
2. Subsequent ticks can continue with `flow_id + state_version` and feedback only.
3. Summarize flow remains fully functional end-to-end.
4. Conformance tests cover idempotency, state conflict, template render, operator validation, and custom-plan/write examples.

## Phase 2: Expand Existing Tasks + Runtime Profiles

Goal:
- reuse same continuation machinery for other existing heavy tasks and prompt flows.

In scope:
1. Add `ocr` and `analyze` profile-defined steps to same local flow engine.
2. Expand template->frame profile compiler in runtime path for:
   - `keep prompt query`
   - `keep now` read/write profiles.
3. Add declarative stage skeleton in system docs (`process_profile`) with bounded predicates, evaluated by kernel.
4. Keep execution boundaries unchanged:
   - foreground low-latency path
   - local pending daemon for heavy tasks.
5. Add metaschema activation semantics:
   - tick-start snapshot, next-tick activation.

Acceptance criteria:
1. `keep put` of scanned PDF runs OCR/summarize/analyze through continuation work lifecycle.
2. `keep prompt query` and `keep now` can be executed through continuation profiles (feature-gated).
3. Cross-task recovery tests pass (restart during OCR/analyze and reconcile correctly).

## Phase 3: Retrofit Existing Primitives to `continue`

Goal:
- route existing get/put/find/prompt orchestration through continuation profiles while preserving CLI/MCP UX.

In scope:
1. Existing CLI/MCP commands compile to continuation calls internally.
2. Add optional control reads:
   - `get_flow(flow_id)`
   - `cancel_flow(flow_id)`.
3. Observability:
   - emit per-tick hashes, mutation_txn_id, and state transitions for debugging/audit.

Acceptance criteria:
1. old commands and new `continue` produce equivalent outcomes for covered workflows.
2. no regression in background processing throughput/latency envelope.
3. deterministic replay tests pass for repeated idempotency keys.

## Cross-Cutting Test Matrix (Run Every Phase)

1. Atomicity:
   - state + work + mutation commit together or not at all.
2. Concurrency:
   - stale `state_version` returns conflict.
3. Idempotency:
   - retry returns same output hash and no duplicate mutation.
4. Termination/anti-accumulation:
   - idle/attempt/fanout/orphan rules enforced.
5. Crash recovery:
   - process kill mid-tick does not corrupt flow state.

## Deferred Items (Explicitly Out of Scope Here)

1. Hosted RBAC (`read|write|admin`) enforcement behavior.
2. External callback/work-result trust proofing (HMAC/JWS).
3. Multi-tenant hosted isolation contract details.
