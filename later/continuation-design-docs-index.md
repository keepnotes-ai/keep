# Continuation Design Docs Index

Date: 2026-03-03

## Canonical

1. `later/continue-wire-contract-v1.md`
   - Normative wire contract for `continue(input) -> output` (schema, concurrency, idempotency, mutation semantics).
2. `later/continuation-api-spec.md`
   - Protocol and runtime semantics (non-normative design detail).
3. `later/continuation-machine-architecture.md`
   - Implementation block architecture and metaschema placement.
4. `later/simplified-model-and-continuation-design.md`
   - Simplified logical model (`Node + Fact`) and operator/state shape.
5. `later/use-case-simulations-with-continuations.md`
   - Worked examples and simplification outcomes across task types.
6. `later/template-frame-state-contract.md`
   - Concrete wire/schema expression for template-driven prompts over Frame/State.
7. `later/local-first-continuation-implementation-outline.md`
   - Phased local-first implementation plan (summarizer vertical slice before general API exposure).
8. `later/discriminator-precompute-spec.md`
   - Precompute model for planner priors (fanout/selectivity/bridgeiness) and continuation discriminator integration.

## Historical (Superseded)

1. `later/keep-er-projection-notes.md`
   - Early normalized E-R framing before simplification.
2. `later/continuation-driven-memory-iteration.md`
   - Early continuation/callback framing before unified work-contract API.

## Naming Standard

- `Frame`: current derived working view.
- `State`: durable flow progress.

If any doc conflicts with this naming or the single-call API, treat it as outdated.
