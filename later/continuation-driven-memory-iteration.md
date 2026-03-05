# Continuation-Driven Iteration (Historical Design Note)

Date: 2026-03-03
Status: Superseded
Superseded by:
- `later/continuation-api-spec.md`
- `later/continuation-machine-architecture.md`

## What Remains Useful

Two principles from this draft remain valid:

1. Push-down vs agentic boundary:
   - deterministic retrieval/scoring stays in the query engine
   - ambiguity, synthesis, and commitment decisions stay agent-side
2. Retrieval and memorialization are one iterative program, not separate systems.

## What Was Replaced

- Callback-centric handshake has been replaced by a unified `work` contract.
- Early multi-endpoint iteration API ideas were replaced by one call: `continue(input) -> output`.
- The active naming is `Frame` (derived view) and `State` (durable progress).

## Canonical References

- `continuation-api-spec.md` for protocol and runtime semantics.
- `continuation-machine-architecture.md` for block-level implementation.
