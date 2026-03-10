# Flow System Convergence

Date: 2026-03-09
Status: Design note (not checked in)

## Current state

Two parallel flow systems:

1. **FlowEngine** (`flow_engine.py`) — original continuation engine. Drives the
   write path (after-write processing). Full lifecycle: cursors, work items,
   mutations, idempotency, optimistic concurrency. Heavy.

2. **State doc runtime** (`state_doc_runtime.py`) — new lightweight runtime.
   Drives the read path (get-context, find-deep, query-resolve). Runs
   synchronously to completion. No cursors, no work items.

## Target

Converge on state doc runtime as the single flow model:

- `run_flow` returns a cursor when it stops (not just when done). Cursor
  encodes current state name, tick count, accumulated bindings.
- `run_flow` accepts a cursor to resume — loads state, restores bindings,
  continues evaluation.
- Single CLI/MCP entry point: `keep flow <state> [params]` to start,
  `keep flow --cursor <cursor> [params]` to resume.
- FlowEngine becomes purely the work lifecycle manager (enqueue, claim,
  execute, complete). State doc runtime handles all flow logic.

## Use cases

1. **Resume after terminal** — flow returned `stopped: ambiguous` or
   `stopped: budget`. Agent reviews partial results, pushes further with
   more budget or different strategy.

2. **Start new flow** — agent invokes a state doc directly with parameters.
   Works for built-in flows and custom user-defined flows (commitment review,
   session reflection, etc.).

## Open questions

- Cursor encoding: lightweight (state name + tick + params) vs persistent
  (flow record in work_store)?
- Should resumed flows share bindings from the previous run, or start fresh
  with adjusted params?
- Write-path migration: when does FlowEngine's frame/decision/work pipeline
  move to state doc evaluation?
