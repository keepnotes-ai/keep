# Flow System Convergence

Date: 2026-03-09
Updated: 2026-03-10
Status: Complete

## Summary

Converged two parallel flow systems into one:

- **Removed:** FlowEngine (`flow_engine.py`, `flow.py`, `flow_policy.py`,
  `flow_executor.py`, `work_store.py`) — the original durable continuation
  engine with frame/decision/mutation pipeline. ~4,500 lines deleted.

- **Kept:** State doc runtime (`state_doc_runtime.py`) — lightweight
  synchronous runtime with YAML-driven rules, inline action execution,
  and self-contained cursors. Handles all flow logic: read paths
  (get-context, find-deep, query-resolve) and write-capable actions
  (summarize, tag, analyze, put).

- **Replaced:** Background task dispatch now uses `work_queue.py` +
  `work_processor.py` — direct SQLite-backed enqueue/claim/complete.
  After-write tasks are driven by the after-write state doc: rules
  are evaluated via `_dispatch_after_write_flow()`, which enqueues
  matched tasks (summarize, ocr, describe, analyze, tag) to the
  work queue.

## What changed

| Before | After |
|--------|-------|
| `keep continue` / `keep continue-work` | `keep flow` |
| `keep_continue` / `keep_continue_work` (MCP) | `keep_flow` (MCP) |
| `Keeper.continue_flow()` | `Keeper.run_flow_command()` |
| FlowEngine cursors (DB-backed) | Self-contained base64url cursors |
| FlowEngine work pipeline | `WorkQueue.enqueue()` + `process_work_batch()` |
| `_put_via_flow()` | `_put_direct()` + `_dispatch_after_write_flow()` |

## Architecture (final)

```
put()  ──→  _put_direct()  ──→  _dispatch_after_write_flow()  ──→  work_queue
get()  ──→  _run_read_flow("get")  ──→  state_doc_runtime
find() ──→  _run_read_flow("find-deep")    ──→  state_doc_runtime
keep flow ──→  run_flow_command()           ──→  state_doc_runtime

work_queue  ←──  process_work_batch()  ──→  _run_local_task_workflow()
```

## Key design decisions

**Self-contained cursors.** Base64url-encoded JSON with state name,
tick count, and accumulated bindings. No database, no server state.
Caller provides params, budget, and state doc source fresh each call.
See [KEEP-FLOW.md](../KEEP-FLOW.md) for details.

**State-doc-driven task dispatch.** After-write tasks are determined
by evaluating the after-write state doc (see `builtin_state_docs.py`).
`_dispatch_after_write_flow()` evaluates the state doc's rules against
the written item's properties, then enqueues matched tasks into the
SQLite queue. The work processor claims and executes them via
`_run_local_task_workflow()`. Supersede-on-enqueue prevents redundant
processing of rapidly-updated items.

**Write-capable action context.** The state doc runtime's action
context delegates provider resolution to Keeper's provider registry,
enabling write actions (summarize, tag, analyze, put) alongside
read actions (find, get, traverse).
