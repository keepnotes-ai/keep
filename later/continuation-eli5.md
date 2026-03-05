# Continuations, ELI5

Date: 2026-03-05
Audience: User-facing explanation

## What is this?

Instead of asking memory with lots of separate commands (`get`, `find`, `prompt`, etc.), we use one command shape:

```text
continue(input) -> output
```

Think of it like a guided search session that remembers where it is.

## Why this exists

Real questions are usually not one-shot.
You start broad, look at what you found, then refine.
Sometimes you need background tools (OCR, summarize). Sometimes the main agent should decide.

Continuations make that normal workflow first-class.

## One big motivation: mixed intelligence

Not every decision should use the same engine.
Some choices are cheap and mechanical (rules + stats).
Some need a tiny model.
Some need a stronger specialist agent.

Continuations let one flow use all of them:
- steering decisions (how to refine next)
- mutation decisions (what to write/tag/summarize)
- execution (which provider/tool actually does the work)

All with one contract shape, so you can swap technologies without redesigning the API.

## How it works (simple)

1. You send a goal + optional query.
2. Keep returns a frame (current evidence + decision hints).
3. If needed, Keep asks for work or plans the next refine step.
4. You call `continue` again with the same `flow_id`.
5. Repeat until done.

So it is not "run one query and hope".
It is "search, inspect, refine, finish".

## What makes decisions?

Keep uses small, practical signals:
- ranking confidence (is there a clear winner?)
- lineage concentration (versions/parts clustering)
- generic tag structure (edge vs facet)
- precomputed priors (fanout/selectivity/cardinality)

From that, it suggests one strategy:
- `single_lane_refine`
- `top2_plus_bridge`
- `explore_more`

No tag key is treated as magic by name.

If confidence is low, decision-making can be delegated upward (to a better decision maker) without changing the flow shape.

## Syntax: start simple

No, it is not premature to draft this. It is a good simplification test.

Start with the smallest useful call:

```json
{
  "goal": "query",
  "frame_request": {
    "seed": {"mode": "query", "value": "open commitments"},
    "pipeline": [{"op": "slice", "args": {"limit": 5}}]
  }
}
```

Then continue the same flow:

```json
{
  "flow_id": "<from previous output>",
  "state_version": 1
}
```

Add delegated work only when needed:

```json
{
  "goal": "ingest",
  "steps": [
    {"kind": "extract.text"},
    {"kind": "summarize"},
    {"kind": "tag"}
  ]
}
```

The runtime decides which executor can run each step. Returned `work_results` reconcile through typed mutations (`upsert_item`, `set_tags`, `set_summary`).

## Why this is valuable

1. Better retrieval for hard questions: broad->refine is built in.
2. Lower cognitive load: one API surface, same loop for many tasks.
3. Safer growth: bounded steps, no `eval()`-style runtime.
4. Flexible domains: works for conversations, files, research notes, operations logs, etc.

## In one sentence

Continuations turn memory access from a static lookup into a small, stateful, guided investigation loop.
