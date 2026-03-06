# Continuations

## The problem with one-shot search

You've stored hundreds or thousands of notes — decisions, commitments, project context, meeting notes. Now you need to find something:

```bash
keep find "what authentication approach should we use?"
```

You get five results. Two are clearly relevant, one is noise, and two more *might* matter — but you can't tell without reading them. So you refine:

```bash
keep find "OAuth2 vs API keys security tradeoffs"
```

Better results. But now you've lost the first set. You open another search, read a related note, notice it links to a decision from last month, and chase that thread too.

This is what real research looks like. It's never one query. You start broad, look at what you found, realize what you actually need, and narrow down. Sometimes you branch sideways. Sometimes you need the system to do background work — summarize a long document, tag something for later, extract text from a PDF.

Continuations make this normal workflow a first-class operation.

## One call, many steps

Instead of juggling separate `get`, `find`, and `put` commands, continuations give you a single operation:

```
continue(input) -> output
```

The first call starts a **flow** — you describe what you're looking for, and keep returns what it found plus hints about what to try next. If you want to keep going, you pass back the cursor from the response and call `continue` again. The runtime remembers where you are, what evidence you've gathered, and what strategies it's already tried.

Here's what that looks like in practice:

```bash
# Start a search
keep continue '{"goal": "query", "params": {"text": "authentication approach"}}'

# The response includes a cursor — pass it back to refine
keep continue '{"cursor": "..."}'
```

Or in Python:

```python
result = kp.continue_flow({
    "goal": "query",
    "params": {"text": "authentication approach"},
})

# Keep going if the runtime thinks more refinement would help
while result["status"] == "in_progress":
    result = kp.continue_flow({"cursor": result["cursor"]})
```

Each call is a **tick** — one step forward in the investigation. The flow ends when the status is `done`, or when you decide you have what you need and stop calling.

## Why not just let the agent loop?

You could build this yourself. Call `find`, read the results, decide what to search next, call `find` again. Many agents do exactly this.

But there are three reasons to let the runtime handle it:

**Each step is a chance to intervene.** This is the key structural insight. A continuation doesn't run to completion in the dark — it pauses after each tick and hands control back. The agent (or the human, or a cheaper model) can inspect what was found, decide whether to refine or branch, add constraints, or stop early. The agent isn't just executing a search plan — it's actively exploring a data domain, incrementally, checking at each step whether it's found what satisfies the goal. That's what makes continuations well-suited to agentic use: the loop is collaborative, not autonomous.

**The runtime handles the bookkeeping.** At each tick, the system computes signals you'd otherwise hand-code: how confident is the top result? Are results clustering around one topic or scattered? Should we narrow or branch? These aren't magic — they're small statistical measures — but they're tedious to reimplement in every agent. And because the runtime tracks what's already been tried, it can avoid redundant queries and suggest strategies the agent wouldn't think to try.

**Mixed intelligence.** Not every decision needs the same brain. Choosing which search to try next? Cheap statistics and rules. Deciding whether to write a summary? A small model. Interpreting contradictory evidence? The strongest agent available. Continuations let one flow use all of these behind a single interface — and because each tick is a handoff point, you can swap what handles each step without redesigning the API.

## What the runtime figures out for you

At each tick, the runtime looks at the evidence it found and computes a handful of practical signals:

- **Is there a clear winner?** If the top result is much better than the second, the search is probably done. If they're close, there's ambiguity worth exploring.
- **Are results clustering?** If most evidence is from the same document version or the same part of a series, that's meaningful — it suggests where to look deeper (or whether to look elsewhere).
- **What tags are in play?** Some tags represent relationships (edges); others are for grouping and filtering (facets). The runtime notices this distinction so refinement strategies can use the right one.

From these signals, it picks one of three strategies:

| Strategy | When it's used | What it does |
|----------|----------------|--------------|
| **Narrow down** | Clear top result | Refine from the best candidate — add constraints, look at related items |
| **Hedge** | Ambiguous — two strong candidates | Explore both top results plus a bridge query, then pick the best |
| **Broaden** | Mixed signals | Expand the search before committing to a direction |

None of this requires you to name specific tags or know the internal schema. The runtime figures out which tags exist and how they relate.

If confidence is low, the runtime can delegate the decision upward — to a better model or to the human — without changing the flow shape.

## Background work

Some tasks can't finish in a single query. If you're ingesting a document, you might need to extract text, generate a summary, and auto-tag it. Continuations handle this too:

```python
result = kp.continue_flow({
    "goal": "ingest",
    "params": {"id": "quarterly-report"},
    "steps": [
        {"kind": "summarize", "runner": {"type": "provider.summarize"}},
        {"kind": "tag", "runner": {"type": "provider.tag"}},
    ],
})
```

If the runtime needs work done, it pauses the flow (status becomes `waiting_work`) and tells you what it needs. You execute the work — locally or with an external service — and feed the results back. The runtime applies the mutations (like setting a summary or adding tags) and advances the flow.

```python
while result["status"] in {"in_progress", "waiting_work"}:
    if result["status"] == "waiting_work":
        work = result["work"][0]
        wr = kp.continue_run_work(result["cursor"], work["work_id"])
        result = kp.continue_flow({
            "cursor": result["cursor"],
            "work_results": [wr],
        })
    else:
        result = kp.continue_flow({"cursor": result["cursor"]})
```

All mutations are typed — `upsert_item`, `set_tags`, `set_summary`. No arbitrary code execution. The flow tells you exactly what it changed in `applied_ops`.

## Processes, not features

Traditional tools hard-code behaviors like summarization, auto-tagging, and content analysis as fixed features. Continuations make these into composable steps that agents and users can define, customize, and chain. Want to summarize with a different model? Swap the runner. Want to add a custom analysis step after tagging? Add it to `steps`. Want an agent to decide at runtime whether tagging is even worth doing? It can — because each step is a handoff point.

This means keep's processing capabilities grow with your needs, not with its release cycle.

---

## Reference

### Starting a flow

Omit `cursor` and provide at least one of `goal`, `profile`, `steps`, or `frame_request`:

```json
{
  "goal": "query",
  "profile": "query.auto",
  "params": {"text": "authentication approach"},
  "frame_request": {
    "seed": {"mode": "query", "value": "authentication approach"},
    "pipeline": [{"op": "slice", "args": {"limit": 5}}]
  }
}
```

**`goal`** — what kind of flow: `"query"`, `"write"`, `"ingest"`, etc.

**`profile`** — a named behavior preset. `"query.auto"` enables automatic multi-step refinement. Without a profile, the flow completes in one tick and leaves strategy decisions to the caller.

**`params`** — goal-specific inputs. For queries, `text` is the search string. For writes, `id` and `content`. For ingest, `id` and optional `steps`.

**`frame_request`** — controls what evidence is retrieved on this tick:
- `seed.mode` — `"query"` (semantic search), `"id"` (fetch by ID), or `"similar_to"` (find similar items)
- `seed.value` — the search text, item ID, or reference ID
- `pipeline` — ordered filters applied to results. Two operators: `where` (tag filter, e.g. `{"facts": ["project=myapp"]}`) and `slice` (limit, e.g. `{"limit": 10}`)
- `budget.max_nodes` — maximum evidence items (1–100, default 10)

If you provide `params.text` without a `frame_request`, the runtime builds one for you.

### Resuming a flow

Pass back the `cursor` from the previous response. Program fields (`goal`, `profile`, `steps`) are locked after flow start — use `overrides` to adjust per-tick behavior:

```json
{
  "cursor": "...",
  "overrides": {
    "frame_request": {
      "pipeline": [{"op": "where", "args": {"facts": ["project=myapp"]}}]
    },
    "decision_policy": {
      "margin_high": 0.25
    }
  }
}
```

**`overrides.decision_policy`** — tune the strategy selection thresholds for this tick.

**`decision_override`** — force a specific strategy: `{"strategy": "single_lane_refine", "reason": "I want focused results"}`.

### Optional fields

**`request_id`** — caller-provided trace ID (auto-generated if omitted).

**`idempotency_key`** — enables safe retry. Same key + same payload replays the cached response.

**`response_mode`** — `"standard"` (default) or `"debug"`. Debug mode adds internal state, output hash, and detailed frame diagnostics.

**`work_results`** — return results from delegated work (see [Background work](#background-work) above).

### Response

```json
{
  "request_id": "...",
  "cursor": "opaque token for next tick",
  "status": "done | in_progress | waiting_work | paused | failed",
  "frame": {
    "evidence": [],
    "decision": {}
  },
  "work": [],
  "applied_ops": [],
  "errors": []
}
```

**`cursor`** — pass this back to continue the flow. Encodes flow identity and version; a stale cursor returns a `state_conflict` error.

**`status`** — what to do next:

| Status | Meaning | Action |
|--------|---------|--------|
| `done` | Flow objective complete | You're finished |
| `in_progress` | Runtime has more autonomous work | Call `continue` with the cursor |
| `waiting_work` | Needs external execution | Run the items in `work`, return `work_results` |
| `paused` | Idle too long, escalated | Review and decide whether to continue |
| `failed` | Error | Check `errors` |

**`frame.evidence`** — the items retrieved this tick. Each entry has:

| Field | Description |
|-------|-------------|
| `id` | Item identifier |
| `role` | `"target"` (fetched by ID), `"candidate"` (search result), or `"neighbor"` (via deep/similar) |
| `score` | Similarity score (0–1) |
| `summary` | Item summary |
| `metadata` | Tags, timestamps, lineage — level controlled by `frame_request.options.metadata` |

**`frame.decision`** — the discriminator signals and strategy recommendation for this tick (version `ds.v1`). Key signals:

| Signal | What it measures |
|--------|-----------------|
| `lane_entropy` | Diversity of result scores (low = one clear answer) |
| `top1_top2_margin` | Gap between best and second-best (high = confident) |
| `pivot_coverage_topk` | How well tag filters match top results |
| `temporal_alignment` | How recent the results are |

**`work`** — items the runtime needs executed, each with `work_id`, `kind`, `input`, `output_contract`, and `quality_gates`.

**`applied_ops`** — flat journal of mutations applied this tick:

```json
{
  "source": "inline | work",
  "work_id": "w_abc123 or null",
  "op": "upsert_item | set_tags | set_summary",
  "target": "note id",
  "status": "applied | noop | failed",
  "mutation_id": "m_..."
}
```

### Limits

| Limit | Value |
|-------|-------|
| Payload size | 512 KB |
| State size | 1 MB |
| Work input/result size | 256 KB each |
| Events per flow | 500 |
| Budget max_nodes | 1–100 |
| Idle ticks before pause | 3 (default) |

### Compatibility with existing API

The continuation API is a superset. Existing operations map to single-tick flows:

| Existing call | Equivalent continuation |
|---------------|------------------------|
| `keep get ID` | `continue(frame_request={seed: {mode: "id", value: "ID"}})` |
| `keep find "query"` | `continue(goal: "query", frame_request={seed: {mode: "query", value: "query"}})` |
| `keep put "text"` | `continue(goal: "write", params={content: "text"})` |

The existing `keep_put`, `keep_find`, `keep_get` tools remain available and unchanged. Continuations are an additional interface for workflows that benefit from multi-step interaction.

## See also

- [API-SCHEMA.md](API-SCHEMA.md) — General keep API reference
- [AGENT-GUIDE.md](AGENT-GUIDE.md) — Working session patterns
- [KEEP-MCP.md](KEEP-MCP.md) — MCP server setup
