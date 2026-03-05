# Continuations

A continuation is a stateful, multi-step interaction with keep's memory store. Instead of separate `get`, `find`, `put` calls, you use one operation:

```
continue(input) -> output
```

Each call is a **tick**. The runtime remembers where the flow is, what evidence has been gathered, and what to do next. You call `continue` again with the same `flow_id` to advance.

## Why continuations exist

Most real questions aren't one-shot. You start broad, inspect results, and refine. Sometimes you need background work (summarize, tag, OCR). Sometimes the caller should decide the next step.

Continuations make this normal workflow first-class:

1. **Better retrieval** — broad-then-refine is built in, not hand-rolled.
2. **Mixed intelligence** — cheap rules handle steering, small models handle mutation, stronger agents handle decisions. All behind one contract.
3. **One API surface** — the same loop shape works for queries, ingestion, and delegated work.
4. **Bounded execution** — every flow has a budget and idle-tick limit. No unbounded loops.

This interface is a preview, and may change; feedback is encouraged.

## Interface

### CLI

```bash
keep continue '{"goal": "query", "frame_request": {"seed": {"mode": "query", "value": "open commitments"}}}'
keep continue @input.json          # read from file
echo '{}' | keep continue -        # read from stdin
keep continue-work FLOW_ID WORK_ID # execute a pending work item
```

Output is JSON. Add `--json` for compact single-line output.

### Python

```python
from keep import Keeper

kp = Keeper()
result = kp.continue_flow({
    "goal": "query",
    "frame_request": {
        "seed": {"mode": "query", "value": "open commitments"},
    },
})
print(result["flow_id"])           # use this to continue
print(result["status"])            # "done", "waiting_work", "paused", "failed"
```

### REST (hosted)

```
POST /v1/continue         — run a tick
POST /v1/continue/work    — execute a work item
```

---

## Quick start examples

### 1. Simple query

Search memory and get results in one tick:

```json
{
  "goal": "query",
  "frame_request": {
    "seed": {"mode": "query", "value": "authentication patterns"},
    "pipeline": [{"op": "slice", "args": {"limit": 5}}]
  }
}
```

Response (abbreviated):

```json
{
  "flow_id": "f_a1b2c3d4e5",
  "state_version": 1,
  "status": "done",
  "frame": {
    "slots": {"flow_id": "f_a1b2c3d4e5", "goal": "query", "query": "authentication patterns"},
    "views": {
      "task": "Goal: query (query=authentication patterns)",
      "evidence": [
        {"id": "%abc123", "role": "candidate", "score": 0.87, "summary": "OAuth2 PKCE flow for public clients..."},
        {"id": "%def456", "role": "candidate", "score": 0.72, "summary": "Token refresh needs clock sync..."}
      ],
      "discriminators": {"policy_hint": {"strategy": "single_lane_refine"}}
    }
  },
  "next": {"recommended": "stop"}
}
```

### 2. Get a specific item

```json
{
  "frame_request": {
    "seed": {"mode": "id", "value": "%a1b2c3"}
  }
}
```

### 3. Continue a flow

Take the `flow_id` and `state_version` from a previous response and call again:

```json
{
  "flow_id": "f_a1b2c3d4e5",
  "state_version": 1
}
```

The runtime re-evaluates evidence and advances the flow. If it has auto-query refinement active (profile `query.auto`), it will refine the search automatically.

### 4. Write an item

```json
{
  "goal": "write",
  "params": {
    "id": "my-note",
    "content": "OAuth2: always use PKCE for public clients"
  }
}
```

This creates or updates the item via a typed `upsert_item` mutation. Status will be `"done"` after one tick.

### 5. Query with tag filter

```json
{
  "goal": "query",
  "frame_request": {
    "seed": {"mode": "query", "value": "deployment issues"},
    "pipeline": [
      {"op": "where", "args": {"facts": ["project=myapp"]}},
      {"op": "slice", "args": {"limit": 10}}
    ]
  }
}
```

### 6. Find similar items

```json
{
  "frame_request": {
    "seed": {"mode": "similar_to", "value": "%a1b2c3"},
    "pipeline": [{"op": "slice", "args": {"limit": 5}}]
  }
}
```

### 7. Auto-refining query

Use the `query.auto` profile for multi-tick search that refines itself:

```json
{
  "goal": "query",
  "profile": "query.auto",
  "params": {"text": "what authentication approach should we use?"},
  "frame_request": {
    "seed": {"mode": "query", "value": "authentication approach"},
    "pipeline": [{"op": "slice", "args": {"limit": 10}}]
  }
}
```

The runtime will:
1. Execute the initial query
2. Analyze result confidence and diversity
3. Choose a refinement strategy (refine, branch, or explore)
4. Return `"next": {"recommended": "continue"}` if more ticks would help
5. On subsequent `continue` calls, automatically apply the chosen strategy

### 8. Delegated work (ingest with steps)

```json
{
  "goal": "ingest",
  "params": {"id": "my-doc"},
  "steps": [
    {"kind": "summarize", "runner": {"type": "provider.summarize"}},
    {"kind": "tag", "runner": {"type": "provider.tag"}}
  ]
}
```

The runtime will:
1. Return `status: "waiting_work"` with work requests in `requests.work`
2. You (or the runtime) execute each work item
3. Feed results back via `feedback.work_results`
4. The runtime applies mutations and advances

---

## Input schema

```json
{
  "request_id":       "string (optional, auto-generated UUID)",
  "idempotency_key":  "string (optional, enables safe retry)",
  "flow_id":          "string (omit to start new flow)",
  "state_version":    "int (required when resuming a flow)",

  "goal":             "string (e.g. 'query', 'write', 'ingest')",
  "profile":          "string (e.g. 'query.auto')",
  "params":           {},
  "frame_request":    {},
  "steps":            [],
  "decision_policy":  {},
  "decision_override": {},
  "feedback":         {"work_results": []}
}
```

### Starting a new flow

Omit `flow_id`. Provide at least one of: `goal`, `profile`, `steps`, or `frame_request`.

### Resuming a flow

Provide `flow_id` and `state_version` (from the previous response). You may also provide new fields to merge into the program.

### Field details

#### `frame_request`

Controls what evidence is retrieved on this tick.

```json
{
  "seed": {
    "mode": "query",
    "value": "search text"
  },
  "pipeline": [
    {"op": "where", "args": {"facts": ["project=myapp", "status=open"]}},
    {"op": "slice", "args": {"limit": 10}}
  ],
  "budget": {"max_nodes": 20},
  "options": {"deep": false, "metadata": "basic"}
}
```

| Field | Description |
|-------|-------------|
| `seed.mode` | `"query"` (semantic search), `"id"` (fetch by ID), or `"similar_to"` (find similar) |
| `seed.value` | Search text, item ID, or reference ID depending on mode |
| `pipeline` | Ordered list of operators applied to results |
| `budget.max_nodes` | Maximum evidence items (1-100, default 10) |
| `options.deep` | Follow tags/edges to discover related items |
| `options.metadata` | Level of metadata per evidence item: `"none"`, `"basic"`, or `"rich"` |

**Pipeline operators:**

| Operator | Args | Effect |
|----------|------|--------|
| `where` | `{"facts": ["key=value", ...]}` | Pre-filter results by tag constraints |
| `slice` | `{"limit": N}` | Limit number of results |

Only `where` and `slice` are supported. Unknown operators return an `invalid_frame_operator` error.

#### `params`

Goal-specific parameters. Common fields:

| Field | Used by | Description |
|-------|---------|-------------|
| `text` | query goals | Search text (used if no explicit `frame_request`) |
| `id` | write/ingest | Target item ID |
| `content` | write | Content to write |

#### `decision_override`

Force a specific strategy for this tick:

```json
{
  "strategy": "single_lane_refine",
  "reason": "I want focused results"
}
```

Strategies: `single_lane_refine`, `top2_plus_bridge`, `explore_more`. See [Decision support](#decision-support) below.

#### `feedback.work_results`

Return results from delegated work:

```json
{
  "work_results": [
    {
      "work_id": "w_abc123",
      "status": "ok",
      "outputs": {"summary": "Generated summary text..."},
      "quality": {"confidence": 0.9, "passed_gates": true}
    }
  ]
}
```

---

## Output schema

```json
{
  "request_id":       "echoed or generated UUID",
  "idempotency_key":  "echoed if provided",
  "flow_id":          "f_xxxxxxxxxxxx",
  "state_version":    1,
  "status":           "done | waiting_work | paused | failed",

  "frame":            {},
  "requests":         {"work": []},
  "applied":          {"work_ops": []},
  "state":            {},
  "next":             {"recommended": "continue | stop", "reason": "..."},
  "errors":           [],
  "output_hash":      "sha256 hex"
}
```

### `status`

| Status | Meaning | What to do |
|--------|---------|------------|
| `done` | Flow completed this tick | Check `next.recommended` — may suggest continuing |
| `waiting_work` | Work items need execution | Execute items in `requests.work`, feed back via `feedback.work_results` |
| `paused` | Idle too long, escalated | Review the escalation, decide whether to continue or stop |
| `failed` | Error occurred | Check `errors` for details |

### `frame`

The frame is the primary payload — it contains what the runtime found and computed.

```json
{
  "slots": {
    "flow_id": "f_abc",
    "note_id": "my-doc",
    "goal": "query",
    "stage": "tick",
    "query": "search text"
  },
  "views": {
    "task": "Goal: query (query=search text)",
    "evidence": [],
    "hygiene": [],
    "discriminators": {}
  },
  "budget_used": {"tokens": 0, "nodes": 5},
  "status": "done"
}
```

#### `frame.slots`

Metadata about the current tick. Not typically consumed directly — use `views` for actionable data.

#### `frame.views.task`

Human-readable description of what this tick is doing.

#### `frame.views.evidence`

Array of items retrieved for this tick. Each evidence item:

```json
{
  "id": "%a1b2c3",
  "role": "candidate",
  "score": 0.87,
  "summary": "OAuth2 PKCE flow for public clients...",
  "metadata": {
    "level": "basic",
    "base_id": "%a1b2c3",
    "source": "inline",
    "created": "2026-01-15T10:30:00",
    "updated": "2026-02-01T14:22:00",
    "tags": {"project": "myapp", "topic": "auth"},
    "total_parts": 3,
    "focus": {"version": "0", "part": ""}
  }
}
```

| Field | Description |
|-------|-------------|
| `id` | Item identifier |
| `role` | `"target"` (fetched by ID), `"candidate"` (search result), or `"neighbor"` (deep/similar) |
| `score` | Similarity score (0-1). Higher = more relevant |
| `summary` | Item summary text |
| `metadata.level` | `"none"`, `"basic"`, or `"rich"` — controls how much metadata is included |
| `metadata.tags` | User tags (system tags excluded) |
| `metadata.focus` | Which version and part this evidence represents |

When `metadata` level is `"rich"`, additional fields appear:

```json
{
  "metadata": {
    "links": {
      "similar": [{"id": "%x", "score": 0.8, "date": "2026-01-14"}],
      "meta_sections": ["todo", "learnings"],
      "edges": {"said": ["%conv1"]}
    },
    "structure": {
      "parts": 3,
      "prev_versions": 2,
      "next_versions": 0
    }
  }
}
```

#### `frame.views.discriminators`

Decision support signals computed from the evidence. See [Decision support](#decision-support).

### `requests.work`

Work items the runtime wants executed. Each entry:

```json
{
  "work_id": "w_abc123",
  "kind": "summarize",
  "executor_class": "local",
  "suggested_executor_id": "",
  "input": {"id": "my-doc"},
  "output_contract": {},
  "quality_gates": {"min_confidence": 0.0, "citation_required": false},
  "escalate_if": []
}
```

To execute work locally: `keep continue-work FLOW_ID WORK_ID`

To execute work and return results:
```json
{
  "flow_id": "f_abc",
  "state_version": 2,
  "feedback": {
    "work_results": [{
      "work_id": "w_abc123",
      "status": "ok",
      "outputs": {"summary": "..."},
      "quality": {"confidence": 0.9, "passed_gates": true}
    }]
  }
}
```

### `applied.work_ops`

Mutations that were applied this tick.

- Inline flow mutations (for example `goal: "write"`) are flat entries with fields like `op`, `target`, `status`, `mutation_id`.
- Work-result mutations are per-work envelopes like:

```json
{
  "work_id": "w_abc123",
  "status": "applied",
  "ops": {
    "ops": [
      {"op": "set_summary", "target": "my-doc", "status": "applied", "mutation_id": "m_..."}
    ]
  }
}
```

The mutation `op` values inside those entries are:

| Op | Fields | Effect |
|----|--------|--------|
| `upsert_item` | `target`, `content`, `tags`, `summary` | Create or update an item |
| `set_tags` | `target`, `tags` | Merge tags onto an item |
| `set_summary` | `target`, `summary` | Update an item's summary |

Only these three mutation types exist. No arbitrary code execution.

### `state`

Internal flow state. You generally don't need to inspect this, but key fields:

```json
{
  "cursor": {"step": 1},
  "frontier": {"evidence_ids": ["...", "..."]},
  "pending": {"work_ids": []},
  "budget_used": {"tokens": 0, "nodes": 5},
  "termination": {"idle_ticks": 0, "max_idle_ticks": 3}
}
```

- **`cursor.step`** — how many ticks have occurred
- **`termination.idle_ticks`** — consecutive ticks with no progress. At `max_idle_ticks` (default 3), flow pauses with an escalation

### `next`

```json
{"recommended": "continue", "reason": "auto-query branch plan pending"}
```

| Value | Meaning |
|-------|---------|
| `"continue"` | More ticks would help. Call `continue` again with the same `flow_id` |
| `"stop"` | Flow is complete or no further progress expected |

### `errors`

Array of error objects:

```json
[{"code": "state_conflict", "message": "Expected version 2, got 3"}]
```

Common error codes: `invalid_input`, `missing_required_field`, `state_conflict`, `unknown_profile`, `invalid_frame_operator`, `forbidden_target`, `payload_too_large`, `mutation_failed`, `frame_evidence_error`.

---

## Decision support

Each tick, the runtime analyzes the evidence and publishes decision signals in `frame.views.discriminators`. These help the caller (or the runtime itself, in auto mode) decide what to do next.

### Discriminator payload

```json
{
  "version": "ds.v1",
  "planner_priors": {
    "fanout": {},
    "selectivity": {},
    "cardinality": {}
  },
  "query_stats": {
    "lane_entropy": 0.65,
    "top1_top2_margin": 0.12,
    "pivot_coverage_topk": 0.4,
    "expansion_yield_prev_step": 0.0,
    "cost_per_gain_prev_step": 0.0,
    "temporal_alignment": 0.8
  },
  "lineage": {
    "version": {
      "coverage_topk": 0.8,
      "dominant_concentration_topk": 0.6,
      "dominant": "0",
      "distinct_topk": 3
    },
    "part": {
      "coverage_topk": 0.5,
      "dominant_concentration_topk": 0.4,
      "dominant": "",
      "distinct_topk": 2
    }
  },
  "tag_profile": {
    "edge_key_count": 1,
    "facet_key_count": 3,
    "edge_keys": ["said"],
    "facet_keys": ["project", "topic", "status"]
  },
  "policy_hint": {
    "strategy": "explore_more",
    "reason_codes": ["mixed_signal"]
  },
  "staleness": {
    "stats_age_s": 120,
    "fallback_mode": false
  }
}
```

### Signal descriptions

**`query_stats`** — computed from this tick's evidence:

| Signal | Range | Meaning |
|--------|-------|---------|
| `lane_entropy` | 0-1 | Diversity of result scores. Low = one clear answer. High = many equally relevant results |
| `top1_top2_margin` | 0-1 | Gap between best and second-best score. High = confident top result |
| `pivot_coverage_topk` | 0-1 | Fraction of top results matching `where` tag filters |
| `expansion_yield_prev_step` | 0-1 | How many new results appeared since last tick |
| `cost_per_gain_prev_step` | 0+ | Results per newly discovered item (efficiency of previous step) |
| `temporal_alignment` | 0-1 | Fraction of results that are recent (within ~1 year) |

**`lineage`** — clustering of results by version and part lineage:

| Signal | Meaning |
|--------|---------|
| `coverage_topk` | Fraction of top results that have lineage metadata |
| `dominant_concentration_topk` | How concentrated results are in one lineage group |
| `dominant` | The dominant lineage value |
| `distinct_topk` | Number of distinct lineage values |

**`tag_profile`** — structural classification of tags in evidence:

| Signal | Meaning |
|--------|---------|
| `edge_keys` | Tags that represent directional relationships (have `_inverse` in their `.tag/` doc) |
| `facet_keys` | Tags used for grouping/filtering (everything else) |

**`planner_priors`** — precomputed statistics about the store:

| Signal | Meaning |
|--------|---------|
| `fanout` | How many branches each tag key creates |
| `selectivity` | How many items match typical constraints |
| `cardinality` | Total item counts |

### Strategies

The runtime uses these signals to suggest (or auto-apply) one of three strategies:

| Strategy | When | What it does |
|----------|------|--------------|
| `single_lane_refine` | High margin + low entropy, or strong lineage concentration | Narrow search: one constrained refine from the dominant result |
| `top2_plus_bridge` | Low margin or high entropy | Hedge: explore two pivot candidates plus a bridge query, score each, pick the best |
| `explore_more` | Mixed signals | Broaden: expand the evidence, then re-evaluate |

**Selection rules** (with default thresholds):

```
margin = top1_top2_margin
entropy = lane_entropy
lineage_dom = max(version dominant concentration, part dominant concentration)

single_lane_refine:   margin >= 0.18 AND entropy <= 0.45
                  OR: lineage_dom >= 0.75 AND entropy < 0.72

top2_plus_bridge:     margin <= 0.08 OR entropy >= 0.72

explore_more:         everything else
```

You can override the strategy per-tick with `decision_override`, or tune thresholds with `decision_policy`.

---

## Auto-query refinement

When using `profile: "query.auto"`, the runtime automatically plans and executes multi-tick searches:

**Tick 1:** Execute initial query, compute discriminators, choose strategy.

**Tick 2+** (if `next.recommended == "continue"`):
- **`single_lane_refine`:** Builds a constrained query from the dominant result's tags. One refine tick, then done.
- **`top2_plus_bridge`:** Generates 2 pivot queries + 1 bridge query. Evaluates each over subsequent ticks. Picks the best by utility score. Applies a final refine.
- **`explore_more`:** Broadens the search, then re-evaluates signals.

The caller's job is simple: keep calling `continue` with the `flow_id` until `next.recommended == "stop"`.

```python
result = kp.continue_flow({
    "goal": "query",
    "profile": "query.auto",
    "params": {"text": "authentication"},
    "frame_request": {"seed": {"mode": "query", "value": "authentication"}}
})

while result.get("next", {}).get("recommended") == "continue":
    result = kp.continue_flow({
        "flow_id": result["flow_id"],
        "state_version": result["state_version"],
    })

# result now has the best evidence after automatic refinement
for item in result["frame"]["views"]["evidence"]:
    print(f'{item["score"]:.2f}  {item["summary"]}')
```

---

## Work delegation

For tasks that need background processing (summarization, tagging, content extraction), the flow can delegate work:

### Flow with steps

```json
{
  "goal": "ingest",
  "params": {"id": "my-doc"},
  "steps": [
    {
      "kind": "summarize",
      "runner": {"type": "provider.summarize"},
      "output_contract": {"summary": "string"},
      "apply": {
        "ops": [{"op": "set_summary", "target": "$input.item_id", "summary": "$output.summary"}]
      }
    }
  ]
}
```

### Work lifecycle

1. Runtime returns `status: "waiting_work"` with `requests.work` listing pending items
2. Execute each work item (locally with `keep continue-work`, or externally)
3. Return results:

```json
{
  "flow_id": "f_abc",
  "state_version": 2,
  "feedback": {
    "work_results": [{
      "work_id": "w_xyz",
      "status": "ok",
      "outputs": {"summary": "A concise summary of the document"},
      "quality": {"confidence": 0.95, "passed_gates": true}
    }]
  }
}
```

4. Runtime applies mutations (e.g., `set_summary`) and advances

### Mutation types

All mutations are typed — no arbitrary code execution:

| Op | Required fields | Effect |
|----|----------------|--------|
| `upsert_item` | `target`, `content` | Create or update item. Optional: `tags`, `summary` |
| `set_tags` | `target`, `tags` | Merge tags onto existing item |
| `set_summary` | `target`, `summary` | Update item summary |

Mutations use `$output.field` and `$input.field` references to bind values from work results.

---

## Concurrency and safety

### Optimistic locking

Every flow has a `state_version` that increments on each tick. When resuming, you must provide the version from the previous response. Mismatch returns `state_conflict`.

### Idempotency

If you provide an `idempotency_key`, replaying the same request returns the cached response. Same key with different payload is an error.

### Mutation deduplication

Mutation IDs are content-addressed (SHA256 of flow + work + op). The same mutation is never applied twice.

### Limits

| Limit | Value |
|-------|-------|
| Payload size | 512 KB |
| State size | 1 MB |
| Work input size | 256 KB |
| Work result size | 256 KB |
| Events per flow | 500 |
| Budget max_nodes | 1-100 |
| Idle ticks before pause | 3 (default) |

### Scope

Flows are backend-scoped. A flow created locally must be resumed locally. A flow created on the hosted service must be resumed there. Cross-system mixing is not supported.

---

## Compatibility with existing API

The continuation API is a superset. Existing operations map to single-tick flows:

| Existing call | Equivalent continuation |
|---------------|------------------------|
| `keep get ID` | `continue(frame_request={seed: {mode: "id", value: "ID"}})` |
| `keep find "query"` | `continue(goal: "query", frame_request={seed: {mode: "query", value: "query"}})` |
| `keep put "text"` | `continue(goal: "write", params={id: "%<content-hash>", content: "text"})` |
| Ingest URL | `continue(goal: "ingest", params={id: "url"}, steps: [...])` — multi-tick |

The existing `keep_put`, `keep_find`, `keep_get` tools remain available and unchanged. Continuations are an additional interface for flows that benefit from multi-step interaction.

---

## See Also

- [API-SCHEMA.md](API-SCHEMA.md) — General keep API reference (all 9 tools)
- [AGENT-GUIDE.md](AGENT-GUIDE.md) — Working session patterns
- [KEEP-MCP.md](KEEP-MCP.md) — MCP server setup
