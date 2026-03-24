# Supernode Review — Background Daemon Task

## Problem

High-cardinality nodes ("supernodes") accumulate inbound references over time — email addresses, URLs, files, projects. Their descriptions are often stubs or stale. Periodically re-describing them from inbound evidence keeps the knowledge graph useful.

## Evidence from a real store (6,662 items)

| Type | Example | Fan-in | Evidence |
|------|---------|--------|----------|
| Mailing list | `discuss@example.com` | 207 | Subject lines, participants, topics |
| Person | `person@gmail.com` | 29 | Sent emails reveal role, interests |
| File | `pyproject.toml` | 128 | Git commits describe changes |
| URL | `https://example.com/` | 53 | Referencing items provide context |

## Design

### Core idea

Background task that automatically analyzes supernodes to maintain useful summaries. `put` a new version of the supernode's content, synthesized from its inbound references. Previous content preserved as a version.

### Selection

**Only review items that are stubs or have new inbound references since last review.**

Score = `fan_in × (1 + new_refs)` where `new_refs` = refs created after `_supernode_reviewed` timestamp.

**Guard against overwriting real content:** Only eligible if:
- Item is a stub (`_source=link` or `_source=auto-vivify`), OR
- Item has been previously reviewed (`_supernode_reviewed` tag exists)

This prevents reviewing freshly-ingested items that already have meaningful content (e.g., git commits with many inbound refs). Items with real authored content are never overwritten by a factsheet.

Cases:
- Stub with no content, never reviewed → all refs are "new" → eligible (stub guard passes)
- Git commit with content, never reviewed → skipped (has real content, not a stub)
- User note that became a hub, previously reviewed, 5 new refs → eligible (reviewed guard passes)
- Previously-reviewed supernode, nothing changed → `new_refs = 0` → skipped

### Prompt-driven eligibility

`find_supernodes` only returns items matching the scope of a `.prompt/supernode/*` doc. Prompts are system docs with a `scope` glob tag matched against the item ID. Most-specific scope wins.

**Shipped prompts:**
```
.prompt/supernode/default     scope: *           — fallback
.prompt/supernode/email       scope: *@*         — email addresses
.prompt/supernode/url         scope: http*://*   — URLs
```

**User-extensible:** add `.prompt/supernode/corp-people` with `scope: *@mycorp.com`.

**Prompt sizing** (~90 tokens system + ~1000 tokens evidence with 20 refs): fits comfortably in 4K context, let alone 16K.

**Infrastructure**: `_resolve_prompt_doc` needs a small extension for scope-glob matching against item ID (alongside existing tag-based matching). `resolve_prompt` on action context already exists.

### Two separate concerns

**1. Context surfacing (meta resolution):** "Show me supernodes relevant to what I'm looking at." A regular `.meta/*` state doc using `find` with `similar_to` + tags. Cheap, inline, no side effects.

**2. Review scheduling (daemon-driven):** "Find supernodes that need refreshing and queue the work." Based on new edges since `_supernode_reviewed`. Runs in the daemon's processing loop, not triggered by meta resolution.

### Context surfacing

A regular meta-doc state doc:

```yaml
# .meta/supernodes
match: sequence
rules:
  - id: relevant
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {_supernode_reviewed: "*"}
      limit: "{params.limit}"
```

Surfaces supernodes whose factsheets are semantically relevant to the current item. Note: tag wildcard matching (`"*"`) may need a tag-exists filter in the `find` action rather than literal value matching.

### Review trigger: daemon queue replenishment

The daemon already runs a processing loop. Supernode review slots in as a **low-priority queue replenishment check**:

1. Daemon checks: are there any pending `supernode_review` tasks in the work queue?
2. If no → run `find_supernodes(min_fan_in=5, limit=5)` to discover candidates
3. If candidates found → enqueue each as a `supernode_review` work item (priority 8)
4. If no candidates → do nothing (or schedule a delayed re-check)
5. Daemon processes each item: get → traverse → generate → put

This is cheap — step 2 is one SQL query. The expensive LLM work only happens in step 5, within the normal work queue processing loop.

**Why separate from meta resolution:**
- Meta resolution is a read path — no side effects
- Review scheduling runs even when no agent is active (daemon-driven)
- The two use the same data differently: meta finds *relevant* supernodes, review finds *stale* ones

### Two-tier processing

**Background LLM (daemon)** produces structured factsheets — mechanical extraction within a 4B model.

**Agent sees results** via `.meta/supernodes` — relevant factsheets surface in context. Agent decides whether to engage further.

### State-doc flow

Each work item processes a single candidate:

```yaml
# .state/review-supernodes
match: sequence
rules:
  - id: target
    do: get
    with:
      id: "{params.item_id}"
  - id: inbound
    do: traverse
    with:
      items: ["{params.item_id}"]
      limit: "20"
  - id: description
    do: generate
    with:
      prompt: "supernode"
      id: "{params.item_id}"
  - id: updated
    do: put
    with:
      id: "{params.item_id}"
      content: "{description.text}"
      tags:
        _supernode_reviewed: "{now}"
  - return: done
```

The daemon enqueues one work item per candidate with `params.item_id` set. The flow runs with `foreground=False` (daemon context), so `generate` (async) executes inline. The `put` creates a new version — previous content is preserved in version history.

## Edge cases

### After-write loop from `put`
The review `put` updates the supernode's content, which fires `_dispatch_after_write_flow`. This enqueues summarize, auto_tag, etc. for the supernode. This is **fine** — the factsheet is real content worth indexing. But:
- `extract_links` doesn't fire on the factsheet (the `_source=link` guard blocks this, and factsheets won't be markdown)
- `analyze` only fires if factsheet > 500 chars (the content-length guard handles this)
- The `put` doesn't trigger *another* supernode review — the scoring only looks for *new inbound refs* since `_supernode_reviewed`, and updating the supernode itself doesn't create new inbound refs

### Self-referential loops
Item A references item B, and item B references item A. Both are supernodes. Reviewing A mentions B, reviewing B mentions A. This is fine — the content is factual ("referenced by B") and the scoring prevents re-review unless genuinely new refs arrive.

### Very high fan-in items
`discuss@example.com` with fan_in=207. The `traverse` limit of 20 means we only sample recent refs. The factsheet says "207 references, most recent 20 shown." Over successive reviews, different samples produce different versions — the version history captures this naturally.

### Orphaned supernodes
A supernode whose referencing items get deleted. Fan-in drops, eventually below min_fan_in threshold. No longer selected. The old factsheet remains as content — stale but harmless.

### Concurrent reviews
Two daemon workers process the same supernode simultaneously. The `supersede_key` mechanism in the work queue prevents this — only one worker claims each item.

### LLM hallucination in factsheets
Mitigated by:
- Tight prompts ("only state what the evidence supports")
- Structured output format (harder to hallucinate in Name/Role/Topics format)
- Version history — bad factsheets get overwritten on next review
- Agent sees the factsheet in context and can correct

### Cost runaway
Store with 10,000 supernodes, all with new_refs > 0 after a bulk import. Mitigated by:
- `budget` parameter caps reviews per daemon cycle
- `min_fan_in` threshold filters low-value nodes
- Work queue priority 8 (low) — runs after all other tasks
- Daemon processes one at a time, never starves real work
- Queue replenishment is rate-limited by the daemon's processing loop

## Future: data-driven provenance via `_mutation_source`

Today `_source` is set ad-hoc by each action (`link`, `auto-vivify`, etc.) and after-write guards hardcode exclusions. A cleaner future pattern: **state docs declare `_mutation_source`**, and the flow runtime propagates it into every mutation. After-write guards become data-driven — no hardcoding in action code. Not urgent — current guards work and the supernode loop is broken by scoring, not provenance.

## Implementation

**Done (v0.107–v0.108):**
- [x] Flow runtime: async action delegation via cursor (v0.107.0)
- [x] After-write dispatch via single flow work item (v0.107.0)
- [x] Meta-docs as state docs with `find(similar_to=..., tags=...)` (v0.108.0)

**Remaining:**
1. **`find_supernodes` action** — edge table query; returns items with `new_refs > 0` matching a `.prompt/supernode/*` scope, scored by `fan_in × (1 + new_refs)`, guarded against overwriting real content
2. **`_resolve_prompt_doc` extension** — scope-glob matching against item ID
3. **`.prompt/supernode/*` system docs** — ship default + email + url
4. **`review-supernodes` state doc** — get → traverse → generate → put (single candidate)
5. **Daemon queue replenishment** — low-priority check in daemon loop
6. **`.meta/supernodes`** — regular meta-doc for context surfacing
