# Supernode Review — Background Agent Task

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

Background task that automatically analyzes the supernodes to maintain a useful summary and identify important changes.

`put` a new version of the supernode's content, synthesized from its inbound references. Previous content preserved as a version. Same mechanism as parts-analysis but outward instead of inward.

### Selection: one rule

**Only review items with new inbound references since last review.**

Score = `fan_in × (1 + new_refs)` where `new_refs` = refs created after `_supernode_reviewed` timestamp.

This single condition handles every case:
- Stub with no content, never reviewed → all refs are "new" → eligible
- Git commit with content, no new refs → `new_refs = 0` → skipped forever
- User note that became a hub, 5 new refs → eligible (new evidence)
- Previously-reviewed supernode, nothing changed → skipped

> TODO review this strategy, we must avoid processing "new git commit" supernode that has content and lots of inbound refs - it already has content! refs are new but only because the supernode itself is new! nothing to do here!

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

**1. Context surfacing (meta resolution):** "Show me supernodes relevant to what I'm looking at." This is a regular meta-doc — embedding/FTS search filtered to supernodes. Supernodes make excellent context because their factsheets are dense summaries.

**2. Review scheduling:** "Find supernodes that need refreshing and queue the work." Based on new edges since `_supernode_reviewed`. This is *not* meta resolution — it's a separate trigger that produces work queue tasks.

These are different concerns with different triggers, different costs, and different outputs.

### Context surfacing

A regular meta-doc. Uses `find` with tag/embedding search, like any other `.meta/*` doc:

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

This surfaces supernodes whose factsheets are semantically relevant to the current item. Cheap, inline, no side effects.

### Review scheduling

The review is triggered by the work queue heuristic: **if the queue has no pending supernode-review tasks, try to find candidates. If there are none, queue a task to check again later.**

This is a **general principle for state-doc flows**: when a sequence hits an expensive action (like `generate` or `put`), everything from that point onward is enqueued as background work. The read path stays fast; the write path runs on the daemon. This delegation boundary applies consistently to `generate`, `put`, etc.

### Two-tier processing

**Background LLM (daemon)** produces structured factsheets — mechanical extraction within a 4B model.

**Agent sees deltas** via `.meta/ongoing` — "person@gmail.com has 8 new refs about governance topics." Agent decides whether to engage further.

### State-doc flow

The flow has two phases separated by the delegation boundary:

```yaml
# .state/review-supernodes
match: sequence
rules:
  # --- inline phase (runs during meta resolution) ---
  - id: discover
    do: find_supernodes
    with:
      min_fan_in: "{params.min_fan_in}"
      limit: "{params.limit}"
  - when: "discover.count == 0"
    return: done

  # --- delegated phase (enqueued to work queue) ---
  - id: target
    do: get
    with:
      id: "{discover.results[0].id}"
  - id: inbound
    do: traverse
    with:
      items: ["{discover.results[0].id}"]
      limit: "20"
  - id: description
    do: generate                    # expensive — triggers delegation
    with:
      prompt: "supernode"
      id: "{discover.results[0].id}"
  - id: updated
    do: put
    with:
      id: "{discover.results[0].id}"
      content: "{description.text}"
      tags:
        _supernode_reviewed: "{now}"
  - return: done
```

The delegation boundary is determined by the action: `generate` is marked expensive, so when the flow runtime encounters it, it enqueues the remaining sequence as a work item and returns the bindings accumulated so far (i.e., `discover` results) to the caller.

This delegation boundary is exactly the same for "put", etc - not new.  But consistent.

Usage: `keep_flow(state="review-supernodes", params={min_fan_in: 5, limit: 5}, budget=3)`

### Review trigger: work queue heuristic

The daemon already runs a processing loop. The supernode review slots in as a **low-priority queue replenishment check**:

1. Daemon checks: are there any pending `supernode_review` tasks in the work queue?
2. If no → run `find_supernodes(min_fan_in=5, limit=5)` to discover candidates
3. If candidates found → enqueue each as a `supernode_review` work item (priority 8)
4. If no candidates → do nothing (or schedule a delayed re-check)
5. Daemon processes `supernode_review` items like any other work: get → traverse → generate → put

This is cheap — step 2 is one SQL query. The expensive LLM work only happens in step 5, within the normal work queue processing loop.

**Why separate from meta resolution:**
- Meta resolution is a read path — it should never enqueue work or have side effects
- Review scheduling needs to run even when no agent is active (daemon-driven)
- The two use the same data differently: meta finds *relevant* supernodes, review finds *stale* ones

## Edge cases

### After-write loop from `put`
The review `put` updates the supernode's content, which fires `_dispatch_after_write_flow`. This enqueues summarize, auto_tag, etc. for the supernode. This is **fine** — the factsheet is real content worth indexing. But we must ensure:
- `extract_links` doesn't fire on the factsheet and auto-vivify new stubs (the `_source=link` guard blocks this, and factsheets won't be markdown)
- `analyze` only fires if factsheet > 500 chars (the content-length guard handles this)
- The `put` doesn't trigger *another* supernode review (it's an update to the target, not a new inbound ref — the scoring won't see it as new evidence)

### Self-referential loops
Item A references item B, and item B references item A. Both are supernodes. Reviewing A mentions B, reviewing B mentions A. This is fine — the content is factual ("referenced by B") and the scoring prevents re-review unless genuinely new refs arrive.

### Very high fan-in items
`discuss@example.com` with fan_in=207. The `traverse` limit of 20 means we only sample recent refs. The factsheet says "207 references, most recent 20 shown." Over successive reviews, different samples produce different versions — the version history captures this naturally. Not a bug, a feature.

### Orphaned supernodes
A supernode whose referencing items get deleted. Fan-in drops, eventually below min_fan_in threshold. No longer selected. The old factsheet remains as content — stale but harmless. A future "stale-notes" ongoing task could surface these.

### Concurrent reviews
Two daemon workers process the same supernode simultaneously (race condition in work queue claim). The `supersede_key` mechanism in the work queue prevents this — only one worker claims each item.

### LLM hallucination in factsheets
The small model invents details not in the evidence. Mitigated by:
- Tight prompts ("only state what the evidence supports")
- Structured output format (harder to hallucinate in Name/Role/Topics format)
- Version history — bad factsheets get overwritten on next review
- Agent sees the factsheet in context and can correct

### Cost runaway
Store with 10,000 supernodes, all with new_refs > 0 after a bulk import. Each review costs one LLM call. Mitigated by:
- `budget` parameter caps reviews per invocation
- `min_fan_in` threshold filters low-value nodes
- Work queue priority 8 (low) — runs after all other tasks
- Daemon processes one at a time, never starves real work
- No work is enqueued until an agent actually queries — bulk imports don't trigger a flood

## Related: data-driven provenance via `_mutation_source`

Today `_source` is set ad-hoc by each action (`link`, `auto-vivify`, etc.) and after-write guards hardcode exclusions. A cleaner pattern: **state docs declare `_mutation_source`**, and the flow runtime propagates it into every mutation:

```yaml
# .state/review-supernodes
tags:
  _mutation_source: supernode-review
```

The runtime injects `_source=supernode-review` into every `put` the flow produces. After-write guards become data-driven — no hardcoding in action code.

This unifies all provenance: `extract_links` doesn't hardcode `_source=link` in its mutations; the after-write fragment that dispatches it declares `_mutation_source: extract-links`. Not urgent — current hardcoded guards work — but it's the path to fully configurable after-write dispatch.

## Implementation

1. **`find_supernodes` action** — edge table query; returns items with `new_refs > 0` matching a `.prompt/supernode/*` scope, scored by `fan_in × (1 + new_refs)`
2. **`_resolve_prompt_doc` extension** — add scope-glob matching against item ID
3. **`.prompt/supernode/*` system docs** — ship default + email + url
4. **`review-supernodes` state doc** — get → traverse → generate → put (processes a single candidate)
5. **Daemon queue replenishment** — low-priority check: if no pending `supernode_review` work, run `find_supernodes` and enqueue candidates
6. **`.meta/supernodes`** — regular meta-doc for context surfacing: find relevant supernodes by embedding similarity, filtered to reviewed items
7. **Flow runtime: expensive-action delegation** — when the flow runner encounters an action marked expensive (e.g., `generate`, `put`), enqueue the remaining sequence to the work queue and return accumulated bindings. General mechanism, not supernode-specific.
