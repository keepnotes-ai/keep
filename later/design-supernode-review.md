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

### Two-tier processing

**Background LLM (daemon)** produces structured factsheets — mechanical extraction within a 4B model.

**Agent sees deltas** via `.meta/ongoing` — "person@gmail.com has 8 new refs about governance topics." Agent decides whether to engage further.

### State-doc flow

```yaml
# .state/review-supernodes
match: sequence
rules:
  - id: discover
    do: find_supernodes
    with:
      min_fan_in: "{params.min_fan_in}"
      limit: "{params.limit}"
  - when: "discover.count == 0"
    return: done
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
    do: generate
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

Usage: `keep_flow(state="review-supernodes", params={min_fan_in: 5, limit: 5}, budget=3)`

### Surfacing

```yaml
# .meta/ongoing/supernode-review
query:
  state: review-supernodes
  params: { min_fan_in: 5, limit: 3 }
template: |
  {count} supernodes have new inbound references.
  Top: {results[0].id} ({results[0].new_refs} new)
```

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

## Future: data-driven provenance via `_mutation_source`

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
3. **`review-supernodes` state doc** — find → get → traverse → generate → put
4. **`.prompt/supernode/*` system docs** — ship default + email + url
5. **`.meta/ongoing/supernode-review`** — surfaces deltas in session context
