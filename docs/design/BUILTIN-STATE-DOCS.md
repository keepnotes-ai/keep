# Built-in State Docs

Date: 2026-03-08
Status: Draft
Related:
- `docs/design/STATE-DOC-SCHEMA.md`
- `docs/design/STATE-ACTIONS.md`

## 1) What ships

Three state docs are **live** — wired into the get/put/find paths
and exercised by tests. They implement the core behaviors.

| State doc | Entry for | Wired in |
|-----------|-----------|----------|
| `.state/after-write` | `put` | `flow_engine.py` |
| `.state/get-context` | `get` (display) | `api.py:get_context()` |
| `.state/find-deep` | `find(deep)` | `api.py:_deep_follow_via_flow()` |

Users can fork or override any of them by creating a `.state/*`
note with the same name.

---

## 2) Write path: after-write

Entry point for all `put` calls. Runs post-write processing
(summarize, tag, OCR, analyze) based on item properties.

**Caller**: `Keeper.put()` after storing the item.

```python
continue({
    "state": "after-write",
    "params": {
        "item_id": "%a1b2c3d4e5f6",
        "max_summary_length": config.max_summary_length,  # default 2000
    },
    "budget": {"ticks": 5},
})
```

### .state/after-write

```yaml
# .state/after-write
# match: all
rules:
  - when: "item.content_length > params.max_summary_length && !item.has_summary"
    id: summary
    do: summarize
  - when: "'_ocr_pages' in item.tags && item.has_uri"
    id: extracted
    do: ocr
  - when: "!item.is_system_note"
    id: analyzed
    do: analyze
  - when: "!item.is_system_note"
    id: tagged
    do: tag
post:
  - return: done
```

One state. All matching rules fire in parallel. The runtime
applies mutations (set_summary, set_tags, content update) from
each action's output. Fire-and-forget from the caller's perspective.

**Summarize** fires when item content exceeds the configured
threshold and no summary exists yet.

**OCR** fires when the item was indexed from a file that needs
text extraction (identified by `_ocr_pages` tag from indexing).

**Analyze** fires for non-system items. Decomposes content into
parts using the analysis provider.

**Tag** fires for all non-system items. The `tag` action discovers
`.tag/*` spec docs from the store and applies each matching
taxonomy. See STATE-ACTIONS.md §tag for details.

---

## 3) Read path: get-context

Context assembly for `keep get`. Gathers similar items, parts,
and meta-doc sections in parallel. Edges and version navigation
are resolved inline by the caller.

**Caller**: `Keeper.get_context()` after resolving the target item.

```python
result = run_flow(
    "get-context",
    {
        "item_id": item_id,
        "similar_limit": 3,
        "meta_limit": 3,
        "parts_limit": 100,
    },
    budget=5,
)
# Edges resolved inline (structural DB queries)
# Version nav computed inline (versions_limit=5)
```

### .state/get-context

```yaml
# .state/get-context
# match: all
rules:
  - id: similar
    do: find
    with:
      similar_to: "{params.item_id}"
      limit: "{params.similar_limit}"
  - id: parts
    do: find
    with:
      prefix: "{params.item_id}@p"
      limit: "{params.parts_limit}"
  - id: meta
    do: resolve_meta
    with:
      item_id: "{params.item_id}"
      limit: "{params.meta_limit}"
post:
  - return: done
```

**Similar** (`find` with `similar_to`): semantic search using the
item's stored embedding.

**Parts** (`find` with `prefix`): lists decomposition parts by
ID prefix convention (`id@p{N}`).

**Meta** (`resolve_meta`): resolves `.meta/*` document definitions
against the item's tags. See STATE-ACTIONS.md §resolve_meta.

**Edges** are resolved inline by the caller — they require direct
database queries (inverse edges, explicit edge-tag lookup) that the
generic `traverse` action doesn't support.

**Version navigation** is computed by the caller from the requested
version offset (`versions_limit`, default 5). Structural, not
content-derived.

---

## 4) Find path: find-deep

Deep find chains a flat search with edge traversal. The CEL
predicate `search.count == 0` short-circuits when search returns
nothing.

**Caller**: `Keeper.find(deep=True)`.

```python
result = run_flow(
    "find-deep",
    {"query": "...", "limit": 10, "deep_limit": 5},
    budget=5,
)
```

### .state/find-deep

```yaml
# .state/find-deep
# match: sequence
rules:
  - id: search
    do: find
    with: { query: "{params.query}", limit: "{params.limit}" }
  - when: "search.count == 0"
    return: done
  - id: related
    do: traverse
    with:
      items: "{search.results}"
      limit: "{params.deep_limit}"
  - return: done
```

> **Wiring note:** `_deep_follow_via_flow()` exists in `api.py`
> but is not yet called from `find()`. The existing
> `_deep_tag_follow` path remains active because it processes items
> as a batch (computing shared tag statistics for discriminative
> tags), while the `traverse` action processes items individually.
> Wiring this requires the traverse action to support batch
> tag-follow semantics.

---

## 5) Available statistics

The runtime computes statistics from `find` action results via
`enrich_find_output()` (`result_stats.py`). These are precomputed
and available as `{id}.*` bindings in state doc predicates:

| Statistic | Type | Meaning |
|-----------|------|---------|
| `margin` | float | Score gap between #1 and #2 result |
| `entropy` | float | Score distribution spread (high = scattered) |
| `lineage_strong` | float | Version/part concentration in top-K |
| `dominant_lineage_tags` | dict | Tags from dominant lineage item |
| `top_facet_tags` | dict | Tags from top discriminative facet |

These statistics are computed today for every `find` action in a
flow. They are consumed by `get-context` (via enrichment on the
`similar` binding) but not yet used for decision-making predicates
like `when: "search.margin > 0.18"`.

---

## 6) Summary

| State doc | Actions | Match | Status |
|-----------|---------|-------|--------|
| `.state/after-write` | summarize, tag, ocr, analyze | all | live |
| `.state/get-context` | find, resolve_meta | all | live |
| `.state/find-deep` | find, traverse | sequence | live (not yet called) |

## 7) Resolved questions

- [x] Should version navigation become a state doc rule? **No.**
      Structural, not content-derived. The caller applies
      `versions_limit` (default 5) inline.
- [x] `resolve_meta` action: standalone action or a mode of `find`?
      **Standalone.** Wraps `Keeper.resolve_meta()`. Separate action
      is cleaner.
- [x] Should edges be a state doc rule? **No.** Edge resolution
      requires structural database queries (inverse edges, explicit
      edge-tag lookup) that the generic `traverse` action can't
      replicate. Kept inline in the caller.

## 8) Open questions

- [ ] `traverse` action needs batch tag-follow mode to replace
      `_deep_tag_follow` — discriminative tag stats require seeing
      the full result set, not individual items.
- [ ] Render hints: should the state doc return render hints, or
      should the rendering layer compute them post-flow?

---

## Appendix: query resolution state docs

> **Status: wired but thresholds untuned.** These state docs are
> bundled as `.md` files and loaded into the store. The flow runtime
> evaluates them via `state_doc_runtime.run_flow()`. Thresholds
> come from `flow_policy.DEFAULT_DECISION_POLICY` and have not yet
> been validated against real query patterns.

Three state docs — `query-resolve`, `query-branch`,
`query-explore` — form the multi-step query resolution loop.
They reference threshold params (`margin_high`,
`entropy_low`, etc.) from `flow_policy.py` defaults.

### .state/query-resolve

```yaml
# match: sequence
rules:
  - id: search
    do: find
    with: { query: "{params.query}", limit: "{params.limit}" }
  - when: "search.margin > params.margin_high"
    return: done
  - when: "search.lineage_strong > params.lineage_strong"
    do: find
    with: { query: "{params.query}", tags: "{search.dominant_lineage_tags}", limit: 5 }
    then: query-resolve
  - when: "search.margin < params.margin_low || search.entropy > params.entropy_high"
    then:
      state: query-branch
      with:
        facets_1: "{search.top_facet_tags}"
  - when: "search.entropy < params.entropy_low"
    do: find
    with: { query: "{params.query}", limit: 5 }
    then: query-resolve
  - then: query-explore
```

### .state/query-branch

```yaml
# match: all
rules:
  - id: pivot1
    do: find
    with: { query: "{params.query}", limit: "{params.pivot_limit}" }
  - id: bridge
    do: find
    with: { query: "{params.query}", limit: "{params.bridge_limit}" }
post:
  - when: "pivot1.margin > params.margin_high || bridge.margin > params.margin_high"
    return: done
  - when: "budget.remaining > 0"
    then: query-resolve
  - return:
      status: stopped
      with:
        reason: "ambiguous"
```

### .state/query-explore

```yaml
# match: sequence
rules:
  - id: search
    do: find
    with: { query: "{params.query}", limit: "{params.explore_limit}" }
  - when: "search.margin > params.margin_high"
    return: done
  - when: "budget.remaining > 0"
    do: find
    with: { query: "{params.query}", limit: "{params.explore_limit_wide}" }
    then: query-resolve
  - return: stopped
    with: { reason: "budget" }
```

### Default thresholds

```python
margin_high:    0.18    # confident winner
entropy_low:    0.45    # concentrated results
margin_low:     0.08    # ambiguous
entropy_high:   0.72    # scattered results
lineage_strong: 0.75    # version/part concentration
```

These values come from `flow_policy.DEFAULT_DECISION_POLICY`.
They have not yet been validated against real query patterns.
