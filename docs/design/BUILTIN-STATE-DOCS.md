# Built-in State Docs

Date: 2026-03-08
Status: Draft
Related:
- `docs/design/STATE-DOC-SCHEMA.md`
- `docs/design/STATE-ACTIONS.md`

## 1) What ships

These are the state docs that ship as `category: system` notes. They
implement the core `put`, `get`, and `find` paths. Users can fork or
override them.

---

## 2) Write path: after-write

Entry point for all `put` calls. Runs post-write processing
(summarize, tag, OCR) based on item properties. The state doc
determines what runs — the caller passes only the item ID and
config thresholds.

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

No `processing` flags. The state doc's `when:` predicates inspect
the item and decide what to do. Users customize behavior by
editing the state doc (add/remove/reorder rules), not by passing
flags from the caller.

### .state/after-write

```yaml
# .state/after-write
# match: all
rules:
  - when: "item.content_length > params.max_summary_length && !item.has_summary"
    id: summary
    do: summarize
  - when: "item.has_tag('_ocr_pages') && item.has_uri"
    id: extracted
    do: ocr
  - when: "!item.has_tag('category', 'system')"
    id: tagged
    do: tag
post:
  - return: done
```

One state. All matching rules fire in parallel. The runtime
applies mutations (set_summary, set_tags, content update) from
each action's output. The caller gets `{status: "stopped",
reason: "background", flow_id: "..."}` immediately —
fire-and-forget.

**Summarize** fires when item content exceeds the configured
threshold and no summary exists yet. The threshold comes from
`config.max_summary_length` via params.

**OCR** fires when the item was indexed from a file that needs
text extraction (identified by `_ocr_pages` tag from indexing).

**Tag** fires for all non-system items. The `tag` action discovers
`.tag/*` spec docs from the store and applies each matching
taxonomy. What gets tagged (and how) is determined by which
`.tag/*` specs exist — not by caller flags. Users add new tagging
by creating spec docs (e.g. `.tag/sentiment/*`), remove tagging
by deleting or disabling specs, and customize existing tagging by
editing the spec doc content. See STATE-ACTIONS.md §tag for
details.

---

## 3) Query path: get

Entry point for `keep get`. Two phases: **resolution** (find the
target item) and **context assembly** (gather related items for
display). Both are state docs — both customizable.

Direct ID gets (`keep get %abc`) skip resolution and go straight to
context assembly.

### How the caller initiates

```python
# In Keeper.get_context():
def get_context(self, id, **kwargs):
    # 1. Resolve target (may use query-resolve flow for queries)
    item_id = self._resolve_target(id)

    # 2. Assemble context via state doc
    result = continue({
        "state": "get-context",
        "params": {
            "item_id": item_id,
            "similar_limit": kwargs.get("similar_limit", 3),
            "meta_limit": kwargs.get("meta_limit", 3),
        },
        "budget": {"ticks": 5},
    })

    # 3. result contains similar, parts, edges, meta
    return ItemContext.from_flow_result(result)
```

When the get is query-based, resolution runs first:

```python
# Query-based get — resolve, then assemble context
target = continue({
    "state": "query-resolve",
    "params": {
        "query": "authentication patterns",
        "limit": 10,
        # Thresholds from config — see continuation_policy.py
        **config.continuation.thresholds,
    },
    "budget": {"ticks": 5},
})

# target.status == "done" → pass target.results[0].id to get-context
```

### .state/get-context

Assembles display context for a resolved item. Four parallel queries
— all sync store operations, ~10ms total, zero persistence.

Users fork this to customize what context appears: remove similar
items, add custom sections, change limits.

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
      limit: 100
  - id: edges
    do: traverse
    with:
      items: ["{params.item_id}"]
  - id: meta
    do: resolve_meta
    with:
      item_id: "{params.item_id}"
      limit: "{params.meta_limit}"
post:
  - return: done
```

**Similar** (`find` with `similar_to`): semantic search using the
item's stored embedding. Returns items with highest cosine
similarity, deduplicated to distinct base documents.

**Parts** (`find` with `prefix`): lists the item's decomposition
parts by ID prefix convention (`id@p{N}`).

**Edges** (`traverse`): follows both explicit edges (tag values
referencing other items) and inverse edges (items whose tags point
to this item). This is the logic previously hidden inside `deep`
mode — now explicitly customizable. Remove this rule to skip
edge display; increase the limit for deeper traversal.

**Meta** (`resolve_meta`): resolves `.meta/*` document definitions
against the current item. Each meta-doc defines tag-based queries;
the action evaluates them against the item's tags and returns
matching items grouped by meta-doc name. Wraps
`Keeper.resolve_meta()`. New action — see STATE-ACTIONS.md.

Version navigation is computed by the runtime from the requested
version offset. It's structural (which other versions exist), not
content-derived, so it stays outside the state doc.

### .state/query-resolve

The main resolution state. Searches, evaluates statistics, and
either returns (clear winner) or routes to specialized states.

```yaml
# .state/query-resolve
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
        facets_1: "{search.top_facet_tags(1)}"
        facets_2: "{search.top_facet_tags(2)}"
  - when: "search.entropy < params.entropy_low"
    do: find
    with: { query: "{params.query}", tags: "{search.top_facet_tags(1)}", limit: 5 }
    then: query-resolve
  - then: query-explore
```

**Fast path**: search returns a dominant result (rule 2) → `done` on
first tick. This is the common case — no overhead beyond a single
`find`. Zero persistence (pure sync query).

**Lineage path**: strong version/part concentration (rule 3) →
re-search constrained to dominant lineage tags, loop back.

**Ambiguous path**: low margin or high entropy (rule 4) → branch
into parallel pivots.

**Concentrated path**: low entropy but no dominant (rule 5) →
re-search with top facet tags, loop back.

**Fallback**: none of the above → explore with wider limit.

### .state/query-branch

Ambiguous results — two strong candidates, no clear winner. Three
parallel re-searches to break the tie.

```yaml
# .state/query-branch
# match: all
rules:
  - id: pivot1
    do: find
    with: { query: "{params.query}", tags: "{params.facets_1}", limit: "{params.pivot_limit}" }
  - id: pivot2
    do: find
    with: { query: "{params.query}", tags: "{params.facets_2}", limit: "{params.pivot_limit}" }
  - id: bridge
    do: find
    with: { query: "{params.query}", limit: "{params.bridge_limit}" }
post:
  - when: "pivot1.margin > params.margin_high || pivot2.margin > params.margin_high || bridge.margin > params.margin_high"
    return: done
  - when: "budget.remaining > 0"
    then: query-resolve
  - return: stopped
    with:
      reason: "ambiguous"
      results: "{best_of(pivot1, pivot2, bridge)}"
```

The caller injects `pivot_limit` (default 5) and `bridge_limit`
(default `limit + 5`) via params from config.

### .state/query-explore

Mixed signals — broaden the search and try again.

```yaml
# .state/query-explore
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

The caller injects `explore_limit` (default `limit + 5`) and
`explore_limit_wide` (default `limit + 10`) via params from config.

---

## 4) Query path: find

`keep find` returns a list of results, not a single resolved item.
The same `query-resolve` states handle resolution, but the caller
interprets the result differently (full list, not single item).

For deep find (edge-following), the caller uses `.state/find-deep`
instead. This chains a flat search with a `traverse` action —
the same edge-following logic that `get-context` uses, but without
similar/meta/parts assembly:

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

```python
# Keeper.find() — flat search, uses query-resolve for resolution
result = continue({
    "state": "query-resolve",
    "params": {
        "query": "authentication patterns",
        "limit": 10,
        ...thresholds...
    },
    "budget": {"ticks": 5},
})

# Keeper.find(deep=True) — search + edge traversal
result = continue({
    "state": "find-deep",
    "params": {
        "query": "authentication patterns",
        "limit": 10,
        "deep_limit": 5,
    },
    "budget": {"ticks": 5},
})
```

The same resolution states serve both `get` and `find`. Deep mode
is where they diverge: `get` uses `get-context` (which includes
traverse among other context); `find(deep=True)` uses `find-deep`
(traverse only, no similar/meta/parts).

---

## 5) Design rationale: mapping from current code

The query resolution states express the existing strategy selection
logic from `continuation_policy.py`. The current code uses three
named strategies; the state docs express the same logic as `when:`
predicates with threshold params from config.

### Current thresholds (from `choose_strategy()`)

```python
margin_high:    0.18    # confident winner
entropy_low:    0.45    # concentrated results
margin_low:     0.08    # ambiguous
entropy_high:   0.72    # scattered results
lineage_strong: 0.75    # version/part concentration
```

### Strategy mapping

| Strategy | State doc path | When |
|----------|---------------|------|
| `single_lane_refine` | query-resolve rules 3, 5 | high margin + low entropy, or strong lineage |
| `top2_plus_bridge` | query-branch | low margin or high entropy |
| `explore_more` | query-explore | mixed signals (fallback) |

All strategies are just different calls to `find` with different
parameters — no separate "refine" action needed.

### Parity requirements

The state docs must reproduce current behavior:

- **Simple get/find**: `find` returns dominant result → `done` on
  first tick. No continuation overhead for clear matches.
- **Ambiguous get**: close results → `query-branch` fires parallel
  searches → resolves or escalates.
- **Deep mode**: `find-deep` chains flat search + traverse.
- **Budget exhaustion**: after N ticks → `stopped` with reason `"budget"`.

---

## 6) Summary: what ships

| State doc | Entry for | Match | Actions used |
|-----------|-----------|-------|-------------|
| `.state/after-write` | `put` | all | summarize, tag, ocr |
| `.state/query-resolve` | `get`, `find` | sequence | find |
| `.state/query-branch` | (internal) | all | find |
| `.state/query-explore` | (internal) | sequence | find |
| `.state/get-context` | `get` (display) | all | find, traverse, resolve_meta |
| `.state/find-deep` | `find(deep)` | sequence | find, traverse |

Six state docs. Five actions. Resolution uses only `find` — all
variation is in parameters and routing predicates. Context assembly
uses `find`, `traverse`, and `resolve_meta` in parallel.

## 7) Open questions

- [ ] Should version navigation become a state doc rule? Currently
      computed by the runtime from version offset — structural, not
      content-based. But a user might want to customize how many
      previous versions to show.
- [ ] `resolve_meta` action: standalone action or a mode of `find`?
      Wraps `Keeper.resolve_meta()` (specialized tag-query
      resolution). Separate action is cleaner; folding into `find`
      keeps the action count lower.
- [ ] Render hints: should the state doc return render hints, or
      should the rendering layer compute them post-flow?
- [ ] How do lineage signals (version/part concentration) propagate
      across ticks? Runtime tracks them in frontier state?
