# Find Cache Filtered Invalidation

## Purpose

`resolve_meta()` is expensive mainly because `.meta/*` docs expand into
multiple inner `find(similar_to=..., tags=...)` calls. The current cache
shape is backwards for that workload:

- the hot inner `find` calls inside meta-doc evaluation do not share the
  main action cache
- the outer `resolve_meta` cache (`MetaCache`) invalidates too broadly
- `put now ...` followed immediately by `get now` misses the meta cache
  by design

This note replaces the earlier `MetaCache` plan in
[design-context-component-cache.md](/Users/hugh/play/keep/later/design/design-context-component-cache.md)
with a simpler model:

1. cache the inner `find` calls
2. invalidate cached `find` entries precisely when their selector may
   have changed
3. remove the outer `MetaCache`

## Scope

In scope:

- `find` action caching for selector-bearing read queries
- `resolve_meta()` using the shared action cache for its inner flows
- precise invalidation for the subset of `find` queries whose membership
  can be derived cheaply from their resolved params
- removal of `MetaCache`

Out of scope:

- persistent cache storage
- precise invalidation for arbitrary semantic search queries
- query result reranking changes
- projection-layer changes

## Current Problem

The hot meta docs are structurally simple:

- `.meta/todo` runs several `find` rules with:
  - `similar_to: "{params.item_id}"`
  - exact `tags` filters like `{act: commitment, status: open}`
- `.meta/learnings` runs several `find` rules with:
  - `similar_to: "{params.item_id}"`
  - exact `tags` filters like `{type: learning}`

These are already the dependency surface. The `with:` params on `find`
fully describe selection and ranking for this purpose.

The expensive part is not `resolve_meta()` as a composition step. The
expensive part is repeatedly running those inner `find` calls.

## Design

### 1. Remove outer `MetaCache`

`resolve_meta()` should remain a pure composition step:

- load `.meta/*` docs
- resolve templates into params
- run the inner flows
- collect section outputs

The outer cache adds complexity without owning the expensive work. Once
the inner `find` actions are cached, recomputing the outer composition is
cheap.

Result:

- delete `MetaCache` from `ContextCache`
- delete `resolve_meta`-specific cache hydration/storage paths
- keep caching only where the expensive selector-bearing work occurs

### 2. Share the main action cache with inner meta flows

`resolve_meta()` currently creates its own action runner for evaluating
meta-doc flows. That runner must receive the shared `ContextCache`.

This is the first required change. Without it, the hot meta `find`
queries cannot benefit from the existing action-level cache.

### 3. Add selector-aware metadata to cached `find` entries

For cacheable `find` entries, store not only IDs and scores, but also a
normalized selector summary.

Proposed entry shape:

```python
@dataclass(slots=True)
class _FindEntry:
    ids_scores: list[tuple[str, float | None]]
    anchor_id: str | None
    selector_tags: dict[str, tuple[str, ...]] | None
    include_hidden: bool
    scope: str | None
    since: str | None
    until: str | None
    precise: bool
    generation: int
    created_at: float
```

Only entries with `precise=True` participate in filtered invalidation.

### 4. Precise invalidation subset

Precise invalidation should apply only when the cached query is simple
enough that membership can be determined from the selector alone.

Initial precise subset:

- action is `find`
- `similar_to` is set
- `query` is not set
- `deep` is false
- `prefix` is not set
- `tags` is a plain exact-match dict
- `tag_keys` is not set
- `scope`, `since`, and `until` are absent in phase 1

This subset already covers the important `.meta/todo` and
`.meta/learnings` rules.

Anything outside that subset stays coarse:

- query-based semantic search
- deep search
- broad list queries
- time-windowed selectors
- dynamic selectors we cannot safely reason about

### 5. Invalidate by selector scan, not dependency graph

The cache is small and bounded. The simplest maintainable strategy is to
scan cached precise `find` entries on write and evict matching ones.

Eviction rules for a write to item `X`:

1. If `entry.anchor_id == X`, evict.
2. Else if the entry selector matches `old_tags`, evict.
3. Else if the entry selector matches `new_tags`, evict.

The old/new distinction matters:

- old match: the item may already be in the cached result and should be
  removed or reranked
- new match: the item may now enter the cached result

This keeps the invalidation logic local to the cache entry shape and
avoids maintaining a separate dependency graph for meta-docs.

### 6. Update cache notifications to carry old and new tags

Current cache invalidation only receives the final tags.

That is insufficient for precise invalidation. The cache API should
become:

```python
def notify_write(
    self,
    item_id: str,
    *,
    old_tags: dict[str, Any] | None,
    new_tags: dict[str, Any] | None,
) -> None: ...
```

Callers should pass:

- `_upsert()`: `existing_tags` and `merged_tags`
- `_tag_direct()`: `current_tags` and `final_tags`
- `_delete_direct()`: existing tags as `old_tags`, `new_tags=None`

### 7. Exact tag matching semantics

Selector matching must reuse keep's existing tag semantics:

- exact key/value conjunction
- multi-value tags supported
- no separate cache-specific tag language

If a selector says:

```python
tags={"act": "request", "status": "open"}
```

then the invalidator should use the same helper semantics as normal tag
matching when checking `old_tags` or `new_tags`.

### 8. Coarse fallback remains for hard cases

We do not need perfect invalidation for every `find` cache entry.

Recommended behavior:

- precise subset: selector scan invalidation
- non-precise subset: existing generation+TTL behavior

This keeps correctness while limiting complexity.

## Why This Is Better Than a Meta Dependency Graph

The dependency-bearing unit is already the inner `find` selector.

Building a separate dependency graph at the `resolve_meta` layer would:

- duplicate selector information already present in `find` params
- add extra invalidation machinery
- still require special handling for non-meta `find` users

The `find` cache is the right abstraction boundary because:

- it matches the expensive operation
- it works for meta and non-meta callers
- it keeps `resolve_meta()` simple

## Implementation Plan

### Phase 1: Structural cleanup

1. Pass the shared `ContextCache` into the runner used by
   `resolve_meta()` / `_run_meta_flow()`.
2. Remove `MetaCache` from `ContextCache`.
3. Remove `resolve_meta`-specific cache hydration/storage code.

### Phase 2: Precise `find` invalidation

1. Rename or generalize `SimilarCache` into a `FindCache`.
2. Extend cached `find` entries with selector metadata.
3. Teach `store()` to mark entries as `precise` only for the supported
   subset.
4. Teach `on_write()` to evict by:
   - anchor match
   - old tag match
   - new tag match
5. Keep generation+TTL behavior for non-precise entries.

### Phase 3: Write-path integration

1. Change `notify_write()` to accept old/new tags.
2. Update `_upsert()`, `_tag_direct()`, and `_delete_direct()`.
3. Add tracing for:
   - cache eligibility
   - cache hit/miss
   - invalidation reason
   - entries evicted

## Tests

Required regression coverage:

1. Inner meta `find` calls use the shared action cache.
2. Repeated `get_context("now")` reuses cached `.meta/todo` and
   `.meta/learnings` inner searches.
3. Writing an unrelated item does not evict those cached entries.
4. Writing an item with `type=learning` evicts only the learning-style
   selectors that match.
5. Writing an item with `act=request,status=open` evicts the matching
   todo selector entries.
6. Writing the anchor item (`similar_to=item_id`) evicts all cached
   entries anchored on that item.
7. Non-precise `find` entries continue to use coarse invalidation.
8. Removing `MetaCache` does not change `resolve_meta()` results.

## Non-Goals

- no DSL change required
- no manual dependency annotations
- no persistent cache
- no attempt to make arbitrary semantic search precisely invalidatable

## Decision

Keep the existing `find` selector shape (`with:` plus `similar_to` and
`tags`) as the dependency surface. Do not add a new `resolve_meta`
dependency graph. Remove `MetaCache`, cache the inner `find` calls, and
make filtered invalidation a property of cached `find` entries.
