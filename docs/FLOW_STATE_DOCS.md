# Built-in State Docs

State docs are YAML documents stored as `.state/*` notes that drive keep's processing flows. Six ship by default. Each is loaded from disk on first use and can be edited in the store.

To view the current state docs: `keep list .state --all`
To reset to defaults: `keep config --reset-system-docs`
To view the state diagram: `keep config --state-diagram`

---

## .state/after-write

**Trigger:** Every `put()` call.
**Mode:** `match: all` — all matching rules fire in parallel.
**Path:** Background (returns immediately, work runs async).

Runs post-write processing on new or updated items. Four rules evaluate independently:

| Rule | Condition | Action |
|------|-----------|--------|
| `summary` | Content exceeds max summary length and no summary exists | `summarize` |
| `extracted` | Item has `_ocr_pages` tag and a URI | `ocr` |
| `analyzed` | Non-system item | `analyze` (decompose into parts) |
| `tagged` | Non-system item | `tag` (classify against `.tag/*` specs) |

System notes (IDs starting with `.`) skip analysis and tagging to avoid recursive processing.

---

## .state/get-context

**Trigger:** `get()` and `now()` calls.
**Mode:** `match: all` — all queries run in parallel.
**Path:** Synchronous (completes before returning to caller).

Assembles the display context shown when you retrieve a note. Three parallel queries:

| Rule | Action | Purpose |
|------|--------|---------|
| `similar` | `find` (by similarity) | Semantically related items |
| `parts` | `find` (by prefix) | Structural parts from `analyze` |
| `meta` | `resolve_meta` | Meta-doc sections (learnings, todos, etc.) |

---

## .state/find-deep

**Trigger:** `find()` with `--deep` flag.
**Mode:** `match: sequence` — rules evaluate top-to-bottom.
**Path:** Synchronous.

Searches, then follows edges from results to discover related items.

1. Run semantic search with the query
2. If no results, return immediately
3. Traverse edges from search hits to find connected items
4. Return combined results

---

## .state/query-resolve

> Thresholds for query resolution are configurable but not yet tuned
> against real query patterns. Results are functional but may route
> suboptimally in edge cases.

**Trigger:** Internal query resolution (multi-step search).
**Mode:** `match: sequence` — first matching rule wins.
**Path:** Synchronous, with tick budget.

The entry point for iterative query refinement. Searches, evaluates result quality, and routes:

| Condition | Action |
|-----------|--------|
| High margin (clear winner) | Return done |
| Strong lineage signal | Re-search with dominant lineage tags, loop back |
| Low margin or high entropy | Transition to `query-branch` |
| Low entropy (tight cluster) | Widen search, loop back |
| No strong signal (fall-through) | Transition to `query-explore` |

**Signals used:** `search.margin`, `search.entropy`, `search.lineage_strong`, `search.dominant_lineage_tags`, `search.top_facet_tags`

---

## .state/query-branch

**Trigger:** Transition from `query-resolve` when results are ambiguous.
**Mode:** `match: all` — parallel faceted searches.
**Path:** Synchronous, shares tick budget with caller.

Runs two parallel queries to break ambiguity:

| Rule | Purpose |
|------|---------|
| `pivot1` | Facet-narrowed search using top tag facets |
| `bridge` | Cross-facet bridging search |

After both complete:
- If either has high margin → return done
- If budget remains → transition back to `query-resolve`
- Otherwise → return `stopped: ambiguous`

---

## .state/query-explore

**Trigger:** Transition from `query-resolve` as last resort.
**Mode:** `match: sequence`.
**Path:** Synchronous, shares tick budget with caller.

Wider exploratory search when resolve and branch haven't produced high-confidence results.

1. Broad search with expanded limit
2. If high margin → return done
3. If budget remains → even wider search, then transition back to `query-resolve`
4. Otherwise → return `stopped: budget`

---

## Editing state docs

State docs are regular keep notes. To edit one:

```bash
keep get .state/after-write          # View current content
keep put ".state/after-write" ...    # Replace with new content
keep config --reset-system-docs      # Restore all defaults
```

Changes take effect on the next flow invocation. The built-in versions are compiled into keep as a fallback — if a state doc is missing from the store, the bundled version is used automatically.

## See also

- [FLOWS.md](FLOWS.md) — How flows work, with narrative and diagram
- [KEEP-FLOW.md](KEEP-FLOW.md) — Running, resuming, and steering flows
- [design/STATE-DOC-SCHEMA.md](design/STATE-DOC-SCHEMA.md) — Full schema specification
- [design/STATE-ACTIONS.md](design/STATE-ACTIONS.md) — Available actions reference
- [design/BUILTIN-STATE-DOCS.md](design/BUILTIN-STATE-DOCS.md) — Design rationale
