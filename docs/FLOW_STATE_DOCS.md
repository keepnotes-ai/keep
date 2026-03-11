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

Runs post-write processing on new or updated items. Five rules evaluate independently:

| Rule | Condition | Action |
|------|-----------|--------|
| `summary` | Content exceeds max summary length and no summary exists | `summarize` |
| `extracted` | Item has `_ocr_pages` tag and a URI | `ocr` |
| `described` | Item has a URI, media content, and a media provider configured | `describe` |
| `analyzed` | Non-system item | `analyze` (decompose into parts) |
| `tagged` | Non-system item with content | `tag` (classify against `.tag/*` specs) |

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

## Extending state docs

You can add processing steps to any state doc without editing the original. Create a child note under the state doc's path:

```bash
# Add a custom step to after-write
keep put --id .state/after-write/obsidian-links 'rules:
  - when: "item.content_type == '\''text/markdown'\''"
    id: obsidian-links
    do: extract_links'
```

Child fragments are discovered automatically and merged into the base doc. Each fragment has a `rules:` list (same syntax as a full state doc) and an optional `order:` field.

### Ordering

The `order` field controls where fragment rules are inserted:

| Value | Effect |
|-------|--------|
| `after` (default) | Appended after all base rules |
| `before` | Prepended before all base rules |
| `after:{rule_id}` | Inserted after the named base rule |
| `before:{rule_id}` | Inserted before the named base rule |

For `match: all` pipelines (like `after-write`), order rarely matters — all rules run in parallel. For `match: sequence` pipelines, order determines execution position.

### Enabling and disabling

Fragments are active by default. To disable one without deleting it:

```bash
keep tag .state/after-write/obsidian-links active=false    # disable
keep tag .state/after-write/obsidian-links -r active        # re-enable
```

### Listing fragments

```bash
keep list --prefix .state/after-write/ --all
```

Shows all fragments with their tags, so active/inactive status is visible at a glance.

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
- [design/STATE-DOC-COMPOSITION.md](design/STATE-DOC-COMPOSITION.md) — Composition design
- [design/STATE-ACTIONS.md](design/STATE-ACTIONS.md) — Available actions reference
- [design/BUILTIN-STATE-DOCS.md](design/BUILTIN-STATE-DOCS.md) — Design rationale
