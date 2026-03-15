# Flow Actions

Actions are the operations available inside state doc flows. Each action searches, reads, or enriches items in the store.

State docs invoke actions via `do: <name>`. Parameters come from the rule's `with:` block. The runtime calls the action, receives an output dict, and makes the output available to subsequent rules via `{rule_id.field}` template references.

## Search actions

### find

Search the store by semantic query, or list items by tags/prefix.

```yaml
- id: search
  do: find
  with:
    query: "search terms"
    tags: { topic: "auth" }
    bias: { "now": 0, "preferred-doc": 2.0 }
    limit: 10
    since: "P7D"
    offset: 0
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `query` | str | — | Search query text (semantic + full-text) |
| `similar_to` | str | — | Item ID to find similar items to |
| `tags` | dict | — | Tag filter — only items matching all specified tags |
| `bias` | dict | — | Score multiplier per item ID: `0`=exclude, `1`=neutral, `>1`=boost |
| `prefix` | str | — | ID prefix filter (e.g. `"doc@P"` for parts) |
| `limit` | int | 10 | Maximum results |
| `since` | str | — | ISO duration (`P7D`) or date — items updated since |
| `until` | str | — | ISO duration or date — items updated before |
| `offset` | int | 0 | Skip first N results (pagination) |
| `include_hidden` | bool | false | Include system notes (dot-prefix IDs) |
| `order_by` | str | `updated` | Sort order for list mode: `updated`, `accessed`, `created` |

At least one of `query`, `similar_to`, `tags`, `prefix`, or `since` is required. `query` and `similar_to` are mutually exclusive.

**Output:**

```python
{
    "results": [
        {"id": "...", "summary": "...", "tags": {...}, "score": 0.87},
        ...
    ],
    "count": 5,
    # Statistics (added by runtime for search-mode queries):
    "margin": 0.15,       # score gap between #1 and #2 (0=tied, 1=dominant)
    "entropy": 0.7,       # score distribution spread (0=peaked, 1=uniform)
    "lineage_strong": 0.0,
    "dominant_lineage_tags": null,
    "top_facet_tags": [{"topic": "auth"}]
}
```

Access via `{search.results}`, `{search.margin}`, `{search.entropy}`, etc.

### traverse

Follow edges from items to discover related items.

```yaml
- id: related
  do: traverse
  with:
    items: "{search.results}"
    limit: 5
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `items` | list | required | Items to follow edges from |
| `limit` | int | 5 | Max related items per source |

**Output:** `{"groups": {"source-id": [{id, summary, tags}, ...], ...}, "count": N}`

## Context actions

### get

Retrieve a specific item by ID.

```yaml
- id: target
  do: get
  with:
    id: "item-id"
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | str | required | Item identifier |

**Output:** `{"id": "...", "summary": "...", "tags": {...}}` or `{}` if not found.

### list_parts

List decomposed parts (structural children) of an item.

```yaml
- id: parts
  do: list_parts
  with:
    id: "{params.item_id}"
    limit: 5
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | str | required | Parent item ID |
| `limit` | int | 5 | Maximum parts to return |

**Output:** `{"results": [{id, summary, tags, score}, ...], "count": N}`

### list_versions

List version history for an item.

```yaml
- id: versions
  do: list_versions
  with:
    id: "{params.item_id}"
    limit: 3
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | str | required | Item identifier |
| `limit` | int | 3 | Maximum versions to return |

**Output:** `{"versions": [{"offset": 1, "summary": "...", "date": "2026-03-15"}, ...], "count": N}`

### resolve_meta

Resolve meta-document definitions against a target item. Meta-docs (`.meta/*`) define tag-based queries; this evaluates them against the target item's tags.

```yaml
- id: meta
  do: resolve_meta
  with:
    item_id: "{params.item_id}"
    limit: 3
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `item_id` | str | required | Item to resolve meta-docs for |
| `limit` | int | 3 | Max items per meta-doc section |

**Output:** `{"sections": {"learnings": [{id, summary, tags}, ...], "todo": [...]}, "count": N}`

### resolve_edges

Resolve forward and inverse edges for an item.

```yaml
- id: edges
  do: resolve_edges
  with:
    id: "{params.item_id}"
    limit: 5
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | str | required | Item identifier |
| `limit` | int | 5 | Max edges per predicate |

**Output:** `{"edges": {"references": [{id, summary, predicate, date}, ...], ...}, "count": N}`

## Mutation actions

### move

Move versions from a source item into a named target.

```yaml
- id: moved
  do: move
  with:
    name: "project-notes"
    source: "now"
    tags: { project: "myapp" }
    only_current: false
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | str | required | Target item ID (created if new) |
| `source` | str | `now` | Source item to extract from |
| `tags` | dict | — | Only extract versions matching these tags |
| `only_current` | bool | false | Only move the current version, not history |

**Output:** `{"id": "project-notes", "summary": "..."}`

### delete

Permanently delete an item and its version history.

```yaml
- do: delete
  with:
    id: "old-item"
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | str | required | Item to delete |

**Output:** `{"deleted": "old-item"}`

## Processing actions

These run during `after-write` flows to enrich newly stored items.

### summarize

Generate a summary for a target item using the configured LLM provider.

```yaml
- do: summarize
  with:
    item_id: "{params.item_id}"
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `item_id` | str | required | Item to summarize |

**Output:** `{"summary": "..."}`
**Mutations:** `[{"op": "set_summary", "summary": "..."}]`

### tag

Set explicit tags on one or more items. Accepts a single item ID or a list of search results.

```yaml
# Single item
- do: tag
  with:
    id: "my-item"
    tags: { project: "security-audit" }

# Bulk: tag all search results
- do: tag
  with:
    items: "{search.results}"
    tags: { reviewed: "true" }
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | str | — | Single item ID to tag |
| `items` | list | — | List of items (result dicts or IDs) to tag |
| `tags` | dict | required | Tags to apply |

One of `id` or `items` is required.

**Output:** `{"count": N, "ids": ["..."], "mutations": [...]}`
**Mutations:** `[{"op": "set_tags", "target": "id", "tags": {...}}, ...]`

### auto_tag

Classify a target item against tag specs (`.tag/*` docs) in the store using an LLM. Used by the `after-write` flow for automatic classification.

```yaml
- do: auto_tag
  with:
    item_id: "{params.item_id}"
```

What gets tagged depends on which `.tag/*` specs exist. Users add new tagging vocabularies by creating spec docs.

**Output:** `{"tags": {"act": "commitment", "status": "open", ...}}`
**Mutations:** `[{"op": "set_tags", "tags": {...}}]`

### analyze

Decompose a target item into structural parts.

```yaml
- do: analyze
  with:
    item_id: "{params.item_id}"
```

**Output:** `{"parts": [{summary, content, tags, part_num}, ...]}`
**Mutations:** `[{"op": "put_item", "id": "item@p1", ...}, ...]`

### describe

Extract text description from images or media content.

```yaml
- do: describe
  with:
    item_id: "{params.item_id}"
```

### resolve_duplicates

Detect items with identical content and link them via edge tags.

```yaml
- id: resolve-duplicates
  do: resolve_duplicates
  with:
    tag: duplicates
```

**Output:** `{"duplicates": ["id1", "id2", ...]}` or `{"skipped": true, "reason": "..."}`
**Mutations:** `[{"op": "set_tags", "tags": {"duplicates": [...]}}]`

### extract_links

Extract markdown links from content and create edge relationships.

```yaml
- do: extract_links
  with:
    tag: references
    create_targets: "true"
```

### generate

Raw LLM prompt — the escape hatch for custom processing.

```yaml
- do: generate
  with:
    system: "You are a classifier."
    user: "{item.content}"
    max_tokens: 4096
    format: json
```

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `system` | str | `""` | System prompt |
| `user` | str | `""` | User prompt |
| `max_tokens` | int | 4096 | Maximum tokens in response |
| `format` | str | — | If `"json"`, parse response as structured data |

**Output:** `{"text": "..."}` — with parsed fields merged in when `format: json`.

## Result statistics

When the `find` action runs in search mode, the runtime computes statistics from the result set:

| Statistic | Description |
|-----------|-------------|
| `margin` | Score gap between #1 and #2 (0=tied, 1=clear winner) |
| `entropy` | Score distribution spread (0=peaked at one result, 1=uniform) |
| `lineage_strong` | Version/part lineage concentration |
| `dominant_lineage_tags` | Tags from the dominant lineage group |
| `top_facet_tags` | Most common non-system tag constraints |

These are available as `{rule_id.margin}`, `{rule_id.entropy}`, etc. on the rule that invoked `find`. List-mode queries (no search scores) produce `null` for score-based statistics.

## Custom actions

Actions live in `keep/actions/`. To add a new action:

```python
# keep/actions/my_action.py
from keep.actions import action

@action(id="my_action")
class MyAction:
    def run(self, params, context):
        item = context.get(params["id"])
        return {"result": "..."}
```

Actions are auto-discovered on startup. Use them in state docs with `do: my_action`.

## See also

- [FLOWS.md](use keep_help with topic="flows") — How flows work
- [KEEP-FLOW.md](use keep_help with topic="keep-flow") — Running flows from CLI/MCP
- [FLOW_STATE_DOCS.md](use keep_help with topic="flow_state_docs") — Built-in state doc reference
