# Keep API Schema Reference

Concise reference for the keep memory API. Covers the data model, tools, parameter types, and return formats.

Interface: MCP (`keep_flow`, `keep_prompt`, `keep_help`), CLI (`keep <cmd>`), or Python (`Keeper`).

---

## Data Model

### Item

Every piece of stored content is an **item**.

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique identifier (see ID Formats below) |
| `summary` | string | Generated or user-provided summary of the content |
| `tags` | `{key: value}` | Key-value metadata. Values are strings or lists of strings |
| `score` | float or null | Similarity score (0-1), present only in search results |

Items also carry system-managed timestamps in tags: `_created`, `_updated`, `_accessed`.

### Versions

New archived versions are created when `put` updates an existing item's content.
Tag-only updates (`tag`) and unchanged writes update in place.

| Selector | Meaning |
|----------|---------|
| `@V{0}` | Current version (default) |
| `@V{1}` | Previous version |
| `@V{N}` | N versions back |
| `@V{-1}` | Oldest archived version |

Append the selector to any ID: `%a1b2c3@V{1}`

### Parts

`analyze` decomposes a document into structural **parts** — sections with their own summaries, tags, and embeddings.

| Selector | Meaning |
|----------|---------|
| `@P{1}` | First part (1-indexed) |
| `@P{N}` | Nth part |

Parts appear independently in search results. Retrieve with: `keep_flow(state="get", params={item_id:"DOC_ID@P{1}")`

---

## ID Formats

| Format | Example | Created by |
|--------|---------|------------|
| `%hexhash` | `%a1b2c3d4e5f6` | Inline text when `id` is omitted (CLI, MCP, Python API) |
| URL | `https://example.com/doc` | URI put |
| `file://` URI | `file:///path/to/doc.pdf` | Local file put |
| Custom string | `my-notes` | User-specified `id` parameter |
| `now` | `now` | Working context (singleton) |
| `.tag/KEY` | `.tag/act` | Tag description (system doc) |
| `.tag/KEY/VALUE` | `.tag/act/commitment` | Tag value description (system doc) |

---

## Tags

Key-value pairs on every item. Keys are alphanumeric (plus `_`, `-`). Values are strings or lists of strings.

### Setting and removing

```
keep_flow(state="tag", params={id:"ID", tags={"topic": "auth"})           # set
keep_flow(state="tag", params={id:"ID", tags={"old-tag": ""})              # remove (empty string)
keep_flow(state="put", params={content:"text", tags={"project": "myapp"})  # set on create
```

### Filtering

Tags on `find` and `list` are **pre-filters** — the search only considers matching items.

```
keep_flow(state="query-resolve", params={query:"auth", tags={"project": "myapp"})
keep_flow(state="query-resolve", params={tags:{"status": "open"})
```

Multiple tags use AND logic: all must match.

### Built-in tags

| Key | Constrained | Singular | Values |
|-----|:-----------:|:--------:|--------|
| `act` | yes | yes | `commitment`, `request`, `offer`, `assertion`, `assessment`, `declaration` |
| `status` | yes | yes | `open`, `blocked`, `fulfilled`, `declined`, `withdrawn`, `renegotiated` |
| `type` | no | no | `learning`, `breakdown`, `gotcha`, `reference`, `teaching`, `meeting`, `pattern`, `possibility`, `decision` |
| `project` | no | no | user-defined |
| `topic` | no | no | user-defined |

**Constrained:** only listed values accepted. **Singular:** new values replace old (not accumulate).

### System tags (auto-managed, read-only)

`_created`, `_updated`, `_updated_date`, `_accessed`, `_accessed_date`, `_source`, `_content_type`

These are hidden from default display but accessible via `--json` or Python API.

---

## Time Filters

Both `since` and `until` accept two formats:

| Format | Example | Meaning |
|--------|---------|---------|
| ISO 8601 duration | `P3D` | 3 days ago |
| | `P1W` | 1 week ago |
| | `P1M` | ~30 days ago |
| | `PT1H` | 1 hour ago |
| | `P1Y` | ~365 days ago |
| Date | `2026-01-15` | Specific date |

`since` = items updated on or after. `until` = items updated before.

---

## Tools

### put (state doc)

Store text, a URL, or a document.
For inline text without an explicit `id`, keep uses a content-addressed ID.

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `content` | string | yes | — | Text to store, or URI (`http://`, `https://`, `file://`) to fetch and index |
| `id` | string | no | auto | Custom ID. If omitted: URI inputs use the URI as ID; inline text uses a content hash |
| `summary` | string | no | auto | User-provided summary (skips auto-summarization) |
| `tags` | `{str: str}` | no | none | Tags to set. Example: `{"topic": "auth"}` |
| `analyze` | bool | no | false | Decompose into searchable parts after storing |

**Returns:** `"Stored: %a1b2c3"` or `"Unchanged: %a1b2c3"` (idempotent on same content)

**Examples:**
```
keep_flow(state="put", params={content:"OAuth2 uses PKCE for public clients", tags={"topic": "auth"})
keep_flow(state="put", params={content:"https://docs.example.com/api", tags={"type": "reference"})
keep_flow(state="put", params={content:"Long document...", analyze=true)
keep_flow(state="put", params={content:"My design notes", id="design-notes", summary="Architecture decisions")
```

---

### query-resolve (state doc)

Search memory by meaning. Returns items ranked by semantic similarity with recency weighting.

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `query` | string | yes | — | Natural language search query |
| `tags` | `{str: str}` | no | none | Pre-filter: only search items matching all tags |
| `since` | string | no | none | Time filter (see Time Filters) |
| `until` | string | no | none | Time filter (see Time Filters) |
| `deep` | bool | no | false | Follow tags and edges to discover related items beyond direct matches |
| `show_tags` | bool | no | false | Include non-system tags in each result |
| `token_budget` | int | no | 4000 | Approximate token budget for the response |

**Returns:** Formatted list of results, one per line:
```
- ID  (score) date  summary text...
```

**Examples:**
```
keep_flow(state="query-resolve", params={query:"authentication patterns")
keep_flow(state="query-resolve", params={query:"open tasks", tags={"project": "myapp"}, since="P7D")
keep_flow(state="query-resolve", params={query:"architecture decisions", deep=true, token_budget=8000)
```

---

### get (state doc)

Retrieve one item with full context: similar items, meta sections, structural parts, and version history.

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `id` | string | yes | — | Item ID. Use `"now"` for current working context |

**Returns:** YAML frontmatter with sections:

```yaml
---
id: %a1b2c3
tags:
  project: "myapp"
  topic: "auth"
similar:
  - %e5f6a7 (0.89) 2026-01-14 Related item summary...
meta/todo:
  - %d3e4f5 Open task related to this item...
parts:
  - @P{1} Section one summary...
prev:
  - @V{1} 2026-01-13 Previous version summary...
---
Item summary or content here
```

**Examples:**
```
keep_flow(state="get", params={item_id:"now")              # current working context
keep_flow(state="get", params={item_id:"%a1b2c3")          # specific item
keep_flow(state="get", params={item_id:"%a1b2c3@V{1}")    # previous version
keep_flow(state="get", params={item_id:"%a1b2c3@P{1}")    # first structural part
keep_flow(state="get", params={item_id:".tag/act")         # tag description doc
```

---

### Updating now (via put)

Update the current working context. Persists across sessions.
Implemented via `put(id="now", ...)`, so it creates a version when content changes.

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `content` | string | yes | — | Current state, active goals, recent decisions |
| `tags` | `{str: str}` | no | none | Tags for this context update |

**Returns:** `"Context updated: now"`

To **read** current context, use `keep_flow(state="get", params={item_id:"now")`.

**Examples:**
```
keep_flow(state="put", params={content:"Investigating flaky auth test. Suspect timing issue.")
keep_flow(state="put", params={content:"Fixed the bug. Next: add regression test.", tags={"project": "myapp"})
```

---

### tag (state doc)

Add, update, or remove tags on an existing item. Does not re-process content.

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `id` | string | yes | — | Item ID |
| `tags` | `{str: str}` | yes | — | Tags to set. Empty string `""` deletes the tag |

**Returns:** `"Tagged %abc: set topic=auth; removed old-tag"`

**Examples:**
```
keep_flow(state="tag", params={id:"%a1b2c3", tags={"status": "fulfilled"})
keep_flow(state="tag", params={id:"%a1b2c3", tags={"topic": "auth", "obsolete": ""})
```

---

### delete (state doc)

Permanently delete an item and all its versions.

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `id` | string | yes | — | Item ID to delete |

**Returns:** `"Deleted: %a1b2c3"` or `"Not found: %a1b2c3"`

---

### Listing items (via query-resolve)

List recent items. Supports filtering by ID prefix, tags, and time range.

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `prefix` | string | no | none | ID prefix or glob pattern (e.g. `".tag/*"`) |
| `tags` | `{str: str}` | no | none | Filter by tag key=value pairs |
| `since` | string | no | none | Time filter |
| `until` | string | no | none | Time filter |
| `limit` | int | no | 10 | Maximum results |

**Returns:** List of items, one per line:
```
- ID  date  summary text...
```

**Examples:**
```
keep_flow(state="query-resolve", params={)                                              # recent items
keep_flow(state="query-resolve", params={tags:{"act": "commitment", "status": "open"})  # open commitments
keep_flow(state="query-resolve", params={prefix=".tag/*")                               # all tag docs
keep_flow(state="query-resolve", params={since="P7D", limit=20)                         # last week, up to 20
```

---

### move (state doc)

Move versions from a source item into a named target. Used to archive working context or reorganize notes.

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `name` | string | yes | — | Target item ID (created if new, appended if exists) |
| `source_id` | string | no | `"now"` | Source item to extract from |
| `tags` | `{str: str}` | no | none | Only move versions whose tags match (all must match) |
| `only_current` | bool | no | false | Move only the tip version, not full history |

**Returns:** `"Moved to: my-notes"`

**Examples:**
```
keep_flow(state="move", params={name="auth-work", tags={"project": "myapp"})     # archive matching versions from now
keep_flow(state="move", params={name="design-log", only_current=true)             # snapshot current context
keep_flow(state="move", params={name="topic-notes", source_id="old-doc", tags={"topic": "auth"})
```

---

### keep_prompt

Render an agent prompt template with live context injected from memory. Templates use `{get}` and `{find}` placeholders that expand to current item context and search results.

| Parameter | Type | Required | Default | Description |
|-----------|------|:--------:|---------|-------------|
| `name` | string | no | none | Prompt name. Omit to list available prompts |
| `text` | string | no | none | Search query for `{find}` placeholder |
| `id` | string | no | `"now"` | Item ID for `{get}` placeholder |
| `tags` | `{str: str}` | no | none | Filter search context by tags |
| `since` | string | no | none | Time filter for search context |
| `until` | string | no | none | Time filter for search context |
| `deep` | bool | no | false | Follow tags to discover related items |
| `token_budget` | int | no | template default | Token budget for search results |

**Returns:** Rendered prompt text with placeholders expanded, or list of available prompts.

**Available prompts:**

| Name | Purpose |
|------|---------|
| `reflect` | Structured reflection on actions and outcomes |
| `session-start` | Context and open commitments at session start |
| `query` | Answer a question using memory context |
| `conversation` | Conversation analysis |
| `subagent-start` | Subagent initialization context |

**Examples:**
```
keep_prompt()                                          # list available prompts
keep_prompt(name="reflect")                            # reflect on current context
keep_prompt(name="session-start")                      # start-of-session context
keep_prompt(name="query", text="what do I know about auth?")
keep_prompt(name="reflect", text="deployment", since="P3D")
```

---

## Common Patterns

### Session lifecycle
```
keep_prompt(name="session-start")                      # 1. orient
keep_flow(state="get", params={item_id:"now")                                     # 2. check intentions
# ... do work ...
keep_flow(state="put", params={content:"Completed X. Next: Y.")              # 3. update context
keep_prompt(name="reflect")                            # 4. reflect
```

### Store and retrieve
```
keep_flow(state="put", params={content:"insight text", tags={"type": "learning", "topic": "auth"})
keep_flow(state="query-resolve", params={query:"authentication insights")
keep_flow(state="get", params={item_id:"%returned_id")
```

### Track commitments
```
keep_flow(state="put", params={content:"Will fix bug by Friday", tags={"act": "commitment", "status": "open"})
keep_flow(state="query-resolve", params={tags:{"act": "commitment", "status": "open"})   # check open work
keep_flow(state="tag", params={id:"ID", tags={"status": "fulfilled"})            # close the loop
```

### Index a document
```
keep_flow(state="put", params={content:"https://docs.example.com/api", tags={"type": "reference", "topic": "api"})
keep_flow(state="query-resolve", params={query:"API documentation")
```

### Archive and pivot
```
keep_flow(state="move", params={name="auth-work", tags={"project": "myapp"})     # archive
keep_flow(state="put", params={content:"Starting on database migration")          # fresh context
```

---

## See Also

- [AGENT-GUIDE.md](AGENT-GUIDE.md) — Working session patterns and reflective practice
- [TAGGING.md](TAGGING.md) — Full tag system reference (speech acts, constraints, edge tags)
- [OUTPUT.md](OUTPUT.md) — How to read the YAML frontmatter output format
- [KEEP-MCP.md](KEEP-MCP.md) — MCP server setup and integration
- [REFERENCE.md](REFERENCE.md) — CLI command reference
