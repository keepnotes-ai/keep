# Python API Reference

The CLI (`keep put`, `keep find`, etc.) is the primary interface. The Python API is for embedding keep into applications.

## Installation

```bash
pip install keep-skill
```

For LangChain/LangGraph integration:
```bash
pip install keep-skill[langchain]
```

See [QUICKSTART.md](QUICKSTART.md) for provider configuration (API keys or local models).

## Quick Start

```python
from keep import Keeper

kp = Keeper()  # Uses ~/.keep/ by default

# Index documents
kp.put(uri="file:///path/to/doc.md", tags={"project": "myapp"})
kp.put(uri="https://example.com/page", tags={"topic": "reference"})
kp.put("Important insight about auth patterns")

# Search
results = kp.find("authentication", limit=5)
for r in results:
    print(f"[{r.score:.2f}] {r.id}: {r.summary}")

# Retrieve
item = kp.get("file:///path/to/doc.md")
print(item.summary)
print(item.tags)
```

## API Reference

### Imports

```python
from keep import Keeper, Item
from keep.document_store import VersionInfo, PartInfo  # for version/part history
```

### Initialization

```python
# Default store (~/.keep/)
kp = Keeper()

# Custom store location
kp = Keeper(store_path="/path/to/store")

```

### Core Operations

#### Indexing

```python
# Index from URI (file:// or https://)
kp.put(uri=uri, tags={}, summary=None) → Item

# Index inline content
kp.put(content, tags={}, summary=None) → Item

# Full signature:
# kp.put(content=None, *, uri=None, id=None, summary=None,
#        tags=None, created_at=None, force=False) → Item

# Notes:
# - Exactly one of content or uri must be provided
# - If summary provided, skips auto-summarization
# - Inline content used verbatim if short (≤max_summary_length)
# - User tags (domain, topic, etc.) provide context for summarization
# - id: override auto-generated ID (for managed imports)
# - created_at: ISO timestamp override (for historical imports)
# - force: re-process even if content unchanged
```

#### Search

```python
# Semantic search (default)
kp.find("auth", limit=10) → list[Item]

# With tag pre-filtering
kp.find("auth", tags={"user": "alice"}, limit=10) → list[Item]

# Scoped to specific IDs (glob pattern)
kp.find("auth", scope="file:///path/to/notes/*") → list[Item]

# Deep search — follow edges to discover related items
kp.find("auth", deep=True) → list[Item]

# Find similar to an existing note
kp.find(similar_to="note-id", limit=10) → list[Item]

# Time filtering
kp.find("auth", since="P7D")             # Last 7 days
kp.find("auth", since="P7D", until="P1D")  # Between 7 and 1 days ago
kp.find("auth", since="2026-01-15")      # Since date

# Full signature:
# kp.find(query=None, *, tags=None, similar_to=None, limit=10,
#         since=None, until=None, include_self=False,
#         include_hidden=False, deep=False, scope=None) → list[Item]
```

#### Listing and Filtering

```python
# Unified listing with composable filters (all optional, AND'd together)
kp.list_items(limit=10) → list[Item]
kp.list_items(tags={"project": "myapp"}, since="P7D")
kp.list_items(tag_keys=["act"], since="P3D")      # Key-only: any value
kp.list_items(prefix=".tag/act")                   # ID prefix
kp.list_items(order_by="accessed", limit=20)       # Sort by access time

```

#### Item Access

```python
# Get by ID
kp.get(id) → Item | None

# Check existence
kp.exists(id) → bool
```

#### Tags

```python
# Update tags only (no re-processing)
kp.tag(id, tags={}) → Item | None

# Delete tag by setting empty value
kp.tag("doc:1", {"obsolete": ""})  # Removes 'obsolete' tag

# Remove tag keys entirely
kp.tag("doc:1", remove=["obsolete"])

# Remove specific values from a multi-value tag
kp.tag("doc:1", remove_values={"topic": "old-topic"})

# Edit tags on a part (parts are otherwise immutable)
kp.tag_part("doc:1", 1, tags={"topic": "oauth2"}) → PartInfo | None
kp.tag_part("doc:1", 1, tags={"topic": ""})  # Remove tag

# List tag keys or values
kp.list_tags(key=None) → list[str]
kp.list_tags()          # All tag keys
kp.list_tags("project") # All values for 'project' key
```

#### Version History

```python
# Get previous version
kp.get_version(id, offset=1) → Item | None
# selector: 0=current, 1=previous, 2=two versions ago, -1=oldest archived

# List all versions
kp.list_versions(id, limit=10) → list[VersionInfo]

# Get navigation metadata
kp.get_version_nav(id, current_version=None, limit=3) → dict
# Returns: {"prev": [VersionInfo, ...], "next": [VersionInfo, ...]}
```

#### Current Intentions (Now)

```python
# Get current intentions (auto-creates if missing)
kp.get_now() → Item

# Per-user scoped intentions
kp.get_now(scope="alice") → Item

# Set current intentions
kp.set_now(content, tags={}) → Item

# Per-user scoped update (auto-tags user=alice)
kp.set_now(content, scope="alice") → Item
```

#### Analysis and Parts

```python
# Analyze a document into structural parts
kp.analyze(id) → dict  # Returns {"parts": [...], "status": "applied"}

# List parts of an analyzed document
kp.list_parts(id) → list[PartInfo]

# Get a specific part
kp.get_part(id, part_num) → PartInfo | None
```

#### Flows and Prompts

```python
# Run a state-doc flow (same as keep_flow MCP tool)
result = kp.run_flow_command("query-resolve", params={"query": "auth"})
# result.status, result.data, result.bindings, result.cursor

# Render an agent prompt with context injection
result = kp.render_prompt("reflect", "auth flow")
# result.prompt, result.context, result.search_results, result.flow_bindings

# Render with scoped search
result = kp.render_prompt("query", "auth", scope="file:///notes/*")

# Render with token budget (truncates context to fit)
result = kp.render_prompt("reflect", "auth", token_budget=4000)
```

#### Move

```python
# Move versions from one note to another
kp.move("target-id", source_id="source-id") → Item

# Move with new tags
kp.move("target-id", source_id="source-id", tags={"project": "new"})

# Move only the current version (not full history)
kp.move("target-id", source_id="source-id", only_current=True)
```

#### Context

```python
# Get complete display context for a note (similar, meta, parts, versions)
kp.get_context(id) → dict

# Customize what's included
kp.get_context(id,
    include_similar=True, similar_limit=5,
    include_meta=True, meta_limit=5,
    include_parts=True, parts_limit=10,
    include_versions=True, versions_limit=5,
)
```

#### Deletion

```python
# Delete item and its versions
kp.delete(id) → bool
kp.delete(id, delete_versions=False) → bool  # Keep version history

# Delete a specific version
kp.delete_version(id, offset=1) → bool  # offset: 1=previous, 2=two ago, etc.

# Revert to previous version (if history exists)
kp.revert(id) → Item | None
```

### Utility Methods

```python
# Count notes in store
kp.count() → int

# List collections
kp.list_collections() → list[str]

# List available prompt templates
kp.list_prompts() → list[str]

# Export all data (for backup/migration)
data = kp.export_data(include_system=False) → dict

# Import data
kp.import_data(data, mode="merge") → dict  # mode: "merge" or "replace"

# Close resources (embedding providers, etc.)
kp.close()
```

## Data Types

### Item

Fields:
- `id` (str) — Document identifier
- `summary` (str) — Human-readable summary
- `tags` (dict[str, str | list[str]]) — Key-value tags (single or multi-value per key)
- `score` (float | None) — Similarity score (search results only)

Properties (from tags):
- `item.created` → datetime | None
- `item.updated` → datetime | None  
- `item.accessed` → datetime | None

### VersionInfo

Fields for version history listings:
- `version` (int) — Version offset (0=current, 1=previous, etc.)
- `summary` (str) — Summary of this version
- `tags` (dict[str, str | list[str]]) — Tags at this version
- `created_at` (str) — ISO timestamp when this version was created
- `content_hash` (str | None) — Content hash for deduplication

### PartInfo

Fields for structural parts (from `analyze()`):
- `part_num` (int) — Part number (1-indexed)
- `summary` (str) — Summary of this part
- `tags` (dict[str, str | list[str]]) — Tags on this part
- `content` (str) — Full text of this part
- `created_at` (str) — ISO timestamp when this part was created

## Tags

### Tag Merge Order

When indexing, tags merge in priority order (later wins):
1. Existing tags (from previous version)
2. Config tags (from `[tags]` in keep.toml)
3. Environment tags (from `KEEP_TAG_*` variables)
4. User tags (passed to `put()` or `tag()`)

### Tag Rules

- **Multi-value keys**: Adding a value for an existing key keeps prior distinct values
- **System tags** (prefixed `_`) cannot be set by users
- **Empty string deletes key**: `kp.tag(id, {"key": ""})` removes all values for that key

### Environment Tags

```python
import os
os.environ["KEEP_TAG_PROJECT"] = "myapp"
os.environ["KEEP_TAG_OWNER"] = "alice"

kp.put("note")  # Auto-tagged with project=myapp, owner=alice
```

### Config Tags

Add to `keep.toml`:
```toml
[tags]
project = "my-project"
owner = "alice"
required = ["user"]                    # Enforce required tags on put()
namespace_keys = ["category", "user"]  # LangGraph namespace mapping
```

- **`required`** — List of tag keys that must be present on every `put()`. System docs (dot-prefix IDs) are exempt. Scoped `set_now(scope=...)` auto-tags `user`, satisfying a `user` requirement.
- **`namespace_keys`** — Positional mapping from LangGraph namespace tuples to Keep tag names. See [LANGCHAIN-INTEGRATION.md](LANGCHAIN-INTEGRATION.md).

### Recommended Tags

See [TAGGING.md](TAGGING.md#organizing-by-project-and-topic) for details on:
- `project` — Bounded work context
- `topic` — Cross-project subject area
- `act` — Speech-act category (commitment, request, assertion, etc.)
- `status` — Lifecycle state (open, fulfilled, declined, etc.)

### System Tags (Read-Only)

Protected tags (prefix `_`) managed automatically:
- `_created` — ISO timestamp
- `_updated` — ISO timestamp  
- `_updated_date` — Date only (YYYY-MM-DD)
- `_accessed` — ISO timestamp
- `_accessed_date` — Date only (YYYY-MM-DD)
- `_content_type` — MIME type
- `_source` — Origin (inline, file, http)

Query system tags:
```python
kp.list_items(tags={"_updated_date": "2026-01-30"})
kp.list_items(tags={"_source": "inline"})
```

See [SYSTEM-TAGS.md](SYSTEM-TAGS.md) for complete reference.

## Version Identifiers

Append `@V{N}` to specify version by offset:
- `ID@V{0}` — Current version
- `ID@V{1}` — Previous version
- `ID@V{2}` — Two versions ago
- `ID@V{-1}` — Oldest archived version

```python
item = kp.get("doc:1@V{1}")  # Get previous version
versions = kp.list_versions("doc:1")
```

## Error Handling

```python
from keep import Keeper

try:
    kp = Keeper()
    item = kp.get("nonexistent")
    if item is None:
        print("Item not found")
except Exception as e:
    print(f"Error: {e}")
```

Common errors:
- Missing provider configuration (no API key or local models)
- Invalid URI format
- Embedding provider changes (auto-enqueued, use `keep pending --reindex` or `kp.enqueue_reindex()`)

## LangChain / LangGraph

For LangChain and LangGraph integration (BaseStore, retriever, tools, middleware), see [LANGCHAIN-INTEGRATION.md](LANGCHAIN-INTEGRATION.md).

## See Also

- [QUICKSTART.md](QUICKSTART.md) — Installation and setup
- [REFERENCE.md](REFERENCE.md) — CLI reference
- [LANGCHAIN-INTEGRATION.md](LANGCHAIN-INTEGRATION.md) — LangChain/LangGraph integration
- [AGENT-GUIDE.md](AGENT-GUIDE.md) — Working session patterns
- [ARCHITECTURE.md](ARCHITECTURE.md) — System internals
