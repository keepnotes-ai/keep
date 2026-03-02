# Reflective Memory — Agent Reference Card

**Purpose:** Persistent memory for documents with semantic search.

**Default store:** `~/.keep/` in user home (auto-created)

## Commands

| Command | Description | Docs |
|---------|-------------|------|
| `keep now` | Get or set current working intentions | [KEEP-NOW.md](KEEP-NOW.md) |
| `keep put` | Add or update a document | [KEEP-PUT.md](KEEP-PUT.md) |
| `keep get` | Retrieve item(s) by ID | [KEEP-GET.md](KEEP-GET.md) |
| `keep find` | Search by meaning or text | [KEEP-FIND.md](KEEP-FIND.md) |
| `keep list` | List recent items, filter by tags | [KEEP-LIST.md](KEEP-LIST.md) |
| `keep config` | Show configuration and paths | [KEEP-CONFIG.md](KEEP-CONFIG.md) |
| `keep move` | Move versions into a named item (`-t` or `--only` required) | [KEEP-MOVE.md](KEEP-MOVE.md) |
| `keep analyze` | Decompose a note into structural parts | [KEEP-ANALYZE.md](KEEP-ANALYZE.md) |
| `keep prompt` | Render agent prompt with context | [KEEP-PROMPT.md](KEEP-PROMPT.md) |
| `keep mcp` | Start MCP stdio server for AI agents | [KEEP-MCP.md](KEEP-MCP.md) |
| `keep del` | Remove item or revert to previous version | — |
| `keep tag` | Add, update, or remove tags | [TAGGING.md](TAGGING.md) |
| `keep data export` | Export store to JSON for backup or migration | [KEEP-DATA.md](KEEP-DATA.md) |
| `keep data import` | Import documents from JSON export file | [KEEP-DATA.md](KEEP-DATA.md) |
| `keep pending` | Process pending tasks (summarize, embed, OCR, analyze, reindex) | — |

## Global Flags

```bash
keep --json  (-j) <cmd>  # Output as JSON
keep --ids   (-I) <cmd>  # Output only versioned IDs (for piping)
keep --full  (-F) <cmd>  # Output full YAML frontmatter
keep -v <cmd>            # Enable debug logging to stderr
```

## Output Formats

Three output formats, consistent across all commands:

### Default: Summary Lines
One line per item: `id date summary` (search results include score: `id (score) date summary`)
```
%a1b2c3d4         2026-01-14 URI detection should use proper scheme validation...
%e5f6a7b8         (0.89) 2026-01-14 OAuth2 token refresh pattern...
file:///path/doc  2026-01-15 Document about authentication patterns...
```

### With `--ids`: Versioned IDs Only
```
%a1b2c3d4@V{0}
file:///path/doc@V{0}
```

### With `--full`: YAML Frontmatter
Full details with tags, similar items, and version navigation:
```yaml
---
id: %a1b2c3d4
tags: {project: myapp, status: reviewed}
similar:
  - %e5f6a7b8@V{0} (0.89) 2026-01-14 Related authentication...
meta/todo:
  - %c9d0e1f2 Update auth docs for new flow
score: 0.823
prev:
  - @V{1} 2026-01-14 Previous summary text...
---
Document summary here...
```

**Note:** `keep get` and `keep now` default to full format since they display a single item.

### With `--json`: JSON Output
```json
{"id": "...", "summary": "...", "tags": {...}, "score": 0.823}
```

Version numbers are **selectors**: @V{0} = current, @V{1} = previous, @V{2} = two versions ago, @V{-1} = oldest archived, @V{-2} = second-oldest.
Part numbers are **1-indexed**: @P{1} = first part, @P{2} = second part, etc.

**Output width:** Summaries are truncated to fit the terminal. When stdout is not a TTY (e.g., piped through hooks), output uses 200 columns for wider summaries.

### Pipe Composition

```bash
keep --ids find "auth" | xargs keep get              # Get full details for all matches
keep --ids list -n 5 | xargs keep get                # Get details for recent items
keep --ids list --tag project=foo | xargs keep tag --tag status=done
keep --json --ids find "query"                       # JSON array: ["id@V{0}", ...]

# Version history composition
keep --ids now --history | xargs -I{} keep get "{}"  # Get all versions
diff <(keep get doc:1) <(keep get "doc:1@V{1}")      # Diff current vs previous
```

## Quick CLI

```bash
# Current intentions
keep now                              # Show current intentions
keep now "What's important now"       # Update intentions
keep prompt reflect                   # Structured reflection practice
keep prompt reflect "auth flow"       # Reflect with search context
keep prompt query "what do I know about auth?"  # Answer from memory context
keep prompt conversation              # Conversation analysis
keep move "name" -t project=foo       # Move matching versions from now
keep move "name" --only               # Move just the current version
keep move "name" --from "source" -t X # Reorganize between items
keep move "name" --only --analyze     # Move + decompose into parts

# Add or update
keep put "inline text" -t topic=auth  # Text mode
keep put file:///path/to/doc.pdf      # URI mode
keep put /path/to/folder/             # Directory mode
keep put "note" --suggest-tags        # Show tag suggestions
keep put doc.pdf --analyze            # Index + decompose into parts

# Retrieve
keep get ID                           # Current version
keep get ID -V 1                      # Previous version
keep get "ID@P{1}"                    # Part 1 (from analyze)
keep get ID --history                 # List all versions
keep get ID --parts                   # List structural parts

# Search
keep find "query"                     # Semantic search
keep find "query" --text              # Full-text search
keep find "query" --deep              # Follow tags/edges to discover related items
keep find "query" --since P7D         # Last 7 days

# List and filter
keep list                            # Recent items
keep list --tag project=myapp        # Filter by tag
keep list --tags=                    # List all tag keys

# Modify
keep tag ID --tag key=value   # Add/update tag
keep tag ID --remove key      # Remove tag
keep tag "ID@P{1}" -t topic=x # Edit tags on a part
keep del ID                          # Remove item or revert to previous version

# Analyze (skips if parts are already current)
keep analyze ID                      # Decompose into parts (background)
keep analyze ID -t topic -t type     # With guidance tags
keep analyze ID --fg                 # Wait for completion
keep analyze ID --force              # Re-analyze even if current

# Data management
keep data export backup.json         # Export store to JSON
keep data export - | gzip > bk.gz    # Export to stdout, compress
keep data import backup.json         # Import (merge, skip existing)
keep data import backup.json -m replace  # Import (replace all)

# Maintenance
keep pending                         # Process pending tasks, tail progress
keep pending --reindex               # Re-embed all items + process
keep pending --retry                 # Reset failed items back to pending
keep pending --stop                  # Stop background processor
```

## Python API

See [PYTHON-API.md](PYTHON-API.md) for complete Python API reference.

```python
from keep import Keeper
kp = Keeper()
kp.put("note", tags={"project": "myapp"})
results = kp.find("authentication", limit=5)
```

### LangChain / LangGraph

```python
from keep.langchain import KeepStore, KeepNotesToolkit, KeepNotesRetriever
```

See [LANGCHAIN-INTEGRATION.md](LANGCHAIN-INTEGRATION.md) for full details.

## When to Use
- `put` / `put(uri=...)` — when referencing any file/URL worth remembering
- `put` / `put("text")` — capture conversation insights, decisions, notes
- `find` — before searching filesystem; may already be indexed
- `find --since` — filter to recent items when recency matters

## Topics

- [OUTPUT.md](OUTPUT.md) — How to read the frontmatter output
- [TAGGING.md](TAGGING.md) — Tags, speech acts, project/topic organization
- [VERSIONING.md](VERSIONING.md) — Document versioning and history
- [META-TAGS.md](META-TAGS.md) — Contextual queries (`.meta/*`)
- [PROMPTS.md](PROMPTS.md) — Prompts for summarization, analysis, and agent workflows
- [SYSTEM-TAGS.md](SYSTEM-TAGS.md) — Auto-managed system tags

## More

- [AGENT-GUIDE.md](AGENT-GUIDE.md) — Working session patterns
- [QUICKSTART.md](QUICKSTART.md) — Installation and setup
- [PYTHON-API.md](PYTHON-API.md) — Python API reference
- [LANGCHAIN-INTEGRATION.md](LANGCHAIN-INTEGRATION.md) — LangChain/LangGraph integration
- [ARCHITECTURE.md](ARCHITECTURE.md) — How it works under the hood
- `.domains` — Domain organization patterns (`keep get .domains`)
- `.conversations` — Conversation framework (`keep get .conversations`)
