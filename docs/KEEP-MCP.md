# keep mcp

MCP (Model Context Protocol) server for AI agent integration.

Provides MCP access to keep's reflective memory, using a local interface (stdio).

## Quick Start

```bash
keep mcp                    # Start stdio server
keep mcp --store ~/mystore  # Custom store path
```

### Claude Desktop

```bash
keep config mcpb              # Generate and open .mcpb bundle
```

Generates a `.mcpb` bundle and opens it with Claude Desktop. You will be prompted to install the `keep` connector, which gives Claude Desktop full access to the memory system and help pages.

### Claude Code

```
/plugin marketplace https://github.com/keepnotes-ai/keep.git
/plugin install keepnotes-ai/keep
```

The first command registers the marketplace, the second installs the plugin (MCP tools, skill instructions, and session hooks).

Alternatively, add just the MCP server manually:

```bash
claude mcp add --scope user keep -- keep mcp
```

### Kiro

```bash
kiro-cli mcp add --name keep --scope global -- keep mcp
```

### Codex

```bash
codex mcp add keep -- keep mcp
```

### VS Code

```bash
code --add-mcp '{"name":"keep","command":"keep","args":["mcp"]}'
```

The server respects the `KEEP_STORE_PATH` environment variable for store location.

## Tools

11 tools, all prefixed `keep_` to avoid collision with other MCP servers:

| Tool | Description | Annotations |
|------|-------------|-------------|
| `keep_put` | Store text, URL, or document in memory | idempotent |
| `keep_find` | Search by natural language query | read-only |
| `keep_get` | Retrieve item with full context (similar items, parts, versions) | read-only |
| `keep_now` | Update current working context | idempotent |
| `keep_tag` | Add, update, or remove tags on an item | idempotent |
| `keep_delete` | Permanently delete an item | destructive |
| `keep_list` | List recent items with optional filters | read-only |
| `keep_move` | Move versions between items | — |
| `keep_prompt` | Render an agent prompt with context injected | read-only |
| `keep_flow` | Run a state-doc flow synchronously | — |
| `keep_help` | Browse keep documentation | read-only |

All tools return human-readable strings. Errors are returned as strings (never raised to the protocol layer).

### keep_put

```
content:  "Text to store" or "https://example.com/doc"
id:       optional custom ID
summary:  optional (skips auto-summarization)
tags:     {"topic": "auth", "project": "myapp"}
analyze:  true to decompose into searchable parts
```

URIs (`http://`, `https://`, `file://`) are fetched and indexed automatically.

### keep_find

```
query:        "natural language search"
tags:         {"project": "myapp"}       # filter (all must match)
since:        "P3D" or "2026-01-15"     # time filter
until:        "P1D" or "2026-02-01"
deep:         true                       # follow tags/edges to discover related items
show_tags:    true                       # show non-system tags for each result
token_budget: 4000                       # token budget for results context
```

### keep_get

```
id:  "now"         # current working context
id:  "%a1b2c3"    # specific item
```

Returns YAML frontmatter with similar items, meta sections, parts manifest, and version history.

### keep_now

```
content:  "Current state, goals, decisions"
tags:     {"project": "myapp"}
```

To read current context, use `keep_get` with `id="now"`.

### keep_tag

```
id:    "%a1b2c3"
tags:  {"topic": "new-value", "old-tag": ""}   # empty string deletes
```

### keep_delete

```
id:  "%a1b2c3"
```

### keep_list

```
prefix:  ".tag/*"                  # ID prefix or glob
tags:    {"project": "myapp"}
since:   "P7D"
until:   "2026-02-01"
limit:   10
```

### keep_move

```
name:          "my-topic"          # target (created if new)
source_id:     "now"               # default
tags:          {"topic": "auth"}   # filter which versions to move
only_current:  true                # just the tip, not history
```

### keep_prompt

```
name:   "reflect"                  # prompt name (omit to list available)
text:   "auth flow"                # optional search context
id:     "now"                      # item for context injection
tags:   {"project": "myapp"}       # filter search results
since:  "P7D"
until:  "2026-02-01"
limit:  5
```

Returns the rendered prompt with `{get}` and `{find}` placeholders expanded with live context. See [KEEP-PROMPT.md](KEEP-PROMPT.md) for prompt details and template syntax.

## Agent Workflow

An MCP agent's session looks like this:

1. **Start:** `keep_prompt("session-start")` — get current context and open commitments
2. **Work:** Use `keep_find`, `keep_get`, `keep_put`, `keep_now`, `keep_tag` as needed
3. **Reflect:** `keep_prompt("reflect")` — structured review of actions and outcomes
4. **Update:** `keep_now` with new intentions, `keep_put` for learnings, `keep_move` to organize

The prompt tools provide the *when* and *why*; the other tools provide the *how*.

## Concurrency

All Keeper calls are serialized through a single `asyncio.Lock`. This is safe for the local SQLite + ChromaDB stores. Cross-process safety (multiple agents sharing a store) is handled at the store layer.

## Local vs Hosted

If you use [keepnotes.ai](https://keepnotes.ai), the hosted service provides the same 9 tools over streamable-http. **Use one or the other, not both** — the tools share the same names (`keep_put`, `keep_find`, etc.), so installing both MCP servers would give the agent duplicate tools and unpredictable routing.

## See Also

- [KEEP-PROMPT.md](KEEP-PROMPT.md) — Agent prompts with context injection
- [AGENT-GUIDE.md](AGENT-GUIDE.md) — Working session patterns
- [REFERENCE.md](REFERENCE.md) — CLI quick reference
- [LANGCHAIN-INTEGRATION.md](LANGCHAIN-INTEGRATION.md) — LangChain/LangGraph integration
