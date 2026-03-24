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
/plugin marketplace add https://github.com/keepnotes-ai/keep.git
/plugin install keep@keepnotes-ai
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

Three tools:

| Tool | Description | Annotations |
|------|-------------|-------------|
| `keep_flow` | Run any operation as a state-doc flow | idempotent |
| `keep_prompt` | Render an agent prompt with context injected | read-only |
| `keep_help` | Browse keep documentation | read-only |

All operations (search, put, get, tag, delete, move, stats) go through `keep_flow` with named state docs. See [FLOW-ACTIONS.md](use keep_help with topic="flow-actions") for the full action reference.

### keep_flow

```
state:          "query-resolve"                    # state doc name
params:         {query: "auth", bias: {now: 0}}    # flow parameters
budget:         3                                   # max ticks
token_budget:   2000                                # token-budgeted rendering
cursor:         "abc123"                            # resume a stopped flow
state_doc_yaml: "..."                               # inline YAML (custom flows)
```

Common state docs:

| State | Purpose | Key params |
|-------|---------|------------|
| `query-resolve` | Search with multi-step refinement | `query`, `tags`, `bias`, `since`, `until` |
| `get` | Retrieve item with similar/meta/versions/edges | `item_id` |
| `find-deep` | Search with edge traversal | `query` |
| `put` | Store content or index a URI | `content` or `uri`, `tags`, `id` |
| `tag` | Apply tags to one or more items | `id` or `items`, `tags` |
| `delete` | Remove an item | `id` |
| `move` | Move versions between items | `name`, `source`, `tags` |
| `stats` | Store profiling for query planning | `top_k` |

### keep_prompt

```
name:   "reflect"                  # prompt name (omit to list available)
text:   "auth flow"                # optional search context
id:     "now"                      # item for context injection
tags:   {"project": "myapp"}       # filter search results
since:  "P7D"
scope:  "file:///path/to/dir*"     # constrain results to ID glob
```

Returns the rendered prompt with placeholders expanded. Supports `{get}`, `{find}`, `{text}`, and `{binding_name}` placeholders (when the prompt doc has a `state` tag referencing a state doc flow). See [KEEP-PROMPT.md](use keep_help with topic="keep-prompt") for prompt details.

### keep_help

```
topic:  "index"                    # documentation topic (default: index)
```

## Agent Workflow

```
keep_prompt(name="session-start")                                              # 1. Start
keep_flow(state="query-resolve", params={query: "topic"}, token_budget=2000)   # 2. Search
keep_flow(state="put", params={content: "insight", tags: {type: "learning"}})  # 3. Capture
keep_prompt(name="reflect")                                                     # 4. Reflect
keep_flow(state="put", params={content: "next steps", id: "now"})              # 5. Update
```

## Concurrency

All Keeper calls are serialized through a single `asyncio.Lock`. This is safe for the local SQLite + ChromaDB stores. Cross-process safety (multiple agents sharing a store) is handled at the store layer.

## See Also

- [FLOW-ACTIONS.md](use keep_help with topic="flow-actions") — Action reference for all operations
- [KEEP-FLOW.md](use keep_help with topic="keep-flow") — Running, resuming, and steering flows
- [KEEP-PROMPT.md](use keep_help with topic="keep-prompt") — Agent prompts with context injection
- [AGENT-GUIDE.md](use keep_help with topic="agent-guide") — Working session patterns
