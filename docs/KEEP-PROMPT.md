# keep prompt

Render an agent prompt with injected context from the store.

## Usage

```bash
keep prompt --list                        # List available prompts
keep prompt reflect                       # Render the reflect prompt
keep prompt reflect "auth flow"           # With search context
keep prompt reflect --id %abc123          # Context from specific item
keep prompt reflect --since P7D           # Recent context only
keep prompt reflect --tag project=myapp   # Scoped to project
```

## Options

| Option | Description |
|--------|-------------|
| `--list`, `-l` | List available agent prompts |
| `--id ID` | Item ID for `{get}` context (default: `now`) |
| `--tag`, `-t` KEY=VALUE | Filter search context by tag (repeatable) |
| `--since DURATION` | Only items updated since (ISO duration or date) |
| `--until DURATION` | Only items updated before (ISO duration or date) |
| `--scope`, `-S` GLOB | Constrain search results to IDs matching glob |
| `-n`, `--limit N` | Max search results (default: 5) |

## Template placeholders

Prompt docs may contain placeholders that are expanded at render time:

| Placeholder | Expands to |
|-------------|------------|
| `{get}` | Full context for `--id` target (default: `now`) from the prompt's state-doc bindings |
| `{find}` | Search results for the text argument from the prompt's state-doc bindings |
| `{find:deep}` | Deep search — follows edges from results to discover related items |
| `{find:deep:N}` | Deep search with explicit token budget N |
| `{text}` | Raw query text passed as the text argument |
| `{since}` | The `--since` filter value (ISO duration or date), empty if not given |
| `{until}` | The `--until` filter value (ISO duration or date), empty if not given |
| `{binding_name}` | Flow binding — see *State-doc backed prompts* below |

When no text argument is given, `{find}` and `{text}` expand to empty. When no `--id` is given, `{get}` shows the `now` document context.

## State-doc backed prompts

A prompt doc can reference a state doc via a `state` tag. When present, the state doc flow runs and its **bindings** become `{binding_name}` placeholders in the template. This allows custom retrieval pipelines to be composed into prompts.

Dynamic prompts are state-doc backed: a prompt that uses `{get}` or `{find}` but has no `state` tag is treated as invalid instead of falling back to a built-in Python retrieval path.

### Example

Given the `.state/get` state doc (with its openclaw fragment) that produces bindings `intentions`, `similar`, `meta`, `edges`, `session`:

```bash
keep put "$(cat <<'EOF'
## Prompt
{intentions}
{similar}
{session}

Question: {text}
Answer precisely using the context above.
EOF
)" --id .prompt/agent/my-query -t state=get -t context=prompt
```

Now `keep prompt my-query "auth flow"` runs the `get` state doc and renders its bindings into the template.

Each binding is rendered by type:
- **find results** (`{results: [...]}`) — token-budgeted search results with summaries
- **get results** (`{id, summary}`) — YAML frontmatter context
- **meta sections** (`{sections: {...}}`) — grouped meta items
- **edges** (`{edges: {...}}`) — resolved edge references

The token budget is distributed across bindings. Bindings not referenced in the template are ignored.

## Prompt docs

Agent prompts live in the store as `.prompt/agent/*` system documents. They use the same `## Prompt` section format as other keep prompt docs, but contain agent-facing instructions rather than LLM system prompts.

Bundled prompts, loaded on first use:

| Prompt | ID | Purpose |
|--------|----|---------|
| `reflect` | `.prompt/agent/reflect` | Full structured reflection practice |
| `conversation` | `.prompt/agent/conversation` | Conversation analysis — commitments, breakdowns, moods |
| `query` | `.prompt/agent/query` | Answer questions using retrieved memory context |
| `session-start` | `.prompt/agent/session-start` | Context injection at session start |
| `subagent-start` | `.prompt/agent/subagent-start` | Context injection for subagent initialization |

### Viewing and editing

```bash
keep get .prompt/agent/reflect            # View the prompt doc
```

Prompt docs are editable — they version like any other store document. User edits are preserved across upgrades (content-hash detection).

## keep reflect

`keep reflect` is an alias for `keep prompt reflect`. It accepts the same text argument and `--id` option:

```bash
keep reflect                              # Same as: keep prompt reflect
keep reflect "auth flow"                  # Same as: keep prompt reflect "auth flow"
keep reflect --id %abc123                 # Same as: keep prompt reflect --id %abc123
```

## See Also

- [KEEP-MCP.md](KEEP-MCP.md) — MCP server (`keep_prompt` tool)
- [KEEP-NOW.md](KEEP-NOW.md) — Current intentions
- [AGENT-GUIDE.md](AGENT-GUIDE.md) — Working session patterns
- [REFERENCE.md](REFERENCE.md) — Quick reference index
