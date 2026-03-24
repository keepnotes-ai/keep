# Prompt / State Doc Alignment

## Problem

Context assembly has three inconsistent paths:

1. **`keep get`/`keep now`** → calls `get_context()` → runs `get-context` state doc → renders frontmatter. No prompt involved.
2. **`keep prompt openclaw-assemble`** → runs `openclaw-assemble` state doc (via `state:` tag on prompt doc) → renders bindings into template. Clean separation.
3. **`keep prompt reflect`** → no state doc, hardcoded `get_context()` + `find()` → renders `{get}`/`{find}` magic placeholders. Bypasses flows entirely.

Path 2 is correct. Paths 1 and 3 need alignment.

## Design

### Three layers

**Layer 1: State doc** (`.state/*`) — runs queries, produces bindings. Pure data.
- `get-context` → similar, parts, meta, edges
- `openclaw-assemble` → intentions, similar, meta, edges, session
- `query-resolve` → search results

**Layer 2: `keep get`** — renders item + bindings as structured output.
- Input: item (content) + state doc bindings (frontmatter)
- Output: YAML frontmatter + body (fixed format)
- This is the data layer — JSON, YAML, or frontmatter

**Layer 3: Prompt** (`.prompt/*`) — wraps structured output in a template.
- Input: state doc bindings (via `state:` tag)
- Output: text for a specific audience (LLM, agent, human)
- Template uses binding names as placeholders: `{similar}`, `{meta}`, etc.

### Output modes

| Command | Layer | Format |
|---------|-------|--------|
| `keep get <id> --json` | 2 | JSON (item + bindings) |
| `keep get <id> --yaml` | 2 | YAML frontmatter + body |
| `keep get <id>` (default) | 2 | Frontmatter (current behavior) |
| `keep prompt <name>` | 3 | Rendered text (prompt template filled with bindings) |
| MCP `keep_prompt` | 3 | Rendered text (same as above, via MCP) |

### Every prompt has a state doc

All `.prompt/*` docs reference a state doc via their `state:` tag. The prompt template uses binding names from that state doc. No more hardcoded `{get}`/`{find}` paths.

```
.prompt/agent/reflect     state: get-context
.prompt/agent/assemble    state: get-context-full    (renamed from openclaw-assemble)
.prompt/agent/conversation state: get-context
.prompt/agent/session-start state: get-context
.prompt/agent/query       state: query-resolve
```

The hardcoded path in `render_prompt()` (Path B: "without state doc") goes away. If a prompt doesn't have a `state:` tag, it's a static prompt (no context assembly).

### Rename: state doc only

The `openclaw-assemble` **state doc** is not openclaw-specific — it's a general "full context assembly" that includes intentions, similar, meta, edges, and session. Rename to `get-context-full` (or merge differences into `get-context`).

The **prompt doc** `.prompt/agent/openclaw-assemble` stays as-is. It's the openclaw-specific template that wraps the general state doc — users may customize it for their openclaw setup without affecting other prompts. The prompt's `state:` tag updates to point to the renamed state doc.

### `get-context` → `.state/get` with fragment composition

Today's two state docs converge into one base + fragment:

**`.state/get`** (renamed from `get-context`) — the base context assembly:
```yaml
match: all
rules:
  - id: similar
    when: "!has(params.prompt) || params.prompt == ''"
    do: find
    with:
      similar_to: "{params.item_id}"
      limit: "{params.similar_limit}"
  - id: parts
    do: find
    with:
      prefix: "{params.item_id}@p"
      limit: "{params.parts_limit}"
  - id: meta
    do: resolve_meta
    with:
      item_id: "{params.item_id}"
      limit: "{params.meta_limit}"
  - id: edges
    do: resolve_edges
    with:
      id: "{params.item_id}"
      limit: "{params.edges_limit}"
```

Edge resolution moves into the state doc (the `resolve_edges` action already exists). The `similar` rule has a `when` guard so the openclaw fragment can provide an alternative search strategy.

**`.state/get/openclaw`** — fragment adding openclaw-specific rules:
```yaml
order: before:similar
rules:
  - id: search
    when: "has(params.prompt) && params.prompt != ''"
    do: find
    with:
      query: "{params.prompt}"
      bias: { "now": 0 }
      limit: 7
  - id: intentions
    do: get
    with:
      id: "now"
  - id: session
    do: get
    with:
      id: "{params.session_id}"
```

The fragment uses `order: before:similar` to insert its `search` rule before the base `similar` rule. Complementary `when` guards ensure exactly one fires: `search` when `params.prompt` exists (agent context), `similar` when it doesn't (CLI context).

**Result:** `load_state_doc("get")` composes the base + fragment into a single doc. CLI users get the base (4 rules). Openclaw gets the composed doc (7 rules). Same state doc name, different compositions.

This mirrors the `after-write` pattern where fragments extend the base without modifying it.

### `{get}` and `{find}` migration

Old prompts use `{get}` and `{find:query text}`. These become:
- `{get}` → the prompt references a state doc; bindings are expanded by name. `{get}` is sugar for "render all bindings" or maps to specific bindings like `{similar}`, `{meta}`.
- `{find:query text}` → becomes a `find` rule in the state doc with `query: "{params.query}"`. The prompt uses the binding name.

During migration, `{get}` can be kept as a compatibility alias that expands to the state doc's combined output. But new prompts should use explicit binding names.

## What changes

1. **Rename `state-get-context.md` → `state-get.md`**: ID becomes `.state/get`. All callers update.
2. **Edges into the state doc**: Move edge resolution from inline `_resolve_edge_refs()` into `.state/get` as a `resolve_edges` rule. The action already exists.
3. **Create `.state/get/openclaw` fragment**: Adds `search`, `intentions`, `session` rules. Ships as a system doc.
4. **Delete `state-openclaw-assemble.md`**: Replaced by `.state/get` + `.state/get/openclaw` composition.
5. **Update `.prompt/agent/openclaw-assemble`**: Change `state:` tag from `openclaw-assemble` → `get`.
6. **All prompt docs get `state: get` tags**: Prompts that lack a `state:` tag get wired to `.state/get`.
7. **`render_prompt()` Path B removed**: The hardcoded `get_context()` + `find()` fallback goes away. All prompts go through state docs.
8. **`{get}`/`{find}` compat**: Kept as aliases during migration. `{get}` expands to combined binding output. `{find:X}` expands to a search binding.
9. **`get_context()` updated**: Calls `_run_read_flow("get", ...)` instead of `"get-context"`. Params gain `edges_limit`.

## What stays

- State docs are unchanged in syntax (same `match`/`rules`/`do`/`with`)
- Prompt docs are unchanged in syntax (frontmatter + template body)
- `keep get` / `keep now` CLI behavior unchanged (still renders frontmatter)
- MCP `keep_prompt` tool unchanged (still calls `render_prompt`)
- `.prompt/agent/openclaw-assemble` keeps its name (platform-specific template)

## Implementation order

1. Rename `state-get-context.md` → `state-get.md`, add edges rule, add `when` guard on `similar`
2. Create `state-get/openclaw.md` fragment (search, intentions, session)
3. Delete `state-openclaw-assemble.md`
4. Update `get_context()` to call `_run_read_flow("get", ...)`
5. Add `state: get` tag to prompt docs that lack it
6. Update `render_prompt()` to always use the state-doc path
7. Migrate `{get}`/`{find}` to binding-name placeholders in prompt templates
8. Remove hardcoded fallback path from `render_prompt()`
9. Update tests and docs
