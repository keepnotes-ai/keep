# Prompt Templates Over State Doc Bindings

Date: 2026-03-17
Status: Implemented (v0.103.0)

## Problem

Prompts currently support only `{get}` and `{find}` placeholders with
limited parameter overrides (`{find:deep:3000}`). Custom retrieval
pipelines like `state-openclaw-assemble` (5 parallel queries) can't be
expressed in this syntax.

## Design

A prompt doc can reference a state doc as its retrieval strategy via a
`state:` tag in frontmatter. The state doc runs, and its **bindings**
become available as `{binding_name}` placeholders in the template.

### Example

```markdown
---
tags:
  context: prompt
  state: openclaw-assemble
---
# .prompt/agent/openclaw-query

Answer questions using OpenClaw memory.

## Prompt
{intentions}
{similar}
{recent}

Question: {text}
Answer precisely using the context above.
```

The state doc `openclaw-assemble` produces bindings `intentions`,
`similar`, `meta`, `edges`, `recent`. The prompt template selects
which bindings to render and where.

## Key properties

- **Prompts stay markdown** — human-readable agent instructions
- **State docs stay YAML** — retrieval pipeline definitions
- **Separation of concerns** — prompt = what to say, state doc = what to fetch
- **Backward compatible** — `{get}` and `{find}` keep working when no `state:` tag
- **Composable** — different prompts can share a state doc, or swap state docs
- **No new DSL** — one frontmatter field + binding-name placeholders

## Implementation changes

1. `render_prompt()` — if prompt doc has `state` tag, run that flow
   instead of hardcoded get/find
2. `expand_prompt()` — expand `{binding_name}` placeholders from flow
   bindings, rendered by type-appropriate formatters
3. `_render_binding()` helper — dispatch on binding shape:
   - find results -> `render_find_context`
   - get result -> `render_context`
   - meta -> meta section format
   - edges -> edge format
4. Parameter override syntax (`{find:deep:N}`) becomes unnecessary —
   the state doc controls all retrieval parameters
