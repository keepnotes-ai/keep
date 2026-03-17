# Keep as OpenClaw Context Engine

Design doc — 2026-03-16, Hugh + Keeper

## Summary

Replace the current hook-based OpenClaw plugin with a full **ContextEngine**
implementation. Keep becomes the system that owns how context is captured,
assembled, compacted, and enriched — not just a side-channel that injects
`now` output.

## Background

OpenClaw 2026.3.12 introduced `ContextEngine` as a plugin slot
(`plugins.slots.contextEngine`). The interface owns the full context
lifecycle: bootstrap, ingest, assemble, compact, afterTurn, and subagent
lifecycle. The default is `"legacy"` (flat-file conversation + naive
summarization).

Keep's flow system (state docs, token-budgeted responses, background
processing) maps directly onto this interface. The practice layer (reflect
before, during, after) maps onto the lifecycle hooks.

## Architecture

### Transport: Bundled MCP SDK over stdio

The TypeScript plugin bundles `@modelcontextprotocol/sdk` (~30KB) and
spawns `keep mcp` as a persistent stdio process at gateway start. All
keep operations go through MCP tool calls (`keep_flow`, `keep_prompt`).

```
OpenClaw Gateway (Node.js)
  └── keep plugin (TypeScript, in-process)
        └── MCP Client (stdio transport)
              └── keep mcp (Python, persistent process)
                    └── keep store (SQLite + ChromaDB)
```

Latency: ~10-50ms per call (Python processing only, no process spawn).

Lifecycle:
- `gateway_start` hook → spawn `keep mcp`, connect client
- `gateway_stop` hook → close transport, terminate process
- Reconnect on transport error (process crash → respawn)

### Plugin Manifest

```json
{
  "id": "keep",
  "name": "keep",
  "description": "Reflective memory context engine",
  "version": "0.99.0",
  "kind": "context-engine",
  "configSchema": {
    "type": "object",
    "properties": {
      "contextBudgetRatio": {
        "type": "number",
        "default": 0.3,
        "description": "Fraction of token budget for keep context"
      },
      "captureHeartbeats": {
        "type": "boolean",
        "default": false
      },
      "inflectionDetection": {
        "type": "boolean",
        "default": true
      },
      "assembleStateDoc": {
        "type": "string",
        "default": "openclaw-assemble"
      },
      "compactStateDoc": {
        "type": "string",
        "default": "openclaw-compact"
      }
    }
  }
}
```

Setting `kind: "context-engine"` registers keep for the exclusive
context-engine slot. Activated via:
```yaml
plugins:
  slots:
    contextEngine: keep
```

## ContextEngine Method Mapping

### bootstrap(sessionId, sessionKey, sessionFile)

**Practice moment: Reflect BEFORE**

1. Call `keep_prompt(name="session-start")` → rendered prompt with
   current intentions, recent context, open commitments
2. If `sessionFile` exists (resuming), index it:
   `keep_flow(state="put", params={uri: sessionFile, tags: {session: sessionId}})`
3. Return `{ bootstrapped: true, systemPromptAddition: rendered_prompt }`

### ingest(sessionId, message, isHeartbeat)

**Practice moment: Capture (continuous)**

Every message becomes a `now` version. This is by design — the version
history IS the session trace. Background processing (summarize, analyze,
tag, extract_links) fires automatically via `.state/after-write`.

```
skip if isHeartbeat and !config.captureHeartbeats
keep_flow(state="put", params={
  content: truncate(message.content, 500),
  id: "now",
  tags: {session: sessionId, role: message.role}
})
```

Tool results: ingest with `role: "tool"`, content is the result summary
(not the full output — that's often huge).

### ingestBatch(sessionId, messages, isHeartbeat)

Same as ingest but batched. For a multi-message turn, capture the
user message and the final assistant message. Skip intermediate tool
calls unless they're short.

### assemble(sessionId, messages, tokenBudget)

**Practice moment: Surface what matters**

Replaces the current `keep now -n 10` shell-out. Uses a custom state
doc (`.state/openclaw-assemble`) that runs five parallel queries:

```yaml
# .state/openclaw-assemble
match: all
rules:
  - id: intentions
    do: get
    with:
      id: "now"

  - id: similar
    do: find
    with:
      query: "{params.prompt}"
      bias: { "now": 0 }
      limit: 7

  - id: meta
    do: resolve_meta
    with:
      item_id: "now"
      limit: 3

  - id: edges
    do: resolve_edges
    with:
      id: "now"
      limit: 5

  - id: recent
    do: find
    with:
      tags: { session: "{params.session_id}" }
      limit: 5
      order_by: "updated"
```

The token_budget parameter controls how much text is rendered. Keep's
token-budgeted rendering handles truncation.

Returns:
```
{
  messages: [...passed through...],
  estimatedTokens: conversation_tokens + keep_context_tokens,
  systemPromptAddition: rendered_keep_context
}
```

The state doc is editable in the store. Users customize what context
surfaces by editing `.state/openclaw-assemble` — no TypeScript changes.

### afterTurn(sessionId, messages, prePromptMessageCount)

**Practice moment: Reflect DURING**

1. Background processing from ingest is already running
2. Detect inflection signals in new messages:
   - Topic shift (embedding distance between recent messages)
   - Explicit markers ("let's move on", "that's done", "next:")
   - Commitment language ("I will", "let's do", "we should")
   - Significant time gap since last message
3. On deep inflection: trigger `keep_prompt(name="reflect")` in background
4. On topic shift: `keep_flow(state="move", params={name: "thread-{topic}", tags: {session: sessionId}})`

### compact(sessionId, sessionFile, tokenBudget, force)

**Practice moment: Reflect AFTER**

`ownsCompaction: true` — keep handles compaction, not OpenClaw's default.

Keep's compaction preserves structure instead of producing a flat summary:

1. Extract open commitments, decisions, learnings from the conversation
   being compacted → store as tagged items
2. Run `keep_prompt(name="reflect")` on the compacted content
3. Store the reflection as a learning
4. Build a structural summary referencing the extracted items
5. Move older session versions to archive:
   `keep_flow(state="move", params={name: "session-{id}-archive", ...})`

The compacted content is gone from the context window, but the structure
lives in the store and resurfaces via `assemble()` on future turns.

### prepareSubagentSpawn(parentSessionKey, childSessionKey)

Extract relevant context from the parent session and prepare a context
seed for the child. The child's `bootstrap()` will pick it up.

### onSubagentEnded(childSessionKey, reason)

Archive the child session's versions:
`keep_flow(state="move", params={name: "session-{child}", tags: {session: childSessionKey}})`

Index any learnings from the child's work.

## Static System Prompt Injection

The `before_prompt_build` hook supports `appendSystemContext` for static,
cacheable instructions. Keep's protocol block (practice instructions,
tool usage patterns) should go here instead of `prependContext`. This
enables Anthropic/OpenAI prompt caching — the static portion is cached,
only the dynamic context changes per turn.

Split:
- `appendSystemContext` → practice instructions, keep tool reference
  (static, cached)
- `systemPromptAddition` (from assemble) → live context, intentions,
  similar items (dynamic, per-turn)

## State Docs (ship as system docs in keep)

### .state/openclaw-assemble
Context assembly for agent turns. Five parallel queries.
Editable by the user to customize what context surfaces.

### .state/openclaw-compact
Structural compaction. Extract-then-summarize pattern.
Editable to change what gets preserved during compaction.

### .state/openclaw-inflection (future)
Inflection detection rules. Could be a state doc that evaluates
signals and returns a decision (deep-reflect, archive-thread, none).

## Migration Path

1. **v0.99**: Add `kind: "context-engine"` to manifest. Implement
   `ingest()` + `assemble()` + MCP transport. Keep existing hooks
   as fallback for non-context-engine mode.
2. **v1.0**: Add `compact()` with `ownsCompaction: true`. Add
   `afterTurn()` inflection detection. Add `bootstrap()`.
3. **v1.1**: Add subagent lifecycle methods. Remove legacy hook
   fallbacks.

## Open Questions

- **Compaction quality**: Need to validate that keep's structural
  compaction produces better agent behavior than OpenClaw's default
  summarization. Benchmark before committing to `ownsCompaction: true`.
- **Token budget split**: The `contextBudgetRatio` (default 0.3) may
  need tuning per model context window size. 30% of 200K is very
  different from 30% of 8K.
- **Tool result capture**: Full tool results can be huge. Current plan
  is to capture summaries only. But some tool results (code, data) are
  the most valuable context. May need a size-aware capture strategy.
- **Multi-store**: When keep is used as context engine, should it use
  a separate store per agent/session, or the shared user store? The
  shared store means cross-session context accumulates (the whole point),
  but also means noise from unrelated sessions.

## Decisions Made

- Bundle `@modelcontextprotocol/sdk` in the plugin (small, typed, reconnect)
- Persistent `keep mcp` process over stdio (not CLI per-call)
- Every user/assistant message into `now` versions (by design, not noise)
- Skip system and tool messages from ingest (huge, low-signal)
- State docs for configurable behavior (YAML > TypeScript)
- Practice integration at every lifecycle point
- `afterTurn()` with inflection detection for reflection triggers
- Start with assemble + ingest, add compaction ownership after validation
- `isContextEngine` flag set in factory (not bootstrap) to guard legacy hooks
- `ingestBatch()` implemented for turn-level capture
- `formatAssembleData()` renders structured flow output as readable markdown
