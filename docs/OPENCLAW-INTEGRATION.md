# OpenClaw Integration

## Why

OpenClaw writes memory files (markdown) which are a solid foundation for
long-term memory. But flat files aren't enough to reliably recall what's
happening, what needs follow-up, or to build a continuous improvement loop.

[Keep](https://github.com/keepnotes-ai/keep) integrates with OpenClaw as a
**context engine** — it owns how context is captured, assembled, and enriched
across the agent's lifecycle.

What this gives you:

1. **Semantic context assembly** — every agent turn, keep surfaces relevant
   memories: similar items, open commitments, learnings, edge relationships.
   The agent starts each turn knowing what matters, not just what's recent.
2. **Continuous capture** — user and assistant messages are ingested into keep's
   versioned store. Background processing (summarize, analyze, tag, link
   extraction) runs automatically.
3. **Inflection-aware reflection** — after each turn, keep detects topic shifts,
   commitments, and significant moments. When it finds one, it triggers the
   reflective practice in the background.
4. **Memory indexing** — when OpenClaw compacts context, keep indexes workspace
   memory files and turns them into searchable, tagged, versioned knowledge.
5. **Session archival** — when a session ends, keep archives its message trace
   and cleans up the working context.

---

## Quick Start

### 1. Install keep

```bash
uv tool install keep-skill                    # API providers included
# Or: uv tool install 'keep-skill[local]'    # Local models, no API keys needed
```

### 2. Configure providers

```bash
keep config --setup                           # Interactive wizard
# Or: just have Ollama running (auto-detected)
# Or: set OPENAI_API_KEY, GEMINI_API_KEY, etc.
```

### 3. Install the plugin

```bash
openclaw plugins install -l $(keep config openclaw-plugin)
openclaw gateway restart
```

### 4. Verify

```bash
openclaw plugins list                         # Should show: keep (loaded, 0.99.0)
openclaw keep doctor                          # Check keep store health
```

The plugin spawns a persistent `keep mcp` process at gateway start. All keep
operations go through MCP for low latency (~10-50ms per call).

---

## How It Works

### Context Engine (automatic)

When the plugin is installed, OpenClaw activates keep as the context engine
(`plugins.slots.contextEngine: keep`). This means keep participates in every
stage of the agent lifecycle:

| Lifecycle method | What keep does |
|-----------------|----------------|
| **bootstrap** | Initialize session state, index resume file if present |
| **ingest** | Capture each user and assistant message as a `now` version |
| **assemble** | Run `.state/openclaw-assemble` flow — 5 parallel queries for intentions, similar items, meta-tags, edges, and recent session context |
| **afterTurn** | Detect inflection points (topic shifts, commitments, substantial work), trigger background reflection |
| **compact** | Index the session transcript into keep's store (advisory — OpenClaw still manages its own compaction in v0.99) |
| **session_end** | Archive session message versions, clean up working context |

The agent doesn't need to do anything — context flows in automatically.

### Practice Layer (agent-initiated)

The agent also has voluntary access to keep via CLI commands:

```
keep prompt reflect                              # Full reflection practice
keep flow get-context -p item_id=now             # Current intentions + context
keep flow query-resolve -p query="topic"         # Semantic search
keep flow put -p content="insight" -p 'tags={"type":"learning"}'  # Capture
keep flow put -p content="next steps" -p id=now  # Update intentions
```

These are injected into the agent's system prompt as cacheable static
instructions (`appendSystemContext`), so they don't add per-turn token cost.

### Memory Indexing (on compaction)

After OpenClaw compacts conversation context, the plugin indexes workspace
memory files (`memory/` directory and `MEMORY.md`) into keep. This uses the
file-stat fast-path (mtime+size check) — unchanged files are skipped without
even reading them. Safe to run on every compaction.

---

## Customizing Context Assembly

The context assembly flow is a keep state doc: `.state/openclaw-assemble`.
Edit it to change what context the agent sees:

```bash
keep get .state/openclaw-assemble              # View current
keep put --id .state/openclaw-assemble ...     # Replace
keep config --reset-system-docs                # Restore defaults
```

The default runs five parallel queries:

1. **intentions** — current `now` content (what the agent is working on)
2. **similar** — semantically similar items to the current user prompt
3. **meta** — resolved meta-docs (learnings, todos, open commitments)
4. **edges** — edge relationships from `now` (references, speakers, threads)
5. **recent** — recent items from this session

All results are token-budgeted so they fit within the context window.

---

## Plugin Configuration

Configure in OpenClaw's config under `plugins.entries.keep.config`:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `contextBudgetRatio` | number | 0.3 | Fraction of token budget for keep context (0.0–1.0) |
| `captureHeartbeats` | boolean | false | Whether to capture heartbeat messages |

Example:

```yaml
plugins:
  entries:
    keep:
      enabled: true
      config:
        contextBudgetRatio: 0.25
```

---

## CLI Commands

The plugin registers CLI commands under `openclaw keep`:

| Command | Description |
|---------|-------------|
| `openclaw keep setup` | Run keep's interactive setup wizard (providers, models) |
| `openclaw keep doctor` | Check keep store health and diagnose issues |

---

## Provider Configuration

keep auto-detects AI providers from environment variables:

```bash
export OPENAI_API_KEY=...      # Handles both embeddings + summarization
# Or: GEMINI_API_KEY=...       # Also does both
# Or: VOYAGE_API_KEY=... and ANTHROPIC_API_KEY=...  # Separate services
```

If Ollama is running locally, it's auto-detected with no configuration needed.

For explicit setup: `openclaw keep setup` or `keep config --setup`

For local-only operation (no API keys): `uv tool install 'keep-skill[local]'`

See [QUICKSTART.md](QUICKSTART.md) for full provider options and troubleshooting.

---

## Recommended: Daily Reflection Cron

For automatic deep reflection with document analysis:

```bash
openclaw cron add \
  --name keep-reflect \
  --cron "0 21 * * *" --tz "America/New_York" \
  --session isolated \
  --agent-turn "Daily memory sync and reflection.

1. Index memory files with analysis: run \`keep put /path/to/workspace/memory/ --analyze\`
2. Reflect: run \`keep prompt reflect\` and follow the practice.

This is the deeper review — not just what happened, but what it means."
```

This runs nightly in an isolated session. The `--analyze` flag finds themes
within each document; unchanged files cost nothing via hash-skip.

---

## Integration Layers

| Layer | Trigger | What it does | Latency |
|-------|---------|-------------|---------|
| **Context engine** | Every agent turn | Ingest messages, assemble context, detect inflections | ~10-50ms |
| **Memory sync** | After compaction | Index workspace memory files | Background |
| **Session archival** | Session end | Archive message trace, clean up `now` | Background |
| **Daily reflection** | Cron (optional) | Deep analysis + practice reflection | Isolated session |
| **Agent practice** | Agent-initiated | Voluntary reflection, search, capture | On demand |

---

## Architecture

```
OpenClaw Gateway (Node.js)
  └── keep plugin (TypeScript, in-process)
        ├── Context Engine (bootstrap, ingest, assemble, afterTurn, compact)
        ├── Legacy Hooks (fallback when not context engine)
        │     ├── before_prompt_build → context injection
        │     ├── after_compaction → memory file indexing
        │     └── session_end → version archival
        └── MCP Client (stdio transport)
              └── keep mcp (Python, persistent process)
                    └── keep store (SQLite + ChromaDB)
```

The MCP transport bundles `@modelcontextprotocol/sdk` and spawns `keep mcp` as
a persistent stdio process at gateway start. All operations have per-type
timeouts (8s for assemble, 15s for writes, 30s for queries). If the MCP
process fails, the plugin falls back to CLI (`execFileSync`) for all operations.

---

## Upgrading

After upgrading keep:

```bash
uv tool upgrade keep-skill
openclaw plugins install -l $(keep config openclaw-plugin)
openclaw gateway restart
```

The plugin source lives at `$(keep config openclaw-plugin)` — this resolves to
the `openclaw-plugin/` directory inside the installed keep package. The build
step (`npm install + npm run build`) runs automatically during pip packaging.

---

## Troubleshooting

**Plugin loads but MCP not connected:**
```bash
keep config                    # Verify keep is configured
keep config providers          # Verify providers are set
openclaw keep doctor           # Run health check
```

**Context engine not activating:**
Check that `plugins.slots.contextEngine` is set to `"keep"`:
```bash
grep contextEngine ~/.openclaw/openclaw.json
```

**Slow context assembly:**
The `.state/openclaw-assemble` flow runs 5 parallel queries. If one is slow
(e.g., embedding provider timeout), it blocks the whole assembly. Check:
```bash
keep doctor --log              # Watch flow execution in real time
```

**Falling back to legacy hooks:**
If the MCP transport fails to connect, the plugin falls back to the original
hook-based behavior (CLI shell-outs). Check gateway logs for:
```
[keep] MCP connect failed, falling back to CLI
```

**Reset to defaults:**
```bash
# Reset context engine slot to legacy
# Edit ~/.openclaw/openclaw.json: set plugins.slots.contextEngine to "legacy"
openclaw gateway restart

# Reset keep's state docs
keep config --reset-system-docs
```
