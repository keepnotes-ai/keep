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
5. **Session persistence** — each session is a versioned item keyed by its
   routing identity. Turns accumulate as versions. No archival step needed.

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
openclaw plugins list                         # Should show: keep (loaded)
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
| **bootstrap** | Mark session for first-assemble enrichment |
| **ingest** | Capture each message as a version of the session item (keyed by `sessionKey`) |
| **ingestBatch** | Capture a complete turn as a single version of the session item |
| **assemble** | Render the `openclaw-assemble` prompt template — retrieves intentions, similar items, meta, edges, and session history via state-doc bindings |
| **afterTurn** | Detect inflection points (topic shifts, commitments, substantial work), trigger background reflection, set up workspace watches |
| **compact** | Advisory only — logs diagnostics (OpenClaw manages its own compaction) |
| **prepareSubagentSpawn** | Link child session to parent via tags, write spawn marker as first version of child item |
| **onSubagentEnded** | Clean up tracking state (child session item persists) |

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

### Memory Indexing (automatic)

The plugin sets up daemon-driven watches on the workspace directory (including
`memory/` and `MEMORY.md`) on the first agent turn. The daemon polls for
changes automatically — no manual indexing needed. Git repositories in the
workspace are also discovered and their commit history is ingested incrementally.

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

1. **intentions** — current `now` content (cross-session intentions)
2. **similar** — semantically similar items to the current user prompt
3. **meta** — resolved meta-docs (learnings, todos, open commitments)
4. **edges** — edge relationships from `now` (references, speakers, threads)
5. **session** — the current session item (versioned turn history)

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

For automatic deep reflection:

```bash
openclaw cron add \
  --name keep-reflect \
  --cron "0 21 * * *" --tz "America/New_York" \
  --session isolated \
  --agent-turn "Daily reflection. Run \`keep prompt reflect\` and follow the practice.

This is the deeper review — not just what happened, but what it means."
```

This runs nightly in an isolated session. Memory files (workspace `memory/`
directory, `MEMORY.md`, etc.) are indexed automatically by the plugin's
workspace watch — no manual `keep put` needed. The watch is set up on the
first agent turn after gateway start, and the daemon keeps it alive across
restarts.

---

## Integration Layers

| Layer | Trigger | What it does | Latency |
|-------|---------|-------------|---------|
| **Context engine** | Every agent turn | Ingest messages, assemble context, detect inflections | ~10-50ms |
| **Workspace watches** | Daemon-driven | Index files, memory, git history automatically | Background |
| **Daily reflection** | Cron (optional) | Deep practice reflection | Isolated session |
| **Agent practice** | Agent-initiated | Voluntary reflection, search, capture | On demand |

---

## Architecture

```
OpenClaw Gateway (Node.js)
  └── keep plugin (TypeScript, in-process)
        ├── Context Engine
        │     ├── bootstrap, ingest, ingestBatch
        │     ├── assemble (via prompt template + state doc)
        │     ├── afterTurn (inflection detection, workspace watches)
        │     ├── compact (advisory)
        │     └── prepareSubagentSpawn, onSubagentEnded
        └── MCP Client (stdio transport)
              └── keep mcp (Python, persistent process)
                    ├── keep store (SQLite + ChromaDB)
                    └── daemon (watches, git ingest, background work)
```

The MCP transport bundles `@modelcontextprotocol/sdk` and spawns `keep mcp` as
a persistent stdio process at gateway start. All operations have per-type
timeouts (8s for assemble/prompts, 15s for writes, 30s for queries).

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
