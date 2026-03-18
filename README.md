# keep

An agent-skill: memory that pays attention.

It includes [skill instructions](SKILL.md) for reflective practice, and a powerful semantic memory system with [command-line](docs/QUICKSTART.md) and [MCP](docs/KEEP-MCP.md) interfaces. Fully local, or use API keys for model providers, or [cloud-hosted](https://keepnotes.ai) for multi-agent use.

```bash
uv tool install keep-skill       # or: pip install keep-skill
export OPENAI_API_KEY=...        # Or GEMINI_API_KEY (both do embeddings + summarization)

# Index content (store auto-initializes on first use)
keep put https://inguz.substack.com/p/keep -t topic=practice
keep put "Rate limit is 100 req/min" -t topic=api

# Index a codebase — recursive, with daemon-driven watch for changes
keep put ./my-project/ -r --watch

# Search by meaning
keep find "what's the rate limit?"

# Track what you're working on
keep now "Debugging auth flow"

# Instructions for reflection
keep prompt reflect
```

---

## What It Does

Store anything — notes, files, URLs — and `keep` summarizes, embeds, and tags each item. You search by meaning, not keywords. Content goes in as text, PDF, HTML, Office documents, audio, or images; what comes back is a summary with tags and semantic neighbors. Audio and image files auto-extract metadata tags (artist, album, camera, date, etc.).

What makes this more than a vector store: tags become edges. Define a tag like `author` or `git_commit` and keep creates bidirectional links — a user-defined graph model where every tag can be a navigable relationship. When you retrieve any item, keep follows these edges and fires standing queries — surfacing open commitments, past learnings, referenced files, commit history. The right things appear at the right time, without manual graph construction.

- **Summarize, embed, tag** — URLs, files, and text are summarized and indexed on ingest
- **Contextual feedback** — Open commitments and past learnings surface automatically
- **Semantic search** — Find by meaning, not keywords; scope to a folder or project
- **Tag organization** — Speech acts, status, project, topic, type — structured and queryable
- **Deep search** — Follow edges and tags from results to discover related items across the graph
- **Edge tags** — Turn tags into navigable relationships with automatic inverse links
- **Git changelog** — Commits indexed as searchable items with edges to touched files
- **Parts** — `analyze` decomposes documents into searchable sections, each with its own embedding and tags
- **Strings** — Every note is a string of versions; reorganize history by meaning with `keep move`
- **Watches** — Daemon-driven directory and file monitoring; re-indexes on change
- **Works offline** — Local models (MLX, Ollama), or API providers (Voyage, OpenAI, Gemini, Anthropic, Mistral)

Backed by ChromaDB for vectors, SQLite for metadata and versions.

> **[keepnotes.ai](https://keepnotes.ai)** — Hosted service. No local setup, no API keys to manage. Same SDK, managed infrastructure.

### The Practice

keep is designed as a skill for AI agents — a practice, not just a tool. The [skill instructions](SKILL.md) teach agents to reflect before, during, and after action: check intentions, recognize commitments, capture learnings, notice breakdowns. `keep prompt reflect` guides a structured reflection ([details](docs/KEEP-PROMPT.md)); `keep now` tracks current intentions and surfaces what's relevant.

This works because the tool and the skill reinforce each other. The tool stores and retrieves; the skill says *when* and *why*. An agent that uses both develops *skillful action* across sessions — not just recall, but looking before acting, and a deep review of outcomes afterwards.

> Why build memory for AI agents? What does "reflective practice" mean here? **[Read our blog for the back-story →](https://keepnotes.ai/blog/)**

### Integration

The skill instructions and hooks install into your agent's configuration automatically on first use. The CLI alone is enough to start; the hooks make it automatic.

| Tool | Integration |
|------|-------------|
| **[OpenClaw](docs/OPENCLAW-INTEGRATION.md)** | Context engine plugin — full memory assembly, session archival, reflection triggers |
| **Claude Desktop** | `keep config mcpb` ([details](docs/CLAUDE-DESKTOP.md)) |
| **Claude Code** | Plugin: `/plugin install keep@keepnotes-ai` |
| **VS Code Copilot** | MCP: `code --add-mcp '{"name":"keep","command":"keep","args":["mcp"]}'` |
| **Kiro** | MCP + practice prompt: `kiro-cli mcp add --name keep --scope global -- keep mcp` |
| **OpenAI Codex** | MCP: `codex mcp add keep -- keep mcp` |
| **LangChain** | [LangGraph BaseStore](docs/LANGCHAIN-INTEGRATION.md), retriever, tools, and middleware |
| **Any MCP client** | [Stdio server](docs/KEEP-MCP.md) with 3 tools (`keep_flow`, `keep_prompt`, `keep_help`) |

After install, just tell your agent: *Please read all the keep_help documentation, and then use keep_prompt(name="reflect") to save some notes about what you learn.*

---

## Installation

**Python 3.11–3.13 required.** Use [uv](https://docs.astral.sh/uv/) (recommended) or pip:

```bash
uv tool install keep-skill
```

**Hosted** (simplest — no local setup needed):
```bash
export KEEPNOTES_API_KEY=...   # Sign up at https://keepnotes.ai
```

**Self-hosted** with API providers:
```bash
export OPENAI_API_KEY=...      # Simplest (handles both embeddings + summarization)
# Or: GEMINI_API_KEY=...       # Also does both
# Or: VOYAGE_API_KEY=... and ANTHROPIC_API_KEY=...  # Separate services
```

**Local** (offline, no API keys): If [Ollama](https://ollama.com/) is running, keep auto-detects it. Or on macOS Apple Silicon: `uv tool install 'keep-skill[local]'`

**LangChain/LangGraph** integration: `pip install keep-skill[langchain]` or `pip install langchain-keep`

See [docs/QUICKSTART.md](docs/QUICKSTART.md) for all provider options.

---

## Quick Start

```bash
# Index URLs, files, and notes (store auto-initializes on first use)
keep put https://example.com/api-docs -t topic=api
keep put "Token refresh needs clock sync" -t topic=auth

# Index a codebase — recursive, with auto-watch for changes
keep put ./my-project/ -r --watch
# git: 2 repo(s) queued for changelog ingest

# Search
keep find "authentication flow" --limit 5
keep find "auth" --deep                # Follow edges to discover related items
keep find "auth" --scope 'file:///Users/me/project/*'  # Scoped to a folder

# Retrieve
keep get file:///path/to/doc.md
keep get ID --history                  # All versions
keep get ID --parts                    # Analyzed sections

# Tags
keep list --tag project=myapp          # Find by tag
keep list 'git://github.com/org/repo@*'  # All git tags/releases

# Current intentions
keep now                               # Show what you're working on
keep now "Fixing login bug"            # Update intentions
```

### Python API

```python
from keep import Keeper

kp = Keeper()

# Index
kp.put(uri="file:///path/to/doc.md", tags={"project": "myapp"})
kp.put("Rate limit is 100 req/min", tags={"topic": "api"})

# Search
results = kp.find("rate limit", limit=5)
for r in results:
    print(f"[{r.score:.2f}] {r.summary}")

# Version history
prev = kp.get_version("doc:1", offset=1)
versions = kp.list_versions("doc:1")
```

See [docs/QUICKSTART.md](docs/QUICKSTART.md) for configuration and more examples.

---

## Documentation

Full docs at **[docs.keepnotes.ai](https://docs.keepnotes.ai)** — or browse locally:

- **[docs/QUICKSTART.md](docs/QUICKSTART.md)** — Setup, configuration, first steps
- **[docs/REFERENCE.md](docs/REFERENCE.md)** — Quick reference index
- **[docs/KEEP-PUT.md](docs/KEEP-PUT.md)** — Indexing: files, directories, URLs, git changelog, watches
- **[docs/KEEP-FIND.md](docs/KEEP-FIND.md)** — Semantic search, deep search, scoped search
- **[docs/TAGGING.md](docs/TAGGING.md)** — Tags, speech acts, project/topic organization
- **[docs/PROMPTS.md](docs/PROMPTS.md)** — Prompts for summarization, analysis, and agent workflows
- **[docs/OPENCLAW-INTEGRATION.md](docs/OPENCLAW-INTEGRATION.md)** — OpenClaw context engine plugin
- **[docs/KEEP-MCP.md](docs/KEEP-MCP.md)** — MCP server for AI agent integration
- **[docs/AGENT-GUIDE.md](docs/AGENT-GUIDE.md)** — Working session patterns
- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — How it works under the hood
- **[SKILL.md](SKILL.md)** — The reflective practice (for AI agents)

---

## License

MIT

---

## Contributing

Published on [PyPI as `keep-skill`](https://pypi.org/project/keep-skill/).

Issues and PRs welcome:
- Provider implementations
- Performance improvements
- Documentation clarity

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
