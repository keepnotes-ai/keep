# keep

An agent-skill: memory that pays attention.

It includes [skill instructions](SKILL.md) for reflective practice, and a powerful semantic memory system with command-line and MCP interfaces.  Local-only, hybrid or cloud.

```bash
uv tool install keep-skill       # or: pip install keep-skill
export OPENAI_API_KEY=...        # Or GEMINI_API_KEY (both do embeddings + summarization)

# Index content (store auto-initializes on first use)
keep put https://inguz.substack.com/p/keep -t topic=practice
keep put "file://$(keep config tool)/docs/library/han_verse.txt" -t type=teaching
keep put "Rate limit is 100 req/min" -t topic=api

# Search by meaning
keep find "what's the rate limit?"

# Track what you're working on
keep now "Debugging auth flow"
keep now -V 1                    # Previous intentions

# Instructions for reflection
keep prompt reflect
```

---

## What It Does

Store anything — URLs, files, notes — and `keep` summarizes, embeds, and tags each item. You search by meaning, not keywords. Content goes in as text, PDF, HTML, Office documents, audio, or images; what comes back is a summary with tags and semantic neighbors. Audio and image files auto-extract metadata tags (artist, album, camera, date, etc.).

What makes this more than a vector store: when you view your current context (`keep now`) or retrieve any item (`keep get`), keep automatically surfaces relevant open commitments, past learnings, and breakdowns — ranked by similarity and recency. The right things appear at the right time. That's what makes reflection real.

- **Summarize, embed, tag** — URLs, files, and text are summarized and indexed on ingest
- **Contextual feedback** — Open commitments and past learnings surface automatically
- **Semantic search** — Find by meaning, not keywords
- **Tag organization** — Speech acts, status, project, topic, type — structured and queryable
- **Deep search** — Follow edges and tags from results to discover related items across the graph
- **Edge tags** — Turn tags into navigable relationships with automatic inverse links
- **Parts** — `analyze` decomposes documents into searchable sections, each with its own embedding and tags
- **Strings** — Every note is a string of versions; reorganize history by meaning with `keep move`
- **Works offline** — Local models (MLX, Ollama), or API providers (Voyage, OpenAI, Gemini, Anthropic, Mistral)

Backed by ChromaDB for vectors, SQLite for metadata and versions.

> **[keepnotes.ai](https://keepnotes.ai)** — Hosted service. No local setup, no API keys to manage. Same SDK, managed infrastructure.

### The Practice

keep is designed as a skill for AI agents — a practice, not just a tool. The [skill instructions](SKILL.md) teach agents to reflect before, during, and after action: check intentions, recognize commitments, capture learnings, notice breakdowns. `keep prompt reflect` guides a structured reflection ([details](docs/KEEP-PROMPT.md)); `keep now` tracks current intentions and surfaces what's relevant.

This works because the tool and the skill reinforce each other. The tool stores and retrieves; the skill says *when* and *why*. An agent that uses both develops *skillful action* across sessions — not just recall, but looking before acting, and a deep review of outcomes afterwards.

> Why build memory for AI agents? What does "reflective practice" mean here? I wrote a story: **[Wisdom, or Prompt-Engineering?](https://inguz.substack.com/p/keep)**

### Integration

The skill instructions and hooks install into your agent's configuration automatically on first use (Claude Code, Kiro, OpenAI Codex, OpenClaw). Hooks inject `keep now` context at session start, on each prompt, and at session end — so the agent always knows its current intentions.

| Layer | What it does |
|-------|-------------|
| **Skill prompt** | Always in system prompt — guides reflection, breakdown capture, document indexing |
| **Hooks** | Inject `keep now -n 10` context at session start and prompt submit |
| **MCP** | [Stdio server](docs/KEEP-MCP.md) with 9 tools — any MCP-compatible agent gets full memory |
| **LangChain** | [LangGraph BaseStore](docs/LANGCHAIN-INTEGRATION.md), retriever, tools, and middleware |
| **Daily cron** | Scheduled deep reflection in an isolated session ([OpenClaw cron](SKILL.md#openclaw-integration)) |

The CLI alone is enough to start. The hooks make it automatic.

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
keep put https://inguz.substack.com/p/keep -t topic=practice
keep put "file://$(keep config tool)/docs/library/han_verse.txt" -t type=teaching
keep put "Token refresh needs clock sync" -t topic=auth

# Search
keep find "authentication flow" --limit 5
keep find "auth" --deep                # Follow edges to discover related items
keep find "auth" --since P7D           # Last 7 days

# Retrieve
keep get file:///path/to/doc.md
keep get ID -V 1                       # Previous version
keep get "ID@V{1}"                     # Same as -V 1 (version identifier)
keep get ID --history                  # All versions

# Tags
keep list --tag project=myapp          # Find by tag
keep find "auth" -t topic=auth         # Cross-project topic search
keep list --tags=                      # List all tag keys

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

- **[docs/QUICKSTART.md](docs/QUICKSTART.md)** — Setup, configuration, async summarization
- **[docs/REFERENCE.md](docs/REFERENCE.md)** — Quick reference index
- **[docs/TAGGING.md](docs/TAGGING.md)** — Tags, speech acts, project/topic organization
- **[docs/VERSIONING.md](docs/VERSIONING.md)** — Document versioning and history
- **[docs/META-TAGS.md](docs/META-TAGS.md)** — Contextual queries (`.meta/*`)
- **[docs/PROMPTS.md](docs/PROMPTS.md)** — Prompts for summarization, analysis, and agent workflows
- **[docs/EDGE-TAGS.md](docs/EDGE-TAGS.md)** — Edge tags and relationship navigation
- **[docs/ANALYSIS.md](docs/ANALYSIS.md)** — Document decomposition into searchable parts
- **[docs/AGENT-GUIDE.md](docs/AGENT-GUIDE.md)** — Working session patterns
- **[docs/LANGCHAIN-INTEGRATION.md](docs/LANGCHAIN-INTEGRATION.md)** — LangChain/LangGraph integration
- **[docs/KEEP-MCP.md](docs/KEEP-MCP.md)** — MCP server for AI agent integration
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
