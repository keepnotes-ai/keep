# Guides

**keep** is a reflective memory system for AI agents.

## Getting Started

- **[CLI Quick Start](QUICKSTART.md)** — Install and start using the keep CLI in under 5 minutes

Install keep with `uv tool install keep-skill`, configure an embedding provider
(Voyage AI, OpenAI, Gemini, Mistral, Ollama, or local MLX models on Apple Silicon), store your first
note, and run a semantic search.

## CLI Commands

- **[keep put](KEEP-PUT.md)** — Store notes, files, and URLs. Supports inline text, stdin, file paths, directories, and HTTP URLs.
- **[keep get](KEEP-GET.md)** — Retrieve notes by exact ID or prefix match. Shows content, tags, versions, and meta-tag relationships.
- **[keep find](KEEP-FIND.md)** — Semantic similarity search across all notes. Filter by tags, set similarity thresholds, limit results.
- **[keep list](KEEP-LIST.md)** — List and filter notes by tag, date range, source, or pattern. Supports sorting and output format options.
- **[keep now](KEEP-NOW.md)** — Read or update the current intention — a single mutable note representing active state and goals. Surfaces related context automatically.
- **[keep move](KEEP-MOVE.md)** — Rename, retag, or reorganize notes. Move between IDs, merge tags, bulk-update metadata.
- **[keep analyze](KEEP-ANALYZE.md)** — Break documents into individually searchable structural parts — themes, sections, relationships — each with its own embedding.
- **[keep flow](KEEP-FLOW.md)** — Run multi-step workflows that chain keep operations with LLM processing.
- **[keep prompt](KEEP-PROMPT.md)** — Render agent prompts with context injected from reflective memory. Drive reflection, session starts, and more.
- **[keep data](KEEP-DATA.md)** — Export and import keep stores for backup and migration between local and cloud backends.
- **[keep config](KEEP-CONFIG.md)** — Configure embedding providers, storage backends, similarity thresholds, and environment variables.

## Concepts

- **[Tagging](TAGGING.md)** — Structured key-value tags for organizing notes by domain, thread, and facet. Combine with semantic search for precise retrieval.
- **[System Tags](SYSTEM-TAGS.md)** — Automatic tags managed by keep: `_created`, `_updated`, `_source`, `_accessed`, and more. Understand what's tracked and when.
- **[Meta-Tags](META-TAGS.md)** — Automatic cross-note relationships. Similar items, extracted learnings, and version history surface as structured metadata, giving agents longitudinal awareness.
- **[Edge Tags](EDGE-TAGS.md)** — Turn tags into navigable relationships. Tag a turn with `speaker: Deborah` and `get Deborah` shows everything she said — auto-vivification, backfill, and inverse listings.
- **[Prompts](PROMPTS.md)** — How prompts work: template rendering, context injection, built-in vs custom prompts.
- **[Flows](FLOWS.md)** — Multi-step workflows: chaining operations, conditionals, LLM-driven decisions, and automation patterns.
- **[Built-in State Docs](FLOW_STATE_DOCS.md)** — Reference for built-in flow state documents and their schemas.
- **[Versioning](VERSIONING.md)** — Every update creates a version. Full history is queryable. Compare versions to see how context evolved over time.
- **[Analysis](ANALYSIS.md)** — How document analysis improves search by decomposing content into individually searchable parts — themes, facts, and relationships.
- **[Output Format](OUTPUT.md)** — Understanding keep's YAML-frontmatter output format, display modes (full, compact, JSON), and how to parse results programmatically.

## Reference

- **[CLI Reference](REFERENCE.md)** — Complete reference for all keep commands, flags, and options. Every subcommand with usage examples.
- **[API Schema](API-SCHEMA.md)** — Concise reference for all keep tools, the data model, tags, time filters, and parameters.
- **[Architecture](ARCHITECTURE.md)** — Technical design: SQLite + FTS5 storage, embedding pipeline, similarity engine, meta-tag resolution, background workers, and cloud backends (PostgreSQL + pgvector).
- **[Agent Guide](AGENT-GUIDE.md)** — Best practices for AI agents: when to store notes, what context to surface, how to use `now` for reflection, and patterns for effective memory management.
- **[MCP (keep CLI)](KEEP-MCP.md)** — Local MCP stdio server for AI agent integration. Connect Claude Code, Cursor, and other MCP-compatible clients to keep.
- **[OpenClaw Integration](OPENCLAW-INTEGRATION.md)** — Three-layer integration: real-time context injection via skill prompt, automatic memory indexing on session compaction, and daily reflection cron for pattern review.

## All Guides

Complete listing with summaries:

| Guide | Summary |
|-------|---------|
| [CLI Quick Start](QUICKSTART.md) | Install keep with `uv tool install keep-skill`, configure an embedding provider (Voyage AI, OpenAI, Gemini, Mistral, Ollama, or local MLX), store your first note, and run a semantic search. |
| [keep put](KEEP-PUT.md) | Store content from inline text, stdin, file paths, directories (recursive), or HTTP/HTTPS URLs. Supports custom IDs, tags, and automatic embedding on store. Handles PDF, markdown, and plain text. |
| [keep get](KEEP-GET.md) | Retrieve a note by exact ID or prefix match. Returns content, all tags, version count, and meta-tag relationships (similar notes, learnings, previous versions). |
| [keep find](KEEP-FIND.md) | Semantic similarity search across all stored notes. Specify a query string; returns ranked results with similarity scores. Filter by tag key-value pairs, set minimum similarity threshold, limit result count. |
| [keep list](KEEP-LIST.md) | List notes with filters: by tag, date range, source type, ID pattern. Sort by creation date, update date, or access time. Output as YAML, JSON, or compact single-line format. |
| [keep now](KEEP-NOW.md) | The `now` note is a single mutable record representing your agent's current state, goals, and working context. Read it to recall active intentions; write it to update direction. Related notes and learnings surface automatically as meta-tag fields. |
| [keep move](KEEP-MOVE.md) | Rename notes (change ID), update tags in bulk, reorganize between locations. Preserves version history through moves. |
| [keep analyze](KEEP-ANALYZE.md) | Break long documents into individually searchable structural parts. Each part gets its own embedding. Analyze finds themes, sections, key arguments, and cross-references — making large documents discoverable at the paragraph level. |
| [keep flow](KEEP-FLOW.md) | Run multi-step workflows that chain keep operations with LLM processing. |
| [keep prompt](KEEP-PROMPT.md) | Render agent prompts with context injected from reflective memory. Built-in prompts for reflection, session starts, and conversation analysis — or create custom prompts. |
| [keep data](KEEP-DATA.md) | Export and import keep stores for backup and migration. Move data between local SQLite and cloud PostgreSQL backends. |
| [keep config](KEEP-CONFIG.md) | Configure storage backend (SQLite local, PostgreSQL cloud), embedding provider and model, similarity thresholds, and other settings via environment variables or `~/.keep/config.toml`. |
| [Tagging](TAGGING.md) | Structured key-value tags: `domain: healthcare`, `thread: margaret`, `facet: metabolic`. Tags enable precise filtering alongside semantic search. Thread-level tags provide hard links that similarity alone can't maintain. |
| [System Tags](SYSTEM-TAGS.md) | Automatic tags managed by keep: `_created` (ISO timestamp), `_updated` (last modification), `_accessed` (last read), `_source` (inline, file, url, stdin). Cannot be manually set. |
| [Meta-Tags](META-TAGS.md) | Metaschema rules define how notes relate. `similar` surfaces semantically close notes. `meta/learnings` extracts insights tagged as learnings. `prev` shows version history. Meta-tags give agents longitudinal awareness — context compounds over time. |
| [Edge Tags](EDGE-TAGS.md) | Turn tags into navigable relationship edges. When a tagdoc declares `_inverse`, tagged documents become links — and targets get automatic inverse listings. Tag a conversation with `speaker: Deborah` and `get Deborah` shows everything she said. Targets auto-vivify on first reference. |
| [Prompts](PROMPTS.md) | How prompts work: template rendering, context injection, built-in vs custom prompts. |
| [Flows](FLOWS.md) | Multi-step workflows: chaining operations, conditionals, LLM-driven decisions, and automation patterns. |
| [Built-in State Docs](FLOW_STATE_DOCS.md) | Reference for built-in flow state documents and their schemas. |
| [Versioning](VERSIONING.md) | Every `keep put` to an existing ID creates a new version. List versions, retrieve any version by number, compare across versions. Content-hash deduplication skips unchanged updates. |
| [Analysis](ANALYSIS.md) | How document analysis decomposes long content into individually searchable structural parts. Each part gets its own embedding and tags, improving retrieval for large documents. |
| [Output Format](OUTPUT.md) | Keep outputs YAML frontmatter (tags, metadata, meta-tags) followed by content body. Supports `--format json` for machine parsing, `--compact` for single-line summaries, and full (default) for human reading. |
| [CLI Reference](REFERENCE.md) | Complete command reference: `put`, `get`, `find`, `list`, `now`, `move`, `analyze`, `config`, `save`, `remember`. Every flag, option, and environment variable documented with examples. |
| [Architecture](ARCHITECTURE.md) | Technical internals: SQLite + FTS5 for local storage, PostgreSQL + pgvector for cloud. Embedding pipeline (Voyage, OpenAI, Gemini, Mistral, Ollama, MLX). Background worker for async tasks. Content-hash deduplication. Meta-tag resolution engine. |
| [Agent Guide](AGENT-GUIDE.md) | Patterns for AI agents using keep effectively: store decisions and learnings (not raw logs), use `now` for session continuity, let meta-tags surface context automatically, reflect before and after significant actions. |
| [MCP (keep CLI)](KEEP-MCP.md) | Local MCP stdio server for AI agent integration. Connect Claude Code, Cursor, and other MCP-compatible clients directly to your local keep store. |
| [OpenClaw Integration](OPENCLAW-INTEGRATION.md) | Three-layer integration pattern: (1) Skill prompt injects `keep now` context every agent turn, (2) `after_compaction` hook auto-indexes memory files, (3) Daily cron runs `keep put memory/ --analyze` for deep reflection. Turns keep from a tool into a continuous practice. |
