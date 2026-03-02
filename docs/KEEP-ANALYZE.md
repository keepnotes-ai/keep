# keep analyze

Decompose a note or string into meaningful parts.

## Usage

```bash
keep analyze ID                       # Analyze using configured provider
keep analyze ID -t topic -t type      # With guidance tags
```

## What it does

`analyze` uses an LLM to decompose content into meaningful sections, each
with its own summary, tags, and embedding. This enables targeted search:
`find` matches specific sections, not just whole documents.

Two modes, auto-detected:
- **Documents** (URI sources): structural decomposition — chapters, topics,
  headings, thematic units
- **Strings** (inline notes with version history): episodic decomposition —
  the version history is assembled chronologically and decomposed into
  distinct phases, topic shifts, or narrative arcs

Parts are the structural counterpart to versions:
- **Versions** (`@V{N}`) are temporal — each `put` adds one
- **Parts** (`@P{N}`) are structural — each `analyze` replaces all parts

## Options

| Option | Description |
|--------|-------------|
| `-t`, `--tag KEY` | Guidance tag keys (repeatable). Fetches `.tag/KEY` descriptions to guide decomposition |
| `--foreground`, `--fg` | Run in foreground and wait for results (default: background) |
| `--force` | Re-analyze even if parts are already current |
| `-s`, `--store PATH` | Override store directory |

## Background processing

By default, `analyze` runs in the background, serialized with other ML work
(summarization, embedding). Use `--fg` to wait for results:

```bash
keep analyze doc:1                    # Returns immediately, runs in background
keep analyze doc:1 --fg               # Waits for completion
```

Background tasks are processed by the same queue as `keep pending` summaries.

## Part addressing

Append `@P{N}` to any ID to access a specific part:

```bash
keep get "doc:1@P{1}"           # Part 1
keep get "doc:1@P{3}"           # Part 3
```

Parts include prev/next navigation:
```yaml
---
id: doc:1@P{2}
tags: {topic: analysis}
prev:
  - @P{1}
next:
  - @P{3}
---
Detailed analysis of the main argument...
```

## Parts in get output

When a document has parts, `keep get` shows a parts manifest:

```yaml
---
id: doc:1
similar:
  - doc:2 (0.85) 2026-01-14 Related document...
parts:
  - @P{1} Introduction and overview of the topic
  - @P{2} Detailed analysis of the main argument
  - @P{3} Conclusions and future directions
prev:
  - @V{1} 2026-01-13 Previous summary...
---
Document summary here...
```

## Parts in search results

Parts have their own embeddings and appear naturally in `find` results:

```bash
keep find "main argument"
# doc:1@P{2}  2026-01-14 Detailed analysis of the main argument...
```

## Smart skip

Analysis is expensive (LLM call per document). To avoid redundant work,
`analyze` tracks a content hash at the time of analysis. If the document
hasn't changed since the last analysis, the call is skipped:

```bash
keep analyze doc:1                    # Analyzes, stores _analyzed_hash
keep analyze doc:1                    # Skipped — parts are current
keep put doc:1 "updated content"      # Content changes
keep analyze doc:1                    # Re-analyzes (content changed)
```

This makes `put --analyze` safe for cron jobs — point it at a folder daily
and only new or changed files get analyzed:

```bash
keep put /path/to/docs/ --analyze     # Only analyzes what needs it
```

Use `--force` to override the skip:

```bash
keep analyze doc:1 --force            # Re-analyze regardless
```

## Part immutability

Parts are machine-generated analysis results, not human observations.
They are treated as derived data — immutable except for tag corrections.

**Allowed:**
- Read, search, list — parts appear in `get`, `find`, `list` normally
- Tag editing — correct or override analyzer tagging decisions:
  ```bash
  keep tag "doc:1@P{2}" -t topic=oauth2    # Fix a tag
  keep tag "doc:1@P{2}" -r topic            # Remove a tag
  ```
- Re-analyze — `analyze` replaces all parts atomically
- Delete parent — removing the parent document removes its parts

**Blocked:**
- `put` with a part ID — parts cannot be created or overwritten directly
- `del` on individual parts — use re-analyze or delete the parent
- `move` to a part ID — parts belong to their parent

If a part's summary or content is wrong, re-analyze (with `--force` or better
guidance tags). The right fix is a better prompt, not manual editing.

## Re-analysis

Running `analyze` on changed content (or with `--force`) replaces all
previous parts:

```bash
keep analyze doc:1                    # Creates parts
keep analyze doc:1 -t topic --force   # Re-analyze with guidance — replaces all parts
```

## Guidance tags

Tag keys passed with `-t` fetch the corresponding `.tag/KEY` system documents
(e.g., `.tag/topic`, `.tag/type`). These descriptions tell the LLM what each
tag means and what values are appropriate, producing better decomposition and
more consistent tagging — even with smaller models.

```bash
keep analyze doc:1 -t topic -t type   # Guided by tag descriptions
```

## Python API

```python
kp = Keeper()

# Analyze (skips if parts are current)
parts = kp.analyze("doc:1")
parts = kp.analyze("doc:1", tags=["topic", "type"])
parts = kp.analyze("doc:1", force=True)  # Override skip

# Enqueue for background processing (returns False if skipped)
enqueued = kp.enqueue_analyze("doc:1")
enqueued = kp.enqueue_analyze("doc:1", force=True)

# Access parts
part = kp.get_part("doc:1", 1)        # Returns Item
parts = kp.list_parts("doc:1")        # Returns list[PartInfo]

# Edit tags on a part (the only allowed mutation)
kp.tag_part("doc:1", 1, tags={"topic": "oauth2"})  # Update tag
kp.tag_part("doc:1", 1, tags={"topic": ""})         # Remove tag
```

## Custom analysis prompts

The LLM prompt used for analysis is configurable. Create a `.prompt/analyze/*` document whose match rules target specific tags, and its `## Prompt` section replaces the default system prompt for matching documents. See [PROMPTS.md](PROMPTS.md) for details.

## See Also

- [VERSIONING.md](VERSIONING.md) — Versions (temporal) vs parts (structural)
- [KEEP-GET.md](KEEP-GET.md) — Retrieving items and parts
- [KEEP-FIND.md](KEEP-FIND.md) — Search results include parts
- [META-TAGS.md](META-TAGS.md) — Contextual queries (`.meta/*`)
- [PROMPTS.md](PROMPTS.md) — Prompts for summarization, analysis, and agent workflows
- [REFERENCE.md](REFERENCE.md) — Quick reference index
