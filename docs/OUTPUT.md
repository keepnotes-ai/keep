# Reading the Output

When you run `keep get` or `keep now`, items are displayed in YAML frontmatter format. Each section carries meaning.

## Annotated Example

<!-- tabs -->
```yaml
---
id: %a1b2c3d4                                            # 1. Identity
tags:                                                     # 2. Tags
  act: "commitment"
  project: "myapp"
  said: conv1 [2026-01-12] "Deborah said let's ship it"
  status: "open"
  topic: "auth"
score: 0.823                                              # 3. Relevance
similar:                                                  # 4. Similar items
  - %e5f6a7b8         (0.89) 2026-01-14 OAuth2 token refresh pattern...
  - %c9d0e1f2         (0.85) 2026-01-13 Token handling notes...
  - .tag/act           (0.45) 2026-01-10 Speech-act categories...
meta/todo:                                                # 5. Meta sections
  - %d3e4f5a6 validate redirect URIs
  - %b7c8d9e0 update auth docs for new flow
meta/learnings:
  - %f1a2b3c4 Token refresh needs clock sync
parts:                                                    # 6. Parts (structural)
  - @P{1}  OAuth2 Flow Design (§1, pp.1-7)
  - @P{2}  Token Storage Strategy (§2, pp.8-13)
  - @P{3}  Session Management (§3, pp.14-18)
prev:                                                     # 7. Version navigation
  - @V{1} 2026-01-14 Previous version of this item...
  - @V{2} 2026-01-13 Older version...
---
I'll fix the auth bug by Friday                           # 8. Content
```
```json
{
  "id": "%a1b2c3d4",                                      // 1. Identity
  "tags": {                                                // 2. Tags
    "project": "myapp",
    "topic": "auth",
    "act": "commitment",
    "status": "open"
  },
  "score": 0.823,                                          // 3. Relevance
  "similar": [                                             // 4. Similar items
    {"id": "%e5f6a7b8", "score": 0.89, "date": "2026-01-14", "summary": "OAuth2 token refresh pattern..."},
    {"id": "%c9d0e1f2", "score": 0.85, "date": "2026-01-13", "summary": "Token handling notes..."},
    {"id": ".tag/act",  "score": 0.45, "date": "2026-01-10", "summary": "Speech-act categories..."}
  ],
  "meta": {                                                // 5. Meta sections
    "todo": [
      {"id": "%d3e4f5a6", "summary": "validate redirect URIs"},
      {"id": "%b7c8d9e0", "summary": "update auth docs for new flow"}
    ],
    "learnings": [
      {"id": "%f1a2b3c4", "summary": "Token refresh needs clock sync"}
    ]
  },
  "parts": [                                               // 6. Parts (structural)
    {"ref": "@P{1}", "summary": "OAuth2 Flow Design (§1, pp.1-7)"},
    {"ref": "@P{2}", "summary": "Token Storage Strategy (§2, pp.8-13)"},
    {"ref": "@P{3}", "summary": "Session Management (§3, pp.14-18)"}
  ],
  "prev": [                                                // 7. Version navigation
    {"ref": "@V{1}", "date": "2026-01-14", "summary": "Previous version of this item..."},
    {"ref": "@V{2}", "date": "2026-01-13", "summary": "Older version..."}
  ],
  "content": "I'll fix the auth bug by Friday"             // 8. Content
}
```
<!-- /tabs -->

## Sections

### 1. `id:` — Identity

The document's unique identifier.

| Format | Meaning |
|--------|---------|
| `%a1b2c3d4` | Content-addressed (text mode, hash of content) |
| `file:///path/to/doc` | Local file URI |
| `https://example.com` | Web URL |
| `now` | The nowdoc (current intentions) |
| `.conversations` | System document (dotted prefix) |
| `.tag/act` | Tag description document |

When viewing an old version, a suffix appears: `id: %a1b2c3d4@V{1}`

### 2. `tags:` — Metadata and Edge References

Key-value pairs in YAML block format. All scalar values are quoted. A key can hold one value or a list of values.

```yaml
tags:
  act: "commitment"
  project: "myapp"
  status: "open"
  topic: "auth"
```

Edge tags (tags whose values reference other items) are rendered inline with the target's date and summary:

```yaml
tags:
  said: conv1 [2026-01-12] "Deborah said let's ship it"
  speaker: "Deborah"
```

Both regular tags and resolved edge references appear in the same `tags:` block, sorted by key. When an edge tag has multiple values, they render as a list:

```yaml
tags:
  said:
    - conv1 [2026-01-12] "Deborah said let's ship it"
    - conv2 [2026-01-15] "Great, the tests pass now"
  status: "open"
```

Tags you set are shown here. System tags (prefixed `_`) are hidden from display but accessible via `--json`.

Key tag patterns:
- **`project`** / **`topic`** — organize by bounded work vs cross-cutting subject. See [TAGGING.md](TAGGING.md#organizing-by-project-and-topic).
- **`act`** / **`status`** — speech-act tracking (commitment, request, assertion + lifecycle). See [TAGGING.md](TAGGING.md#speech-act-tags).
- **`type`** — content classification (learning, breakdown, reference, teaching)
- **Edge tags** — tags with `_inverse` definitions render as resolved references. See [EDGE-TAGS.md](EDGE-TAGS.md).

### 3. `score:` — Relevance

Appears on search results (`keep find`). A similarity score between 0 and 1, with recency decay applied.

Higher = more relevant. Scores above 0.7 are strong matches. This field is absent when viewing items directly (not via search).

### 4. `similar:` — Semantic neighbors

Items semantically close to this one, ranked by similarity. Each line:

```
  - ID               (score) date summary...
```

- **ID** — the similar item's identifier (use with `keep get`)
- **(score)** — cosine similarity (0–1)
- **date** — last updated date
- **summary** — truncated summary

Similar items are occasions for reflection: what else do I know about this? Control with `-n` (limit).

### 5. `meta/*:` — Contextual sections

Meta-tags surface items matching tag-based queries relevant to what you're viewing. Each section name maps to a `.meta/*` query:

| Section | Surfaces | Source |
|---------|----------|--------|
| `meta/todo:` | Open requests and commitments | `.meta/todo` |
| `meta/learnings:` | Relevant learnings | `.meta/learnings` |
| `meta/decisions:` | Related decisions | `.meta/decisions` |

These are dynamically resolved — the same item shows different meta sections depending on its tags. For example, an item tagged `project=myapp` surfaces todos and learnings also tagged with that project.

See [META-TAGS.md](META-TAGS.md) for how contextual queries work and how to create custom ones.

### 6. `parts:` — Structural decomposition

Sections of a document identified by `keep analyze`. Each line:

```
  - @P{N}  Section summary...
```

- **`@P{N}`** — part number (1-indexed). Use with `keep get "ID@P{1}"` to view a specific part.
- Parts have their own tags, embeddings, and similar items — they appear independently in search results.
- Only appears when a note has been analyzed. View all parts with `keep get ID --parts`.
- Parts are the structural counterpart to versions: versions are temporal (`@V{N}`), parts are spatial (`@P{N}`).

See [KEEP-ANALYZE.md](KEEP-ANALYZE.md) for how to decompose documents into parts.

### 7. `prev:` / `next:` — Version navigation

Navigate through the item's version history.

```yaml
prev:
  - @V{1} 2026-01-14 Previous version summary...
  - @V{2} 2026-01-13 Older version summary...
```

- **`@V{N}`** — version offset (1=previous, 2=two ago). Use with `keep get "ID@V{1}"` or `keep get ID -V 1`.
- Only appears on single-item display (`keep get`, `keep now`).
- `next:` appears when viewing an old version, pointing toward newer versions.

See [VERSIONING.md](VERSIONING.md) for full versioning details.

### 8. Content (after `---`)

The item's summary, below the closing `---`. For short content this is the full text; for long documents it's a generated summary.

## Output Formats

The frontmatter format is one of four output modes:

| Flag | Format | Use case |
|------|--------|----------|
| *(default)* | Summary line or frontmatter | Frontmatter for single items, summary lines for lists |
| `--ids` | Versioned IDs only | Piping to other commands |
| `--full` | YAML frontmatter (forced) | When you want full details on list/find results |
| `--json` | JSON | Programmatic access |

See [REFERENCE.md](REFERENCE.md#output-formats) for examples of each format.

## See Also

- [TAGGING.md](TAGGING.md) — Tag system and conventions
- [VERSIONING.md](VERSIONING.md) — Version history and navigation
- [META-TAGS.md](META-TAGS.md) — Contextual queries (`.meta/*`)
- [PROMPTS.md](PROMPTS.md) — Prompts for summarization, analysis, and agent workflows
- [SYSTEM-TAGS.md](SYSTEM-TAGS.md) — Hidden system tags
- [REFERENCE.md](REFERENCE.md) — Quick reference index
