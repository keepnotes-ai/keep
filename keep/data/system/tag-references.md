---
tags:
  category: system
  context: tag-description
  _inverse: referenced_by
---
# Tag: `references` — Document Links

The `references` tag links a document to items it references via explicit links (wiki-style `[[links]]` or markdown-style `[text](url)`). It is populated automatically by the `extract_links` action when enabled as an after-write fragment.

The `_inverse` declaration means this tag creates a navigable relationship edge. If document A has `references: B`, then `get B` will show A under `referenced_by:`.

## Characteristics

- **Edge-creating**: The `_inverse: referenced_by` declaration makes this an edge tag.
- **Auto-vivifying**: If the target doesn't exist as an item, it can be created automatically.
- **Multi-valued**: A document can reference many targets.
- **Machine-populated**: Set by `extract_links`, not typically set manually.

## Usage

```bash
# Enable link extraction on after-write
keep put --id .state/after-write/extract-links 'order: after:tagged
rules:
  - when: "item.content_type == '\''text/markdown'\''"
    id: linked
    do: extract_links'

# See what a document references
keep get my-note
# → references:
# →   - other-note  [2026-03-11] Other note content...
# →   - https://example.com  [2026-03-11] Example site

# See what references a document
keep get other-note
# → referenced_by:
# →   - my-note  [2026-03-11] My note with [[other-note]] link
```
