---
tags:
  category: system
  context: tag-description
  _inverse: duplicates
---
# Tag: `duplicates` — Duplicate Relationship

The `duplicates` tag links a document to another document that covers the same content. This is a symmetric (self-inverse) edge: if A has `duplicates: B`, then `get B` will also show A under `duplicates:`.

## Characteristics

- **Edge-creating**: The `_inverse: duplicates` declaration makes this a self-inverse edge tag.
- **Symmetric**: The relationship is the same in both directions.
- **Multi-valued**: A document can be linked to multiple duplicates.

## Usage

```bash
# Mark two notes as duplicates
keep put "meeting notes v2" --id notes-v2 -t duplicates=notes-v1

# Both directions are visible
keep get notes-v1
# → duplicates:
# →   - notes-v2  [2026-03-13] meeting notes v2...
```

## When to use

- Two notes capture the same information from different sources or sessions
- An item was re-indexed or re-captured and the duplicate should be linked rather than deleted
- Merging knowledge: link duplicates before deciding which to keep
