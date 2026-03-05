---
tags:
  category: system
  context: tag-description
  _inverse: informed_by
---
# Tag: `informs` — Provenance and Context

The `informs` tag links a source document to a note or learning it contributed to. When a document was encountered, consulted, or drawn upon during the work that a note captures, tag the source with `informs: note-id`.

The `_inverse` declaration means this tag creates a navigable relationship edge. If `https://example.com/doc` has `informs: auth-decision`, then `get auth-decision` will show it under `informed_by:`.

## Characteristics

- **Edge-creating**: The `_inverse: informed_by` declaration makes this an edge tag. Tagged documents become navigable links.
- **Auto-vivifying**: If the target note doesn't exist as a document, it's created automatically.
- **Unconstrained**: Values are free-form — any document ID is valid.
- **Multi-valued**: A source can inform multiple notes, and a note can be informed by multiple sources.

## Usage

```bash
# Tag a source document with what it informs
keep put "https://example.com/oauth-spec" -t informs=auth-decision -t topic=auth

# Or tag the note with its sources (equivalent, uses the inverse direction)
keep put "We chose OAuth2 because..." --id auth-decision -t informed_by="[https://example.com/oauth-spec, prior-discussion]"

# See what informed a note
keep get auth-decision
# → informed_by:
# →   - https://example.com/oauth-spec  [2025-03-15] OAuth 2.0 spec...
# →   - prior-discussion  [2025-03-10] Team sync on auth approach...

# See what a source has informed
keep get "https://example.com/oauth-spec"
# → informs:
# →   - auth-decision  [2025-03-15] We chose OAuth2 because...
```

## When to use

- A URL or file was read during a work session and shaped a decision or learning
- A prior note provided context that fed into a new synthesis
- A conversation or meeting informed subsequent work captured in a note

## Prompt

Link sources to the knowledge they contribute to. When capturing a learning or decision, trace it back to what informed it — documents read, conversations had, prior notes revisited. This makes the provenance of knowledge navigable.
