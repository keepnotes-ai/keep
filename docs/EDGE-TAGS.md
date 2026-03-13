# Edge Tags

Edge tags turn ordinary tags into navigable relationships.  Usually if a note has tag `speaker: Kate`, this is just a label.  But when `speaker` is an _edge tag_, this becomes a reference (a graph edge) between the note and another note "Kate".  Moreover, that note "Kate" has edges pointing back to all the notes where it was tagged.

This behavior is defined by the _tagdoc_: the note that defines the tag, which has id like `.tag/*`.  When a `.tag/KEY` document declares `_inverse: VERB`, any document tagged with `KEY=target` creates a link to the target — and the target gets an automatic inverse listing under the `VERB` key in the unified `tags:` block.

Edge definitions are user-editable on purpose. They let you decide which relationships matter enough to become first-class navigation for you and your agent.

- Add edges to make important entities easier to traverse.
- Rename inverses to fit your domain language.
- Remove edges when they create noise instead of signal.

## How it works

The bundled `.tag/speaker` tagdoc has `_inverse: said`. This means:

```bash
# Tag a conversation part with a speaker
keep put "I think we should refactor the auth module" -t speaker=Deborah

# Deborah now has an inverse listing
keep get Deborah
```

Output:

```yaml
id: Deborah
tags:
  said:
    - conv1@P{5} [2025-03-15] "I think we should refactor the auth module"
    - conv2@P{3} [2025-03-18] "The API needs rate limiting..."
```

The `said` entries under `tags:` are computed from the edges table — they're not stored as tags on `Deborah`. Each entry links back to the source document, rendered as `id [date] "summary"`.

## Auto-vivification

If the target doesn't exist, it's created as an empty document automatically. In the example above, `speaker=Deborah` creates a `Deborah` document if one doesn't exist yet. You can add content to it later:

```bash
keep put "Deborah is the tech lead on project X" --id Deborah
```

The inverse edges survive — the `said` entries under `tags:` still show everything Deborah said.

## Creating edge tags

Any tag can become an edge tag by adding `_inverse` to its tagdoc. Edge tagdocs are system documents, so `_inverse` is set via the tagdoc's frontmatter (like `_constrained`), not through `keep put -t`.

To create a custom edge tag, write a tagdoc with `_inverse` in its tags:

```bash
keep put "$(cat <<'EOF'
---
tags:
  _inverse: contents
---
# Tag: `contains`

Items that contain other items. The inverse `contents` shows
what container an item belongs to.
EOF
)" --id .tag/contains
```

Now `contains=item-B` on document A creates an edge, and `get item-B` shows `contents: A [date] "summary"` in its `tags:` block.

### Symmetric tagdocs

When `.tag/contains` declares `_inverse: contents`, keep automatically creates `.tag/contents` with `_inverse: contains` (if it doesn't already exist). This makes the relationship navigable in both directions — tagging with either key creates edges that the other key can resolve. If `.tag/contents` already exists with a different `_inverse`, that's a conflict error.

### Backfill

When you add `_inverse` to an existing tagdoc, keep automatically backfills edges for all documents already tagged with that key. This runs in the background — edges may take a moment to appear.

## Bundled edge tags

| Tag | `_inverse` | Example | Meaning |
|-----|-----------|---------|---------|
| `speaker` | `said` | `speaker: Deborah` on a turn | `get Deborah` → `said:` entries in `tags:` |
| `informs` | `informed_by` | `informs: auth-decision` on a URL | `get auth-decision` → `informed_by:` entries in `tags:` |
| `references` | `referenced_by` | `references: other-note` via link extraction | `get other-note` → `referenced_by:` entries in `tags:` |
| `duplicates` | `duplicates` | `duplicates: notes-v1` on a duplicate | Symmetric: both sides show `duplicates:` entries |

## Rules

- **Case-sensitive values**: `speaker: Deborah` and `speaker: deborah` link to different targets. Be consistent.
- **Multi-valued**: A document can have multiple values per edge tag (e.g., `speaker: [alice, bob]`). Each value creates a separate edge. Multiple documents can point at the same target.
- **Singular edge tags**: An edge tag with `_singular: true` on its tagdoc replaces the old value (and its edge) when a new value is set. For example, an `assignee` edge tag that is singular would reassign the edge rather than accumulating multiple assignees.
- **System doc targets skipped**: Tag values starting with `.` (like `.meta/todo`) don't create edges.
- **Removal**: Setting a tag to empty (`-t speaker=`) deletes that edge without affecting other edges on the document.

## Edge vs meta: choosing the right tool

Use **edge tags** for explicit graph relationships:

- "Who said this?"
- "What does this contain?"
- "Which notes point at this entity?"

Use **meta docs** for contextual reflection policy:

- "What open commitments should appear here?"
- "What learnings should be surfaced in this project?"
- "What context should appear only when prerequisite tags exist?"

Edge tags optimize navigability and relationship fidelity.
Meta docs optimize relevance and situational awareness.

## Finding edge sources

Outbound edges are normal tags, so `keep find` works:

```bash
keep find -t speaker=Deborah    # All docs where Deborah is the speaker
```

Inverse edges (the resolved `said:` entries in `tags:`) are only visible through `keep get` on the target.

## See Also

- [TAGGING.md](TAGGING.md) — Tag descriptions, constrained values, filtering
- [META-TAGS.md](META-TAGS.md) — Contextual queries (`.meta/*`)
- [SYSTEM-TAGS.md](SYSTEM-TAGS.md) — Auto-managed system tags (`_created`, `_updated`, etc.)
