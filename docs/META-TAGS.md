# Meta-Tags

Meta-tags are system documents stored at `.meta/*` that define **contextual queries** — flow-based rules that surface relevant items when you view context with `keep now` or `keep get`. They answer: *what else should I be aware of right now?*

Meta docs are user-editable on purpose. They are your "attention policy" — the control surface for what gets surfaced during reflection and planning.

- What should always be surfaced before action?
- Which open loops should be hard to ignore?
- Which learnings should follow you across projects?

If your workflow changes, edit these docs so the system behavior changes with it.

## How they work

Meta-docs are **state docs** — the same `match`/`rules`/`do`/`with` format used by all other keep flows. Each rule typically calls the `find` action with `similar_to` (for context-relevant ranking) and `tags` (for filtering).

When you run `keep now` while working on a project tagged `project=myapp`:

```yaml
---
id: now
tags:
  project: "myapp"
  topic: "auth"
similar:
  - %a1b2 OAuth2 token refresh pattern
meta/todo:
  - %c3d4 validate redirect URIs
  - %e5f6 update auth docs for new flow
meta/learnings:
  - %g7h8 JSON validation before deploy saves hours
---
Working on auth flow refactor
```

The `meta/todo:` section appeared because you previously captured commitments tagged with `act=commitment` and `status=open`:

```bash
keep put "validate redirect URIs" -t act=commitment -t status=open -t project=myapp
```

The results are **ranked by semantic similarity** to the item you're viewing — so auth-related commitments surface first when you're looking at auth work.

## Meta vs edge: choosing the right tool

Use **meta docs** when you want dynamic, contextual surfacing:

- Open commitments, requests, and blocked work
- Past learnings and breakdowns relevant to current work
- Items sharing a tag value (genre, artist, album)

Use **edge tags** when you want explicit relationship navigation:

- Entity links and inverse views (`speaker -> said`)
- Graph-style traversal (who/what points to this)
- Relationship-first deep search behavior

In practice:

- Meta answers "what else should I pay attention to now?"
- Edges answer "what is explicitly related to this entity?"

## Bundled contextual queries

keep ships with five `.meta/*` documents:

### `.meta/todo` — Open Loops

Surfaces unresolved commitments, requests, offers, and blocked work. Uses `match: all` with four `find` rules, each filtering by a specific speech-act type.

### `.meta/learnings` — Experiential Priming

Surfaces past learnings, breakdowns, and gotchas before you start work. Uses `match: all` with three `find` rules for each type.

### `.meta/genre` — Same Genre

Groups media items by genre. Uses a `when` guard to skip items without a `genre` tag — equivalent to the old `genre=*` prerequisite.

### `.meta/artist` — Same Artist

Groups media items by artist. Same guard pattern as genre.

### `.meta/album` — Same Album

Groups tracks from the same release. Same guard pattern as genre.

## State-doc format

Meta-docs use the standard state-doc syntax:

```yaml
# Comments explain the document's purpose (ignored by parser)
match: all
rules:
  - id: commitments
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {act: commitment, status: open}
      limit: "{params.limit}"
  - id: requests
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {act: request, status: open}
      limit: "{params.limit}"
```

### Key concepts

- **`match: all`**: All matching rules fire independently (for OR-style queries). Use `match: sequence` when you need guards or short-circuiting.
- **`do: find`**: Calls the `find` action, which supports tag filtering and similarity ranking.
- **`similar_to: "{params.item_id}"`**: Ranks results by semantic similarity to the item being viewed. This replaces the old separate ranking step.
- **`tags: {key: value}`**: Filters results to items matching all specified tags.
- **`when` guards**: Replace the old prerequisite (`key=*`) syntax. Example: `when: "!(has(params.genre) && params.genre != '')"` returns early if the viewed item has no `genre` tag.

### Available params

Meta resolution injects these params from the viewed item:

| Param | Source |
|-------|--------|
| `params.item_id` | ID of the item being viewed |
| `params.limit` | Max results per rule (default: 3) |
| `params.*` | All non-underscore tags from the viewed item (e.g., `params.project`, `params.topic`) |

### Async actions

Meta-docs can use any action, including expensive ones that require the daemon (e.g., `generate`, `summarize`). If a meta-doc flow hits an async action, the sync portion completes immediately and returns partial results. The remainder is delegated to the work queue for background execution. Next time the meta-doc is evaluated, the results from the background work are available.

## Viewing definitions

```bash
keep get .meta/todo        # See the todo query definition
keep get .meta/learnings   # See the learnings query definition
keep list .meta            # All contextual query definitions
```

## Creating custom meta-docs

Create a `.meta/*` document using the state-doc format:

```bash
keep put '
match: all
rules:
  - id: recent_bugs
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {type: bug, status: open}
      limit: "{params.limit}"
' -t category=system -t context=meta --id .meta/bugs
```

This creates a meta-doc that surfaces open bugs relevant to whatever you're looking at.

### Gated meta-docs

To create a meta-doc that only activates for items with a specific tag:

```yaml
match: sequence
rules:
  - when: "!(has(params.language) && params.language != '')"
    return: done
  - id: same_language
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {language: "{params.language}"}
      limit: "{params.limit}"
  - return:
      status: done
      with:
        same_language: "{same_language}"
```

## Feeding the loop

Meta-tags only surface what you put in. The tags that matter:

```bash
# Commitments and requests (surface in meta/todo)
keep put "I'll fix the login bug" -t act=commitment -t status=open -t project=myapp
keep put "Can you review the PR?" -t act=request -t status=open -t project=myapp

# Resolve when done
keep put "Login bug fixed" -t act=commitment -t status=fulfilled -t project=myapp

# Learnings and breakdowns (surface in meta/learnings)
keep put "Always check token expiry before refresh" -t type=learning -t topic=auth
keep put "Assumed UTC, server was local time" -t type=breakdown -t project=myapp

# Gotchas (surface in meta/learnings)
keep put "CI cache invalidation needs manual clear after dep change" -t type=gotcha -t topic=ci
```

### Media library

The media queries (`genre`, `artist`, `album`) surface related media automatically:

```bash
keep put ~/Music/OK_Computer/01_Airbag.flac -t artist=Radiohead -t album="OK Computer" -t genre=rock
```

Now `keep get` on that item shows `meta/artist:`, `meta/album:`, and `meta/genre:` sections with related tracks.

## See Also

- [EDGE-TAGS.md](EDGE-TAGS.md) — Edge tags: navigable relationships via `_inverse`
- [TAGGING.md](TAGGING.md) — Tag descriptions, constrained values, and filtering
- [PROMPTS.md](PROMPTS.md) — Prompts for summarization, analysis, and agent workflows
- [SYSTEM-TAGS.md](SYSTEM-TAGS.md) — Auto-managed system tags (`_created`, `_updated`, etc.)
- [REFERENCE.md](REFERENCE.md) — Complete CLI reference
