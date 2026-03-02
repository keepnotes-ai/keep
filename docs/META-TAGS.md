# Meta-Tags

Meta-tags are system documents stored at `.meta/*` that define **contextual queries** — tag-based rules that surface relevant items when you view context with `keep now` or `keep get`. They answer: *what else should I be aware of right now?*

Meta docs are user-editable on purpose. They are your "attention policy" — the control surface for what gets surfaced during reflection and planning.

- What should always be surfaced before action?
- Which open loops should be hard to ignore?
- Which learnings should follow you across projects?

If your workflow changes, edit these docs so the system behavior changes with it.

## How they work

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

The `meta/todo:` section appeared because you previously captured commitments tagged with `project=myapp`:

```bash
keep put "validate redirect URIs" -t act=commitment -t status=open -t project=myapp
```

## Meta vs edge: choosing the right tool

Use **meta docs** when you want dynamic, contextual surfacing:

- Project/topic-scoped reminders (`project=`, `topic=`)
- Prerequisite-gated context (`genre=*`, `artist=*`)
- Ranked context windows (similarity + recency)

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

Surfaces unresolved commitments, requests, offers, and blocked work.

**Queries:** `act=commitment status=open`, `act=request status=open`, `act=offer status=open`, `status=blocked`
**Context keys:** `project=`, `topic=`

### `.meta/learnings` — Experiential Priming

Surfaces past learnings, breakdowns, and gotchas before you start work.

**Queries:** `type=learning`, `type=breakdown`, `type=gotcha`
**Context keys:** `project=`, `topic=`

### `.meta/genre` — Same Genre

Groups media items by genre. Only activates for items with a `genre` tag.

**Prerequisites:** `genre=*`
**Context keys:** `genre=`

### `.meta/artist` — Same Artist

Groups media items by artist. Only activates for items with an `artist` tag.

**Prerequisites:** `artist=*`
**Context keys:** `artist=`

### `.meta/album` — Same Album

Groups tracks from the same release. Only activates for items with an `album` tag.

**Prerequisites:** `album=*`
**Context keys:** `album=`

## Query structure

A `.meta/*` document contains prose (for humans and LLMs) plus structured lines:

- **Query lines** like `act=commitment status=open` — each `key=value` pair is an AND filter; multiple query lines are OR'd together
- **Context-match lines** like `project=` — a bare key whose value is filled from the current item's tags
- **Prerequisite lines** like `genre=*` — the current item must have this tag or the entire query is skipped

Context matching is what makes these queries contextual. If the current item has `project=myapp`, then `act=commitment status=open` combined with context key `project=` becomes `act=commitment status=open project=myapp` — scoped to the current project.

Prerequisites act as gates. A query with `genre=*` only activates for items that have a `genre` tag — items without one skip it entirely.

## Ranking

Results are ranked by:

1. **Embedding similarity** to the current item — semantically related items rank higher
2. **Recency decay** — recent items get a boost

Each contextual query returns up to 3 items. Sections with no matches are omitted.

## Viewing definitions

```bash
keep get .meta/todo        # See the todo query definition
keep get .meta/learnings   # See the learnings query definition
keep list .meta            # All contextual query definitions
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
