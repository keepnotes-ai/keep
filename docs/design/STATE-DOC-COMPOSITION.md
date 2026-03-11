# State Doc Composition

Date: 2026-03-11
Status: Draft
Related:
- `docs/design/STATE-DOC-SCHEMA.md`
- `docs/design/BUILTIN-STATE-DOCS.md`

## Problem

State docs are the sole source of truth for processing pipelines (e.g.
what happens after a `put`). But today, extending a pipeline means
editing the existing state doc — you have to understand its full
structure and be careful not to break it.

This makes it hard to:
- Add a custom processing step without touching the builtin
- Package reusable extensions (e.g. "obsidian link extraction")
- Temporarily disable a step without deleting it

## Design: child-doc composition

A state doc at `.state/{name}` can be extended by child docs at
`.state/{name}/{fragment}`. The loader collects the base doc and
all active children, merging their rules into a single rule list
before evaluation.

### Example

```
.state/after-write                       ← base (builtin)
.state/after-write/obsidian-links        ← user fragment
.state/after-write/custom-classifier     ← user fragment
```

The base doc defines the `match` mode (`all` or `sequence`) and the
core rules. Fragments contribute additional rules that get merged in.

### Fragment format

A fragment is a note whose content is a YAML rule list:

```yaml
# .state/after-write/obsidian-links
rules:
  - when: "item.content_type == 'text/markdown'"
    id: obsidian-links
    do: extract_links
```

The optional `order` field controls insertion point (defaults to `"after"`
if omitted):
- `after` (default) — appended after all base rules
- `before` — prepended before all base rules
- `after:{rule_id}` — inserted after the base rule with that id
- `before:{rule_id}` — inserted before the base rule with that id

For `match: all` pipelines, order rarely matters (all rules run in
parallel). For `match: sequence`, order determines execution position.

### Active/inactive toggle

Fragments are active by default. To disable one without deleting it:

```bash
keep tag .state/after-write/obsidian-links active=false
```

To re-enable:

```bash
keep tag .state/after-write/obsidian-links -r active
```

The loader skips any fragment where `tags.active == "false"`.

### Listing configured fragments

```bash
keep list --prefix .state/after-write/
```

Shows all fragments with their tags — active/inactive is visible
at a glance.

## Merge rules

1. Load the base state doc (store override or builtin fallback)
2. List all notes with prefix `.state/{name}/`
3. Filter out fragments with `active: false` tag
4. Sort fragments alphabetically by id (stable ordering)
5. Parse each fragment's rules
6. Insert rules at the position declared by `order`
7. Return the merged `StateDoc` to the evaluator

The evaluator itself does not change — it sees a single rule list.

### Ordering algorithm

Starting from the base rule list:

1. Collect fragments into three buckets: `before`, `after`, and
   positional (`before:{id}` / `after:{id}`)
2. For positional fragments, find the target rule by id in the
   current list and insert adjacent to it
3. Prepend all `before` fragments (in alphabetical order)
4. Append all `after` fragments (in alphabetical order)

If a positional target id is not found, the fragment falls back to
`after` with a warning.

## Interaction with base doc override

Users can still replace `.state/after-write` entirely for full
control. Fragments merge into whatever base doc is loaded — whether
it's the builtin or a user override.

## What fragments cannot do

- **Change the `match` mode**: The base doc owns this. A fragment
  can't switch a pipeline from `all` to `sequence`.
- **Define `post` blocks**: Only the base doc has a post block.
- **Reference bindings from other fragments' rules** in `match: all`
  mode (bindings aren't available across parallel rules). In
  `match: sequence` mode, earlier bindings are available as usual.
