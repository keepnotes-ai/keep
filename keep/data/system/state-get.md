---
tags:
  category: system
  context: state
  replaces: .state/get-context
---
# Assembles display context for get/now and agent prompts.
# All queries run in parallel (match: all).
#
# This is the base context assembly state doc. Fragments in
# state-get/ extend it for specific platforms (e.g., openclaw
# adds search, intentions, and session rules).
#
# The `similar` rule has a `when` guard so fragments can provide
# an alternative search strategy (e.g., query-based search for
# agent turns instead of item similarity for CLI get).
match: all
rules:
  - id: item
    # The viewed note itself, for prompt rendering compatibility.
    do: get
    with:
      id: "{params.item_id}"
  - id: find_results
    # Optional query-driven retrieval for prompt docs using {find}.
    when: "has(params.query) && params.query != ''"
    do: find
    with:
      query: "{params.query}"
      tags: "{params.tags}"
      since: "{params.since}"
      until: "{params.until}"
      scope: "{params.scope}"
      limit: "{params.limit}"
  - id: similar
    # Semantically related items — skipped when a fragment provides query-based search
    when: "!has(params.prompt) || params.prompt == ''"
    do: find
    with:
      similar_to: "{params.item_id}"
      limit: "{params.similar_limit}"
  - id: parts
    # Decomposed parts (from analyze)
    do: find
    with:
      prefix: "{params.item_id}@p"
      limit: "{params.parts_limit}"
  - id: meta
    # Meta-doc sections (learnings, todos, etc.)
    do: resolve_meta
    with:
      item_id: "{params.item_id}"
      limit: "{params.meta_limit}"
  - id: edges
    # Linked items via edge tags
    do: resolve_edges
    with:
      id: "{params.item_id}"
      limit: "{params.edges_limit}"
post:
  - return: done
