---
tags:
  category: system
  context: state
---
# Assembles display context for get/now. All queries run in parallel.
match: all
rules:
  - id: similar
    # Semantically related items
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
post:
  - return: done
