---
tags:
  category: system
  context: state
---
# Simple scoped search for memory_search tool.
match: all
rules:
  - id: results
    do: find
    with:
      query: "{params.query}"
      scope: "{params.scope}"
      limit: "{params.limit}"
post:
  - return: done
