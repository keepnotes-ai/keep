---
tags:
  category: system
  context: state
---
# Search then traverse edges from results. Sequential: skip traverse if empty.
match: sequence
rules:
  - id: search
    # Initial semantic search
    do: find
    with:
      query: "{params.query}"
      limit: "{params.limit}"
  - when: "search.count == 0"
    return: done
  - id: related
    # Follow edges from search hits
    do: traverse
    with:
      items: "{search.results}"
      limit: "{params.deep_limit}"
  - return: done
