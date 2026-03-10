---
tags:
  category: system
  context: state
---
# Parallel faceted search. Tries pivot and bridge queries, then
# returns to query-resolve if results are still ambiguous.
match: all
rules:
  - id: pivot1
    # Facet-narrowed search
    do: find
    with:
      query: "{params.query}"
      limit: "{params.pivot_limit}"
  - id: bridge
    # Cross-facet bridging search
    do: find
    with:
      query: "{params.query}"
      limit: "{params.bridge_limit}"
post:
  - when: "pivot1.margin > params.margin_high || bridge.margin > params.margin_high"
    return: done
  - when: "budget.remaining > 0"
    then: query-resolve
  - return:
      status: stopped
      with:
        reason: "ambiguous"
