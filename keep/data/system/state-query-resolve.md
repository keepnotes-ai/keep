---
tags:
  category: system
  context: state
---
# Entry point for multi-step query resolution. Evaluates search quality
# and routes to branch/explore states when results are ambiguous.
match: sequence
rules:
  - id: search
    # Primary search
    do: find
    with:
      query: "{params.query}"
      limit: "{params.limit}"
  - when: "search.margin > params.margin_high"
    # High confidence — done
    return: done
  - when: "search.lineage_strong > params.lineage_strong"
    # Strong lineage signal — re-search with dominant tags
    do: find
    with:
      query: "{params.query}"
      tags: "{search.dominant_lineage_tags}"
      limit: 5
    then: query-resolve
  - when: "search.margin < params.margin_low || search.entropy > params.entropy_high"
    # Low margin or high entropy — branch into faceted search
    then:
      state: query-branch
      with:
        facets_1: "{search.top_facet_tags}"
  - when: "search.entropy < params.entropy_low"
    # Low entropy (tight cluster) — widen the search
    do: find
    with:
      query: "{params.query}"
      limit: 5
    then: query-resolve
  # No strong signal — fall through to exploratory search
  - then: query-explore
