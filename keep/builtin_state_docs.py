"""Built-in state doc definitions.

These YAML bodies are the system defaults for state-doc flows.
They're used as fallbacks when a ``.state/*`` note doesn't exist
in the store — users can override any of them by creating a note
with the same ID.

See docs/design/BUILTIN-STATE-DOCS.md for the full spec.
"""

from __future__ import annotations

BUILTIN_STATE_DOCS: dict[str, str] = {
    # -----------------------------------------------------------------
    # Write path
    # -----------------------------------------------------------------
    "after-write": """\
match: all
rules:
  - when: "item.content_length > params.max_summary_length && !item.has_summary"
    id: summary
    do: summarize
  - when: "'_ocr_pages' in item.tags && item.has_uri"
    id: extracted
    do: ocr
  - when: "!item.is_system_note"
    id: analyzed
    do: analyze
  - when: "!item.is_system_note"
    id: tagged
    do: tag
post:
  - return: done
""",

    # -----------------------------------------------------------------
    # Read path: context assembly
    # -----------------------------------------------------------------
    "get-context": """\
match: all
rules:
  - id: similar
    do: find
    with:
      similar_to: "{params.item_id}"
      limit: "{params.similar_limit}"
  - id: parts
    do: find
    with:
      prefix: "{params.item_id}@p"
      limit: "{params.parts_limit}"
  - id: meta
    do: resolve_meta
    with:
      item_id: "{params.item_id}"
      limit: "{params.meta_limit}"
post:
  - return: done
""",

    # -----------------------------------------------------------------
    # Read path: query resolution
    # -----------------------------------------------------------------
    "query-resolve": """\
match: sequence
rules:
  - id: search
    do: find
    with:
      query: "{params.query}"
      limit: "{params.limit}"
  - when: "search.margin > params.margin_high"
    return: done
  - when: "search.lineage_strong > params.lineage_strong"
    do: find
    with:
      query: "{params.query}"
      tags: "{search.dominant_lineage_tags}"
      limit: 5
    then: query-resolve
  - when: "search.margin < params.margin_low || search.entropy > params.entropy_high"
    then:
      state: query-branch
      with:
        facets_1: "{search.top_facet_tags}"
  - when: "search.entropy < params.entropy_low"
    do: find
    with:
      query: "{params.query}"
      limit: 5
    then: query-resolve
  - then: query-explore
""",

    "query-branch": """\
match: all
rules:
  - id: pivot1
    do: find
    with:
      query: "{params.query}"
      limit: "{params.pivot_limit}"
  - id: bridge
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
""",

    "query-explore": """\
match: sequence
rules:
  - id: search
    do: find
    with:
      query: "{params.query}"
      limit: "{params.explore_limit}"
  - when: "search.margin > params.margin_high"
    return: done
  - when: "budget.remaining > 0"
    do: find
    with:
      query: "{params.query}"
      limit: "{params.explore_limit_wide}"
    then: query-resolve
  - return:
      status: stopped
      with:
        reason: "budget"
""",

    # -----------------------------------------------------------------
    # Read path: deep find (search + edge traversal)
    # -----------------------------------------------------------------
    "find-deep": """\
match: sequence
rules:
  - id: search
    do: find
    with:
      query: "{params.query}"
      limit: "{params.limit}"
  - when: "search.count == 0"
    return: done
  - id: related
    do: traverse
    with:
      items: "{search.results}"
      limit: "{params.deep_limit}"
  - return: done
""",
}
