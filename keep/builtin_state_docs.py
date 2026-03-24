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
    # Simple operations (thin wrappers for flow-based access)
    # -----------------------------------------------------------------
    "put": """\
match: sequence
rules:
  - id: stored
    do: put
    with:
      content: "{params.content}"
      uri: "{params.uri}"
      id: "{params.id}"
      tags: "{params.tags}"
      summary: "{params.summary}"
  - return: done
""",

    "tag": """\
match: sequence
rules:
  - id: tagged
    do: tag
    with:
      id: "{params.id}"
      items: "{params.items}"
      tags: "{params.tags}"
  - return: done
""",

    "delete": """\
match: sequence
rules:
  - id: result
    do: delete
    with:
      id: "{params.id}"
  - return: done
""",

    "move": """\
match: sequence
rules:
  - id: moved
    do: move
    with:
      name: "{params.name}"
      source: "{params.source}"
      tags: "{params.tags}"
      only_current: "{params.only_current}"
  - return: done
""",

    "stats": """\
match: sequence
rules:
  - id: profile
    do: stats
    with:
      top_k: "{params.top_k}"
  - return: done
""",

    # -----------------------------------------------------------------
    # Write path: post-processing
    # -----------------------------------------------------------------
    "after-write": """\
match: all
rules:
  - when: "item.content_length > params.max_summary_length && !item.has_summary"
    id: summary
    do: summarize
  - when: "item.has_uri && item.has_media_content && system.has_media_provider"
    id: described
    do: describe
post:
  - return:
      status: done
      with:
        item_id: "{params.id}"
        summary: "{summary}"
        described: "{described}"
""",

    # -----------------------------------------------------------------
    # Read path: context assembly
    # -----------------------------------------------------------------
    "get": """\
match: all
rules:
  - id: similar
    when: "!has(params.prompt) || params.prompt == ''"
    do: find
    with:
      similar_to: "{params.item_id}"
      limit: "{params.similar_limit}"
  - id: parts
    do: list_parts
    with:
      id: "{params.item_id}"
      limit: "{params.parts_limit}"
  - id: meta
    do: resolve_meta
    with:
      item_id: "{params.item_id}"
      limit: "{params.meta_limit}"
  - id: versions
    do: list_versions
    with:
      id: "{params.item_id}"
      limit: "{params.versions_limit}"
  - id: edges
    do: resolve_edges
    with:
      id: "{params.item_id}"
      limit: "{params.edges_limit}"
post:
  - return:
      status: done
      with:
        item_id: "{params.item_id}"
        similar: "{similar}"
        parts: "{parts}"
        meta: "{meta}"
        versions: "{versions}"
        edges: "{edges}"
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
      tags: "{params.tags}"
      bias: "{params.bias}"
      since: "{params.since}"
      until: "{params.until}"
      offset: "{params.offset}"
  - when: "search.margin > params.margin_high"
    return:
      status: done
      with:
        results: "{search.results}"
        margin: "{search.margin}"
        entropy: "{search.entropy}"
  - when: "search.lineage_strong > params.lineage_strong"
    do: find
    with:
      query: "{params.query}"
      tags: "{search.dominant_lineage_tags}"
      limit: 5
      tags: "{params.tags}"
      bias: "{params.bias}"
      since: "{params.since}"
      until: "{params.until}"
      offset: "{params.offset}"
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
      tags: "{params.tags}"
      bias: "{params.bias}"
      since: "{params.since}"
      until: "{params.until}"
      offset: "{params.offset}"
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
      tags: "{params.tags}"
      bias: "{params.bias}"
      since: "{params.since}"
      until: "{params.until}"
      offset: "{params.offset}"
  - id: bridge
    do: find
    with:
      query: "{params.query}"
      limit: "{params.bridge_limit}"
      tags: "{params.tags}"
      bias: "{params.bias}"
      since: "{params.since}"
      until: "{params.until}"
      offset: "{params.offset}"
post:
  - when: "pivot1.margin > params.margin_high || bridge.margin > params.margin_high"
    return:
      status: done
      with:
        results: "{pivot1.results}"
        bridge_results: "{bridge.results}"
        margin: "{pivot1.margin}"
        bridge_margin: "{bridge.margin}"
  - when: "budget.remaining > 0"
    then: query-resolve
  - return:
      status: stopped
      with:
        reason: "ambiguous"
        results: "{pivot1.results}"
        bridge_results: "{bridge.results}"
        margin: "{pivot1.margin}"
        bridge_margin: "{bridge.margin}"
""",

    "query-explore": """\
match: sequence
rules:
  - id: search
    do: find
    with:
      query: "{params.query}"
      limit: "{params.explore_limit}"
      tags: "{params.tags}"
      bias: "{params.bias}"
      since: "{params.since}"
      until: "{params.until}"
      offset: "{params.offset}"
  - when: "search.margin > params.margin_high"
    return:
      status: done
      with:
        results: "{search.results}"
        margin: "{search.margin}"
        entropy: "{search.entropy}"
  - when: "budget.remaining > 0"
    do: find
    with:
      query: "{params.query}"
      limit: "{params.explore_limit_wide}"
      tags: "{params.tags}"
      bias: "{params.bias}"
      since: "{params.since}"
      until: "{params.until}"
      offset: "{params.offset}"
    then: query-resolve
  - return:
      status: stopped
      with:
        reason: "budget"
        results: "{search.results}"
        margin: "{search.margin}"
        entropy: "{search.entropy}"
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
      tags: "{params.tags}"
      bias: "{params.bias}"
      since: "{params.since}"
      until: "{params.until}"
      offset: "{params.offset}"
  - when: "search.count == 0"
    return:
      status: done
      with:
        results: "{search.results}"
        count: "{search.count}"
  - id: related
    do: traverse
    with:
      items: "{search.results}"
      limit: "{params.deep_limit}"
  - return:
      status: done
      with:
        results: "{search.results}"
        count: "{search.count}"
        related: "{related}"
""",
}

# Builtin state doc fragments — fallback for test environments and
# the brief window before migration runs on a fresh store.
# Keyed by parent state doc name → fragment name → YAML body.
BUILTIN_STATE_FRAGMENTS: dict[str, dict[str, str]] = {
    "after-write": {
        "ocr": """\
rules:
  - when: "'_ocr_pages' in item.tags && item.has_uri"
    id: extracted
    do: ocr
    with:
      item_id: "{params.item_id}"
""",
        "analyze": """\
rules:
  - when: "!item.is_system_note && (item.content_length > 500 || item.has_uri) && !(has(item.tags._source) && item.tags._source == 'link') && !(has(item.tags._source) && item.tags._source == 'auto-vivify')"
    id: analyzed
    do: analyze
    with:
      item_id: "{params.item_id}"
""",
        "tag": """\
rules:
  - when: "!item.is_system_note && item.has_content && !(has(item.tags._source) && item.tags._source == 'link')"
    id: tagged
    do: auto_tag
    with:
      item_id: "{params.item_id}"
""",
        "links": """\
rules:
  - when: "!item.is_system_note && item.has_content && item.content_type == 'text/markdown' && !(has(item.tags._source) && item.tags._source == 'link')"
    id: linked
    do: extract_links
    with:
      item_id: "{params.item_id}"
      tag: references
      create_targets: "true"
""",
        "resolve-stubs": """\
rules:
  - when: "item.has_uri && !item.is_system_note && !(has(item.tags._source) && item.tags._source == 'link')"
    id: resolve_stubs
    do: resolve_stubs
    with:
      item_id: "{params.item_id}"
""",
        "duplicates": """\
rules:
  - when: "!item.is_system_note && item.has_content"
    id: resolve-duplicates
    do: resolve_duplicates
    with:
      item_id: "{params.item_id}"
      tag: duplicates
""",
    },
    "get": {
        "openclaw": """\
order: before:similar
rules:
  - id: search
    when: "has(params.prompt) && params.prompt != ''"
    do: find
    with:
      query: "{params.prompt}"
      bias: { "now": 0 }
      limit: 7
  - id: intentions
    do: get
    with:
      id: "now"
  - id: session
    do: get
    with:
      id: "{params.item_id}"
""",
    },
}
