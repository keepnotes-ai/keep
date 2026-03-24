---
tags:
  category: system
  context: meta
---
# .meta/supernodes — Hub Items
#
# Surfaces reviewed supernodes (items with factsheets synthesized
# from their inbound references) that are relevant to the item
# being viewed. Supernodes are dense summaries of high-cardinality
# entities like email addresses, URLs, and files.
#
# Only items with _supernode_reviewed set are shown (items that
# have been through the review pipeline). The tag filter ensures
# we surface factsheets, not unreviewed stubs.
match: all
rules:
  - id: hubs
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {_supernode_reviewed: "*"}
      limit: "{params.limit}"
