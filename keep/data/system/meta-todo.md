---
tags:
  category: system
  context: meta
---
# .meta/todo — Open Loops
#
# Surfaces unresolved commitments, requests, offers, and blocked work
# during `keep now` and `keep get`.
#
# This is a state doc. Each rule uses `find` with `similar_to` to rank
# results by relevance to the item being viewed, and `tags` to filter
# to a specific speech-act type. Results are already ranked by semantic
# similarity — no separate ranking step needed.
#
# Available params (injected by resolve_meta):
#   params.item_id  — ID of the item being viewed
#   params.limit    — max results per rule
#   params.*        — all non-underscore tags from the viewed item
#
# To customize: edit this doc or create your own .meta/* docs using
# the same match/rules/do/with syntax.
match: all
rules:
  - id: commitments
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {act: commitment, status: open}
      limit: "{params.limit}"
  - id: requests
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {act: request, status: open}
      limit: "{params.limit}"
  - id: offers
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {act: offer, status: open}
      limit: "{params.limit}"
  - id: blocked
    do: find
    with:
      similar_to: "{params.item_id}"
      tags: {status: blocked}
      limit: "{params.limit}"
