---
tags:
  category: system
  context: state
  replaces: .state/openclaw-assemble
---
# OpenClaw-specific context assembly additions.
# Extends .state/get with agent-turn context:
#   - search: query-based find (replaces similar_to when prompt is present)
#   - intentions: current working context from "now"
#   - session: session history for the current item
#
# Inserted before the base `similar` rule so the `when` guards
# on `similar` and `search` are complementary — exactly one fires.
order: before:similar
rules:
  - id: search
    # Query-based search for agent turns (replaces similar_to)
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
