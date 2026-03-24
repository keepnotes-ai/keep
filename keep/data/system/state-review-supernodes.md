---
tags:
  category: system
  context: state
---
# Reviews a single supernode candidate.
#
# The daemon enqueues one work item per candidate with params.item_id
# set. This flow runs in daemon context (foreground=False), so the
# generate action (async) executes inline.
#
# Steps:
#   1. Load the target item (current content/summary)
#   2. Traverse inbound references (evidence for the factsheet)
#   3. Generate a new factsheet via LLM using a .prompt/supernode/* doc
#   4. Put the factsheet as a new version, marking _supernode_reviewed
#
# The put creates a new version — previous content is preserved in
# version history. The after-write flow will summarize/tag the
# factsheet as normal content.
match: sequence
rules:
  - id: target
    do: get
    with:
      id: "{params.item_id}"
  - id: inbound
    do: traverse
    with:
      items: ["{params.item_id}"]
      limit: "20"
  - id: description
    do: generate
    with:
      prompt: "supernode"
      id: "{params.item_id}"
      user: "Item: {params.item_id}\nCurrent summary: {target.summary}\nFan-in: {params.fan_in}\n\nInbound references:\n{inbound}"
  - id: updated
    do: put
    with:
      id: "{params.item_id}"
      content: "{description.text}"
      tags:
        _supernode_reviewed: "{now}"
  - return: done
