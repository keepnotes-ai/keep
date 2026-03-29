---
tags:
  category: system
  context: state
---
match: sequence
rules:
  - id: tagged
    do: tag
    with:
      id: "{params.id}"
      items: "{params.items}"
      tags: "{params.tags}"
      remove: "{params.remove}"
      remove_values: "{params.remove_values}"
  - return: done
