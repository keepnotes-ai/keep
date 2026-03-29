---
tags:
  category: system
  context: state
---
match: sequence
rules:
  - id: result
    do: delete
    with:
      id: "{params.id}"
      delete_versions: "{params.delete_versions}"
  - return: done
