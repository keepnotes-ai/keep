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
  - return:
      status: done
      with:
        deleted: "{result.deleted}"
