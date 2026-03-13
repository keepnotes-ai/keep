---
tags:
  category: system
  context: state-fragment
---
rules:
  - id: find-duplicates
    when: "!item.is_system_note && item.has_content"
    do: find_duplicates
    with:
      tag: duplicates
