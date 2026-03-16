---
tags:
  category: system
  context: state-fragment
---
rules:
  - id: linked
    when: "!item.is_system_note && item.has_content && (item.content_type == 'text/markdown' || item.content_type == 'text/html' || item.content_type == 'message/rfc822')"
    do: extract_links
    with:
      tag: references
      create_targets: "true"
