---
tags:
  category: system
  context: state-fragment
---
rules:
  - id: linked
    when: "!item.is_system_note && item.has_content && (item.content_type == 'text/markdown' || item.content_type == 'text/html' || item.content_type == 'message/rfc822' || item.content_type == 'application/pdf' || item.content_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' || item.content_type == 'application/vnd.openxmlformats-officedocument.presentationml.presentation')"
    do: extract_links
    with:
      tag: references
      create_targets: "true"
