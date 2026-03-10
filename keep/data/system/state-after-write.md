---
tags:
  category: system
  context: state
---
# Runs after each put(). All matching rules fire in parallel.
match: all
rules:
  - id: summary
    # Long content without an existing summary
    when: "item.content_length > params.max_summary_length && !item.has_summary"
    do: summarize
  - id: extracted
    # URI-backed items with image pages needing OCR
    when: "'_ocr_pages' in item.tags && item.has_uri"
    do: ocr
  - id: analyzed
    # Decompose non-system items into parts
    when: "!item.is_system_note"
    do: analyze
  - id: tagged
    # Classify non-system items against tag specs
    when: "!item.is_system_note"
    do: tag
post:
  - return: done
