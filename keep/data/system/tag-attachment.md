---
tags:
  category: system
  context: tag-description
  _inverse: has_attachment
---
# Tag: `attachment` — Email Attachment

The `attachment` tag links an attachment item to its parent email. It is populated automatically when ingesting multipart email files with attachments.

The `_inverse` declaration means this tag creates a navigable relationship edge. If an attachment has `attachment: email-id`, then `get email-id` will show it under `has_attachment:`.

## Characteristics

- **Edge-creating**: The `_inverse: has_attachment` declaration makes this an edge tag.
- **Machine-populated**: Set automatically during email attachment extraction.
- **Single-valued**: Each attachment belongs to exactly one parent email.
