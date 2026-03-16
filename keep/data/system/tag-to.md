---
tags:
  category: system
  context: tag-description
  _inverse: recipient_of
---
# Tag: `to` — Email Recipient

The `to` tag identifies recipients of an email message. It is populated automatically when ingesting email files (`.eml` or RFC 822 format).

The `_inverse` declaration means this tag creates a navigable relationship edge. If an email has `to: bob@example.com`, then `get bob@example.com` will show it under `recipient_of:`.

## Characteristics

- **Edge-creating**: The `_inverse: recipient_of` declaration makes this an edge tag.
- **Auto-vivifying**: If the recipient doesn't exist as an item, it's created automatically.
- **Multi-valued**: An email can have multiple recipients.
- **Machine-populated**: Set by email extraction during `put`, not typically set manually.
- **Lowercase**: Email addresses are normalized to lowercase.
