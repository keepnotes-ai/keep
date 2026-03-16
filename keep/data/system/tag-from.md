---
tags:
  category: system
  context: tag-description
  _inverse: sender_of
---
# Tag: `from` — Email Sender

The `from` tag identifies the sender of an email message. It is populated automatically when ingesting email files (`.eml` or RFC 822 format).

The `_inverse` declaration means this tag creates a navigable relationship edge. If an email has `from: alice@example.com`, then `get alice@example.com` will show it under `sender_of:`.

## Characteristics

- **Edge-creating**: The `_inverse: sender_of` declaration makes this an edge tag.
- **Auto-vivifying**: If the sender doesn't exist as an item, it's created automatically.
- **Machine-populated**: Set by email extraction during `put`, not typically set manually.
- **Lowercase**: Email addresses are normalized to lowercase.
