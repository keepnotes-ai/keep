---
tags:
  category: system
  context: tag-description
  _inverse: cc_recipient_of
---
# Tag: `cc` — Email CC Recipient

The `cc` tag identifies CC'd recipients of an email message. It is populated automatically when ingesting email files (`.eml` or RFC 822 format).

The `_inverse` declaration means this tag creates a navigable relationship edge. If an email has `cc: carol@example.com`, then `get carol@example.com` will show it under `cc_recipient_of:`.

## Characteristics

- **Edge-creating**: The `_inverse: cc_recipient_of` declaration makes this an edge tag.
- **Auto-vivifying**: If the recipient doesn't exist as an item, it's created automatically.
- **Multi-valued**: An email can CC multiple recipients.
- **Machine-populated**: Set by email extraction during `put`, not typically set manually.
- **Lowercase**: Email addresses are normalized to lowercase.
