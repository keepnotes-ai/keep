---
tags:
  category: system
  context: tag-description
  _inverse: bcc_recipient_of
---
# Tag: `bcc` — Email BCC Recipient

The `bcc` tag identifies BCC'd recipients of an email message. It is populated automatically when ingesting email files (`.eml` or RFC 822 format) that contain Bcc headers.

The `_inverse` declaration means this tag creates a navigable relationship edge. If an email has `bcc: dave@example.com`, then `get dave@example.com` will show it under `bcc_recipient_of:`.

## Characteristics

- **Edge-creating**: The `_inverse: bcc_recipient_of` declaration makes this an edge tag.
- **Auto-vivifying**: If the recipient doesn't exist as an item, it's created automatically.
- **Multi-valued**: An email can BCC multiple recipients.
- **Machine-populated**: Set by email extraction during `put`, not typically set manually.
- **Lowercase**: Email addresses are normalized to lowercase.
