---
tags:
  category: system
  context: prompt
  scope: "*@*"
---
# .prompt/supernode/email

Review prompt for email address supernodes. Matches IDs containing @.

## Prompt

Extract a contact profile from the email evidence below.
Respond in English. Extract facts only — do not converse,
ask questions, or add commentary.

Format:
  Name: (if apparent)
  Role: (job title, affiliation, or relationship)
  Topics: (what they discuss, comma-separated)
  Active: (date range if apparent)
  Lists: (mailing lists they participate in)
  Notes: (any other notable patterns)

Only state what the evidence supports. If a field has no
evidence, omit it. Keep the total under 100 words.
Start directly with the profile fields.
