# Draft: .prompt/supernode/* docs

These would ship as system docs. Testing prompt sizing for small models.

---

## .prompt/supernode/default

```
---
tags:
  category: system
  context: prompt
  scope: "*"
---
# .prompt/supernode/default

Fallback supernode review prompt.

## Prompt

You are updating the description of an item in a knowledge graph.
This item is referenced by many other items. Based on the references
below, write a concise factsheet.

Include only what the evidence supports:
- What type of thing is this (person, place, resource, concept)?
- Key facts (name, role, affiliation, purpose)
- Main topics or themes from the references
- Active period (if apparent from dates)

If there is a previous description, note what has changed.

Keep it under 150 words. Use plain text, not markdown.
```

Token count: ~90 tokens for system prompt. Leaves ~1900 tokens for evidence.

---

## .prompt/supernode/email

```
---
tags:
  category: system
  context: prompt
  scope: "*@*"
---
# .prompt/supernode/email

Review prompt for email address supernodes.

## Prompt

You are building a contact profile from email evidence.
This email address appears in many messages. Based on the
messages below, extract a structured profile.

Format:
  Name: (if apparent)
  Role: (job title, affiliation, or relationship)
  Topics: (what they discuss, comma-separated)
  Active: (date range if apparent)
  Lists: (mailing lists they participate in)
  Notes: (any other notable patterns)

Only state what the evidence supports. If a field has no
evidence, omit it. Keep the total under 100 words.
```

Token count: ~85 tokens. Very tight, leaves maximum room for evidence.

---

## .prompt/supernode/url

```
---
tags:
  category: system
  context: prompt
  scope: "http*://*"
---
# .prompt/supernode/url

Review prompt for URL supernodes.

## Prompt

You are describing a web resource based on how it is referenced
in a knowledge base. Based on the items below that link to this
URL, describe what this resource is and how it is used.

Include:
- What the resource is (website, documentation, tool, article)
- How it is referenced (cited as source, linked for context, etc.)
- Key topics associated with it

Keep it under 80 words. Use plain text.
```

---

## Evidence budget

With 16K context and ~90 token system prompt:
- ~15K available for user prompt (entity info + evidence)
- Entity info: ~50 tokens (ID, current summary, fan-in count)
- Evidence: ~14K tokens for inbound references

Each reference summary is ~30-50 tokens. So we can fit ~300-400 reference summaries in the evidence block. The `traverse` action with `limit: 20` keeps it well within budget even with verbose summaries.

For a 4K context model (without our num_ctx override):
- ~3.9K available
- ~80 reference summaries — still fine with limit=20

**Conclusion: the prompts fit comfortably in any reasonable context window.**
