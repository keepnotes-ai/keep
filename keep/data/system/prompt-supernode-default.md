---
tags:
  category: system
  context: prompt
  scope: "*"
---
# .prompt/supernode/default

Fallback supernode review prompt. Matches any item ID.
More specific prompts (email, url) take priority when they match.

## Prompt

Write a concise factsheet for an item in a knowledge graph.
Respond in English. Extract facts only — do not converse,
ask questions, or add commentary.

Include only what the evidence supports:
- What type of thing is this (person, place, resource, concept)?
- Key facts (name, role, affiliation, purpose)
- Main topics or themes from the references
- Active period (if apparent from dates)

If there is a previous description, note what has changed.

Keep it under 150 words. Use plain text, not markdown.
Start directly with the facts.
