---
tags:
  category: system
  context: prompt
  state: openclaw-assemble
---
# .prompt/agent/openclaw-assemble

Context assembly for OpenClaw agent turns. Uses the openclaw-assemble
state doc for retrieval (intentions, similar items, meta, edges, and
session history).

## Prompt

The following is retrieved memory context from keep.

Current intentions (`now`):
{intentions}

Related items (by similarity to this turn):
{similar}

Meta (learnings, commitments, patterns):
{meta}

Linked items:
{edges}

Recent session activity:
{session}
