---
tags:
  category: system
  context: prompt
  state: get
---
# .prompt/agent/openclaw-assemble

Context assembly for OpenClaw agent turns. Uses the `get` state doc
(with openclaw fragment) for retrieval — intentions, similar items,
meta, edges, and session history.

## Prompt

The following is retrieved memory context from keep.

Current intentions (`now`):
{intentions}

Related items:
{search}{similar}

Meta (learnings, commitments, patterns):
{meta}

Linked items:
{edges}

Recent session activity:
{session}

無常迅速
What past actions led to this?  Will the next action be skillful?
Track your questions, assessments, requests, offers, and commitments
using `keep_flow(state="put", params={content: "...", tags: {...}})` as the tool for improvement.
Use `keep_prompt(name="reflect")` for deeper review.
慎勿放逸
