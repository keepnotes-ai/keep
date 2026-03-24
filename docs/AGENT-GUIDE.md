# Reflective Memory — Agent Guide

Patterns for using the reflective memory store effectively in working sessions.

For the practice (why and when), see [../SKILL.md](../SKILL.md).
For CLI reference, see [REFERENCE.md](REFERENCE.md). For the output format, see [OUTPUT.md](OUTPUT.md).

> **Note:** Examples below use `keep_flow` (the primary MCP interface). CLI equivalents (`keep flow put -p content=X`, etc.) are available for hooks and terminal use — see [REFERENCE.md](REFERENCE.md).

---

## The Practice

This guide assumes familiarity with the reflective practice in [SKILL.md](../SKILL.md). The key points:

**Reflect before acting:** Check your current work context and intentions.
- What kind of conversation is this? (Action? Possibility? Clarification?)
- What do I already know?
```
keep_flow(state="get", params={item_id: "now"}, token_budget=2000)
keep_flow(state="query-resolve", params={query: "this situation"}, token_budget=2000)
```

**While acting:** Is this leading to harm? If yes: give it up.

**Reflect after acting:** What happened? What did I learn?
```
keep_flow(state="put", params={content: "what I learned", tags: {type: "learning"}})
```

**Periodically:** Run a full structured reflection ([details](KEEP-PROMPT.md)):
```
keep_prompt(name="reflect")
```

This cycle — reflect, act, reflect — is the mirror teaching. Memory isn't storage; it's how you develop skillful judgment.

---

## Working Session Pattern

Use the nowdoc as a scratchpad to track where you are in the work. This isn't enforced structure — it's a convention that helps you (and future agents) maintain perspective.

```
# 1. Starting work — check context and intentions
keep_flow(state="get", params={item_id: "now"}, token_budget=2000)

# 2. Update context as work evolves
keep_flow(state="put", params={content: "Diagnosing flaky test in auth module", id: "now", tags: {project: "myapp", topic: "testing"}})
keep_flow(state="put", params={content: "Found timing issue", id: "now", tags: {project: "myapp"}})

# 3. Record learnings
keep_flow(state="put", params={content: "Flaky timing fix: mock time instead of real assertions", tags: {topic: "testing", type: "learning"}})
```

**Key insight:** The store remembers across sessions; working memory doesn't. When you resume, read context first. All updates create version history automatically.

---

## Agent Handoff

**Starting a session:**
```
keep_flow(state="get", params={item_id: "now"}, token_budget=2000)
keep_flow(state="query-resolve", params={query: "recent work", since: "P1D"}, token_budget=1500)
```

**Ending a session:**
```
keep_flow(state="put", params={content: "Completed OAuth2 flow. Token refresh working. Next: add tests.", id: "now", tags: {topic: "auth"}})
keep_flow(state="move", params={name: "auth-string", tags: {project: "myapp"}})
```

---

## Strings

As you work, `now` accumulates a string of versions — a trace of how intentions evolved. The `move` flow lets you name and archive that string, making room for what's next.

**Snapshot before pivoting.** When the conversation shifts topic:
```
keep_flow(state="move", params={name: "auth-string", tags: {project: "myapp"}})
keep_flow(state="put", params={content: "Starting on database migration", id: "now"})
```

**Incremental archival.** Move to the same name repeatedly — versions append:
```
keep_flow(state="move", params={name: "design-log", tags: {project: "myapp"}})
```

**Tag-filtered extraction.** When a session mixes projects:
```
keep_flow(state="move", params={name: "frontend-work", tags: {project: "frontend"}})
```

---

## Index Important Documents

Whenever you encounter documents important to the task, index them:

```
keep_flow(state="put", params={content: "https://docs.example.com/auth", tags: {topic: "auth", project: "myapp"}})
keep_flow(state="put", params={content: "file:///path/to/design.pdf", tags: {type: "reference", topic: "architecture"}})
```

Ask: what is this? Why is it important? Tag appropriately.

---

## Breakdowns as Learning

When the normal flow is interrupted — an assumption has been revealed. **First:** complete the immediate conversation. **Then record:**

```
keep_flow(state="put", params={content: "Assumed user wanted full rewrite. Actually: minimal patch.", tags: {type: "breakdown"}})
```

Breakdowns are how agents learn.

---

## Tracking Commitments

Use speech-act tags to make the commitment structure visible:

```
# Track promises
keep_flow(state="put", params={content: "I'll fix the auth bug", tags: {act: "commitment", status: "open", project: "myapp"}})

# Track requests
keep_flow(state="put", params={content: "Please review the PR", tags: {act: "request", status: "open"}})

# Query open work
keep_flow(state="query-resolve", params={query: "open commitments", tags: {act: "commitment", status: "open"}}, token_budget=1000)

# Close the loop
keep_flow(state="tag", params={id: "ID", tags: {status: "fulfilled"}})
```

See [TAGGING.md](use keep_help with topic="tagging") for the full speech-act framework.

---

## Data Model

An item has:
- A unique identifier (URI, content hash, or system ID)
- Timestamps (`_created`, `_updated`)
- A summary of the content
- Tags (`{key: value, ...}`)
- Version history (previous versions archived automatically)

The full original document is not stored. Summaries are contextual — tags shape how new items are understood. See [KEEP-PUT.md](use keep_help with topic="keep-put").

---

## System Documents

Bundled system docs provide patterns and conventions, accessible via `get-context`:

| ID | What it provides |
|----|------------------|
| `.domains` | Domain-specific organization patterns |
| `.conversations` | Conversation framework (action, possibility, clarification) |
| `.tag/act` | Speech-act categories |
| `.tag/status` | Lifecycle states |
| `.tag/project` | Project tag conventions |
| `.tag/topic` | Topic tag conventions |

---

## See Also

- [FLOW-ACTIONS.md](use keep_help with topic="flow-actions") — Action reference
- [KEEP-FLOW.md](use keep_help with topic="keep-flow") — Running and steering flows
- [REFERENCE.md](use keep_help with topic="reference") — CLI quick reference
- [TAGGING.md](use keep_help with topic="tagging") — Tags, speech acts, project/topic
