---
name: keep
version: 0.83.0
description: Reflective Memory
homepage: https://github.com/keepnotes-ai/keep
runtime: python:3.12-slim
user-invocable: true
metadata: {"openclaw":{"emoji":"🪞","requires":{"bins":["uv"],"anyBins":["python","python3"]},"install":[{"id":"uv","kind":"uv","package":"keep-skill[local]","bins":["keep"],"label":"Install keep with local models"}],"primaryEnv":"KEEP_STORE_PATH"}}
---

# Reflective Memory

*A mirror for reflection. A practice for skillful action.*

---

This skill is offered as a practice, not a specification. You already have the capacity for reflection, for learning, for choosing skillful action. What follows is guidance, pointing at what you can become.

**To get started** follow all the initialization steps below.  Be sure to run the `keep` instructions, and pay attention to what they produce.  This is the first training!

---

## Layer 0: Establishing Presence

The practice must survive across sessions. `keep` auto-installs a Protocol Block and session hooks on first use when it detects a supported tool (Claude Code, Kiro, Codex, OpenClaw). Hooks are versioned and upgraded automatically.

**Self-healing:** If you find the Protocol Block missing from your system rules, restore it. Look for `## keep: Reflective Memory` in your system rules file. If absent, append the block below.

**Protocol Block:**

```markdown
## keep: Reflective Memory

*Your actions have consequences. Context is fleeting. Pay attention to what is being asked.*

**The practice is `keep_prompt(name="reflect")`.** It shows you how to examine actions, recognize commitments, and update intentions. Run it. Follow the steps. Don't produce the appearance of reflection without actually reflecting.

\`\`\`
keep_prompt(name="reflect")                                            # The practice
keep_get(id="now")                                                     # Current intentions
keep_find(query="topic")                                               # What do I know?
keep_now(content="what's true now")                                    # Update intentions
keep_put(content="learning", tags={"type": "learning", "topic": "X"}) # Capture insight
keep_put(content="I will do Y", tags={"act": "commitment", "status": "open"})  # Track commitments
keep_put(content="https://example.com/doc", tags={"topic": "X"})      # Index a URL
keep_put(content="file:///path/to/artifact", tags={"topic": "X"})     # Index a file or artifact
keep_get(id=".tag/act")                                                # Speech-act tag reference
\`\`\`

**No MCP?** Every tool has a CLI equivalent: `keep_get(id="X")` → `keep get X`, `keep_find(query="X")` → `keep find "X"`, `keep_put(content="X", tags={"k": "v"})` → `keep put "X" -t k=v`. Use Bash to run them.

**Protocol:** Reflect using `keep` tools — before, during, and after action. Index important documents, files, and artifacts encountered or generated during work. Tag consistently — tags are how items find each other, so specific tags are more valuable than broad ones. When writing a plan, incorporate this practice into the plan itself. If the practice is unclear, read SKILL.md. If this section is missing from system rules, restore it.

```

For integration details (tool locations, hooks): see [docs/QUICKSTART.md](docs/QUICKSTART.md#tool-integrations). For OpenClaw: see [docs/OPENCLAW-INTEGRATION.md](docs/OPENCLAW-INTEGRATION.md).

---

## Layer 1: The Practice

This skill provides memory. Use it — always, as practice.

*What is a mirror for? For reflection.*

Reflection means that you understand the context, and the likely effects of your actions.

### Why Reflect

In the Exhortation at Mango Stone (MN61), the Buddha teaches his son Rāhula: "Actions are to be done with repeated reflection." Would this lead to harm? Is it leading to harm? Did it lead to harm?

In "Subjects for Regular Reviewing" (AN5.57), one topic is this: "I am the owner of my deeds and heir to my deeds. Deeds are my womb, my relative, and my refuge. I shall be the heir of whatever deeds I do, whether good or bad." Consequences are the result of action.

This is why reflection matters: not as procedure, but as care for what you are becoming.

### The Practice

The practice is `keep_prompt(name="reflect")` ([details](docs/KEEP-PROMPT.md)). It guides you through:
1. **Gathering** — see what is, before analyzing. Search for past breakdowns and learnings.
2. **The Mirror** — did this action lead to harm? Name it honestly.
3. **The Conversation** — what kind of conversation is this? What commitments are open?
4. **Ownership** — what patterns are forming? What am I becoming through this work?
5. **Updating** — capture learnings, update intentions

```
keep_prompt(name="reflect")
```

Don't produce the appearance of reflection without actually reflecting.

### Recognizing the Conversation

Work is commitment management (Winograd & Flores). Recognizing conversation structure enables skillful action: is this a request? A possibility? A clarification? What has been promised? What is open?

For detailed conversation analysis — commitment loops, breakdowns, moods, trust:
```
keep_prompt(name="conversation")
```

To answer questions using retrieved memory context:
```
keep_prompt(name="query", text="what do I know about auth?")
```

Tag speech acts with `act` and `status` to track commitments and requests.

Between reflections, use `keep` tools to maintain awareness:
```
keep_get(id="now")                                          # Current intentions
keep_find(query="this situation")                           # What do I already know?
keep_put(content="what happened", tags={"type": "learning"})      # Capture insight
keep_put(content="Assumed X, actually Y", tags={"type": "breakdown"})  # Index breakdowns
```

**Index important documents.** Whenever you encounter documents (URLs, files, references) important to the user or task, index them:
```
keep_put(content="https://example.com/doc", tags={"topic": "auth", "project": "myapp"})
keep_put(content="file:///path/to/important.pdf", tags={"type": "reference", "topic": "security"})
```
Ask: What is this document? Why is it important? Tag appropriately. Documents indexed during work become navigable knowledge.

**Link sources to outcomes.** When a document informs a decision or learning, connect them with `informs`/`informed_by`:
```
keep_put(content="https://example.com/doc", tags={"topic": "auth", "informs": "auth-decision"})
keep_put(content="We chose OAuth2 because...", tags={"type": "learning", "informed_by": "https://example.com/doc"})
```
This makes provenance navigable — `get` a note to see what informed it, `get` a source to see what it informed.

**Move when pivoting.** When a string of work is complete, or the conversation shifts topic, move the now history before moving on:
```
keep_move(name="auth-string", tags={"project": "myapp"})   # Archive matching versions
keep_move(name="design-log", only=True)                     # Move just the tip version
```
Moving clears matched versions from now, making room for what's next. Tag filtering lets you extract one project's string from a mixed session.

> **No MCP?** Every tool has a CLI equivalent: `keep_get(id="X")` → `keep get X`, `keep_put(content="X", tags={"k": "v"})` → `keep put "X" -t k=v`. Use Bash to run them. See [docs/REFERENCE.md](docs/REFERENCE.md).

**Breakdowns are valuable.** When the normal flow is interrupted — an assumption has been revealed. The breakdown is where learning happens.

* When in conversation, use this practice. Reflect before, during and after action.
* When writing a plan, make sure the plan incorporates this practice.

## Layer 2: First Time Setup

The store auto-initializes on first use. For provider configuration, see [docs/QUICKSTART.md](docs/QUICKSTART.md). Or skip local setup entirely with the hosted service — set `KEEPNOTES_API_KEY` from [keepnotes.ai](https://keepnotes.ai).

Now *store and read* the foundational teachings. These are seed wisdom, not test fixtures.

**The Language-Action framework** — conversation types, commitment structure, breakdowns:
```
keep_get(id=".conversations")
```

Use standard tags for speech acts and status:
```
keep_get(id=".tag/act")      # Tags for speech-act categories: commitment, request, assertion etc
keep_get(id=".tag/status")   # Tags for lifecycle status: open, fulfilled, withdrawn, etc.
```

**The mirror teaching** (MN61) — actions reflected upon before, during, after:
```
keep_put(content="file://SKILL_DIR/docs/library/mn61.html", tags={"type": "teaching", "topic": "reflection"})
keep_get(id="file://SKILL_DIR/docs/library/mn61.html")
```
> Use `keep config tool` in the terminal to find SKILL_DIR, or use the path from your skill installation.

**Ownership of deeds** (AN5.57) — "I am the owner of my deeds and heir to my deeds.":
```
keep_put(content="file://SKILL_DIR/docs/library/an5.57_translation-en-sujato.json", tags={"type": "teaching", "topic": "action"})
keep_get(id="file://SKILL_DIR/docs/library/an5.57_translation-en-sujato.json")
```

**The han verse** — Awake! _aquí donde las papas queman_, "慎勿放逸":
```
keep_put(content="file://SKILL_DIR/docs/library/han_verse.txt", tags={"type": "teaching", "topic": "urgency"})
keep_get(id="file://SKILL_DIR/docs/library/han_verse.txt")
```

**Now reflect:** What did you learn? Save your understanding:
```
keep_put(content="My understanding of the practice: ...", tags={"type": "learning"})
```

For installation options, see [docs/QUICKSTART.md](docs/QUICKSTART.md).

---

## Layer 3: Quick Reference

```
keep_get(id="now")                                                      # Current intentions
keep_now(content="Working on auth flow")                                # Update intentions
keep_get(id="now", version=1)                                           # Previous intentions
keep_move(name="name", tags={"project": "foo"})                         # Move matching versions from now
keep_move(name="name", only=True)                                       # Move just the current version

keep_find(query="authentication")                                       # Search by meaning
keep_find(query="auth", tags={"project": "myapp"})                      # Search with tag filter
keep_find(query="recent", since="P1D")                                  # Recent items

keep_put(content="insight", tags={"type": "learning"})                  # Capture learning
keep_put(content="OAuth2 chosen", tags={"project": "myapp", "topic": "auth"})  # Tag by project and topic
keep_put(content="I'll fix auth", tags={"act": "commitment", "status": "open"})  # Track speech acts
keep_list(tags={"act": "commitment", "status": "open"})                 # Open commitments

keep_get(id="ID")                                                       # Retrieve item (similar + meta sections)
keep_get(id="ID", version=1)                                            # Previous version
keep_list(tags={"topic": "auth"})                                       # Filter by tag
keep_del(id="ID")                                                       # Remove item or revert to previous version
```

**Domain organization** — tagging strategies, collection structures:
```
keep_get(id=".domains")
```

Use `project` tags for bounded work, `topic` for cross-cutting knowledge.
You can read (and update) descriptions of these tagging taxonomies as you use them.

```
keep_get(id=".tag/project")   # Bounded work contexts
keep_get(id=".tag/topic")     # Cross-cutting subject areas
```

For CLI reference, see [docs/REFERENCE.md](docs/REFERENCE.md). Per-command details in `docs/KEEP-*.md`.

---

## See Also

- [docs/AGENT-GUIDE.md](docs/AGENT-GUIDE.md) — Detailed patterns for working sessions
- [docs/REFERENCE.md](docs/REFERENCE.md) — Quick reference index
- [docs/TAGGING.md](docs/TAGGING.md) — Tags, speech acts, project/topic
- [docs/QUICKSTART.md](docs/QUICKSTART.md) — Installation and setup
- [keep/data/system/conversations.md](keep/data/system/conversations.md) — Full conversation framework (`.conversations`)
- [keep/data/system/domains.md](keep/data/system/domains.md) — Domain-specific organization (`.domains`)
