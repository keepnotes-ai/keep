# State Doc Examples: Complex Flows

Date: 2026-03-07
Status: Draft
Related:
- `docs/design/STATE-DOC-SCHEMA.md`
- `docs/design/STATE-ACTIONS.md`

## 1) Purpose

These examples draft flow-based processes more complex than
get/put. They combine query, analysis, and update within a single
flow. They're intended to test the expressiveness of the state doc
schema and surface any missing primitives.

---

## 2) Example: Nightly commitment review

**Trigger**: Scheduled nightly by the agentic harness.
**Goal**: Find all open commitments, rank them, generate a summary
document that links to each one.

### Flow

```
continue({
    "state": "review-commitments",
    "params": { "since": "P1D", "max_items": 20 },
    "budget": { "ticks": 10 },
})
```

### State docs

```yaml
# .state/review-commitments
# match: sequence
rules:
  - id: open
    do: find
    with:
      tags: { act: "commitment", status: "open" }
      limit: "{params.max_items}"
      order_by: created
  - when: "size(open.results) == 0"
    return: done
    with: { note: "No open commitments" }
  - id: report
    do: generate
    with:
      system: "You are a project reviewer."
      user: |
        Review these open commitments. For each one:
        - Summarize what was committed to
        - Note how old it is
        - Flag if stale (>7 days with no activity)

        Commitments:
        {open.results}
  - do: put
    with:
      content: "{report.text}"
      tags:
        category: system
        type: review
        topic: commitments
  - return: done
```

### What this shows

**Tag-only queries don't need `query:`.** The commitments are
identified by tags (`act: commitment`, `status: open`), not by
semantic search. `find` with just `tags:` and `order_by:` is a
structured listing — internally routed to `Keeper.list_items()`.

**No iteration needed.** The `generate` action receives the full
result list and produces a single document. The LLM handles the
per-item breakdown. No `for-each` primitive required.

**`put` as an action.** The flow creates a new item — the review
document. `put` returns a mutation; the runtime creates the item.
Edge relationships to the commitments would be expressed as tags
on the new document (using existing edge-tag conventions).

---

## 3) Example: Session reflection review

**Trigger**: Scheduled hourly, or on session end.
**Goal**: Review recent activity, evaluate skillfulness against
intentions, capture learnings.

This is the more interesting case. The agent initiates the flow and
is presented with structured context. The agent then uses the flow
as scaffolding — making substantive decisions, possibly spawning
sub-flows to reflect on specific topics.

### Flow

```
continue({
    "state": "review-recent",
    "params": { "since": "PT1H", "session_id": "abc-123" },
    "budget": { "ticks": 15 },
})
```

### State docs

```yaml
# .state/review-recent
# match: sequence
rules:
  - id: recent_notes
    do: find
    with:
      since: "{params.since}"
      limit: 50
      order_by: created
  - id: recent_actions
    do: find
    with:
      tags: { act: "commitment" }
      since: "{params.since}"
      limit: 20
  - id: open_commitments
    do: find
    with:
      tags: { act: "commitment", status: "open" }
      limit: 20
  - when: "size(recent_notes.results) == 0"
    return: done
    with: { note: "No recent activity" }
  - id: assessment
    do: generate
    with:
      system: |
        You are reviewing an AI agent's recent work session.
        Evaluate whether the logged outcomes were skillful
        based on the logged intentions.
      user: |
        ## Recent notes
        {recent_notes.results}

        ## Actions and decisions
        {recent_actions.results}

        ## Open commitments
        {open_commitments.results}

        For each topic area you identify:
        1. What was the intention?
        2. What actually happened?
        3. Was the outcome skillful? Why or why not?
        4. What should be remembered for next time?

        Return a structured assessment with topic_areas[].
      format: json
  - then:
      state: review-reflect
      with:
        topic_areas: "{assessment.topic_areas}"
```

```yaml
# .state/review-reflect
# match: all
rules:
  - when: "size(params.topic_areas) > 0"
    id: learnings
    do: generate
    with:
      system: "Distill actionable learnings from this assessment."
      user: "{params.topic_areas}"
  - when: "size(params.topic_areas) > 0"
    id: reminders
    do: generate
    with:
      system: |
        Extract essential reminders — things the agent should
        keep in mind in future sessions. Be concise and specific.
      user: "{params.topic_areas}"
post:
  - then:
      state: review-persist
      with:
        learnings: "{learnings}"
        reminders: "{reminders}"
```

```yaml
# .state/review-persist
# match: sequence
rules:
  - when: "params.learnings"
    do: put
    with:
      content: "{params.learnings.text}"
      tags:
        type: learning
        topic: reflection
        _session: "{params.session_id}"
  - when: "params.reminders"
    do: put
    with:
      content: "{params.reminders.text}"
      tags:
        type: reminder
        topic: reflection
        _session: "{params.session_id}"
  - return: done
```

### What this shows

**Multi-query gather.** The first state runs three `find` actions in
sequence to assemble context from different angles: all recent notes,
recent commitments, and open commitments. Could be parallel with
`match: all` + named bindings.

**Structured LLM output.** The assessment step uses `format: json`
and the transition explicitly passes `assessment.topic_areas` to the
next state via `with:`.

**Explicit parameter passing across transitions.** `review-reflect`
produces `learnings` and `reminders` via `match: all`, then passes
both explicitly to `review-persist` via `with:`. Each state doc is
self-contained — it depends only on `params.*`.

**Multiple puts in one flow.** The persist state creates two items.
Each `put` is a separate mutation applied in sequence.

---

## 4) Variant: Agent-in-the-loop reflection

The above is fully automated. But the user described something richer:
the agent reviews the assessment and *actively decides* what to
reflect on further.

The pattern uses **`return: stopped`** — the flow pauses and returns
its results to the caller. The caller (the agent) inspects the results
and resumes with direction:

```yaml
# .state/review-decide
# match: sequence
rules:
  - return: stopped
    with:
      reason: "awaiting_guidance"
      results: "{params.topic_areas}"
```

The flow stops immediately. The agent receives the topic areas and
decides what to do. The agent then resumes, steering the flow:

```python
# Agent dismisses — no further action
# (simply doesn't resume)

# Agent accepts — proceed to capture learnings
continue({"cursor": cursor, "state": "review-reflect"})

# Agent wants deeper reflection on specific topics
continue({
    "cursor": cursor,
    "state": "review-deep-reflect",
    "params": {"topics": ["commitment-tracking", "search-quality"]},
})
```

The caller controls the next state and params. No decision protocol,
no `decision.*` namespace — the caller takes over and steers.

### Deep reflection (iterative)

```yaml
# .state/review-deep-reflect
# match: sequence
rules:
  - id: topic_notes
    do: find
    with:
      query: "{params.topics[0]}"
      since: "{params.since}"
      limit: 10
  - id: deep_reflection
    do: generate
    with:
      system: |
        Reflect deeply on this topic. What patterns do you see?
        What would you do differently? What should be remembered?
      user: |
        Topic: {params.topics[0]}
        Related notes: {topic_notes.results}
  - do: put
    with:
      content: "{deep_reflection.text}"
      tags:
        type: learning
        topic: "{params.topics[0]}"
        act: reflection
  - when: "size(params.topics) > 1"
    then:
      state: review-deep-reflect
      with:
        topics: "{params.topics[1:]}"
  - return: done
```

Self-transition with a shrinking list handles iteration. Each pass
processes one topic, then transitions to itself with the remainder.
The topic list comes through `params.topics`, passed explicitly from
`review-decide`.

### What this shows

**`stopped` is the only interaction point.** The flow stops, the
caller inspects results, and resumes with direction. No special
decision mechanism — just `return: stopped` and `continue()`.

**The agent is actively using the flow.** The flow presents
structured context, the agent makes a substantive choice, and
resumes to the appropriate state. The flow is scaffolding for the
agent's reasoning, not a replacement for it.

**Iteration via self-transition.** Processing a list item-by-item
doesn't need a `for-each` primitive. Self-transition with a shrinking
parameter list handles it. Each iteration is a full re-evaluation
of the state doc's rules.

---

## 5) Observations

These examples use only three actions: `find`, `generate`, `put`.
The variation comes from the state doc structure and the prompts.

All "query refinement" is just calling `find` again with different
parameters. All "decisions" surface as `stopped` — the caller resumes with
direction. All "iteration" is self-transition. The schema's
primitives are sufficient without extension.
