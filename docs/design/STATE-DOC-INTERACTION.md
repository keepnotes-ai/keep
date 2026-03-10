# State Doc Interaction Patterns

Date: 2026-03-07
Status: Draft
Related:
- `docs/design/STATE-DOC-SCHEMA.md`
- `docs/design/STATE-DOC-EXAMPLES.md`

## 1) The question

How do results surface to the caller (person or agent)?
When does the caller see anything? When does the caller act?
What is the relationship between the flow and decision-making?

State docs orchestrate memory operations — every action reads
from or enriches the keep store. Within that scope, actions are
either **sync** (store queries — milliseconds) or **async**
(provider calls that enrich items — seconds). Sync and async
mix freely; the flow waits for results only when it needs them.

## 2) Write path: immediate return, background processing

The caller gets their item ID back immediately. Processing
(summarize, tag, OCR) happens in the background. The `flow_id`
provides traceability — the caller can inspect the execution
trace at any later point.

```mermaid
sequenceDiagram
    participant C as Caller
    participant K as Keeper.put()
    participant S as Store
    participant R as Runtime
    participant P as Provider (LLM)

    C->>K: put("Meeting notes...", tags: {topic: v2})
    K->>S: store item
    S-->>K: item_id = %a1b2c3
    K->>R: continue(state: after-write, params: {item_id, max_summary_length})
    R->>R: load .state/after-write (match: all)
    R->>R: dispatch async work: summarize, tag
    R-->>K: {status: stopped, reason: "background", flow_id: "f-xyz"}
    K-->>C: {item_id: "%a1b2c3", flow_id: "f-xyz"}

    Note over C: Caller continues immediately

    Note over R,P: Background workers pick up work

    P->>R: summarize result: {summary: "..."}
    R->>S: apply mutation: set_summary
    P->>R: tag result: {tags: {act: "decision"}}
    R->>S: apply mutation: set_tags
    R->>R: all work complete → done
```

**What the caller sees**: the item ID and a flow ID. The caller
can ignore the flow ID (fire-and-forget) or use it later:

```python
result = keeper.put("Meeting notes...")
# result.item_id = "%a1b2c3"
# result.flow_id = "f-xyz"

# Later — inspect what happened:
flow = store.get_flow(result.flow_id)
# flow.status → "done"
# flow.state → full trace: mutations applied,
#   summary generated, tags assigned, work results
```

The work store (`work_store.py`) already records
every mutation, every work result, every state transition. The
`flow_id` is the existing handle to all of it. Nothing new needed.

The item itself also shows the results — `get` it later and it
has its summary, tags, etc. The flow trace is for when you want
to know *how* and *when* those were produced.

## 3) Query path: synchronous, caller blocks

Store queries are fast. The caller blocks while the runtime
searches, evaluates, and potentially re-searches.

```mermaid
sequenceDiagram
    participant C as Caller
    participant R as Runtime
    participant S as Store

    C->>R: continue(state: query-resolve, params: {query, thresholds})

    Note over R: tick 1 (sync)
    R->>S: find(query, limit: 10)
    S-->>R: {results, margin: 0.35, entropy: 0.2}
    R->>R: margin > margin_high → done

    R-->>C: {status: done, results: [...]}
```

One tick. All synchronous. No mutations, no async work — so
**no flow is persisted**. The runtime evaluates rules in memory
and returns the result. Zero writes to the flow database. This
must be indistinguishable in cost from calling `find()` directly.

Flow persistence is only needed when:
- Async work is dispatched (flow must survive between dispatch and result)
- Mutations are applied (trace has value)
- `stopped` suspends (flow must survive between suspension and resume)

### Multi-tick query (still synchronous)

```mermaid
sequenceDiagram
    participant C as Caller
    participant R as Runtime
    participant S as Store

    C->>R: continue(state: query-resolve, params: {query, thresholds})

    Note over R: tick 1 (sync)
    R->>S: find(query, limit: 10)
    S-->>R: {results, margin: 0.05, entropy: 0.8}
    R->>R: ambiguous → query-branch

    Note over R: tick 2 (sync)
    par
        R->>S: find(query, tags: {topic: "auth"}, limit: 5)
    and
        R->>S: find(query, tags: {topic: "crypto"}, limit: 5)
    and
        R->>S: find(query, limit: 15)
    end
    S-->>R: pivot1.margin > threshold → done

    R-->>C: {status: done, results: [...]}
```

Two ticks, but still synchronous. Every action is a store query.
The caller blocks for maybe 50ms total. This is transparent —
the caller doesn't know it took multiple searches.

## 4) Mixing sync and async

Store queries are sync (milliseconds). Provider calls are async
(seconds). **Async actions dispatch immediately** — the flow
continues evaluating subsequent rules without blocking.

| Action | Sync/Async | Why |
|--------|-----------|-----|
| `find` | sync | Store query |
| `get` | sync | Store lookup |
| `put` | sync | Store write |
| `summarize` | async | LLM provider call |
| `tag` | async | LLM provider call |
| `ocr` | async | OCR provider call |
| `analyze` | async | LLM provider call |
| `generate` | async | LLM provider call |

**`match: all`**: All matching rules dispatch their actions.
Async actions run concurrently in the background. The `post:`
block runs only after all actions complete (the runtime
suspends with `stopped` reason `"background"` while waiting).

**`match: sequence`**: Rules evaluate top-to-bottom. Each
action completes before the next rule is evaluated, because
the next rule may reference the previous action's output by
`id:`. Sync actions (store queries) complete inline. Async
actions (provider calls) cause the flow to suspend with
`stopped` reason `"background"` — the flow resumes from the
same rule position when the result arrives.

In both strategies, the flow returns `stopped` (reason:
`"background"`) when it has dispatched async work and has
nothing left to do synchronously. The caller polls or waits
for notification, then resumes with the cursor.

### Reflection flow: sync and async mixed

```mermaid
sequenceDiagram
    participant A as Agent
    participant R as Runtime
    participant S as Store
    participant W as Background Worker
    participant P as Provider (LLM)

    A->>R: continue(state: review-recent, params: {since: PT1H})

    Note over R: tick 1 — gather (sync)
    R->>S: find(since: PT1H, limit: 50)
    S-->>R: recent_notes
    R->>S: find(tags: {act: commitment}, since: PT1H)
    S-->>R: recent_actions
    R->>S: find(tags: {act: commitment, status: open})
    S-->>R: open_commitments

    Note over R: tick 2 — assess (async)
    R->>R: dispatch work: generate(assessment)
    R-->>A: {status: stopped, reason: "background"}

    Note over A: Agent is free — not blocking

    W->>P: generate(system: "evaluate skillfulness", user: ...)
    P-->>W: assessment {topic_areas: [...]}
    W->>R: report work result

    Note over R: tick 3 — resume, route to decision
    R->>R: transition → review-decide
    R->>R: no more sync work → stopped
    R-->>A: {status: stopped, reason: "awaiting_guidance", results: assessment}

    Note over A: Agent reads assessment
    Note over A: Agent decides to reflect deeper

    A->>R: resume(cursor, state: review-deep-reflect, params: {topics: [...]})

    Note over R: tick 4 — deep reflect (async)
    R->>S: find(query: "search-quality", since: PT1H)
    S-->>R: topic_notes
    R->>R: dispatch work: generate(reflection)
    R-->>A: {status: stopped, reason: "background"}

    W->>P: generate(system: "reflect deeply", user: ...)
    P-->>W: deep_reflection
    W->>R: report work result

    Note over R: tick 5 — persist (sync)
    R->>S: put(content: reflection, tags: {type: learning})
    S-->>R: {id: "%abc123"}
    R-->>A: {status: done, id: "%abc123"}
```

### What the agent sees, in order:

1. **`stopped`** (reason: `"background"`) — async work dispatched,
   nothing left to do synchronously. Agent is free.
2. **`stopped`** (on check) — still processing (the LLM assessment
   hasn't completed yet).
3. **`stopped`** (reason: `"awaiting_guidance"`) — the assessment
   is ready, results attached. This is the first time the agent
   sees content.
4. **`stopped`** (reason: `"background"`) — the deep reflection is
   being generated.
5. **`done`** — the learning was persisted.

The agent's interaction is: start → poll/wait → receive results +
decide next step → poll/wait → receive result. Every non-terminal
response is `stopped` — the reason tells the agent why.

## 5) The three caller experiences

```
┌─────────────────────────────────────────────────────────┐
│  Fire-and-forget (write path)                           │
│                                                         │
│  Caller: put("content") → gets ID immediately           │
│  Flow: runs entirely in background                      │
│  Caller never polls, never waits                        │
│  Item gains summary/tags asynchronously                 │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Synchronous resolve (query path)                       │
│                                                         │
│  Caller: find("query") → blocks → gets results          │
│  Flow: all store queries, runs in ~50ms                 │
│  Caller sees one response: done + results               │
│  Indistinguishable from a direct find() call            │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Interactive (scheduled/agent flows)                    │
│                                                         │
│  Caller: continue() → stopped → resume → done           │
│  Flow: mixes sync queries + async LLM calls             │
│  Caller polls for status between async steps            │
│  Caller steers with state/params on resume              │
│  Multiple round-trips possible                          │
└─────────────────────────────────────────────────────────┘
```

## 6) Where does decisionmaking live?

The diagrams reveal three layers:

```
┌──────────────────────────────────────────────┐
│  Policy (caller)                             │
│  - thresholds, budgets, preferences          │
│  - "what counts as good enough"              │
│  - injected via params.*                     │
└──────────────┬───────────────────────────────┘
               │
┌──────────────▼───────────────────────────────┐
│  Structure (state doc)                       │
│  - what to do, in what order                 │
│  - when to search, when to stop              │
│  - when to ask the caller                    │
│  - expressed as rules                        │
└──────────────┬───────────────────────────────┘
               │
┌──────────────▼───────────────────────────────┐
│  Judgment (caller, at stopped)               │
│  - substantive choices                       │
│  - "I want this one" / "dig deeper here"     │
│  - requires understanding the context        │
└──────────────────────────────────────────────┘
```

**Policy** is pre-set. The caller says "margin_high is 0.18" before
the flow starts. Policy doesn't require interaction during the flow.

**Structure** is the state doc. It decides the sequence of actions,
the routing logic, when to transition. Editable but stable — changes
between flows, not during them.

**Judgment** happens when the flow returns `stopped`. The caller
receives partial results and context, then decides how to proceed —
resume to a new state, adjust params, or accept what's there.
Everything else is automated.

The design question for any flow: **which decisions are Policy
(settable in advance), which are Structure (expressible as rules),
and which require Judgment (interactive)?**

## 7) What the caller sees: the surface contract

Every `continue()` call returns a status. Flows that dispatch
async work, apply mutations, or suspend with `stopped` are
**persisted** — the `flow_id` in the response is the handle
to the full execution trace (mutations, work results, state
transitions) stored in `work_store`.

Flows that complete synchronously with no mutations and no
async work (e.g. a pure-query `match: sequence` that returns
`done` in one tick) are **ephemeral** — they run entirely in
memory with zero writes to the flow database (see §3). The
response still includes a `flow_id` for API consistency, but
nothing is stored. This keeps sync queries indistinguishable
in cost from calling `find()` directly.

```python
# Resolved
{"status": "done", "flow_id": "...", "results": [...]}

# Paused — flow cannot proceed without caller
{"status": "stopped", "flow_id": "...", "cursor": "...", "reason": "...", "results": {...}}

# Failed
{"status": "error", "flow_id": "...", "error": "..."}
```

The `reason` field on `stopped` tells the caller why the flow paused:

| Reason | Meaning |
|--------|---------|
| `"background"` | Async work dispatched, nothing left to do synchronously |
| `"budget"` | Tick budget exhausted |
| `"ambiguous"` | Multiple candidates, no clear winner |
| (other) | State doc can set any reason string |

The caller's code:

```python
result = continue(input)

while result.status == "stopped":
    if result.reason == "background":
        # poll, or wait for notification
        result = continue({"cursor": result.cursor})
    else:
        # caller decides what to do next
        result = continue({
            "cursor": result.cursor,
            "state": next_state,       # optional: redirect
            "params": extra_params,    # optional: adjust
            "budget": {"ticks": 5},    # optional: extend
        })

if result.status == "done":
    use(result)
elif result.status == "error":
    handle(result.error)
```

Three statuses, one resume pattern. This is the entire
caller-side contract.

## 8) Parallel mutation ordering

`match: all` dispatches independent actions that run concurrently.
When multiple actions produce mutations targeting the same item,
the ordering is non-deterministic. This is by design — `match: all`
is for independent tasks that don't interfere.

**Known interaction**: OCR changes item content while summarize and
tag read content. If all three fire for the same item:

- OCR extracts text and updates the item's content.
- Summarize and tag read content at dispatch time, before OCR
  completes.
- Result: summary and tags reflect pre-OCR content.

**Mitigation**: this is the correct behavior for the current
after-write flow. OCR augments content with extracted text; the
initial summary and tags are based on what the user actually wrote.
A second pass (re-summarize/re-tag after OCR) is a separate flow
that can be triggered by the OCR action's completion, not by
imposing ordering constraints on `match: all`.

If ordering matters, use `match: sequence` — rules execute in
order, each action completes before the next rule is evaluated.

## 9) Open questions

- [x] Should `stopped` with reason `"background"` support push
      notifications (webhooks, SSE) instead of requiring polling?
      **No.** Polling is sufficient. The caller resumes with `cursor`.
- [ ] Audit trail: can the caller inspect what happened after?
