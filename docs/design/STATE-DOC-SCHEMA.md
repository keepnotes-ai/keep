# State Doc Schema

Date: 2026-03-07
Status: Draft
Related:
- `docs/design/STATE-ACTIONS.md`
- `docs/FLOWS.md`

## 1) What this is for

State docs orchestrate **memory operations** — storing, retrieving,
enriching, and relating items in the keep store. This is not a
general-purpose automation or workflow system. Every action in the
vocabulary operates on the store: writing items, searching for items,
summarizing content, applying tags, extracting text, traversing
relationships. The scope is memory-centric by design.

keep has two main paths: **write** (put content in) and **query**
(get content out). Both paths involve processing steps that may need
to run in sequence, in parallel, or conditionally:

- **Write**: after storing an item, optionally summarize it, extract
  OCR text, apply tags, run analysis — depending on the content.
- **Query**: find matching items, evaluate whether the results are
  good enough, optionally re-search with different parameters,
  decide when to stop.

Today this logic is hardcoded in Python (`flow_engine.py`,
`flow_policy.py`). The goal is to move it into **state docs**:
editable documents in the keep store that define what processing
happens and under what conditions.

## 2) How a flow starts

A user calls `keep put`:

```python
keeper.put("Meeting notes: we decided to ship v2 by Friday", tags={"topic": "v2"})
```

`put` stores the item, then starts a flow to process it:

```python
continue({
    "state": "after-write",
    "params": {
        "item_id": "%a1b2c3d4e5f6",
        "max_summary_length": config.max_summary_length,  # default 2000
    },
    "budget": {"ticks": 5},
})
```

The runtime resolves `state` to `.state/after-write`, loads it, and
evaluates its rules. The rules inspect the item's properties and
decide what processing to do — summarize if the content is long
enough, tag if it's not a system note, OCR if the item has images.
The caller passes only the item ID and config thresholds; the state
doc determines what runs.

**Constants come from the caller, not the state doc.** Thresholds,
limits, and other tunable values are injected via `params.*` — the
caller reads them from config and passes them in. State docs
reference `params.max_summary_length`, never a literal `2000`.
This means users change behavior via config without forking state
docs. The state doc defines *structure* (what to do, in what order);
`params.*` carries *policy* (threshold values, limits, feature
flags).

A query works the same way. A user calls `keep find`:

```python
keeper.find("authentication patterns", limit=10)
```

Under the hood, `find` starts a flow:

```python
continue({
    "state": "query-resolve",
    "params": {
        "query": "authentication patterns",
        "limit": 10,
        # All thresholds from config — never hardcoded in state docs
        "margin_high": config.flow.margin_high,      # default 0.18
        "entropy_low": config.flow.entropy_low,      # default 0.45
        "margin_low": config.flow.margin_low,        # default 0.08
        "entropy_high": config.flow.entropy_high,    # default 0.72
    },
    "budget": {"ticks": 5},
})
```

The state doc searches, evaluates the results, and either returns
them (if there's a clear winner) or refines the search. The
thresholds come from the caller's config, not the state doc.

In both cases, the existing API surface doesn't change. `put` and
`find` work exactly as before — the flow runs behind
them.

Note: the current code has a `goal` field ("write", "query") used as
a routing label. In this schema, `state` replaces that role. The term
"goal" is reserved for future use as **conditions of satisfaction**:
a predicate the runtime can evaluate to verify that the flow's result
actually satisfies what was asked.

## 3) What a state doc contains

A state doc is a keep note (id like `.state/query-resolve`) with a
body that defines **rules** and a **match strategy**.

```yaml
# .state/query-resolve
# match: sequence
rules:
  - id: search
    do: find
    with: { query: "{params.query}", limit: 5 }
  - when: "search.margin > params.margin_high"
    return: done
  - when: "search.entropy < params.entropy_low"
    do: find
    with: { query: "{params.query}", tags: "{search.top_facet_tags(1)}", limit: 5 }
    then: query-resolve
  - when: "search.entropy >= params.entropy_high"
    then: query-broaden
  - return: stopped
```

### Rules

A rule has up to four parts (at least one required):

```
[id: name]  [when: predicate]  →  [do: action, with: params]  →  [then: state | return: terminal]
```

- **`id:`** — optional name for this rule's output. If present, the
  output of `do:` is addressable as `{id}.*` in subsequent rules
  within the same state doc. If absent, output is fire-and-forget.
- **`when:`** — a predicate that must be true for this rule to fire.
  Omit for unconditional rules.
- **`do:`** — an action to execute (see STATE-ACTIONS.md). The action
  runs, and its output is bound to the rule's `id` (if present).
- **`then:`** — transition to another state doc. The runtime loads
  that doc and continues from there. Data crosses state boundaries
  only via explicit `with:` on the transition (see §8).
- **`return:`** — end the flow with a terminal status.

### Match strategy

The match strategy controls how rules are evaluated. It's set per
state doc, not per rule:

**`match: sequence`** (default) — rules run top-to-bottom in order.
Each action completes before the next rule is evaluated. Named outputs
(`id:`) accumulate and are available to all subsequent rules. This is
the common case for query flows where each step depends on a previous
result. It also handles switch/case routing: a sequence of
`when:`/`then:` rules with no `do:` acts as a dispatch table.

**`match: all`** — all rules whose `when:` matches fire, potentially
in parallel. Use this for write-path processing where summarize, tag,
and ocr are independent of each other. Each rule's `id` binds its
output. A `post:` block runs after all actions complete (see below).

## 4) Walk-through: write path

After a user calls `keep put "some content"`, the runtime starts a
flow at `.state/after-write` (see
[BUILTIN-STATE-DOCS.md §2](BUILTIN-STATE-DOCS.md#2-write-path-after-write)
for the full state doc).

The runtime:

1. Loads `.state/after-write` and sees `match: all`.
2. Evaluates each rule's `when:` predicate against the item.
3. All matching rules fire — say the item is long text with no
   images, so `summarize` and `tag` fire but `ocr` does not.
4. Both actions run (possibly in parallel). Each produces an output
   bound to its `id`.
5. After all complete, the `post:` block runs: `return: done`.
6. The flow ends. The runtime applied any mutations (set_summary,
   set_tags) that the actions returned.

No transitions, no looping — the write path is typically one state.

## 5) Walk-through: query path

A user calls `keep find "authentication patterns"`. The runtime
starts a flow at `.state/query-resolve` (see
[BUILTIN-STATE-DOCS.md §3](BUILTIN-STATE-DOCS.md#3-query-path-get)
for the full state docs).

The runtime:

1. Loads `.state/query-resolve`, sees `match: sequence`.
2. Rule 1: `do: find` runs unconditionally. Returns a result set
   bound to `search` — `{results: [...], margin: 0.22, entropy: 0.3}`.
3. Rule 2: `search.margin > params.margin_high` evaluates to true
   (0.22 > 0.18). `return: done`.
4. The flow ends with the result.

But if the results were ambiguous (margin 0.05, entropy 0.8):

3. Rule 2: false (margin too low).
4. Rules 3-5: conditional routing based on lineage, entropy, and
   margin signals. Each routes to a specialized state doc
   (`query-branch`, `query-explore`) or re-searches with narrower
   tags and loops back.
5. Budget check after every tick prevents runaway.

Note: the thresholds (`params.margin_high`, `params.entropy_low`)
come from the **caller**, not from the state doc. The state doc
defines the flow structure; the caller supplies the policy.

## 6) Terminal states

Three terminal states. These are the only ways a flow ends.

| State | Meaning | Caller action |
|-------|---------|---------------|
| `done` | Resolved | Read result |
| `error` | Unrecoverable failure | Inspect, retry or abandon |
| `stopped` | Paused — waiting for caller | Resume with direction |

`stopped` unifies several situations: budget exhaustion, ambiguous
results needing caller guidance, and background work dispatched with
nothing left to do synchronously. The response includes:

- `cursor` — resume point
- `results` — partial results so far (if any)
- `reason` — why it stopped (`"budget"`, `"ambiguous"`, `"background"`, etc.)

The caller resumes a stopped flow by providing direction:

```python
continue({"cursor": result.cursor, "state": "query-resolve", "params": {"limit": 20}})
```

The caller controls what happens next — they can redirect to any
state, adjust params, increase budget, or simply accept the partial
results. There is no structured decision protocol; the caller takes
over and steers.

Budget exhaustion is enforced by the runtime after every action. The
state doc doesn't need to check budget explicitly (though it can via
`budget.remaining`).

## 7) Output binding

Every `do:` produces an output dict. A rule's `id` controls whether
and how the output is addressable. There is one binding mechanism:
**`id`**. No implicit `prev`, no separate `as:`.

**Within a state doc**: a rule with `id: name` makes its output
available as `name.*` in all subsequent rules. Rules without `id`
produce fire-and-forget output.

```yaml
# match: sequence
rules:
  - id: initial
    do: find
    with: { query: "{params.query}" }
  - id: broader
    do: find
    with: { query: "{params.query}", limit: 20 }
  - when: "broader.margin > params.margin_high || initial.margin < 0.05"
    return: done
```

In `match: sequence`, named outputs accumulate top-to-bottom. In
`match: all`, named outputs from all parallel rules are available
in the `post:` block:

```yaml
# match: all
rules:
  - when: "..."
    id: summary
    do: summarize
  - when: "..."
    id: extracted
    do: ocr
post:
  - when: "!summary"
    return: error
  - return: done
```

If no `post:` block, default is `return: done`.

**Across state transitions**: named outputs do NOT carry across.
Each state doc is encapsulated — it depends only on `params.*` and
its own computations. To pass data across a transition, name it
explicitly in `with:` (see §8).

## 8) Transitions

`then:` moves to another state doc by id:

```yaml
then: query-broaden               # simple — only params.* carries over
then:
  state: query-broaden            # with explicit data passing
  with:
    facets: "{search.top_facet_tags(1)}"
    depth: 2
```

Transition `with:` values merge into `params.*` for the destination
state. The caller's original params persist; transition params add to
or override them. Named outputs (`id:` bindings) from the current
state do NOT carry across — only explicit `with:` values cross state
boundaries.

This makes each state doc self-contained: its interface is `params.*`.
You can read any state doc in isolation and understand it completely.

Self-transitions (`then: query-resolve` from within `query-resolve`)
are how loops work — each iteration starts fresh, runs its own rules,
produces its own named outputs. Data from a prior iteration only
persists if explicitly passed via `with:`.

## 8b) Return payload contract

`return: done` ends the flow successfully. The caller receives:

```python
{"status": "done", "results": <payload>, ...}
```

**Default payload**: when `return: done` has no `with:` block,
the runtime collects results from all named action outputs
(`id:` bindings) and returns them as the `results` dict. Each
key is the rule's `id`, each value is the action's output.

```yaml
# return: done without with: → results = {search: {...}, tags: {...}}
rules:
  - id: search
    do: find
    with: { query: "..." }
  - id: tags
    do: tag
post:
  - return: done   # results = {"search": search_output, "tags": tags_output}
```

**Explicit payload**: when `return: done` includes a `with:` block,
the `with:` values become the results. Template resolution applies.

```yaml
  - return:
      status: done
      with:
        results: "{search.results}"
        count: "{size(search.results)}"
```

**`return: error`** sets `status: "error"`. The `with.reason` value
(if present) becomes the error message. No results are returned.

**`return: stopped`** sets `status: "stopped"`. The `with.reason`
value determines the stop reason. Partial results from completed
actions are included.

## 9) Predicate language

CEL-like expressions. Safe, side-effect-free, guaranteed to terminate.

### Namespaces

| Namespace | Contents | Example |
|-----------|----------|---------|
| `item.*` | Current item | `item.content_length`, `item.has_summary` |
| `{id}.*` | Output of rule with that id | `search.margin`, `search.entropy` |
| `params.*` | Caller/transition params | `params.margin_high`, `params.query` |
| `budget.*` | Budget state | `budget.remaining > 0` |
| `flow.*` | Flow state | `flow.tick`, `flow.state` |
| `tags.*` | Item tags | `tags.topic`, `tags.status` |

Named outputs (`{id}.*`) are scoped to the current state doc. They
do not leak across transitions.

### Operators and functions

```
==  !=  <  >  <=  >=  &&  ||  !

item.has_tag("topic")        # tag existence
item.content_length          # numeric property
search.top(N)                # top N results (where search is a rule id)
search.margin                # score gap #1 vs #2
size(search.results)         # collection length
best_of(a, b, ...)           # merge result sets, return top items by score
```

Predicates don't mutate state, don't loop, don't define functions.
Plugins can register new predicate functions (read-only).

## 10) Actions

See **[STATE-ACTIONS.md](STATE-ACTIONS.md)** for the full catalog.

Key points:
- Actions take params + read-only context, return an output dict.
- **Actions read, the runtime writes.** Mutations are returned as
  instructions; the runtime applies them within its transaction pipeline.
- Auto-discovered from `keep/actions/` via `@action` decorator.
- The vocabulary is small (find, get, summarize, tag, ocr, analyze,
  generate, put). Variation comes from documents, not code.

## 11) State doc format

A state doc is a keep note addressed by id:

```markdown
---
tags:
  category: system
  context: state
---
# match: sequence
rules:
  - id: search
    do: find
    with: { query: "{params.query}", limit: 5 }
  - when: "search.margin > params.margin_high"
    return: done
```

Tags are metadata only — they carry no runtime semantics. The id
(`.state/query-resolve`) is the address.

Users can edit, add, disable (`status: disabled`), or fork state docs.
System defaults ship as `category: system` notes.

## 12) Runtime execution model

```
continue(input) ->
  1. Load state doc (from input.state or cursor)
  2. Read match strategy (sequence | all)
  3. Evaluate rules per strategy
  4. On flow control:
     then:   load destination state doc, goto 2
     return: end flow, return result
  5. If no rule matched: return stopped
  6. Budget check after every action
```

Each pass through 2-4 is one **tick**, recorded for auditability.
A `match: all` block counts as one tick regardless of how many rules
fire. Budget counts ticks only — provider cost control belongs in
config, not flow budgets.

## 13) Reusable code from the draft branch

The draft branch (`feat/continuation-store-foundation`) is throwaway.
Code worth reusing:

- `work_store.py` — flow/work/mutation persistence
- `flow_env.py` — environment adapter pattern
- Wire contract shape (`continue(input) -> output`)
- Optimistic concurrency, idempotency, mutation dedup primitives
