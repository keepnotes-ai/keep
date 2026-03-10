# Continuations

## The problem with one-shot search

You've stored hundreds or thousands of notes — decisions, commitments, project context, meeting notes. Now you need to find something:

```bash
keep find "what authentication approach should we use?"
```

You get five results. Two are clearly relevant, one is noise, and two more *might* matter — but you can't tell without reading them. So you refine:

```bash
keep find "OAuth2 vs API keys security tradeoffs"
```

Better results. But now you've lost the first set. You open another search, read a related note, notice it links to a decision from last month, and chase that thread too.

This is what real research looks like. It's never one query. You start broad, look at what you found, realize what you actually need, and narrow down. Sometimes you branch sideways. Sometimes you need the system to do background work — summarize a long document, tag something for later, extract text from a PDF.

Continuations make this normal workflow a first-class operation.

## One call, many steps

Instead of juggling separate `get`, `find`, and `put` commands, continuations give you a single operation:

```
continue(input) -> output
```

The first call starts a **flow** — you describe what you're looking for, and keep returns what it found. If the results are clear, you're done in one call. If they're ambiguous, the runtime refines automatically — re-searching with different parameters, branching into parallel queries, narrowing by tags. Each step is a **tick**, and the flow ends when it finds a clear answer or runs out of budget.

```python
result = continue({
    "state": "query-resolve",
    "params": {
        "query": "authentication approach",
        "limit": 10,
    },
    "budget": {"ticks": 5},
})

# Most queries resolve on the first tick
# result.status == "done", result.results == [...]
```

For the common case — a query with a clear top result — this is indistinguishable from calling `find()` directly. One tick, one search, done. The continuation machinery only kicks in when the results are ambiguous.

## Why not just let the agent loop?

You could build this yourself. Call `find`, read the results, decide what to search next, call `find` again. Many agents do exactly this.

But there are three reasons to let the runtime handle it:

**Each step is a chance to intervene.** A continuation doesn't run to completion in the dark — it can pause after any tick and hand control back. The agent (or the human, or a cheaper model) can inspect what was found, decide whether to refine or branch, add constraints, or stop early. The loop is collaborative, not autonomous.

**The runtime handles the bookkeeping.** At each tick, the system computes signals you'd otherwise hand-code: how confident is the top result? Are results clustering around one topic or scattered? Should we narrow or branch? These aren't magic — they're small statistical measures — but they're tedious to reimplement in every agent. And because the runtime tracks what's already been tried, it can avoid redundant queries.

**The logic is editable.** The processing logic lives in **state docs** — documents in the keep store that define what happens and under what conditions. Want to change how ambiguous results are handled? Edit the state doc. Want to add a custom analysis step after tagging? Add a rule. The behavior is data, not code.

## What the runtime figures out for you

At each tick, the runtime looks at the results and computes a handful of practical signals:

- **Margin** — how much better is the top result than the second? High margin means a clear winner; low margin means ambiguity.
- **Entropy** — are results clustering around one topic or scattered across many? Low entropy means concentrated; high entropy means diverse.
- **Lineage** — are results mostly versions or parts of the same document? Strong lineage suggests narrowing to that document family.

From these signals, the state doc routes to one of several strategies:

| Situation | What happens |
|-----------|-------------|
| Clear winner (high margin) | Done — return the results |
| Strong lineage | Re-search constrained to that document family |
| Ambiguous (low margin, high entropy) | Branch — parallel searches with different tag filters |
| Concentrated but no winner | Narrow — re-search with top facet tags |
| Mixed signals | Explore — broaden the search and try again |

All thresholds come from config, not from the state docs themselves. You tune behavior by adjusting `margin_high`, `entropy_low`, etc. — the state docs define the structure, your config defines the policy.

## Background work

When you store a new item, keep can automatically summarize it, tag it, and extract text — all in the background. This is also a continuation:

```python
result = keeper.put("Meeting notes: we decided to ship v2 by Friday")
# result.item_id = "%a1b2c3"
# result.flow_id = "f-xyz"  (optional — for tracing what happened)
```

The caller gets the item ID back immediately. Behind the scenes, the runtime starts a flow that evaluates the item and runs whatever processing applies:

- **Summarize** — if the content is long enough and no summary exists yet
- **Tag** — for all non-system items, using tag specs (`.tag/*`) in the store
- **OCR** — if the item has images needing text extraction

What runs is determined by the state doc (`.state/after-write`), not by caller flags. Users customize processing by editing the state doc — adding rules, removing them, or changing conditions.

## State docs

The processing logic for every flow lives in a **state doc** — a keep note with an ID like `.state/query-resolve` that contains rules.

```yaml
# .state/query-resolve
# match: sequence
rules:
  - id: search
    do: find
    with: { query: "{params.query}", limit: 5 }
  - when: "search.margin > params.margin_high"
    return: done
  - when: "search.entropy >= params.entropy_high"
    then: query-branch
  - return: stopped
```

Rules have up to four parts: a **condition** (`when:`), an **action** (`do:`), a **transition** (`then:`), or a **terminal** (`return:`). The runtime evaluates them top-to-bottom.

State docs ship as system defaults but are fully editable. Fork one to customize how your queries resolve, what processing runs after a put, or how context is assembled for display.

See `docs/design/` for the full specification.

## Three outcomes

Every flow ends in one of three states:

| Status | Meaning | What to do |
|--------|---------|------------|
| `done` | Complete | Read the results |
| `error` | Failed | Inspect and retry or abandon |
| `stopped` | Paused | Resume with direction, or accept partial results |

`stopped` covers several situations: budget exhausted, ambiguous results needing guidance, or background work dispatched. The response includes a `reason` and a `cursor` for resuming:

```python
result = continue(input)

while result.status == "stopped":
    if result.reason == "background":
        # async work in progress — poll later
        result = continue({"cursor": result.cursor})
    else:
        # caller decides what to do next
        result = continue({
            "cursor": result.cursor,
            "state": "query-resolve",    # redirect if desired
            "params": {"limit": 20},     # adjust params
            "budget": {"ticks": 5},      # extend budget
        })

if result.status == "done":
    use(result)
elif result.status == "error":
    handle(result.error)
```

## Compatibility with existing API

The existing `keep get`, `keep find`, and `keep put` commands work exactly as before. Continuations run behind them — a simple `find` with a clear result resolves in one tick with zero overhead. The continuation machinery is invisible unless results are ambiguous or background processing is needed.

## See also

- [API-SCHEMA.md](API-SCHEMA.md) — General keep API reference
- [AGENT-GUIDE.md](AGENT-GUIDE.md) — Working session patterns
- [KEEP-MCP.md](KEEP-MCP.md) — MCP server setup
- [docs/design/](design/) — State doc schema, actions, and built-in state docs
