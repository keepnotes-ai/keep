# Use-Case Simulations with the Simplified Continuation Framework

Date: 2026-03-03
Related:
- `later/simplified-model-and-continuation-design.md`
- `later/continuation-api-spec.md`

## Baseline Runtime (Simplified)

Across all use-cases, the process runs on:
- Model: `Node + Fact`
- Frame request shape: `seed -> pipeline(where/hop/slice/rank/group/project) -> budget`
- State shape:
  - `objective`
  - `frame_request`
  - `cursor`
  - `frontier`
  - `pending` (proposed writes)
  - `policy` (pushdown + escalation gates)
  - `budget`

Interaction requests only:
- `decision: disambiguate`
- `decision: choose`
- `decision: approve_intentional_write`
- `decision: stop_or_continue`

---

## Use-Case 1: Answer a Focused Question

## High-level goal
"What are the open commitments for project `myapp`, and what should I do next?"

## Expression to process

```yaml
objective:
  task: answer
  question: "open commitments for myapp"
  success_criteria:
    required_evidence: ["at least 3 commitment nodes", "status=open"]
seed: { mode: id, value: now }
pipeline:
  - { op: where, args: { facts: ["project=myapp", "act=commitment", "status=open"] } }
  - { op: rank, args: { recency: 0.5, semantic: 0.5 } }
  - { op: project, args: { sections: ["summary", "open_loops"] } }
budget: { tokens: 900, max_nodes: 60 }
```

## Iteration trace
1. Engine pushdown returns 8 nodes; 5 clearly relevant.
2. Agent sees ambiguity: 2 nodes are stale/renegotiated semantics.
3. Decision request `choose` asks whether `status=renegotiated` belongs in open loops.
4. Agent chooses "exclude".
5. Frame regenerated with 3 items + next-action draft.
6. Agent issues `stop_or_continue=stop`.

## Outcome
- Clear answer + actionable next step list.
- No writes required.

## Simplification observed
- Initially added `group(by=topic)`; removed because it obscured urgency ordering.
- For this goal, `where + rank + project` is enough.

---

## Use-Case 2: Investigative Entity Thread

## High-level goal
"What did Deborah recently say about rollout risk, and how did that change over time?"

## Expression to process

```yaml
objective:
  task: investigate
  question: "Deborah statements about rollout risk over time"
seed: { mode: query, value: "Deborah rollout risk" }
pipeline:
  - { op: hop, args: { keys: ["speaker", "said"], direction: both, depth: 2 } }
  - { op: where, args: { facts: ["topic=rollout", "topic=risk"] } }
  - { op: slice, args: { include_flavors: ["version", "part"], radius: 1 } }
  - { op: rank, args: { lexical: 0.3, semantic: 0.4, recency: 0.3 } }
  - { op: project, args: { sections: ["evidence", "timeline"] } }
budget: { tokens: 1800, max_nodes: 120 }
```

## Iteration trace
1. Engine returns mixed `Deborah` entity hits (person node vs unrelated title usage).
2. Decision request `disambiguate` requests target entity choice.
3. Agent selects canonical person entity.
4. Engine recomputes timeline frame.
5. Agent detects one contradictory note and asks for deeper slice (`radius=2`).
6. Re-run yields coherent sequence with confidence above threshold.

## Outcome
- Threaded evidence with version/part windows.
- Contradictions explicit, not hidden.

## Simplification observed
- Removed branch/fork machinery in this case; single disambiguation decision request was sufficient.
- "Fork by hypothesis" was overkill here.

---

## Use-Case 3: Memorialize Findings from a New Document

## High-level goal
"Read a new architecture doc, capture durable findings, and connect them to existing work."

## Expression to process

```yaml
objective:
  task: memorialize
  question: "capture reusable findings from source doc"
seed: { mode: id, value: "file:///.../new-arch-doc.md" }
pipeline:
  - { op: slice, args: { include_flavors: ["part"] } }
  - { op: rank, args: { semantic: 0.6, recency: 0.4 } }
  - { op: project, args: { sections: ["evidence", "candidate_findings"] } }
budget: { tokens: 1600, max_nodes: 80 }
```

## Iteration trace
1. Engine provides high-signal parts and neighboring nodes.
2. Agent drafts 4 proposed writes in `pending`:
   - 2 learnings
   - 1 decision note
   - 1 open commitment
3. Decision request `approve_intentional_write` invoked for each proposed write (with linked evidence IDs).
4. Agent rejects 1 low-evidence write, edits tags on 2.
5. `commit` applies accepted writes transactionally.
6. Reproject confirms new nodes surface in `open_loops` and `learnings`.

## Outcome
- Memorialization done with evidence-backed writes.
- One-step verification after commit.

## Simplification observed
- Dropped a custom "write-confidence score" operator.
- Simpler rule worked better: every write must cite evidence node IDs.

---

## Use-Case 4: Reflective Session Close

## High-level goal
"Before ending session, update commitments, capture one learning, and reduce open loops."

## Expression to process

```yaml
objective:
  task: review
  question: "close loops and update now"
seed: { mode: id, value: now }
pipeline:
  - { op: where, args: { facts: ["status=open"] } }
  - { op: rank, args: { recency: 0.7, semantic: 0.3 } }
  - { op: project, args: { sections: ["open_loops", "learnings", "next_actions"] } }
budget: { tokens: 700, max_nodes: 40 }
```

## Iteration trace
1. Engine returns open loops and related learning nodes.
2. Agent marks 2 commitments fulfilled, 1 remains open.
3. Agent proposes 1 new learning note from today’s breakdown.
4. Decision request `approve_intentional_write` confirms updates.
5. Commit runs.
6. Reframed output shows reduced open-loop count.

## Outcome
- Session closes with updated state and explicit carry-forward.

## Simplification observed
- No graph hops needed; local scope around `now` was enough.
- Kept runtime minimal: `where -> rank -> project -> commit`.

---

## Use-Case 5: Broad Orientation (When Starting Unclear Work)

## High-level goal
"I am not sure what matters yet; orient me to relevant context quickly."

## Expression to process

```yaml
objective:
  task: orient
  question: "relevant context for current prompt"
seed: { mode: query, value: "<user prompt>" }
pipeline:
  - { op: rank, args: { semantic: 0.5, lexical: 0.2, recency: 0.3 } }
  - { op: group, args: { by: ["project", "topic"] } }
  - { op: project, args: { sections: ["summary", "entities", "open_loops"] } }
budget: { tokens: 1200, max_nodes: 100 }
```

## Iteration trace
1. Engine returns broad mixed set.
2. Agent handles `choose` decision request: pick one project lane to dive into.
3. Continuation rewrites request with `where(project=chosen)` and reduced budget.
4. Second step yields focused context.
5. Agent continues into answer/investigate flow.

## Outcome
- Fast funnel from broad orientation into specific work.

## Simplification observed
- Removed explicit `fork/join`; a single "choose lane then narrow" loop was easier and cheaper.

---

## Cross-Case Lessons ("Where It Got Complex")

1. Too many operators create orchestration drag.
   - Keep canonical operators small: `where/hop/slice/rank/project + commit`.
2. Branching is expensive cognitively.
   - Prefer decision-guided narrowing before full `fork/join`.
3. Write quality gates should be simple.
   - "Evidence-linked writes only" outperformed elaborate scoring.
4. Flow state should stay lean.
   - If a field is not used by at least two goal types, remove it.

## Final Reduced Carrier (After Simulations)

```yaml
State:
  objective: {}
  frame_request: {}
  cursor: { step: 0, engine_cursor: null }
  frontier: { nodes: [], hypotheses: [] }
  pending: { writes: [] }
  budget: {}
  requests: { work: [] }
```

This reduced form still supports all five goals above.

## Quick Presentation Matrix

When a high-level goal is presented, the process should visibly transform it in one or two steps:

| Goal class | Initial expression quality | First process behavior | If complexity rises | Final stabilized form |
|---|---|---|---|---|
| Focused answer | Usually high | Push down `where+rank+project` | Remove grouping and branch paths | Ranked evidence + next actions |
| Investigation | Medium, often ambiguous | Early entity disambiguation decision | Increase local slice radius only | Timeline with explicit contradictions |
| Memorialization | Medium, evidence-heavy | Extract candidate findings + proposed writes | Reject low-evidence writes | Transactional writes with citations |
| Session close | High and local | Pull open loops near `now` | Drop hops and global context | Updated commitments + carry-forward |
| Orientation | Low and broad | Broad ranking + group by lane | Choose one lane, narrow budget | Focused continuation into answer/investigate |

This matrix is the practical test: if a goal cannot be reduced this way, the continuation/request surface is still too complex.
