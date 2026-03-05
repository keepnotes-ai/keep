# Template + Frame/State Contract (Concrete Expression)

Date: 2026-03-03
Status: Draft (Proposed Canonical Addendum)
Related:
- `later/continue-wire-contract-v1.md`
- `later/continuation-api-spec.md`
- `later/simplified-model-and-continuation-design.md`
- `later/continuation-machine-architecture.md`

## 1) Objective

Keep template-driven prompts, but move all retrieval/projection semantics into Frame/State runtime.

Design rule:
- Templates/profiles declare *goal and process skeleton*.
- Frame/State runtime performs *all query, projection, ranking, and continuation*.

No template-level direct retrieval execution.
No template-level eval/loop interpreter.

## 2) Single Runtime Boundary

```text
continue(input) -> output
```

Every operation is modeled as a flow tick, including:
- interactive prompt rendering
- read/query workflows
- write workflows (including async OCR/summarize/analyze)

## 3) Core Wire Types

## 3.1 ContinueInput

```json
{
  "flow_id": "optional",
  "template_ref": ".prompt/agent/query",
  "params": {
    "text": "What are open commitments for myapp?",
    "id": "now",
    "since": "P14D",
    "until": null,
    "tags": {"project": "myapp"}
  },
  "frame_request": null,
  "state": null,
  "feedback": {
    "work_results": []
  }
}
```

Notes:
- `template_ref` is optional for non-prompt flows.
- `frame_request` is optional if template/profile provides it.
- If both are provided, explicit `frame_request` overlays template bindings.

## 3.2 ContinueOutput

```json
{
  "flow_id": "f_123",
  "status": "running|waiting_work|done|failed",
  "frame": {
    "slots": {
      "question": "What are open commitments for myapp?",
      "answer_context": "...rendered evidence block...",
      "now_context": "...rendered item context..."
    },
    "views": {
      "task": "...",
      "evidence": [],
      "hygiene": []
    }
  },
  "rendered": {
    "template_ref": ".prompt/agent/query",
    "text": "Question: ...\nContext: ..."
  },
  "requests": {
    "work": []
  },
  "applied": {
    "work_ops": []
  },
  "state": {
    "cursor": {"step": 0, "stage": "explore", "phase": "explore"},
    "frontier": {}
  }
}
```

## 3.3 TemplateDoc (Retrieval + Render)

Stored as system documents (e.g. `.prompt/agent/*`).

```yaml
id: .prompt/agent/query
kind: template
version: 1
bindings:
  question:
    from: params.text
  answer_context:
    frame_request:
      seed: { mode: query, value: "${params.text}" }
      pipeline:
        - op: where
          args: { facts: ["${params.tags.*}"] }
        - op: slice
          args: { limit: 8 }
      budget: { tokens: 3000, max_nodes: 160 }
render: |
  Question: {{question}}

  Context:
  {{answer_context}}

  Answer the question as precisely as possible.
```

Key property:
- `bindings.*.frame_request` is declarative data, executed by Frame engine only.

## 3.4 ProcessProfile (Stage Skeleton)

Process control is declared as data and evaluated by kernel logic.

```yaml
id: .profile/ingest.put
kind: process_profile
version: 1
stages:
  - name: classify
    when: {"param_true": "run_classify"}
    emits_work: classify.document
  - name: transform
    emits_work: transform.document
  - name: reconcile
    terminal: true
```

Rules:
1. `when` is a bounded predicate language evaluated by kernel (no eval).
2. Stage transitions and preconditions are kernel-owned.
3. Stage declarations are optional in phase-1 implementation; kernel defaults are used when absent.
## 3.5 State

Durable continuation carrier.

```yaml
flow_id: f_123
objective:
  kind: answer
  success_tests:
    - "sufficient evidence for answer"
cursor:
  step: 4
  stage: explore
  phase: explore
frontier:
  active_hypotheses: [h1]
  open_conflicts: []
pending:
  proposed_writes: []
  outstanding_work: []
budget:
  tokens_left: 1200
termination:
  idle_ticks: 0
  max_idle_ticks: 3
```

## 4) Execution Semantics

Given `continue(input)`:

1. Resolve process profile + current stage.
2. Resolve template (if any).
3. Compile effective frame program from:
   - template bindings
   - explicit `frame_request` overlay
   - current `state` cursor/frontier
4. Execute frame program in query engine.
5. Produce `frame.slots` + `frame.views`.
6. Run scheduler:
   - apply safe housekeeping
   - emit work/decision requests
   - advance stage
   - update `state`
7. Render template against `frame.slots` (if template flow).
8. Return `frame`, `rendered`, `requests`, `state`.

All retrieval/projection happens in step 4, not in template rendering.

## 5) Compatibility with Existing Prompt Placeholders

Legacy placeholders are compiled into bindings:

- `{get}` -> slot binding with frame request profile `context:get(id=params.id|now)`
- `{find}` -> slot binding with frame request profile `search(query=params.text)`
- `{find:deep:3000}` -> same with `deep=true`, `budget.tokens=3000`
- `{text}` -> slot binding `from: params.text`
- `{since}` / `{until}` -> slot binding `from: params.since|until`

This preserves current prompt docs while relocating query semantics to frame engine.

## 6) Concrete Profiles for Existing Primitives

## 6.1 `keep prompt query`

- Template: `.prompt/agent/query`
- Flow kind: `prompt_render`
- Frame source: template bindings + params (`text`, `tags`, time filters)
- Output: `rendered.text` and updated `state`

## 6.2 `keep now`

Modeled as two profiles:

- `now.read`
  - frame request: seed=`id(now)`; project sections `item, similar, meta, edges, prev/next`
- `now.write`
  - mutation request: upsert `now` content/tags
  - post-write frame request: same as `now.read`

Both run through `continue`, so read and write share the same state/termination controls.

## 6.3 `keep put` of PDF

Modeled as ingest continuation:

- Tick A (`ingest.fetch_extract`): extract text-layer pages, identify OCR pages, upsert placeholder/initial summary.
- Tick B (`ingest.plan_work`): enqueue work contracts:
  - `ocr` for missing pages
  - `summarize` for long content
- Tick C+ (`ingest.reconcile`): apply work results, update summary/hash/embedding, reproject frame.

This preserves current behavior while expressing it as continuation state transitions.

Simple inline `keep put "text"`:
- profile `write.put` may complete in a single tick (no emitted work).
- multi-tick ingest is only required when heavy transforms are needed.

Optional prefix stage:
- set `params.run_classify=true`
- kernel emits `classify.document` before `transform.document`
- classification outputs can apply any caller-defined tag keys.

Execution boundary by profile:
- `local_only`: Tick B writes to local `pending_summaries`; local daemon executes C+.
- `hybrid_delegate`: Tick B still writes local pending records; daemon may delegate `ocr/summarize/analyze` and reconcile returned results.
- `remote_only`: `put` and ingest continuation execute on hosted API; hosted queue/worker containers own Tick B/C+.

## 7) Minimal New API Surface

Expose only:

1. `continue(input)`
2. `get_flow(flow_id)` (optional convenience read)
3. `cancel_flow(flow_id)` (optional control)

Everything else is represented as templates, frame profiles, and continuation state.

## 8) Why This Is Clean

- Template remains user-editable and metaschema-friendly.
- Query/projection logic is centralized and typed.
- Agent/sub-agent/provider work is unified as runtime `requests.work`.
- Same machinery handles prompt rendering, retrieval iteration, and background ingest.
