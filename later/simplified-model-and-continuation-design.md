# Simplified Logical Model and Continuation Design

Date: 2026-03-03
Related:
- `later/continuation-api-spec.md`
- `later/continuation-machine-architecture.md`

## 1) Simplified Logical Model (Clear Core)

## Principle

Treat memory as **nodes + facts**.

- A note, version, part, tag-doc, prompt-doc, meta-doc, and entity node are all just `Node`.
- Tags are all just `Fact` assertions.
- Some facts are edge-like because their value is an ID and the key is configured as relational.

## Minimal Entities

```mermaid
erDiagram
    NODE ||--o{ FACT : has
    NODE ||--o{ FACT : is_value_node_for

    NODE {
      string id PK
      string summary
      json attrs
      datetime created_at
      datetime updated_at
      datetime accessed_at
    }

    FACT {
      string subject_id FK
      string key
      string value
      string value_type   // scalar|id
      string scope        // head|snapshot
      datetime asserted_at
      string source       // user|system|derived
    }
```

## Semantics in this core

- **Tags**: `Fact(subject, key, value)`.
- **Edge tags**: same fact, but `value_type=id` and key has relational semantics (`_inverse` config).
- **Versions**: nodes with flavor facts like `type=version`, `of=<base-id>`, `ordinal=<n>`.
- **Parts**: nodes with flavor facts like `type=part`, `of=<base-id>`, `ordinal=<n>`.
- **URI/source linkage**: facts like `source_id=<uri-id>`, `source_kind=uri|inline`; still IDs.
- **System docs** (`.tag/*`, `.meta/*`, `.prompt/*`): just nodes with conventional IDs and facts.

## Optional normalization

Scalar values may remain inline. Frequently reused values can be modeled as value nodes (`Node(id="tag:key:value")`) with `value_type=id` facts.

## Clear outcome

All special cases become interpretation rules over one representation, not separate ontologies.

## 2) Where Implementation Has Accumulated Complexity

Current implementation is intentionally denormalized for speed and UX:

1. Separate storage shapes:
   - `documents`, `document_versions`, `document_parts`
   - `edges`, `version_edges`
2. Tags stored as JSON blobs + casefold marker metadata for vector filtering.
3. Separate FTS indexes for documents/parts/versions.
4. Dual stores (SQLite canonical + Chroma vector index) with hydration/merging.
5. Uplift logic (part/version hit -> parent with `_focus_*` tags) for display ergonomics.
6. Materialized inverse edges and backfill bookkeeping.
7. Remote/local capability skew (`RemoteKeeper.get_context` currently lacks parts list).

This is mostly practical complexity, not conceptual complexity.

## 3) Simplified Operator Surface (From the Core Model)

Once everything is Node+Fact, the operator set can be small:

1. `seed(...)` — define start nodes.
2. `where(...)` — fact predicates.
3. `hop(...)` — traverse relational facts (`value_type=id`) with direction/depth.
4. `slice(...)` — fact-based slicing (e.g., versions/parts via flavor facts).
5. `rank(...)` — semantic/lexical/recency/graph scoring.
6. `group(...)` — organize by hypothesis/entity/thread.
7. `project(...)` — produce named view sections.
8. `propose(...)` — stage candidate writes as facts/nodes.
9. `commit(...)` — transactional apply.

No operator needs to be "version-specific" or "part-specific"; those are slice presets.

## 4) Frame Request, Reframed

`FrameRequest` (formerly "projection request") is a graph program over Node+Fact:

```yaml
FrameRequest:
  seed:
    mode: "id|query|similar_to"
    value: "now"
  pipeline:
    - op: where
      args: { facts: ["project=myapp"] }
    - op: hop
      args: { keys: ["speaker", "said"], direction: both, depth: 2 }
    - op: slice
      args: { include_flavors: ["head", "version", "part"], radius: 2 }
    - op: rank
      args: { semantic: 0.45, lexical: 0.25, recency: 0.15, graph: 0.15 }
    - op: group
      args: { by: ["entity", "thread"] }
    - op: project
      args:
        sections: ["summary", "evidence", "timeline", "open_loops"]
  budget:
    tokens: 1800
    max_nodes: 120
```

This request is stable even if backend storage changes.

## 5) State Carrier, Simplified

Use a `State` object that carries just enough durable execution progress:

```yaml
State:
  flow_id: "flow_..."
  objective: {}
  frame_request: {}    # original FrameRequest
  cursor:
    step: 0
    stage: "explore"
    engine_cursor: null
  frontier:
    nodes: []
    evidence: []
    hypotheses: []
  pending:
    proposed_nodes: []
    proposed_facts: []
  program:
    goal: "query|summarize|write|..."
    profile: "optional process profile id"
  policy:
    # optional runtime snapshot, typically derived from profile/metaschema
    pushdown_allowed: ["where", "hop", "slice", "rank", "group", "project"]
    escalate_on: ["low_confidence", "conflict", "destructive_change"]
  budget: {}
```

Step contract:

```yaml
StepResult:
  frame: {}
  state: {}
  requests:
    work: []
```

## 6) Push-Down vs Agentic Evaluation (Cleaner Boundary)

## Push down
- operations with deterministic graph semantics:
  - `where`, `hop`, `slice`, bulk ranking, grouping, pagination

## Agentic
- operations requiring interpretation or commitment:
  - hypothesis choice
  - contradiction adjudication
  - memorialization acceptance (`propose` acceptance)
  - stop/continue judgment

## Design consequence

The continuation runtime is a controller around a pushdown-capable graph/query engine, not a second retrieval system.

## 7) "Shake Until Clear" Design Outcome

After simplification, the architecture reads as:

1. **Graph kernel**: Node+Fact.
2. **Operator algebra**: tiny pipeline over graph facts.
3. **State runtime**: loop + work/decision staging + commit staging.

Everything else (versions, parts, meta docs, edge tags, now/deep presets) is a profile over this core.

That is the clean representation to target in API design, while keeping denormalized storage internally for performance.
