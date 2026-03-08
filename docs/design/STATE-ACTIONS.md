# State Actions

Date: 2026-03-07
Status: Draft
Related:
- `docs/design/STATE-DOC-SCHEMA.md`
- `keep/continuation_executor.py` (current runner system)
- `keep/continuation_env.py` (current env protocol)

## 1) What is an action?

An action is a callable registered by name. State docs invoke actions
via `do: <name>`. The runtime calls the action, passing params and
context, and receives an output dict.

```python
class Action(Protocol):
    def run(self, params: dict, context: ActionContext) -> dict: ...
```

## 2) Store access: actions read, the runtime writes

Actions receive **read access** to the store through `ActionContext`.
Actions **never write directly**. They return an output dict; the
runtime decides what mutations to apply based on the output.

This split exists because:
- The runtime manages transactions, concurrency, and mutation dedup
- Mutations are audited and replayable
- Actions are easier to test (input → output, no side effects)

### ActionContext (read-only surface)

```python
class ActionContext(Protocol):
    def get(self, id: str) -> Item | None: ...
    def find(self, query: str | None = None, *, tags: dict | None = None,
             similar_to: str | None = None, limit: int = 10,
             since: str | None = None, until: str | None = None,
             include_hidden: bool = False) -> list[Item]: ...
    def list_items(self, *, prefix: str | None = None, tags: dict | None = None,
                   since: str | None = None, until: str | None = None,
                   order_by: str = "updated", include_hidden: bool = False,
                   limit: int = 10) -> list[Item]: ...
    def get_document(self, id: str) -> Any | None: ...
    def resolve_meta(self, id: str, limit_per_doc: int = 3) -> dict[str, list[Item]]: ...
    def resolve_provider(self, kind: str, name: str | None = None) -> Any: ...
```

This replaces `ContinuationRuntimeEnv` as the surface actions see.
Read-only: no `put_item`, no `upsert_item`, no `enqueue_task` —
mutations go through the output dict. Methods and signatures are
defined by this protocol, not inherited from the current env. The
`find` action internally decides whether to call `context.find()`
(search mode) or `context.list_items()` (tag-only listing mode).
The `resolve_meta` action uses `context.resolve_meta()` for
meta-doc query resolution.

### Mutation output convention

Actions that want to mutate items return mutation instructions in
their output dict. The runtime interprets and applies them:

```python
# Action returns:
{
    "summary": "A concise summary of the document",
    "mutations": [
        {"op": "set_summary", "summary": "$output.summary"}
    ]
}
```

The `mutations` list uses the same typed ops the current engine already
supports: `upsert_item`, `set_tags`, `set_summary`, `put_item`.

**Reference resolution**: the runtime resolves `$output.*` references
against the action's output dict and `$input.*` against the work input.
For item-scoped actions (summarize, tag, ocr, analyze), `target` is
omitted — the runtime fills it from the flow's current item. Only
cross-item mutations (like `put_item`) need explicit targets.

Actions that don't mutate (pure queries, analysis) just return their
output without a `mutations` key.

## 3) Action catalog

Each action lists its parameters, what it reads, what it outputs,
and what mutations it returns. Parameters come from the rule's
`with:` block. Item-scoped actions (summarize, tag, ocr, analyze)
implicitly operate on the flow's current item — they receive item
content via the runtime, not via params.

### find

Search the store or list items by tags. All searching and
re-searching uses `find` with different parameters — there is no
separate "refine" action.

```yaml
# Search mode — requires query or similar_to
- do: find
  with:
    query: "search terms"
    tags: { topic: "X" }
    limit: 10
    since: "P7D"

# List mode — tag-only or prefix, no search query
- do: find
  with:
    tags: { act: "commitment", status: "open" }
    limit: 20
    order_by: updated

# Prefix mode — list items by ID prefix
- do: find
  with:
    prefix: "my-doc@P"
    limit: 50
```

**Parameters** (all optional, but at least one of `query`,
`similar_to`, `tags`, `prefix`, or `since` required):

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `query` | str | — | Search query text (semantic + FTS) |
| `tags` | dict | — | Tag filter — only items matching all tags |
| `similar_to` | str | — | Item ID to find similar items to |
| `prefix` | str | — | ID prefix filter (e.g. `"doc@P"` for parts) |
| `limit` | int | 10 | Maximum results |
| `since` | str | — | ISO duration (`P7D`) or date — items updated since |
| `until` | str | — | ISO duration or date — items updated before |
| `include_hidden` | bool | false | Include system notes (dot-prefix IDs) |
| `order_by` | str | — | Sort order for list mode: `updated`, `accessed`, `created` |

When `query` or `similar_to` is provided, wraps `Keeper.find()` —
hybrid search with RRF scoring. Otherwise wraps
`Keeper.list_items()` — structured listing with optional ordering.
`prefix` maps to `list_items(prefix=...)`.

`similar_to` and `query` are mutually exclusive.

**Output** (from the action):

```python
{
    "results": [
        {"id": "%a1b2c3", "summary": "...", "tags": {"topic": "auth"}, "score": 0.87},
        {"id": "%d4e5f6", "summary": "...", "tags": {"topic": "auth"}, "score": 0.71},
        ...
    ],
    "count": N,
}
```

Each result item has `id`, `summary`, `tags`, and `score` (float,
0–1, higher is better; `None` in list mode). In search mode, results
are ordered by RRF score. In list mode, by `order_by`.

The **runtime** then computes statistics from the result set and
attaches them to the output available via `{id}.*` (see §4). So a
rule with `id: search` gives access to both `search.results` (the
items) and `search.margin`, `search.entropy`, etc. (the statistics).

### get

Retrieve a specific item by id.

```yaml
- do: get
  with:
    id: "item-id"
```

**Parameters**:

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `id` | str | required | Item identifier (normalized before lookup) |

**Output**:

```python
# Found:
{"id": "%abc123", "summary": "...", "tags": {"topic": "auth"}}

# Not found:
{}
```

If found, the output contains the item's `id`, `summary`, and `tags`.
If not found, the output is empty. Predicates check for presence:
`when: "target.id"` (found) or `when: "!target.id"` (not found).

**Implementation**: wraps `Keeper.get()`. Simple ID lookup.

### traverse

Follow edges from a set of items to discover related items. This
is the primitive behind "deep" mode — extracted from `find(deep=true)`
so that edge-following behavior is customizable via state docs.

```yaml
- id: related
  do: traverse
  with:
    items: "{search.results}"
    limit: 5
```

**Parameters**:

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `items` | list | required | Items to follow edges from (list of IDs or result items) |
| `limit` | int | 5 | Max related items per source |

For each source item, finds items linked via `_inverse` tag
relationships. If no edges exist in the store, falls back to
grouping by shared tag facets.

**Output**:

```python
{
    "groups": {
        "%a1b2c3": [{"id": "%x1", "summary": "...", "tags": {...}}, ...],
        "%d4e5f6": [{"id": "%x2", "summary": "...", "tags": {...}}, ...],
    },
    "count": N,
}
```

`groups` maps each source item ID to its related items, ordered by
recency (most recently updated first). `count` is the total number
of related items across all groups. Items appearing in the `items`
input set are excluded from results (dedup against primaries).
Within each group, results are capped at `limit`.

**Implementation**: wraps the edge-following logic currently in
`Keeper._deep_follow_edges()` and `_deep_follow_tags()`.
Current code: `api.py:3100-3350`.

### resolve_meta

Resolve meta-document definitions against a target item. Meta-docs
(`.meta/*` items) define tag-based queries; this action evaluates
those queries against the target item's tags and returns matching
items grouped by meta-doc name.

```yaml
- id: meta
  do: resolve_meta
  with:
    item_id: "{params.item_id}"
    limit: 3
```

**Parameters**:

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `item_id` | str | required | Item to resolve meta-docs for |
| `limit` | int | 3 | Max items per meta-doc section |

The action finds all `.meta/*` documents, parses their tag-query
definitions, checks prerequisites against the target item's tags,
and runs matching queries. Results are grouped by meta-doc name.

**Output**:

```python
{
    "sections": {
        "learnings": [{"id": "%x1", "summary": "...", "tags": {...}}, ...],
        "todo": [{"id": "%x2", "summary": "...", "tags": {...}}, ...],
    },
    "count": N,
}
```

`sections` maps each meta-doc name to items matching its queries.
Empty sections (no matches) are omitted.

**Implementation**: wraps `Keeper.resolve_meta(id, limit_per_doc)`.
Current code: `api.py:4739`.

### summarize

Generate a summary for the flow's current item.

```yaml
- do: summarize

# With optional params:
- do: summarize
  with:
    max_length: 500
    context: "related context for better summary"
```

**Parameters**:

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `max_length` | int | 500 | Target max summary length in characters (hint, not hard cap — LLM output may vary) |
| `context` | str | — | Related context for contextual summarization |

Content comes from the flow's current item (`item.*`), not from
params. The action reads the item content and sends it to the
summarization provider.

**Output**: `{summary: "..."}`
**Mutations**: `[{op: "set_summary", summary: "$output.summary"}]`

**Implementation**: wraps `SummarizationProvider.summarize(content,
max_length, context)`. Provider selection uses the configured default.
Current code: `continuation_executor.py:180`.

### tag

Classify the flow's current item against tag specs from the store.

```yaml
- do: tag
```

No parameters. Content comes from the flow's current item. The
action loads all `.tag/*` spec docs from the store, builds
classification prompts from their definitions, and classifies the
item against each taxonomy. Results are merged into the item's tags.

What gets tagged is determined by which `.tag/*` specs exist:
- `.tag/act/*` — speech acts (commitment, request, assertion, ...)
- `.tag/status/*` — lifecycle status (open, fulfilled, withdrawn, ...)
- `.tag/topic` — topic extraction
- `.tag/type` — content type classification

Users add new tagging vocabularies by creating spec docs (e.g.
`.tag/sentiment/*`), remove tagging by deleting specs, and customize
existing tagging by editing spec doc content. The action discovers
specs at runtime — no code changes needed.

**Output**: `{tags: {k: v, ...}}`
**Mutations**: `[{op: "set_tags", tags: "$output.tags"}]`

**Implementation**: wraps `TagClassifier` — loads specs via
`TagClassifier.load_specs()`, classifies via
`TagClassifier.classify()`. This is the spec-driven classification
system (currently in `analyzers.py:522`), not the hardcoded
`TaggingProvider.tag()` prompt.
Current code: `analyzers.py:522`.

### ocr

Extract text from the flow's current item (images or scanned PDF
pages).

```yaml
- do: ocr
  with:
    pages: [1, 3, 5]
```

**Parameters**:

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `pages` | list[int] | — | Zero-indexed page numbers to OCR (PDF only) |

The item's URI provides the file path. The item's `_content_type`
tag determines whether to use image OCR or PDF OCR. For PDFs, the
`_ocr_pages` tag (set during indexing) identifies which pages need
OCR; `pages` overrides this if provided.

**Output**: `{text: "extracted...", pages_processed: N}`
**Mutations**: `[{op: "upsert_item", content: "$output.text"}]`
— replaces item content with OCR'd text. If the extracted text is
long enough to need summarization, the after-write state doc's
summarize rule handles that — OCR itself is purely extraction.

**Implementation**: wraps `ocr_image()` / `ocr_pdf()` in
`processors.py`. Current code: `processors.py:115`.

### analyze

Decompose the flow's current item into parts using an analysis
provider.

```yaml
- do: analyze
  with:
    guide_context: "optional decomposition guidance"
```

**Parameters**:

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `guide_context` | str | — | Tag descriptions for guided decomposition |

Content comes from the flow's current item. The analyzer chunks the
content and decomposes each chunk. Analysis prompts are loaded
automatically from `.prompt/analyze/*` docs if they exist. Tag specs
from `.tag/*` are used for classification of generated parts.

**Output**:

```python
{
    "parts": [
        {"summary": "...", "content": "...", "tags": {...}, "part_num": 1},
        ...
    ],
    "mutations": [
        {
            "op": "put_item",
            "id": "{target}@p1",       # runtime resolves {target}
            "content": "...",
            "tags": {"_base_id": "{target}", "_part_num": "1", ...},
        },
        ...
    ]
}
```

Part IDs follow the existing `{base_id}@p{N}` convention. The
`_base_id` and `_part_num` tags enable lineage tracking and
part-to-parent resolution. `{target}` in mutations is resolved by
the runtime to the flow's current item ID.

`parts` is available as data for downstream predicates
(`analyze.parts`). The `put_item` mutations store each part via
the normal mutation pipeline.

**Implementation**: wraps `AnalyzerProvider.analyze(chunks,
guide_context)`, then optionally `TagClassifier.classify()` for each
part. Current code: `analyzers.py:200`, `providers/base.py:420`.

### generate

Raw LLM prompt — system + user message.

```yaml
- do: generate
  with:
    system: "You are a classifier."
    user: "{item.content}"
    max_tokens: 4096
    format: json
```

**Parameters**:

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `system` | str | `""` | System prompt |
| `user` | str | `""` | User prompt |
| `max_tokens` | int | 4096 | Maximum tokens in response |
| `format` | str | — | If `"json"`, parse response into structured fields |

Unlike item-scoped actions, `generate` takes its input entirely
from params. Use template references (`{item.content}`,
`{search.results}`) to inject data from the flow context.

**Output**: `{text: "..."}` — raw LLM response. `text` is always
present regardless of format.

With `format: json`, the response is parsed and its fields are
merged into the output dict (e.g. `{text: "...", topic_areas:
[...]}`). If parsing fails, only `text` is present — no error,
no parsed fields. Parsed fields never override `text`.

This is the escape hatch for custom processing not covered by
other actions.

**Implementation**: wraps `SummarizationProvider.generate(system,
user, max_tokens)`. Current code: `continuation_executor.py:231`.

### put

Create a new item in the store.

```yaml
- do: put
  with:
    content: "{report.text}"
    tags:
      type: review
      topic: commitments
```

**Parameters**:

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `content` | str | — | Inline text content (one of `content` or `uri` required) |
| `uri` | str | — | URI to index (one of `content` or `uri` required) |
| `id` | str | — | Custom item ID (auto-generated if omitted) |
| `tags` | dict | — | Tags for the new item |
| `summary` | str | — | Pre-computed summary (skips summarization) |

**Output**: `{}`
**Mutations**: `[{op: "put_item", content: "...", tags: {...}, ...}]`

The `put` action is pure — it returns a `put_item` mutation. The
runtime applies it, generating the item ID. The created item goes
through normal `after-write` processing as a separate flow with
its own budget. Budget-based termination prevents runaway chains.

As an additional guard, the runtime tracks flow depth. A put inside
a flow creates a child flow at depth N+1. Flows beyond a
configurable max depth (default: 3) skip `after-write` processing.

**Implementation**: the action constructs the mutation dict from
params. The runtime applies it via the mutation pipeline (same
path as `Keeper.put()`).

## 4) Result statistics

Query actions (`find`) return raw result sets. The **runtime**
computes statistics from the results and attaches them to the
evaluation context:

| Statistic | Type | Meaning | Source in current code |
|-----------|------|---------|----------------------|
| `results` | list | The result items | raw result set |
| `count` | int | Total result count | `len(results)` |
| `margin` | float | Score gap between #1 and #2 | `query_stats.top1_top2_margin` |
| `entropy` | float | Score distribution spread (high = scattered) | `query_stats.lane_entropy` |
| `lineage_strong` | float | Version/part concentration | `lineage.dominant_concentration_topk` |
| `dominant_lineage_tags` | dict | Tags from dominant lineage item | runtime: looks up `lineage.dominant` ID, reads its tags |
| `top_facet_tags(N)` | dict | Tag constraint from Nth facet key | runtime: `tag_profile.facet_keys[N-1]` → tag dict |
| `top(N)` | func | Top N results | slice of results |

These are available as `{id}.*` on the rule that invoked `find`
(e.g. `search.margin` if the rule has `id: search`). The action
itself doesn't compute them — they're properties of any result set.

This separation means any call to `find` automatically gets statistics
without the action implementing them. **List-mode queries** (tag-only,
no scores) do not produce score-based statistics: `margin`, `entropy`,
and `lineage_strong` are `None`. The predicate engine treats `None`
as falsy in boolean context and returns `false` for any comparison
(`<`, `>`, `==` to a number) against `None`. This means
`search.margin > 0.1` is `false` when `margin` is `None`.

## 5) Auto-discovery

Actions live in `keep/actions/`. Classes decorated with `@action`
are auto-discovered and registered under the snake_case form of the
class name (e.g. `ResolveMeta` → `resolve_meta`):

```
keep/actions/
    __init__.py       # @action decorator, auto-discovery loader
    find.py           # Find
    get.py            # Get
    traverse.py       # Traverse (edge-following)
    resolve_meta.py   # ResolveMeta (meta-doc resolution)
    summarize.py      # Summarize
    tag.py            # Tag
    ocr.py            # Ocr
    analyze.py        # Analyze
    generate.py       # Generate
    put.py            # Put
```

To add a new action, drop a file:

```python
# keep/actions/transcribe.py
from keep.actions import action

@action
class Transcribe:
    def run(self, params, context):
        audio = context.get_document(params["uri"])
        text = self.do_transcription(audio)
        return {"text": text}
```

## 6) Mapping from current code

| Current | New |
|---------|-----|
| `_run_provider_summarize` | `actions/summarize.py` |
| `_run_provider_tag` / `TagClassifier` | `actions/tag.py` |
| `_run_provider_generate_json` | `actions/generate.py` |
| `_run_echo` | removed (use `generate` or inline) |
| `_run_local_task` | removed (actions replace task dispatch) |
| `_deep_edge_follow` / `_deep_tag_follow` | `actions/traverse.py` |
| `resolve_meta` | `actions/resolve_meta.py` |
| `_resolve_summarization_provider` | `context.resolve_provider("summarization")` |
| `_resolve_tagging_provider` | `context.resolve_provider("tagging")` |
| `_resolve_analyzer_provider` | `context.resolve_provider("analyzer")` |
| `ContinuationExecutorRegistry` | auto-discovery in `actions/__init__.py` |
| `ContinuationRuntimeEnv` (full) | `ActionContext` (read-only subset) |
| Engine mutation pipeline | unchanged — applies mutations from action output |

## 7) Open questions

- [x] Should `analyze` mutations (storing parts) be in the action or
      the runtime? **Action returns `put_item` mutations.** Each part
      becomes a mutation; the existing pipeline stores them. `parts`
      is also in the output as data for downstream predicates.
- [x] Budget accounting per action: **ticks only.** Each rule
      evaluation is one tick; a `match: all` block counts as one tick.
      Simple, predictable, extensible later. Provider cost control
      belongs in config (rate limits, model selection), not flow budgets.
- [x] Provider selection: **config default, no override via `with:`.**
      Provider names are deployment-specific; putting them in state docs
      makes docs non-portable. Per-action provider config already exists
      in keep config. Non-breaking to add `with: provider` later if
      needed.
- [x] Error handling: actions raise exceptions on failure. The runtime
      catches them. In `match: all`, a failed action's `id` binding is
      left unset — predicates check `when: "!summary"` to detect
      failure. In `match: sequence`, an uncaught failure terminates
      the flow with `error` status. No `status` field in action output.
- [x] `find` with tag-only queries: current `Keeper.find()` requires
      `query` or `similar_to`. The `find` action routes tag-only
      queries to `Keeper.list_items()` with `order_by`. This is a new
      routing layer in the action, not a change to the Keeper API.
