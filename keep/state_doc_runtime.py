"""Synchronous state-doc flow runtime.

Evaluates state docs with inline action execution, handling transitions
between states and enforcing tick budgets.  This is the runtime for the
read/query path where flows run synchronously and return immediately.

The write path uses background task dispatch (work_queue.py);
this runtime is for flows that must complete before returning to the
caller: query resolution, context assembly, deep find.

Usage::

    result = run_flow(
        "query-resolve",
        params={"query": "auth patterns", "margin_high": 0.18, ...},
        budget=5,
        load_state_doc=engine._load_state_doc,
        run_action=action_runner,
    )
    if result.status == "done":
        return result.bindings
"""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol

from .result_stats import enrich_find_output
from .state_doc import AsyncActionEncountered, StateDoc, evaluate_state_doc

logger = logging.getLogger(__name__)


@dataclass
class FlowResult:
    """Result of a completed state-doc flow."""

    status: str  # "done", "error", "stopped", "async"
    bindings: dict[str, dict[str, Any]] = field(default_factory=dict)
    data: Optional[dict[str, Any]] = None  # return.with payload
    ticks: int = 0
    history: list[str] = field(default_factory=list)  # state names visited
    cursor: Optional[str] = None  # resumable cursor (set when stopped)
    tried_queries: list[str] = field(default_factory=list)  # queries attempted


@dataclass
class FlowCursor:
    """Decoded cursor — checkpoint state for resuming a stopped flow."""

    state: str  # state doc name to resume at
    ticks: int  # ticks consumed in previous invocations
    bindings: dict[str, dict[str, Any]]  # accumulated results
    tried_queries: list[str] = field(default_factory=list)  # queries attempted


def _slim_bindings(bindings: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Strip large fields from bindings before cursor storage.

    Keeps IDs, scores, and computed stats (margin, entropy, lineage,
    facets).  Drops summaries and full tag dicts from result items.
    """
    slimmed: dict[str, dict[str, Any]] = {}
    for key, binding in bindings.items():
        if not isinstance(binding, dict):
            slimmed[key] = binding
            continue
        slim = {}
        for field, value in binding.items():
            if field == "results" and isinstance(value, list):
                slim[field] = [
                    {"id": r.get("id"), "score": r.get("score")}
                    for r in value if isinstance(r, dict)
                ]
            else:
                slim[field] = value
        slimmed[key] = slim
    return slimmed


def encode_cursor(
    state: str,
    ticks: int,
    bindings: dict,
    tried_queries: list[str] | None = None,
) -> str:
    """Encode a flow checkpoint as a self-contained cursor token."""
    payload: dict[str, Any] = {
        "s": state,
        "t": ticks,
        "b": _slim_bindings(bindings),
    }
    if tried_queries:
        payload["q"] = tried_queries
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def decode_cursor(token: str) -> Optional[FlowCursor]:
    """Decode a cursor token, returning None if invalid."""
    token = (token or "").strip()
    if not token:
        return None
    try:
        padded = token + ("=" * (-len(token) % 4))
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
        state = str(payload.get("s") or "").strip()
        if not state:
            return None
        return FlowCursor(
            state=state,
            ticks=int(payload.get("t", 0)),
            bindings=payload.get("b") or {},
            tried_queries=payload.get("q") or [],
        )
    except Exception:
        return None


class StateDocLoader(Protocol):
    """Callable that loads a compiled StateDoc by name."""

    def __call__(self, name: str) -> Optional[StateDoc]: ...


class ActionRunner(Protocol):
    """Callable that executes a named action and returns its output."""

    def __call__(self, name: str, params: dict[str, Any]) -> dict[str, Any]: ...


def run_flow(
    initial_state: str,
    params: dict[str, Any],
    budget: int = 5,
    *,
    load_state_doc: StateDocLoader,
    run_action: ActionRunner,
    cursor: Optional[FlowCursor] = None,
    foreground: bool = True,
    should_stop: Callable[[], bool] | None = None,
) -> FlowResult:
    """Run a state-doc flow synchronously to completion.

    Args:
        initial_state: Name of the starting state doc (e.g. "query-resolve").
        params: Caller-supplied parameters (thresholds, query, limits).
        budget: Maximum ticks before forced stop (per invocation).
        load_state_doc: Callback to load a compiled StateDoc by name.
        run_action: Callback to execute an action and return its output.
        cursor: Optional cursor from a previous stopped flow to resume.
        foreground: If True (default), async actions trigger delegation
            to the work queue via cursor.  If False (daemon context),
            all actions execute inline.
        should_stop: Optional callback checked between flow ticks. When it
            returns True, the flow stops with a resumable cursor.

    Returns:
        FlowResult with terminal status, accumulated bindings, and
        optional return data. When status is "stopped" or "async",
        the cursor field contains a resumable token.
    """
    from .perf_stats import perf as _perf
    import time as _time
    _flow_t0 = _time.monotonic()

    # Resume from cursor or start fresh
    if cursor is not None:
        current_state = cursor.state
        prior_ticks = cursor.ticks
        accumulated_bindings: dict[str, dict[str, Any]] = dict(cursor.bindings)
        tried_queries: list[str] = list(cursor.tried_queries)
        logger.info("flow: resume %s (prior ticks: %d)", current_state, prior_ticks)
    else:
        current_state = initial_state
        prior_ticks = 0
        accumulated_bindings = {}
        tried_queries = []
        logger.info("flow: start %s", initial_state)

    current_params = dict(params)
    ticks = 0
    history: list[str] = []

    def _record_flow() -> None:
        _perf.record("flow", initial_state, _time.monotonic() - _flow_t0)

    def _stopped_flow(reason: str) -> FlowResult:
        total_ticks = prior_ticks + ticks
        cursor_token = encode_cursor(
            current_state, total_ticks, accumulated_bindings, tried_queries,
        )
        logger.info("flow: %s -> stopped (%s, %d ticks)", current_state, reason, total_ticks)
        _record_flow()
        return FlowResult(
            status="stopped",
            bindings=accumulated_bindings,
            data={"reason": reason},
            ticks=total_ticks,
            history=history,
            cursor=cursor_token,
            tried_queries=tried_queries,
        )

    # Wrap run_action to enrich find output with statistics and track timing.
    # In foreground mode, async actions raise AsyncActionEncountered to
    # delegate the remainder of the flow to the work queue.
    def _action_callback(action_name: str, action_params: dict[str, Any]) -> dict[str, Any]:
        if foreground:
            from .actions import is_async_action
            if is_async_action(action_name):
                raise AsyncActionEncountered(action_name, action_params)
        with _perf.timer("action", action_name):
            output = run_action(action_name, action_params)
        if action_name == "find" and isinstance(output, dict):
            output = enrich_find_output(output)
            q = action_params.get("query")
            if isinstance(q, str) and q and q not in tried_queries:
                tried_queries.append(q)
        return output

    while ticks < budget:
        if should_stop and should_stop():
            return _stopped_flow("shutdown")
        doc = load_state_doc(current_state)
        if doc is None:
            logger.info("flow: %s -> error (state doc not found)", current_state)
            _record_flow()
            return FlowResult(
                status="error",
                data={"reason": f"state doc not found: {current_state}"},
                ticks=prior_ticks + ticks,
                history=history,
            )

        history.append(current_state)
        ticks += 1

        # Build evaluation context
        eval_ctx = _build_eval_context(current_params, budget=budget, tick=ticks)

        # Evaluate state doc
        try:
            result = evaluate_state_doc(doc, eval_ctx, run_action=_action_callback)
        except AsyncActionEncountered as aa:
            # Foreground flow hit an async action — produce cursor for
            # the work queue to resume from.  The cursor points at the
            # current state doc; on resume the daemon re-evaluates from
            # the top (sync actions re-execute — they are idempotent
            # queries) and this time executes the async action inline.
            total_ticks = prior_ticks + ticks
            cursor_token = encode_cursor(
                current_state, total_ticks, accumulated_bindings, tried_queries,
            )
            logger.info(
                "flow: %s -> async (%s, %d ticks)",
                current_state, aa.action_name, total_ticks,
            )
            _record_flow()
            return FlowResult(
                status="async",
                bindings=accumulated_bindings,
                data={"reason": "async_action", "action": aa.action_name},
                ticks=total_ticks,
                history=history,
                cursor=cursor_token,
                tried_queries=tried_queries,
            )
        except Exception as exc:
            logger.warning("State doc %r evaluation failed: %s", current_state, exc)
            _record_flow()
            return FlowResult(
                status="error",
                data={"reason": f"evaluation failed: {exc}"},
                ticks=prior_ticks + ticks,
                history=history,
            )

        # Collect bindings (merge with accumulated from cursor)
        accumulated_bindings.update(result.bindings)

        # Terminal
        if result.terminal is not None:
            logger.info("flow: %s -> %s (%d ticks)", current_state, result.terminal, ticks)
            _record_flow()
            return FlowResult(
                status=result.terminal,
                bindings=accumulated_bindings,
                data=result.terminal_data,
                ticks=prior_ticks + ticks,
                history=history,
                tried_queries=tried_queries,
            )

        # Transition
        if result.transition is not None:
            next_state, transition_params = _parse_transition(result.transition)
            if next_state is None:
                logger.info("flow: %s -> error (invalid transition)", current_state)
                _record_flow()
                return FlowResult(
                    status="error",
                    data={"reason": f"invalid transition: {result.transition}"},
                    ticks=prior_ticks + ticks,
                    history=history,
                )
            logger.info("flow: %s -> %s", current_state, next_state)
            # Transition params merge into (override) current params
            current_params = dict(current_params)
            current_params.update(transition_params)
            current_state = next_state
            continue

        # No terminal, no transition — shouldn't happen (evaluator defaults to done)
        logger.info("flow: %s -> done (implicit, %d ticks)", current_state, ticks)
        _record_flow()
        return FlowResult(
            status="done",
            bindings=accumulated_bindings,
            ticks=prior_ticks + ticks,
            history=history,
            tried_queries=tried_queries,
        )

    # Budget exhausted — return cursor for resumption
    total_ticks = prior_ticks + ticks
    cursor_token = encode_cursor(current_state, total_ticks, accumulated_bindings, tried_queries)
    logger.info("flow: %s -> stopped (budget, %d ticks)", current_state, total_ticks)
    _record_flow()
    # Surface latest search results and signals in stopped data
    stopped_data: dict[str, Any] = {"reason": "budget"}
    for key in ("search", "pivot1", "bridge"):
        binding = accumulated_bindings.get(key)
        if isinstance(binding, dict) and "results" in binding:
            stopped_data["results"] = binding["results"]
            for sig in ("margin", "entropy"):
                if sig in binding:
                    stopped_data[sig] = binding[sig]
            break  # use the first available search binding
    return FlowResult(
        status="stopped",
        bindings=accumulated_bindings,
        data=stopped_data,
        ticks=total_ticks,
        history=history,
        cursor=cursor_token,
        tried_queries=tried_queries,
    )


def _build_eval_context(
    params: dict[str, Any],
    *,
    budget: int,
    tick: int,
) -> dict[str, Any]:
    """Build the evaluation context for a state doc tick.

    Populates ``params.*``, ``budget.*``, and ``flow.*`` namespaces.
    If params contains ``item`` or ``system`` dicts, they are promoted
    to top-level context keys so CEL predicates can use ``item.foo``
    instead of ``params.item.foo``.
    """
    ctx: dict[str, Any] = {
        "params": dict(params),
        "budget": {
            "total": budget,
            "remaining": max(budget - tick, 0),
        },
        "flow": {
            "tick": tick,
        },
    }
    # Promote well-known namespaces to top-level for CEL convenience.
    for key in ("item", "system"):
        val = params.get(key)
        if isinstance(val, dict):
            ctx[key] = val
    return ctx


def _parse_transition(
    transition: str | dict[str, Any],
) -> tuple[Optional[str], dict[str, Any]]:
    """Parse a transition value into (state_name, params).

    Supports both forms:
        ``then: "query-explore"``
        ``then: {state: "query-explore", with: {facets: ...}}``
    """
    if isinstance(transition, str):
        name = transition.strip()
        return (name or None, {})

    if isinstance(transition, dict):
        state = transition.get("state")
        if not isinstance(state, str) or not state.strip():
            return (None, {})
        with_params = transition.get("with")
        if isinstance(with_params, dict):
            return (state.strip(), dict(with_params))
        return (state.strip(), {})

    return (None, {})


# ---------------------------------------------------------------------------
# Factory helpers for wiring into the keep system
# ---------------------------------------------------------------------------

# Module-level cache for compiled builtin state docs (immutable, parse once).
_builtin_cache: dict[str, StateDoc] = {}


def _get_compiled_builtin(name: str, body: str) -> Optional[StateDoc]:
    """Return a cached compiled StateDoc for a builtin, or compile and cache it."""
    cached = _builtin_cache.get(name)
    if cached is not None:
        return cached
    from .state_doc import parse_state_doc
    try:
        doc = parse_state_doc(name, body)
        _builtin_cache[name] = doc
        return doc
    except (ValueError, RuntimeError) as exc:
        logger.warning("Failed to compile builtin state doc %r: %s", name, exc)
        return None


def make_state_doc_loader(
    env: Any,
) -> StateDocLoader:
    """Create a state doc loader backed by a FlowRuntimeEnv.

    Loads ``.state/{name}`` notes from the store, parses their summary
    field as YAML state doc body.  State docs are seeded into the store
    by system doc migration from bundled ``.md`` files.

    Falls back to compiled builtins (from ``builtin_state_docs.py``)
    when the store has no entry — this covers test environments that
    skip full migration, and the brief window before first migration
    on a fresh store.
    """
    from .state_doc import load_state_doc as _load_state_doc

    def _load(name: str) -> Optional[StateDoc]:
        bare_name = name.removeprefix(".state/")

        def _get_note(id: str):
            return env.get(id)

        def _list_children(prefix: str):
            return env.list_items(prefix=prefix, include_hidden=True, limit=50)

        return _load_state_doc(bare_name, get_note=_get_note, list_children=_list_children)

    return _load


def make_action_runner(
    env: Any,
    *,
    writable: bool = False,
    item_id: str | None = None,
    item_content: str | None = None,
    context_cache: Any | None = None,
) -> ActionRunner:
    """Create an action runner backed by a FlowRuntimeEnv.

    Args:
        env: FlowRuntimeEnv providing store operations.
        writable: If True, enable provider resolution for write actions
                  (summarize, tag, analyze). If False (default), provider
                  resolution raises NotImplementedError.
        item_id: Default item ID for item-scoped actions (used when
            the action's params don't specify ``item_id``).
        item_content: Full content text for the item (not stored in the
            document store, so must be passed explicitly for actions
            like summarize that need it).
        context_cache: Optional action-result cache that deduplicates
            repeated action calls within a single flow execution.
    """
    from .actions import get_action

    ctx = _EnvActionContext(
        env, writable=writable, item_id=item_id, item_content=item_content,
    )

    def _run(action_name: str, params: dict[str, Any]) -> dict[str, Any]:
        from .tracing import get_tracer
        _tracer = get_tracer("flow")
        with _tracer.start_as_current_span(f"action:{action_name}") as _span:
            if context_cache is not None:
                cached = context_cache.check(action_name, params, ctx)
                if cached is not None:
                    _span.set_attribute("cache", "hit")
                    return cached
                _span.set_attribute("cache", "miss")
            act = get_action(action_name)
            output = act.run(params, ctx)
            result = dict(output) if isinstance(output, dict) else {}
            if context_cache is not None:
                context_cache.store(action_name, params, result)
            return result

    return _run


class _EnvActionContext:
    """ActionContext backed by a FlowRuntimeEnv."""

    def __init__(
        self,
        env: Any,
        *,
        writable: bool = False,
        item_id: str | None = None,
        item_content: str | None = None,
    ) -> None:
        self._env = env
        self._writable = writable
        self.item_id = item_id
        self.item_content = item_content

    def get(self, id: str) -> Any:
        return self._env.get(id)

    def peek(self, id: str) -> Any:
        """Read without updating accessed_at (for cache hydration)."""
        peek_fn = getattr(self._env, "peek", None)
        return peek_fn(id) if peek_fn else self._env.get(id)

    def find(
        self,
        query: str | None = None,
        *,
        tags: dict[str, Any] | None = None,
        similar_to: str | None = None,
        limit: int = 10,
        since: str | None = None,
        until: str | None = None,
        include_self: bool = False,
        include_hidden: bool = False,
        deep: bool = False,
        scope: str | None = None,
    ) -> list[Any]:
        return self._env.find(
            query,
            tags=tags,
            similar_to=similar_to,
            limit=limit,
            since=since,
            until=until,
            include_self=include_self,
            include_hidden=include_hidden,
            deep=deep,
            scope=scope,
        )

    def list_items(
        self,
        *,
        prefix: str | None = None,
        tags: dict[str, Any] | None = None,
        since: str | None = None,
        until: str | None = None,
        order_by: str = "updated",
        include_hidden: bool = False,
        limit: int = 10,
    ) -> list[Any]:
        return self._env.list_items(
            prefix=prefix,
            tags=tags,
            since=since,
            until=until,
            order_by=order_by,
            include_hidden=include_hidden,
            limit=limit,
        )

    def list_parts(self, id: str) -> list[Any]:
        return self._env.list_parts(id)

    def get_document(self, id: str) -> Any:
        return self._env.get_document(id)

    def find_by_name(self, stem: str, *, vault: str | None = None) -> Any:
        # Delegate to env if available; extract_links runs via _KeeperActionContext
        fn = getattr(self._env, "find_by_name", None)
        if fn is not None:
            return fn(stem, vault=vault)
        return None

    def list_versions(self, id: str, *, limit: int = 3) -> list[Any]:
        return self._env.list_versions(id, limit=limit)

    def resolve_edges(self, id: str, *, limit: int = 5) -> dict[str, Any]:
        return self._env.resolve_edges(id, limit=limit)

    def resolve_meta(self, id: str, limit_per_doc: int = 3) -> dict[str, list[Any]]:
        return self._env.resolve_meta(id, limit_per_doc=limit_per_doc)

    def traverse(self, source_ids: list[str], *, limit: int = 5) -> dict[str, list[Any]]:
        return self._env.traverse_related(source_ids, limit_per_source=limit)

    def get_db_connection(self) -> Any:
        return self._env.get_db_connection()

    def get_document_store(self) -> Any:
        fn = getattr(self._env, "get_document_store", None)
        if fn is not None:
            return fn()
        return None

    def get_collection(self) -> str:
        return self._env.get_collection()

    def find_by_content_hash(
        self,
        content_hash: str,
        *,
        content_hash_full: str = "",
        exclude_id: str = "",
        limit: int = 20,
    ) -> list[Any]:
        ds = self.get_document_store()
        if ds is None:
            return []
        collection = self.get_collection()
        results = ds.find_by_content_hash(
            collection, content_hash,
            content_hash_full=content_hash_full,
            exclude_id=exclude_id, limit=limit,
        )
        if isinstance(results, list):
            return results
        return [results] if results else []

    def put(self, *, content: str | None = None, uri: str | None = None,
            id: str | None = None, tags: dict | None = None,
            summary: str | None = None, created_at: str | None = None,
            force: bool = False) -> Any:
        if not self._writable:
            raise NotImplementedError("put not available in read-only flow context")
        return self._env.put(
            content=content,
            uri=uri,
            id=id,
            tags=tags,
            summary=summary,
            created_at=created_at,
            force=force,
        )

    def tag(
        self,
        id: str,
        tags: dict | None = None,
        *,
        remove: list[str] | None = None,
        remove_values: dict[str, Any] | None = None,
    ) -> Any:
        if not self._writable:
            raise NotImplementedError("tag not available in read-only flow context")
        return self._env.tag(
            id,
            tags,
            remove=remove,
            remove_values=remove_values,
        )

    def move(self, name: str, *, source_id: str = "now",
             tags: dict | None = None, only_current: bool = False) -> Any:
        if not self._writable:
            raise NotImplementedError("move not available in read-only flow context")
        return self._env.move(name, source_id=source_id, tags=tags, only_current=only_current)

    def delete(self, id: str, *, delete_versions: bool = True) -> None:
        if not self._writable:
            raise NotImplementedError("delete not available in read-only flow context")
        self._env.delete(id, delete_versions=delete_versions)

    def resolve_prompt(
        self, prefix: str, doc_tags: dict[str, Any] | None = None, *, item_id: str | None = None,
    ) -> str | None:
        """Resolve a prompt doc matching tags (e.g. .prompt/summarize/*)."""
        resolve = getattr(self._env, "resolve_prompt", None)
        if resolve is None:
            return None
        return resolve(prefix, doc_tags or {}, item_id=item_id)

    def resolve_provider(self, kind: str, name: str | None = None) -> Any:
        if not self._writable:
            raise NotImplementedError(
                "provider resolution not available in read-only flow context"
            )
        # Delegate to the environment's default providers
        _PROVIDER_MAP = {
            "summarization": "get_default_summarization_provider",
            "analyzer": "get_default_analyzer_provider",
            "content_extractor": "get_default_content_extractor_provider",
            "media": "get_default_media_provider",
        }
        method_name = _PROVIDER_MAP.get(kind)
        if method_name is None:
            raise ValueError(f"unknown provider kind: {kind!r}")
        method = getattr(self._env, method_name, None)
        if method is None:
            raise NotImplementedError(f"environment does not support provider kind: {kind!r}")
        return method()
