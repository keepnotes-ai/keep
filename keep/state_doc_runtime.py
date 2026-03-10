"""Synchronous state-doc flow runtime.

Evaluates state docs with inline action execution, handling transitions
between states and enforcing tick budgets.  This is the runtime for the
read/query path where flows run synchronously and return immediately.

The write path uses background task dispatch (flow_engine.py);
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

import logging
from dataclasses import dataclass, field
from typing import Any, Optional, Protocol

from .result_stats import enrich_find_output
from .state_doc import StateDoc, evaluate_state_doc

logger = logging.getLogger(__name__)


@dataclass
class FlowResult:
    """Result of a completed state-doc flow."""

    status: str  # "done", "error", "stopped"
    bindings: dict[str, dict[str, Any]] = field(default_factory=dict)
    data: Optional[dict[str, Any]] = None  # return.with payload
    ticks: int = 0
    history: list[str] = field(default_factory=list)  # state names visited


class StateDocLoader(Protocol):
    def __call__(self, name: str) -> Optional[StateDoc]: ...


class ActionRunner(Protocol):
    def __call__(self, name: str, params: dict[str, Any]) -> dict[str, Any]: ...


def run_flow(
    initial_state: str,
    params: dict[str, Any],
    budget: int = 5,
    *,
    load_state_doc: StateDocLoader,
    run_action: ActionRunner,
) -> FlowResult:
    """Run a state-doc flow synchronously to completion.

    Args:
        initial_state: Name of the starting state doc (e.g. "query-resolve").
        params: Caller-supplied parameters (thresholds, query, limits).
        budget: Maximum ticks before forced stop.
        load_state_doc: Callback to load a compiled StateDoc by name.
        run_action: Callback to execute an action and return its output.

    Returns:
        FlowResult with terminal status, accumulated bindings, and
        optional return data.
    """
    current_state = initial_state
    current_params = dict(params)
    ticks = 0
    history: list[str] = []

    # Wrap run_action to enrich find output with statistics
    def _action_callback(action_name: str, action_params: dict[str, Any]) -> dict[str, Any]:
        output = run_action(action_name, action_params)
        if action_name == "find" and isinstance(output, dict):
            output = enrich_find_output(output)
        return output

    logger.info("flow: start %s", initial_state)

    while ticks < budget:
        doc = load_state_doc(current_state)
        if doc is None:
            logger.info("flow: %s -> error (state doc not found)", current_state)
            return FlowResult(
                status="error",
                data={"reason": f"state doc not found: {current_state}"},
                ticks=ticks,
                history=history,
            )

        history.append(current_state)
        ticks += 1

        # Build evaluation context
        eval_ctx = _build_eval_context(current_params, budget=budget, tick=ticks)

        # Evaluate state doc
        try:
            result = evaluate_state_doc(doc, eval_ctx, run_action=_action_callback)
        except Exception as exc:
            logger.warning("State doc %r evaluation failed: %s", current_state, exc)
            return FlowResult(
                status="error",
                data={"reason": f"evaluation failed: {exc}"},
                ticks=ticks,
                history=history,
            )

        # Collect bindings
        all_bindings = dict(result.bindings)

        # Terminal
        if result.terminal is not None:
            logger.info("flow: %s -> %s (%d ticks)", current_state, result.terminal, ticks)
            return FlowResult(
                status=result.terminal,
                bindings=all_bindings,
                data=result.terminal_data,
                ticks=ticks,
                history=history,
            )

        # Transition
        if result.transition is not None:
            next_state, transition_params = _parse_transition(result.transition)
            if next_state is None:
                logger.info("flow: %s -> error (invalid transition)", current_state)
                return FlowResult(
                    status="error",
                    data={"reason": f"invalid transition: {result.transition}"},
                    ticks=ticks,
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
        return FlowResult(
            status="done",
            bindings=all_bindings,
            ticks=ticks,
            history=history,
        )

    # Budget exhausted
    logger.info("flow: %s -> stopped (budget, %d ticks)", current_state, ticks)
    return FlowResult(
        status="stopped",
        data={"reason": "budget"},
        ticks=ticks,
        history=history,
    )


def _build_eval_context(
    params: dict[str, Any],
    *,
    budget: int,
    tick: int,
) -> dict[str, Any]:
    """Build the evaluation context for a state doc tick.

    Populates ``params.*``, ``budget.*``, and ``flow.*`` namespaces.
    ``item.*`` is populated by the caller via params when relevant.
    """
    return {
        "params": dict(params),
        "budget": {
            "total": budget,
            "remaining": max(budget - tick, 0),
        },
        "flow": {
            "tick": tick,
        },
    }


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
    from .state_doc import parse_state_doc
    from .builtin_state_docs import BUILTIN_STATE_DOCS

    def _load(name: str) -> Optional[StateDoc]:
        bare_name = name.removeprefix(".state/")
        note_id = f".state/{bare_name}"

        # Primary path: load from the store
        doc_note = env.get(note_id)
        if doc_note is not None:
            body = str(getattr(doc_note, "summary", "") or "").strip()
            if body:
                try:
                    return parse_state_doc(bare_name, body)
                except (ValueError, RuntimeError) as exc:
                    logger.warning("Failed to compile state doc %r: %s", note_id, exc)

        # Fallback: compiled builtins (pre-migration or test environments)
        builtin_body = BUILTIN_STATE_DOCS.get(bare_name)
        if builtin_body is not None:
            logger.debug("State doc %r not in store, using builtin fallback", bare_name)
            return _get_compiled_builtin(bare_name, builtin_body)

        return None

    return _load


def make_action_runner(env: Any) -> ActionRunner:
    """Create an action runner backed by a FlowRuntimeEnv.

    Wraps the action registry with a read-only context that delegates
    store operations to the environment.  Suitable for the read/query
    path where actions only need read access.
    """
    from .actions import get_action

    ctx = _EnvActionContext(env)

    def _run(action_name: str, params: dict[str, Any]) -> dict[str, Any]:
        act = get_action(action_name)
        output = act.run(params, ctx)
        return dict(output) if isinstance(output, dict) else {}

    return _run


class _EnvActionContext:
    """Read-only ActionContext backed by a FlowRuntimeEnv."""

    def __init__(self, env: Any) -> None:
        self._env = env

    def get(self, id: str) -> Any:
        return self._env.get(id)

    def find(
        self,
        query: str | None = None,
        *,
        tags: dict[str, Any] | None = None,
        similar_to: str | None = None,
        limit: int = 10,
        since: str | None = None,
        until: str | None = None,
        include_hidden: bool = False,
    ) -> list[Any]:
        return self._env.find(
            query,
            tags=tags,
            similar_to=similar_to,
            limit=limit,
            since=since,
            until=until,
            include_hidden=include_hidden,
            deep=False,
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

    def get_document(self, id: str) -> Any:
        return self._env.get_document(id)

    def resolve_meta(self, id: str, limit_per_doc: int = 3) -> dict[str, list[Any]]:
        return self._env.resolve_meta(id, limit_per_doc=limit_per_doc)

    def traverse(self, source_ids: list[str], *, limit: int = 5) -> dict[str, list[Any]]:
        return self._env.traverse_related(source_ids, limit_per_source=limit)

    def resolve_provider(self, kind: str, name: str | None = None) -> Any:
        raise NotImplementedError(
            "provider resolution not available in sync read runtime"
        )
