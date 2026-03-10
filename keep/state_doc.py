"""State-doc loader, compiler, and evaluator.

Loads `.state/*` documents from the keep store, compiles CEL predicates,
and evaluates rules against a context to produce action dispatches and
state transitions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Compiled rule and state-doc types
# ---------------------------------------------------------------------------


@dataclass
class CompiledRule:
    """A single rule from a state doc with its predicate pre-compiled."""

    id: Optional[str] = None
    when: Optional[Any] = None  # compiled CEL program, or None (unconditional)
    when_source: str = ""  # original predicate string for debugging
    do: Optional[str] = None  # action name
    with_params: Optional[dict[str, Any]] = None  # action params (may contain templates)
    then: Optional[str | dict[str, Any]] = None  # state transition
    return_status: Optional[str] = None  # terminal: "done", "error", "stopped"
    return_with: Optional[dict[str, Any]] = None  # data attached to return


@dataclass
class StateDoc:
    """A parsed and compiled state document."""

    name: str
    match: str  # "sequence" or "all"
    rules: list[CompiledRule] = field(default_factory=list)
    post: list[CompiledRule] = field(default_factory=list)  # post-block for match:all


@dataclass
class RuleResult:
    """Result of evaluating a single rule."""

    rule: CompiledRule
    fired: bool
    output: Optional[dict[str, Any]] = None
    transition: Optional[str | dict[str, Any]] = None
    terminal: Optional[str] = None  # "done", "error", "stopped"
    terminal_data: Optional[dict[str, Any]] = None


@dataclass
class EvalResult:
    """Result of evaluating a complete state doc."""

    actions: list[dict[str, Any]]  # [{action, params, rule_id}, ...]
    bindings: dict[str, dict[str, Any]]  # rule_id -> output dict
    transition: Optional[str | dict[str, Any]] = None
    terminal: Optional[str] = None
    terminal_data: Optional[dict[str, Any]] = None


# ---------------------------------------------------------------------------
# CEL compilation
# ---------------------------------------------------------------------------

_cel_available = False

try:
    import cel as _cel
    _cel_available = True
except ImportError:
    _cel = None  # type: ignore[assignment]


def _compile_predicate(source: str) -> Any:
    """Compile a CEL predicate string into a reusable program."""
    if not _cel_available:
        raise RuntimeError(
            "common-expression-language package required for state-doc predicates; "
            "install with: pip install common-expression-language"
        )
    return _cel.compile(source)


def _eval_predicate(program: Any, context: dict[str, Any], source: str = "") -> bool:
    """Evaluate a compiled CEL predicate against a context dict."""
    try:
        result = program.execute(context)
    except Exception as exc:
        logger.warning("Predicate eval error in %r: %s", source or "<unknown>", exc)
        return False
    if isinstance(result, bool):
        return result
    return bool(result)


# ---------------------------------------------------------------------------
# Template interpolation for `with:` params
# ---------------------------------------------------------------------------

def _resolve_template(value: Any, context: dict[str, Any]) -> Any:
    """Resolve template references in `with:` parameter values.

    Handles three cases:
    - Pure template string: "{params.query}" -> resolved value
    - Mixed template string: "prefix {x.y} suffix" -> interpolated string
    - Non-string values: returned as-is
    - Nested dicts/lists: resolved recursively
    """
    if isinstance(value, str) and "{" in value and "}" in value:
        # Pure template: entire string is one reference
        stripped = value.strip()
        if stripped.startswith("{") and stripped.endswith("}") and stripped.count("{") == 1:
            path = stripped[1:-1].strip()
            resolved = _resolve_path(path, context)
            if resolved is None:
                logger.debug("Template reference %r resolved to None", path)
            return resolved
        # Mixed template: interpolate each {ref}
        import re

        def _replace(match: re.Match) -> str:
            resolved = _resolve_path(match.group(1).strip(), context)
            return str(resolved) if resolved is not None else ""

        return re.sub(r"\{([^}]+)\}", _replace, value)
    if isinstance(value, dict):
        return {k: _resolve_template(v, context) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_template(v, context) for v in value]
    return value


def _resolve_path(path: str, context: dict[str, Any]) -> Any:
    """Resolve a dotted path like 'params.query' against a context dict."""
    parts = path.split(".")
    current: Any = context
    for part in parts:
        if isinstance(current, dict):
            if part not in current:
                return None
            current = current[part]
        else:
            current = getattr(current, part, None)
            if current is None:
                return None
    return current


# ---------------------------------------------------------------------------
# State-doc parsing
# ---------------------------------------------------------------------------

def _parse_rule(raw: dict[str, Any]) -> CompiledRule:
    """Parse and compile a single rule from YAML."""
    rule = CompiledRule()

    rule.id = raw.get("id")
    if isinstance(rule.id, str):
        rule.id = rule.id.strip() or None

    when_src = raw.get("when")
    if isinstance(when_src, str) and when_src.strip():
        rule.when_source = when_src.strip()
        try:
            rule.when = _compile_predicate(rule.when_source)
        except (ValueError, RuntimeError) as exc:
            rule_label = f"rule {rule.id!r}" if rule.id else "rule"
            raise ValueError(f"{rule_label}: failed to compile when: {rule.when_source!r}: {exc}") from exc

    rule.do = raw.get("do")
    if isinstance(rule.do, str):
        rule.do = rule.do.strip() or None

    with_params = raw.get("with")
    if isinstance(with_params, dict):
        rule.with_params = dict(with_params)

    then = raw.get("then")
    if isinstance(then, str):
        rule.then = then.strip() or None
    elif isinstance(then, dict):
        rule.then = dict(then)

    ret = raw.get("return")
    if isinstance(ret, str):
        rule.return_status = ret.strip() or None
    elif isinstance(ret, dict):
        # return: {status: stopped, with: {reason: "..."}}
        rule.return_status = str(ret.get("status") or ret.get("return") or "").strip() or None

    if isinstance(raw.get("return"), dict):
        ret_with = raw["return"].get("with")
        if isinstance(ret_with, dict):
            rule.return_with = dict(ret_with)

    return rule


def parse_state_doc(name: str, body: str) -> StateDoc:
    """Parse a state doc from its YAML body text.

    Args:
        name: State doc name (e.g. "after-write").
        body: YAML text body of the state doc.
    """
    parsed = yaml.safe_load(body)
    if not isinstance(parsed, dict):
        raise ValueError(f"state doc {name!r} must be a YAML mapping")

    match = str(parsed.get("match") or "sequence").strip().lower()
    if match not in ("sequence", "all"):
        raise ValueError(f"state doc {name!r}: match must be 'sequence' or 'all', got {match!r}")

    raw_rules = parsed.get("rules")
    if not isinstance(raw_rules, list):
        raise ValueError(f"state doc {name!r}: rules must be a list")

    rules = []
    for i, r in enumerate(raw_rules):
        if isinstance(r, dict):
            rules.append(_parse_rule(r))
        else:
            logger.warning("state doc %r: rules[%d] is not a mapping, skipping", name, i)

    post: list[CompiledRule] = []
    raw_post = parsed.get("post")
    if isinstance(raw_post, list):
        for i, r in enumerate(raw_post):
            if isinstance(r, dict):
                post.append(_parse_rule(r))
            else:
                logger.warning("state doc %r: post[%d] is not a mapping, skipping", name, i)

    return StateDoc(name=name, match=match, rules=rules, post=post)


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate_state_doc(
    doc: StateDoc,
    context: dict[str, Any],
    *,
    run_action: Optional[Any] = None,
) -> EvalResult:
    """Evaluate a state doc's rules against a context.

    Args:
        doc: Compiled state document.
        context: Evaluation context with item, params, budget, flow, and
            any prior rule output bindings.
        run_action: Optional callback ``(action_name, params) -> output_dict``
            for synchronous action execution. If None, actions are collected
            but not executed (caller dispatches them).

    Returns:
        EvalResult with actions to dispatch, output bindings, and
        optional transition or terminal status.
    """
    if doc.match == "sequence":
        return _eval_sequence(doc, context, run_action=run_action)
    return _eval_all(doc, context, run_action=run_action)


def _eval_sequence(
    doc: StateDoc,
    context: dict[str, Any],
    *,
    run_action: Optional[Any] = None,
) -> EvalResult:
    """Evaluate rules in sequence, short-circuiting on transition/terminal."""
    bindings: dict[str, dict[str, Any]] = {}
    actions: list[dict[str, Any]] = []
    eval_ctx = dict(context)

    for rule in doc.rules:
        # Check predicate
        if rule.when is not None:
            if not _eval_predicate(rule.when, eval_ctx, rule.when_source):
                continue

        # Terminal
        if rule.return_status is not None:
            data = _resolve_template(rule.return_with, eval_ctx) if rule.return_with else None
            return EvalResult(
                actions=actions,
                bindings=bindings,
                terminal=rule.return_status,
                terminal_data=data if isinstance(data, dict) else None,
            )

        # Action
        if rule.do is not None:
            params = _resolve_template(rule.with_params, eval_ctx) if rule.with_params else {}
            if not isinstance(params, dict):
                params = {}

            action_entry = {"action": rule.do, "params": params, "rule_id": rule.id}
            actions.append(action_entry)

            # Execute synchronously if callback provided (sequence needs outputs)
            if run_action is not None:
                try:
                    output = run_action(rule.do, params)
                    if isinstance(output, dict) and rule.id:
                        bindings[rule.id] = output
                        eval_ctx[rule.id] = output
                except Exception as exc:
                    logger.warning("Action %s failed: %s", rule.do, exc)

        # Transition
        if rule.then is not None:
            transition = rule.then
            if isinstance(transition, dict):
                with_data = transition.get("with")
                if isinstance(with_data, dict):
                    transition = dict(transition)
                    transition["with"] = _resolve_template(with_data, eval_ctx)
            return EvalResult(
                actions=actions,
                bindings=bindings,
                transition=transition,
            )

    # Fell through all rules with no terminal or transition
    return EvalResult(actions=actions, bindings=bindings, terminal="done")


def _eval_all(
    doc: StateDoc,
    context: dict[str, Any],
    *,
    run_action: Optional[Any] = None,
) -> EvalResult:
    """Evaluate all matching rules (potentially parallel), then post block."""
    bindings: dict[str, dict[str, Any]] = {}
    actions: list[dict[str, Any]] = []
    eval_ctx = dict(context)

    for rule in doc.rules:
        # Check predicate
        if rule.when is not None:
            if not _eval_predicate(rule.when, eval_ctx, rule.when_source):
                continue

        # Action
        if rule.do is not None:
            params = _resolve_template(rule.with_params, eval_ctx) if rule.with_params else {}
            if not isinstance(params, dict):
                params = {}

            action_entry = {"action": rule.do, "params": params, "rule_id": rule.id}
            actions.append(action_entry)

            # In match:all, actions could run in parallel; for now, sequential
            if run_action is not None:
                try:
                    output = run_action(rule.do, params)
                    if isinstance(output, dict) and rule.id:
                        bindings[rule.id] = output
                        eval_ctx[rule.id] = output
                except Exception as exc:
                    logger.warning("Action %s failed: %s", rule.do, exc)

    # Post block
    if doc.post:
        for rule in doc.post:
            if rule.when is not None:
                if not _eval_predicate(rule.when, eval_ctx):
                    continue
            if rule.return_status is not None:
                data = _resolve_template(rule.return_with, eval_ctx) if rule.return_with else None
                return EvalResult(
                    actions=actions,
                    bindings=bindings,
                    terminal=rule.return_status,
                    terminal_data=data if isinstance(data, dict) else None,
                )
            if rule.then is not None:
                transition = rule.then
                if isinstance(transition, dict):
                    with_data = transition.get("with")
                    if isinstance(with_data, dict):
                        transition = dict(transition)
                        transition["with"] = _resolve_template(with_data, eval_ctx)
                return EvalResult(
                    actions=actions,
                    bindings=bindings,
                    transition=transition,
                )

    # Default terminal for match:all with no post block
    return EvalResult(actions=actions, bindings=bindings, terminal="done")
