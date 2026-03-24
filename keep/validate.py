"""Validation for system documents with parser-based semantics.

System docs (.tag/*, .meta/*, .prompt/*, .state/*) have structured
content that keep parses and interprets at runtime.  Malformed docs
fail silently — a broken tag spec stops classifying, a bad state doc
falls back to templates.

This module provides upfront validation: parse the doc, check structure,
report diagnostics.  Validators mirror the real parsers — they check
exactly what the runtime checks, so validation passing means runtime
will accept the doc.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal

import yaml

from .analyzers import _PROMPT_SECTION_RE
from .utils import _META_CONTEXT_KEY, _META_PREREQ_KEY, _META_QUERY_PAIR

Severity = Literal["error", "warning", "info"]
DocType = Literal["tag", "meta", "prompt", "state", "unknown"]

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@dataclass
class Diagnostic:
    """A single validation finding."""

    severity: Severity
    message: str
    location: str = ""  # e.g. "line 3" or "## Prompt section"

    def __str__(self) -> str:
        loc = f" ({self.location})" if self.location else ""
        return f"[{self.severity}]{loc} {self.message}"


@dataclass
class ValidationResult:
    """Collected diagnostics from validating a system doc."""

    doc_id: str
    doc_type: DocType
    diagnostics: list[Diagnostic] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(d.severity == "error" for d in self.diagnostics)

    @property
    def errors(self) -> list[Diagnostic]:
        return [d for d in self.diagnostics if d.severity == "error"]

    @property
    def warnings(self) -> list[Diagnostic]:
        return [d for d in self.diagnostics if d.severity == "warning"]


# ---------------------------------------------------------------------------
# Shared: meta/prompt line classification
# ---------------------------------------------------------------------------

def _classify_rule_lines(
    text: str,
) -> tuple[int, int, int, list[tuple[int, str]]]:
    """Classify lines using the same logic as ``_parse_meta_doc`` in api.py.

    Returns ``(query_count, context_count, prereq_count, suspect_lines)``
    where suspect_lines are ``(line_num, line)`` tuples for lines
    containing ``=`` that don't parse as valid rules.
    """
    query_count = 0
    context_count = 0
    prereq_count = 0
    suspect_lines: list[tuple[int, str]] = []

    for line_num, raw_line in enumerate(text.split("\n"), start=1):
        line = raw_line.strip()
        if not line:
            continue

        # Markdown headers and frontmatter delimiters are prose —
        # the runtime also skips them (they fail every rule regex).
        if line.startswith("#") or line.startswith("---"):
            continue

        if _META_PREREQ_KEY.match(line):
            prereq_count += 1
            continue

        if _META_CONTEXT_KEY.match(line):
            context_count += 1
            continue

        tokens = line.split()
        is_query = all(_META_QUERY_PAIR.match(t) for t in tokens)

        if is_query and tokens:
            query_count += 1
        elif "=" in line:
            suspect_lines.append((line_num, line))

    return query_count, context_count, prereq_count, suspect_lines


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def validate_system_doc(
    doc_id: str,
    content: str,
    tags: dict[str, Any] | None = None,
) -> ValidationResult:
    """Validate a system doc by ID prefix dispatch."""
    tags = tags or {}

    if doc_id.startswith(".tag/"):
        return _validate_tag_doc(doc_id, content, tags)
    if doc_id.startswith(".meta/"):
        return _validate_meta_doc(doc_id, content, tags)
    if doc_id.startswith(".prompt/"):
        return _validate_prompt_doc(doc_id, content, tags)
    if doc_id.startswith(".state/"):
        return _validate_state_doc(doc_id, content, tags)

    return ValidationResult(
        doc_id=doc_id,
        doc_type="unknown",
        diagnostics=[Diagnostic("info", f"no validator for doc type: {doc_id}")],
    )


# ---------------------------------------------------------------------------
# .tag/* validator
# ---------------------------------------------------------------------------

def _validate_tag_doc(
    doc_id: str,
    content: str,
    tags: dict[str, Any],
) -> ValidationResult:
    result = ValidationResult(doc_id=doc_id, doc_type="tag")
    parts = doc_id.split("/")

    if len(parts) < 2 or not parts[1]:
        result.diagnostics.append(
            Diagnostic("error", "tag doc ID must be .tag/{key} or .tag/{key}/{value}")
        )
        return result

    is_parent = len(parts) == 2
    is_value = len(parts) == 3

    if len(parts) > 3:
        result.diagnostics.append(
            Diagnostic("error", f"tag doc ID too deep: {doc_id} (max: .tag/key/value)")
        )
        return result

    key = parts[1]
    if not _IDENTIFIER_RE.match(key):
        result.diagnostics.append(
            Diagnostic("error", f"invalid tag key {key!r}: must be identifier (letters, digits, underscores)")
        )

    if is_value and not parts[2]:
        result.diagnostics.append(
            Diagnostic("error", "tag value name cannot be empty")
        )

    if not content.strip():
        result.diagnostics.append(
            Diagnostic("warning", "empty content — tag has no description")
        )

    if is_parent:
        _validate_tag_parent(result, content, tags)
    elif is_value:
        _validate_tag_value(result, content)

    return result


def _validate_tag_parent(
    result: ValidationResult,
    content: str,
    tags: dict[str, Any],
) -> None:
    """Validate a parent tag spec (.tag/key)."""
    constrained = tags.get("_constrained")
    singular = tags.get("_singular")
    inverse = tags.get("_inverse")

    if constrained == "true":
        prompt = _PROMPT_SECTION_RE.search(content)
        if not prompt or not prompt.group(1).strip():
            result.diagnostics.append(
                Diagnostic(
                    "warning",
                    "constrained tag spec has no ## Prompt section — classifier will use summary as fallback",
                    "## Prompt section",
                )
            )

    if inverse is not None:
        if not isinstance(inverse, str) or not inverse.strip():
            result.diagnostics.append(
                Diagnostic("error", "_inverse tag must be a non-empty string")
            )
        elif not _IDENTIFIER_RE.match(str(inverse).strip()):
            result.diagnostics.append(
                Diagnostic(
                    "warning",
                    f"_inverse value {inverse!r} is not a valid tag key identifier",
                )
            )

    if singular is not None and singular != "true":
        result.diagnostics.append(
            Diagnostic("warning", f"_singular should be 'true' or absent, got {singular!r}")
        )

    if constrained is not None and constrained != "true":
        result.diagnostics.append(
            Diagnostic("warning", f"_constrained should be 'true' or absent, got {constrained!r}")
        )


def _validate_tag_value(
    result: ValidationResult,
    content: str,
) -> None:
    """Validate a tag value doc (.tag/key/value)."""
    prompt = _PROMPT_SECTION_RE.search(content)
    if prompt and not prompt.group(1).strip():
        result.diagnostics.append(
            Diagnostic("warning", "## Prompt section is present but empty")
        )


# ---------------------------------------------------------------------------
# .meta/* validator
# ---------------------------------------------------------------------------

def _validate_meta_doc(
    doc_id: str,
    content: str,
    tags: dict[str, Any],
) -> ValidationResult:
    result = ValidationResult(doc_id=doc_id, doc_type="meta")
    parts = doc_id.split("/")

    if len(parts) < 2 or not parts[1]:
        result.diagnostics.append(
            Diagnostic("error", "meta doc ID must be .meta/{name}")
        )
        return result

    if not content.strip():
        result.diagnostics.append(
            Diagnostic("error", "empty content — meta doc has no query rules")
        )
        return result

    query_count, context_count, prereq_count, suspect_lines = _classify_rule_lines(content)

    if not query_count and not context_count and not prereq_count:
        result.diagnostics.append(
            Diagnostic("warning", "no valid query rules found — meta doc will match nothing")
        )

    for line_num, line in suspect_lines:
        result.diagnostics.append(
            Diagnostic("warning", f"possible malformed query rule: {line!r}", f"line {line_num}")
        )

    return result


# ---------------------------------------------------------------------------
# .prompt/* validator
# ---------------------------------------------------------------------------

def _validate_prompt_doc(
    doc_id: str,
    content: str,
    tags: dict[str, Any],
) -> ValidationResult:
    result = ValidationResult(doc_id=doc_id, doc_type="prompt")
    parts = doc_id.split("/")

    if len(parts) < 3 or not parts[1] or not parts[2]:
        result.diagnostics.append(
            Diagnostic("error", "prompt doc ID must be .prompt/{prefix}/{name}")
        )
        return result

    prefix = parts[1]
    known_prefixes = {"analyze", "summarize", "agent", "reflect", "review", "tag"}
    if prefix not in known_prefixes:
        result.diagnostics.append(
            Diagnostic("info", f"prompt prefix {prefix!r} is not a known built-in prefix")
        )

    if not content.strip():
        result.diagnostics.append(
            Diagnostic("error", "empty content — prompt doc has no content")
        )
        return result

    # Must have ## Prompt section
    prompt_match = _PROMPT_SECTION_RE.search(content)
    if not prompt_match:
        result.diagnostics.append(
            Diagnostic("error", "missing ## Prompt section — prompt doc will be skipped at runtime")
        )
        return result

    prompt_text = prompt_match.group(1).strip()
    if not prompt_text:
        result.diagnostics.append(
            Diagnostic("error", "## Prompt section is empty — prompt doc will be skipped at runtime")
        )
        return result

    # Validate match rules (content before ## Prompt, same syntax as .meta/*)
    preamble = content[:prompt_match.start()].strip()
    if preamble:
        _, _, _, suspect_lines = _classify_rule_lines(preamble)
        for line_num, line in suspect_lines:
            result.diagnostics.append(
                Diagnostic("warning", f"possible malformed match rule: {line!r}", f"line {line_num}")
            )

    return result


# ---------------------------------------------------------------------------
# .state/* validator
# ---------------------------------------------------------------------------

_VALID_MATCH = {"sequence", "all"}
_VALID_TERMINALS = {"done", "error", "stopped"}



def _validate_state_doc(
    doc_id: str,
    content: str,
    tags: dict[str, Any],
) -> ValidationResult:
    """Validate a .state/* document against the state-doc schema."""
    result = ValidationResult(doc_id=doc_id, doc_type="state")
    parts = doc_id.split("/")

    if len(parts) < 2 or not parts[1]:
        result.diagnostics.append(
            Diagnostic("error", "state doc ID must be .state/{name}")
        )
        return result

    if not content.strip():
        result.diagnostics.append(
            Diagnostic("error", "empty content — state doc has no rules")
        )
        return result

    # Parse YAML
    try:
        parsed = yaml.safe_load(content)
    except Exception as exc:
        result.diagnostics.append(
            Diagnostic("error", f"invalid YAML: {exc}")
        )
        return result

    if not isinstance(parsed, dict):
        result.diagnostics.append(
            Diagnostic("error", "state doc must be a YAML mapping")
        )
        return result

    # Match strategy
    match = str(parsed.get("match") or "sequence").strip().lower()
    if match not in _VALID_MATCH:
        result.diagnostics.append(
            Diagnostic("error", f"match must be 'sequence' or 'all', got {match!r}")
        )

    # Rules list
    raw_rules = parsed.get("rules")
    if not isinstance(raw_rules, list):
        result.diagnostics.append(
            Diagnostic("error", "rules must be a list")
        )
        return result

    if not raw_rules:
        result.diagnostics.append(
            Diagnostic("warning", "rules list is empty")
        )

    # Validate each rule
    rule_ids: set[str] = set()
    for i, raw in enumerate(raw_rules):
        _validate_state_rule(result, raw, i, "rules", rule_ids)

    # Post block
    raw_post = parsed.get("post")
    if raw_post is not None:
        if match != "all":
            result.diagnostics.append(
                Diagnostic("warning", "post block is only evaluated with match: all", "post")
            )
        if isinstance(raw_post, list):
            for i, raw in enumerate(raw_post):
                _validate_state_rule(result, raw, i, "post", rule_ids)
        else:
            result.diagnostics.append(
                Diagnostic("error", "post must be a list", "post")
            )

    # Try full parse (catches CEL compilation errors)
    _try_compile_state_doc(result, doc_id, content)

    return result


def _validate_state_rule(
    result: ValidationResult,
    raw: Any,
    index: int,
    section: str,
    rule_ids: set[str],
) -> None:
    """Validate a single rule entry."""
    loc = f"{section}[{index}]"

    if not isinstance(raw, dict):
        result.diagnostics.append(
            Diagnostic("error", f"rule must be a mapping, got {type(raw).__name__}", loc)
        )
        return

    has_do = "do" in raw
    has_then = "then" in raw
    has_return = "return" in raw
    has_when = "when" in raw

    if not any([has_do, has_then, has_return, has_when]):
        result.diagnostics.append(
            Diagnostic("warning", "rule has no do, then, return, or when — will be skipped", loc)
        )

    # id
    if "id" in raw:
        rule_id = raw["id"]
        if not isinstance(rule_id, str) or not rule_id.strip():
            result.diagnostics.append(
                Diagnostic("error", "rule id must be a non-empty string", loc)
            )
        else:
            rule_id = rule_id.strip()
            if rule_id in rule_ids:
                result.diagnostics.append(
                    Diagnostic("error", f"duplicate rule id {rule_id!r}", loc)
                )
            rule_ids.add(rule_id)

    # when
    if has_when:
        when = raw["when"]
        if not isinstance(when, str) or not when.strip():
            result.diagnostics.append(
                Diagnostic("error", "when must be a non-empty string", loc)
            )

    # do
    if has_do:
        action_name = raw["do"]
        if not isinstance(action_name, str) or not action_name.strip():
            result.diagnostics.append(
                Diagnostic("error", "do must be a non-empty string (action name)", loc)
            )
        else:
            _check_action_name(result, action_name.strip(), loc)

    # with
    if "with" in raw:
        with_params = raw["with"]
        if not isinstance(with_params, dict):
            result.diagnostics.append(
                Diagnostic("error", "with must be a mapping", loc)
            )
    # return
    if has_return:
        ret = raw["return"]
        if isinstance(ret, str):
            if ret.strip() not in _VALID_TERMINALS:
                result.diagnostics.append(
                    Diagnostic("error", f"return status must be one of {_VALID_TERMINALS}, got {ret!r}", loc)
                )
        elif isinstance(ret, dict):
            status = ret.get("status") or ret.get("return")
            if isinstance(status, str) and status.strip() not in _VALID_TERMINALS:
                result.diagnostics.append(
                    Diagnostic("error", f"return status must be one of {_VALID_TERMINALS}, got {status!r}", loc)
                )
            ret_with = ret.get("with")
            if ret_with is not None and not isinstance(ret_with, dict):
                result.diagnostics.append(
                    Diagnostic("error", "return.with must be a mapping", loc)
                )
        else:
            result.diagnostics.append(
                Diagnostic("error", "return must be a string or mapping", loc)
            )

    # then
    if has_then:
        then = raw["then"]
        if isinstance(then, str):
            if not then.strip():
                result.diagnostics.append(
                    Diagnostic("error", "then must be a non-empty state name", loc)
                )
        elif isinstance(then, dict):
            state_name = then.get("state")
            if not isinstance(state_name, str) or not state_name.strip():
                result.diagnostics.append(
                    Diagnostic("error", "then.state must be a non-empty string", loc)
                )
            then_with = then.get("with")
            if then_with is not None:
                if not isinstance(then_with, dict):
                    result.diagnostics.append(
                        Diagnostic("error", "then.with must be a mapping", loc)
                    )
        else:
            result.diagnostics.append(
                Diagnostic("error", "then must be a string or mapping", loc)
            )

    # Conflicting terminal + transition
    if has_return and has_then:
        result.diagnostics.append(
            Diagnostic("warning", "rule has both return and then — return takes precedence", loc)
        )


_known_actions: set[str] | None = None


def _check_action_name(result: ValidationResult, name: str, loc: str) -> None:
    """Check if an action name is known."""
    global _known_actions
    if _known_actions is None:
        try:
            from .actions import list_actions
            _known_actions = set(list_actions())
        except ImportError:
            return
    if name not in _known_actions:
        result.diagnostics.append(
            Diagnostic("warning", f"unknown action {name!r} (known: {', '.join(sorted(_known_actions))})", loc)
        )


# ---------------------------------------------------------------------------
# State-doc diagram generation
# ---------------------------------------------------------------------------

# Entry points: which state docs are reachable from which caller paths.
_STATE_ENTRY_POINTS: dict[str, str] = {
    "after-write": "put()",
    "get": "get()",
    "find-deep": "find(deep)",
    "query-resolve": "query",
}


def state_doc_diagram(
    state_docs: dict[str, str],
) -> str:
    """Generate a Mermaid stateDiagram-v2 from state doc YAML bodies.

    Args:
        state_docs: Mapping of state name to YAML body string.

    Returns:
        Mermaid diagram source wrapped in a markdown fenced code block.
    """
    lines = ["```mermaid", "stateDiagram-v2"]

    # Sanitize names for Mermaid (hyphens → underscores)
    def _node(name: str) -> str:
        return name.replace("-", "_")

    # Sanitize labels (colons break the Mermaid parser)
    def _label(text: str) -> str:
        return text.replace(":", "∶")  # fullwidth colon

    # Collect transitions: (src, dst, edge_label)
    # Terminal edges: (src, "[*]:status", guard)  — status after colon
    edges: list[tuple[str, str, str]] = []

    for name, body in sorted(state_docs.items()):
        try:
            parsed = yaml.safe_load(body)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue

        # Entry points
        if name in _STATE_ENTRY_POINTS:
            edges.append(("[*]", name, _STATE_ENTRY_POINTS[name]))

        # Walk rules and post blocks
        for section in ("rules", "post"):
            raw_rules = parsed.get(section)
            if not isinstance(raw_rules, list):
                continue
            for rule in raw_rules:
                if not isinstance(rule, dict):
                    continue
                guard = str(rule.get("when") or "").strip()
                _extract_edges(name, rule, guard, edges)

    # Per-state terminal nodes: each (src, status) pair gets its own node
    # so "done" from different states doesn't collapse.
    terminal_nodes: dict[tuple[str, str], str] = {}  # (src, status) -> node_id
    for src, dst, _ in edges:
        if dst.startswith("[*]:"):
            status = dst[4:]
            key = (src, status)
            if key not in terminal_nodes:
                safe = _node(status.replace(" ", "_").replace(":", ""))
                terminal_nodes[key] = f"{_node(src)}_{safe}"

    for (src, status) in sorted(terminal_nodes):
        node_id = terminal_nodes[(src, status)]
        lines.append(f"    {node_id} : {_label(status)}")

    if terminal_nodes:
        lines.append("")

    # Emit edges
    seen: set[tuple[str, str, str]] = set()
    for src, dst, label in edges:
        key = (src, dst, label)
        if key in seen:
            continue
        seen.add(key)
        src_node = _node(src)
        if dst.startswith("[*]:"):
            status = dst[4:]
            dst_node = terminal_nodes[(src, status)]
        else:
            dst_node = _node(dst)
        if label:
            lines.append(f"    {src_node} --> {dst_node} : {_label(label)}")
        else:
            lines.append(f"    {src_node} --> {dst_node}")

    lines.append("```")
    return "\n".join(lines) + "\n"


def _extract_edges(
    state_name: str,
    rule: dict[str, Any],
    guard: str,
    edges: list[tuple[str, str, str]],
) -> None:
    """Extract transition/terminal edges from a single rule."""
    # return → terminal (encode status in destination as "[*]:status")
    if "return" in rule:
        ret = rule["return"]
        if isinstance(ret, str):
            status = ret.strip() or "done"
        elif isinstance(ret, dict):
            s = str(ret.get("status") or ret.get("return") or "done")
            reason = ret.get("with", {}).get("reason", "")
            status = f"{s}: {reason}" if reason else s
        else:
            status = "done"
        edges.append((state_name, f"[*]:{status}", guard))

    # then → transition
    if "then" in rule:
        then = rule["then"]
        if isinstance(then, str):
            target = then.strip()
        elif isinstance(then, dict):
            target = str(then.get("state") or "").strip()
        else:
            target = ""
        if target:
            edges.append((state_name, target, guard))


def _try_compile_state_doc(result: ValidationResult, doc_id: str, content: str) -> None:
    """Attempt full parse+compile to catch CEL compilation errors."""
    import yaml
    try:
        from .state_doc import parse_state_doc, parse_fragment
        name = doc_id.split("/", 1)[1] if "/" in doc_id else doc_id
        # If no match: field, try as fragment instead of base state doc
        parsed = yaml.safe_load(content)
        if isinstance(parsed, dict) and "match" not in parsed:
            parse_fragment(name, content)
        else:
            parse_state_doc(name, content)
    except ValueError as exc:
        msg = str(exc)
        if not any(msg in d.message for d in result.diagnostics):
            result.diagnostics.append(
                Diagnostic("error", f"compilation failed: {msg}")
            )
    except Exception as exc:
        result.diagnostics.append(
            Diagnostic("error", f"unexpected error during compilation: {exc}")
        )
