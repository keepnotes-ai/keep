"""CLI interface for reflective memory.

Usage:
    keep find "query text"
    keep put file:///path/to/doc.md
    keep get file:///path/to/doc.md
"""

import importlib.metadata
import importlib.resources
import json
import os
import platform
import re
import select
import shutil
import subprocess
import signal
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Optional

import typer
from typing_extensions import Annotated

# Pattern for version identifier suffix: @V{N} where N may be signed.
# Public semantics:
#   N >= 0: offset from current
#   N < 0: ordinal from oldest archived (-1 oldest, -2 second-oldest, ...)
VERSION_SUFFIX_PATTERN = re.compile(r'@V\{(-?\d+)\}$')

# Pattern for part identifier suffix: @P{N} where N is digits only
PART_SUFFIX_PATTERN = re.compile(r'@P\{(\d+)\}$')

# URI scheme pattern per RFC 3986: scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
# Used to distinguish URIs from plain text in the update command
_URI_SCHEME_PATTERN = re.compile(r'^[a-zA-Z][a-zA-Z0-9+.-]*://')

from .api import Keeper
from .utils import _text_content_id
from .config import get_tool_directory
from .types import (
    SYSTEM_TAG_PREFIX,
    EdgeRef,
    Item,
    ItemContext,
    PartRef,
    VersionRef,
    local_date,
    tag_values,
)
from .logging_config import configure_quiet_mode, enable_debug_mode



def _is_filesystem_path(source: str) -> Optional[Path]:
    """Return resolved Path if source is an existing filesystem path, None otherwise.

    Skips anything that looks like a URI (has ://). Uses expanduser + resolve.
    Conservative: only matches if the path actually exists on disk.
    """
    if _URI_SCHEME_PATTERN.match(source):
        return None
    try:
        resolved = Path(source).expanduser().resolve()
        if resolved.exists():
            return resolved
    except (OSError, ValueError):
        pass
    return None


from .utils import _git_visible_files, _list_directory_files  # noqa: E402


def _output_width() -> int:
    """Terminal width for summary truncation. Use generous default when not a TTY."""
    if not sys.stdout.isatty():
        return 200
    return shutil.get_terminal_size((120, 24)).columns


def _progress_bar(current: int, total: int, label: str, *, err: bool = False) -> None:
    """Render an inline progress bar to stderr, overwriting the current line."""
    cols = shutil.get_terminal_size((80, 24)).columns
    pct = current * 100 // total
    counter = f" {current}/{total}"
    # bar takes ~30 chars, leave rest for label
    bar_width = 20
    filled = current * bar_width // total
    bar = "\u2588" * filled + "\u2591" * (bar_width - filled)
    prefix = f"\r  [{bar}] {pct:3d}%{counter} "
    # Truncate label to fit
    max_label = cols - len(prefix) - 2
    if len(label) > max_label > 0:
        label = label[:max_label - 1] + "\u2026"
    line = f"{prefix}{label}"
    # Pad to overwrite previous longer lines
    line = line.ljust(cols - 1)
    stream = sys.stderr if err else sys.stdout
    stream.write(line)
    stream.flush()


def _has_stdin_data() -> bool:
    """Check if stdin has data available without blocking.

    Returns True only when stdin is a pipe with data ready to read.
    Returns False for TTYs, sockets (exec sandbox), and empty pipes.
    This prevents hanging when stdin is a socket that never sends EOF.
    """
    if sys.stdin.isatty():
        return False
    try:
        ready, _, _ = select.select([sys.stdin], [], [], 0)
        return bool(ready)
    except (ValueError, OSError):
        return False


# ---------------------------------------------------------------------------
# Stdin JSON template expansion
# ---------------------------------------------------------------------------
# Hooks (e.g. Claude Code) pipe JSON on stdin.  Instead of requiring jq,
# keep can expand ${.field} and ${.field:N} (truncate to N chars) directly.

_STDIN_JSON_SENTINEL = object()
_stdin_json_cache: Any = _STDIN_JSON_SENTINEL

_TEMPLATE_RE = re.compile(r'\$\{\.([A-Za-z_][A-Za-z0-9_]*)(?::(\d+))?\}')


def _read_stdin_json() -> dict:
    """Read and cache JSON object from stdin.  Returns {} on failure."""
    global _stdin_json_cache
    if _stdin_json_cache is not _STDIN_JSON_SENTINEL:
        return _stdin_json_cache  # type: ignore[return-value]
    _stdin_json_cache = {}
    if _has_stdin_data():
        try:
            raw = sys.stdin.read()
            obj = json.loads(raw)
            if isinstance(obj, dict):
                _stdin_json_cache = obj
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
    return _stdin_json_cache


def _has_templates(s: str | None) -> bool:
    return s is not None and '${.' in s


def _expand_template(s: str, data: dict) -> str:
    """Expand ${.field} and ${.field:N} in *s* from *data*."""
    def _replace(m: re.Match) -> str:
        key, limit = m.group(1), m.group(2)
        val = data.get(key)
        if val is None:
            return ''
        result = str(val)
        if limit:
            result = result[:int(limit)]
        return result
    return _TEMPLATE_RE.sub(_replace, s)


def _expand_stdin_templates(
    *strings: str | None,
) -> tuple[str | None, ...]:
    """Expand ${.field} templates in one or more strings from stdin JSON.

    Only reads stdin when at least one string contains a template.
    Returns the strings unchanged when no templates are present.
    """
    if not any(_has_templates(s) for s in strings):
        return strings
    data = _read_stdin_json()
    return tuple(
        _expand_template(s, data) if _has_templates(s) else s
        for s in strings
    )


def _expand_stdin_tag_list(
    tags: list[str] | None,
    data: dict | None = None,
) -> list[str] | None:
    """Expand templates in a tag list.  Returns None if input is None."""
    if not tags:
        return tags
    if not any(_has_templates(t) for t in tags):
        return tags
    if data is None:
        data = _read_stdin_json()
    return [_expand_template(t, data) if _has_templates(t) else t for t in tags]


def _parse_json_arg(source: str) -> dict:
    """Parse JSON from inline string, @file, or stdin (-)."""
    if source == "-":
        raw = sys.stdin.read()
        return json.loads(raw)
    if source.startswith("@"):
        path = Path(source[1:])
        if not path.exists():
            raise ValueError(f"JSON file not found: {path}")
        return json.loads(path.read_text(encoding="utf-8"))
    return json.loads(source)


def _tag_display_value(value) -> str:
    """Render scalar/list tag values for compact display."""
    if isinstance(value, list):
        return "[" + ", ".join(str(v) for v in value) + "]"
    return str(value)


def _quote_scalar_tag_value(value) -> str:
    """Render a tag scalar as a quoted JSON/YAML-compatible string."""
    return json.dumps(str(value))


def _render_edge_ref_value(ref: EdgeRef) -> str:
    """Render an edge reference as: id [date] "summary"."""
    ref_id = _shell_quote_id(ref.source_id)
    date_part = f" [{ref.date}]" if ref.date else ""
    summary_part = _quote_scalar_tag_value(ref.summary or "")
    return f"{ref_id}{date_part} {summary_part}"


def _render_tags_frontmatter(
    tags: dict,
    edge_refs: Optional[dict[str, list[EdgeRef]]] = None,
) -> list[str]:
    """Render unified tags block with quoted scalars and edge references."""
    edge_refs = edge_refs or {}
    keys = sorted(set(tags) | set(edge_refs))
    if not keys:
        return []

    lines = ["tags:"]
    for key in keys:
        refs = edge_refs.get(key, [])
        if refs:
            rendered_refs: list[str] = []
            seen_refs: set[str] = set()
            for ref in refs:
                rv = _render_edge_ref_value(ref)
                if rv in seen_refs:
                    continue
                seen_refs.add(rv)
                rendered_refs.append(rv)
            if not rendered_refs:
                continue
            if len(rendered_refs) == 1:
                lines.append(f"  {key}: {rendered_refs[0]}")
            else:
                lines.append(f"  {key}:")
                for rv in rendered_refs:
                    lines.append(f"    - {rv}")
            continue

        values = tag_values(tags, key)
        if not values:
            continue
        if len(values) == 1:
            lines.append(f"  {key}: {_quote_scalar_tag_value(values[0])}")
        else:
            lines.append(f"  {key}:")
            for v in values:
                lines.append(f"    - {_quote_scalar_tag_value(v)}")
    return lines


# Configure quiet mode by default (suppress verbose library output)
# Set KEEP_VERBOSE=1 to enable debug mode via environment
if os.environ.get("KEEP_VERBOSE") == "1":
    enable_debug_mode()
else:
    configure_quiet_mode(quiet=True)


def _version_callback(value: bool):
    if value:
        from importlib.metadata import version
        print(f"keep {version('keep-skill')}")
        raise typer.Exit()


def _verbose_callback(value: bool):
    if value:
        enable_debug_mode()


# Global state for CLI options
_json_output = False
_ids_output = False
_full_output = False
_store_override: Optional[Path] = None


def _json_callback(value: bool):
    global _json_output
    _json_output = value


def _get_json_output() -> bool:
    return _json_output


def _ids_callback(value: bool):
    global _ids_output
    _ids_output = value


def _get_ids_output() -> bool:
    return _ids_output


def _full_callback(value: bool):
    global _full_output
    _full_output = value


def _get_full_output() -> bool:
    return _full_output


def _store_callback(value: Optional[Path]):
    global _store_override
    if value is not None:
        _store_override = value


def _get_store_override() -> Optional[Path]:
    return _store_override


app = typer.Typer(
    name="keep",
    help="Reflective memory with semantic search.",
    no_args_is_help=False,
    invoke_without_command=True,
    rich_markup_mode=None,
)


# Shell-safe character set for IDs (no quoting needed)
_SHELL_SAFE_PATTERN = re.compile(r'^[a-zA-Z0-9_./:@{}\-%]+$')


def _shell_quote_id(id: str) -> str:
    """Quote an ID for safe shell usage if it contains non-shell-safe characters.

    IDs containing only [a-zA-Z0-9_./:@{}%-] are returned as-is.
    Others are wrapped in single quotes with internal single quotes escaped.
    """
    if _SHELL_SAFE_PATTERN.match(id):
        return id
    # Escape any single quotes within the ID: ' → '\''
    escaped = id.replace("'", "'\\''")
    return f"'{escaped}'"


# -----------------------------------------------------------------------------
# Output Formatting
#
# Three output formats, controlled by global flags:
#   --ids:  versioned ID only (id@V{N})
#   --full: YAML frontmatter with tags, similar items, version nav
#   default: summary line (id@V{N} date summary)
#
# JSON output (--json) works with any of the above.
# -----------------------------------------------------------------------------

def _filter_display_tags(tags: dict) -> dict:
    """Filter out internal-only tags for display."""
    from .types import INTERNAL_TAGS
    return {k: v for k, v in tags.items() if k not in INTERNAL_TAGS}


def render_context(ctx: ItemContext, as_json: bool = False) -> str:
    """Render an ItemContext for display.

    This is the single renderer — CLI local, CLI remote, and REST
    all produce ItemContext; this function turns it into output.
    """
    if as_json:
        return json.dumps(ctx.to_dict(), indent=2)
    return _render_frontmatter(ctx)


def _dicts_to_items(raw: list, summary_limit: int = 200) -> "list[Item]":
    """Convert find-action result dicts to Item objects for rendering.

    Truncates summaries to *summary_limit* chars to keep output bounded.
    """
    from .types import Item

    items = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        score = r.get("score")
        summary = str(r.get("summary", ""))
        if len(summary) > summary_limit:
            summary = summary[:summary_limit] + "..."
        items.append(Item(
            id=str(r.get("id", "")),
            summary=summary,
            tags=dict(r.get("tags", {})),
            score=float(score) if isinstance(score, (int, float)) else None,
        ))
    return items


def render_flow_response(
    result: "FlowResult",
    token_budget: int = 4000,
    keeper=None,
) -> str:
    """Render a FlowResult as token-budgeted text for LLM consumption.

    Reuses render_find_context() for search result lists found in the
    flow's return data.
    """
    def _tok(text: str) -> int:
        return len(text) // 4

    lines: list[str] = []
    remaining = token_budget

    # Header
    header = f"flow: {result.status} ({result.ticks} ticks)"
    if result.history:
        header += f" via {' > '.join(result.history)}"
    lines.append(header)
    remaining -= _tok(header)

    # Render data fields — auto-detect result lists, meta sections, scalars
    def _render_result_list(label: str, raw: list) -> None:
        nonlocal remaining
        items = _dicts_to_items(raw)
        if not items or remaining <= 0:
            return
        section_budget = min(remaining, token_budget // 3)
        rendered = render_find_context(
            items, keeper=keeper, token_budget=section_budget,
        )
        section = f"\n{label}:\n{rendered}"
        lines.append(section)
        remaining -= _tok(section)

    if result.data and remaining > 0:
        for key, val in result.data.items():
            if remaining <= 0:
                break
            if val is None:
                continue
            # Direct result list (e.g., data.results)
            if isinstance(val, list) and val and isinstance(val[0], dict):
                _render_result_list(key, val)
            # Action binding with results (e.g., data.similar = {results: [...]})
            elif isinstance(val, dict) and "results" in val:
                raw_results = val["results"]
                if isinstance(raw_results, list) and raw_results:
                    _render_result_list(key, raw_results)
            # Meta sections (e.g., data.meta = {sections: {learnings: [...], todo: [...]}})
            elif isinstance(val, dict) and "sections" in val:
                for section_name, section_items in val["sections"].items():
                    if isinstance(section_items, list) and section_items and remaining > 0:
                        _render_result_list(f"meta/{section_name}", section_items)
            # Versions (e.g., data.versions = {versions: [{offset, summary, date}], count})
            elif isinstance(val, dict) and "versions" in val:
                vers = val["versions"]
                if isinstance(vers, list) and vers and remaining > 0:
                    lines.append(f"\n{key}:")
                    for v in vers:
                        if remaining <= 0:
                            break
                        line = f"  - @V{{{v.get('offset', '?')}}}  [{v.get('date', '')[:10]}]  {str(v.get('summary', ''))[:150]}"
                        lines.append(line)
                        remaining -= _tok(line)
            # Edges (e.g., data.edges = {edges: {predicate: [{id, summary, ...}]}, count})
            elif isinstance(val, dict) and "edges" in val:
                edge_groups = val["edges"]
                if isinstance(edge_groups, dict) and edge_groups and remaining > 0:
                    lines.append(f"\n{key}:")
                    for predicate, refs in edge_groups.items():
                        if remaining <= 0:
                            break
                        if isinstance(refs, list):
                            for ref in refs:
                                if remaining <= 0:
                                    break
                                line = f"  - [{predicate}] {ref.get('id', '')}  {str(ref.get('summary', ''))[:100]}"
                                lines.append(line)
                                remaining -= _tok(line)
            # Scalar (margin, entropy, reason, item_id, etc.)
            elif not isinstance(val, (dict, list)):
                line = f"{key}: {val}"
                lines.append(line)
                remaining -= _tok(line)

    # Tried queries (for stopped flows)
    if result.tried_queries and remaining > 0:
        line = f"queries tried: {', '.join(repr(q) for q in result.tried_queries)}"
        lines.append(line)
        remaining -= _tok(line)

    # Cursor for stopped flows
    if result.cursor:
        lines.append(f"\ncursor: {result.cursor}")

    return "\n".join(lines)


def render_find_context(
    items: "list[Item]",
    keeper=None,
    token_budget: int = 4000,
    show_tags: bool = False,
    deep_primary_cap: int | None = None,
) -> str:
    """Render find results for prompt injection, filling a token budget.

    Three-pass rendering:
      Pass 1: Lay down summary lines for all items (breadth-first).
      Pass 2: If enough summaries (>=2) and remaining budget, backfill
              detail (parts, versions) starting from the top-scoring item.
      Pass 3: Deep sub-items ranked by score from leftover budget.

    When *deep_primary_cap* is set and deep groups exist, only the top N
    primaries are rendered (preferring those with deep groups) and pass 2
    is skipped — this gives maximum budget to deep-discovered evidence,
    useful for multi-hop queries.

    Used by expand_prompt() for {find} expansion and MCP keep_flow.
    """
    from .types import SYSTEM_TAG_PREFIX

    _MIN_ITEMS_FOR_DETAIL = 2

    def _tok(text: str) -> int:
        return len(text) // 4

    if not items or token_budget <= 0:
        return "No results."

    deep_groups = getattr(items, "deep_groups", {})
    compact_mode = bool(deep_groups) and token_budget <= 300
    candidate_items = list(items)

    # With deep mode + primary cap, pick primaries before spending budget.
    # This avoids low budgets being exhausted by unrelated top-ranked primaries
    # before entity/deep-group anchors are even considered.
    if deep_primary_cap is not None and deep_groups and len(candidate_items) > deep_primary_cap:
        def _base_id(it: "Item") -> str:
            return it.id.split("@")[0] if "@" in it.id else it.id

        def _has_deep_group(it: "Item") -> bool:
            bid = _base_id(it)
            return bid in deep_groups or it.id in deep_groups

        # Stable sort preserves original rank order within buckets.
        candidate_items.sort(key=lambda it: (
            not bool(it.tags.get("_entity")),  # query-mentioned entities first
            not _has_deep_group(it),           # then items with deep groups
        ))
        candidate_items = candidate_items[:deep_primary_cap]

    remaining = token_budget

    # Pass 1: summary lines for every item that fits
    # Each entry: (item, [lines_so_far])
    rendered: list[tuple["Item", list[str]]] = []

    for item in candidate_items:
        if remaining <= 0:
            break

        focus = item.tags.get("_focus_summary")
        display_summary = focus if focus else item.summary
        line = f"- {item.id}"
        if item.score is not None:
            line += f" ({item.score:.2f})"
        date = (item.tags.get("_created") or
                item.tags.get("_updated", ""))[:10]
        if date:
            line += f"  [{date}]"
        line += f"  {display_summary}"
        remaining -= _tok(line)
        rendered.append((item, [line]))

    # Pass 2: deep sub-items, ranked by score across all groups.
    # Runs before detail backfill so deep evidence gets budget priority.
    if deep_groups and remaining > 0:
        def _append_line(block_lines: list[str], line: str) -> bool:
            nonlocal remaining
            cost = _tok(line)
            if remaining - cost < 0:
                return False
            block_lines.append(line)
            remaining -= cost
            return True

        def _append_section(
            block_lines: list[str],
            header: str,
            lines: list[str],
        ) -> bool:
            """Append header + as many section lines as fit, or nothing."""
            nonlocal remaining
            if not lines:
                return False

            header_cost = _tok(header)
            if remaining - header_cost < 0:
                return False

            budget_after_header = remaining - header_cost
            take = 0
            for line in lines:
                c = _tok(line)
                if budget_after_header - c < 0:
                    break
                budget_after_header -= c
                take += 1

            # Never emit empty headers.
            if take == 0:
                return False

            block_lines.append(header)
            remaining -= header_cost
            for line in lines[:take]:
                block_lines.append(line)
                remaining -= _tok(line)
            return True

        def _thread_radius() -> int:
            """Adapt version-thread window width as budget tightens."""
            budget_hint = min(token_budget, remaining)
            if budget_hint <= 450:
                return 0  # focus version only
            if budget_hint <= 900:
                return 1  # local neighborhood (3 versions)
            return 2      # full neighborhood (5 versions)

        def _base_id(id_value: str) -> str:
            return id_value.split("@")[0] if "@" in id_value else id_value

        def _deep_key(deep_item: "Item") -> str:
            return (
                deep_item.tags.get("_anchor_id")
                or f"{deep_item.id}|{deep_item.tags.get('_focus_version', '')}|"
                   f"{deep_item.tags.get('_focus_part', '')}|"
                   f"{deep_item.tags.get('_focus_summary', '')}"
            )

        def _add_deep_window(
            parent_id: str,
            deep_items: list["Item"],
            block_lines: list[str],
        ) -> None:
            """Attach a compact narrative window around one parent thread."""
            if compact_mode:
                return
            if keeper is None or remaining <= 0:
                return

            focus_versions = sorted({
                int(v)
                for v in (di.tags.get("_focus_version") for di in deep_items)
                if v and str(v).isdigit()
            })
            focus_parts = sorted({
                int(p)
                for p in (di.tags.get("_focus_part") for di in deep_items)
                if p and str(p).isdigit()
            })

            # Version-thread window: merge local neighborhoods around all focused versions.
            if focus_versions:
                radius = _thread_radius()
                around_map = {}
                for fv in focus_versions:
                    try:
                        around = keeper.list_versions_around(
                            parent_id, int(fv), radius=radius,
                        )
                    except Exception:
                        around = []
                    for v in around:
                        around_map[v.version] = v
                around = [around_map[k] for k in sorted(around_map)]

                # Skip degenerate compact thread windows that only restate
                # the already-rendered deep line (focused version duplicate).
                if around and radius == 0 and len(around) == 1 and len(focus_versions) == 1:
                    v0 = around[0]
                    focus_v = str(focus_versions[0])
                    anchor_item = next(
                        (di for di in deep_items if di.tags.get("_focus_version") == focus_v),
                        None,
                    )
                    deep_summary = (
                        anchor_item.tags.get("_focus_summary", anchor_item.summary)
                        if anchor_item else ""
                    )
                    if (
                        str(v0.version) == focus_v
                        and (v0.summary or "").strip() == (deep_summary or "").strip()
                    ):
                        around = []

                thread_lines = []
                for v in around:
                    prefix = "*" if int(v.version) in focus_versions else "-"
                    thread_lines.append(f"      {prefix} @V{{{v.version}}} {v.summary}")
                _append_section(block_lines, "      Thread:", thread_lines)

            # Parts window: include overview + local part neighbors.
            if remaining <= 0:
                return
            try:
                parts = keeper.list_parts(parent_id)
            except Exception:
                parts = []
            if not parts:
                return

            selected = []
            if focus_parts:
                part_map = {p.part_num: p for p in parts}
                if 0 in part_map:
                    selected.append(part_map[0])  # @P{0} coherence prior
                for fp in focus_parts:
                    for pn in (fp - 1, fp, fp + 1):
                        if pn in part_map and part_map[pn] not in selected:
                            selected.append(part_map[pn])
            else:
                # No focused part: include overview if present.
                part0 = next((p for p in parts if p.part_num == 0), None)
                if part0:
                    selected.append(part0)

            story_lines = [f"      - @P{{{p.part_num}}} {p.summary}" for p in selected]
            _append_section(block_lines, "      Story:", story_lines)

        # Build per-parent deep bundles so evidence stays coherent:
        # render anchor lines and local window together before moving on.
        rendered_map = {}
        block_order: list[int] = []
        for item, block_lines in rendered:
            parent_id = _base_id(item.id)
            rendered_map[parent_id] = block_lines
            rendered_map[item.id] = block_lines
            bid = id(block_lines)
            if bid not in block_order:
                block_order.append(bid)

        bundles: dict[tuple[int, str], dict[str, object]] = {}
        for group_key, group in deep_groups.items():
            block_lines = rendered_map.get(group_key)
            if not block_lines:
                continue
            bid = id(block_lines)
            for deep_item in group:
                parent_id = _base_id(deep_item.id)
                bkey = (bid, parent_id)
                bucket = bundles.setdefault(
                    bkey,
                    {"block_lines": block_lines, "parent_id": parent_id, "items": []},
                )
                bucket["items"].append(deep_item)

        for bucket in bundles.values():
            items_for_parent = bucket["items"]
            # Dedup within a parent-thread bundle by anchor identity.
            deduped: list["Item"] = []
            seen_local: set[str] = set()
            for deep_item in sorted(items_for_parent, key=lambda di: di.score or 0, reverse=True):
                dkey = _deep_key(deep_item)
                if dkey in seen_local:
                    continue
                seen_local.add(dkey)
                deduped.append(deep_item)
            bucket["items"] = deduped
            bucket["score"] = max((di.score or 0.0) for di in deduped) if deduped else 0.0

        bundles_by_block: dict[int, list[dict[str, object]]] = {}
        for (bid, _parent_id), bucket in bundles.items():
            bundles_by_block.setdefault(bid, []).append(bucket)
        for group_list in bundles_by_block.values():
            group_list.sort(key=lambda b: float(b.get("score", 0.0)), reverse=True)

        ordered_bundles: list[dict[str, object]] = []
        used_bundle_keys: set[tuple[int, str]] = set()

        # Coverage first: seed one thread bundle per rendered block.
        for bid in block_order:
            group_list = bundles_by_block.get(bid, [])
            if not group_list:
                continue
            first = group_list[0]
            bkey = (bid, str(first["parent_id"]))
            if bkey not in used_bundle_keys:
                used_bundle_keys.add(bkey)
                ordered_bundles.append(first)

        # Density second: add remaining bundles globally by score.
        tail: list[dict[str, object]] = []
        for bid, group_list in bundles_by_block.items():
            for bucket in group_list:
                bkey = (bid, str(bucket["parent_id"]))
                if bkey in used_bundle_keys:
                    continue
                tail.append(bucket)
        tail.sort(key=lambda b: float(b.get("score", 0.0)), reverse=True)
        ordered_bundles.extend(tail)

        max_anchors_per_bundle = 1 if token_budget <= 900 else 2
        seen_deep: set[str] = set()
        for bucket in ordered_bundles:
            if remaining <= 0:
                break
            block_lines = bucket["block_lines"]
            parent_id = str(bucket["parent_id"])
            deep_items = bucket["items"]

            emitted: list["Item"] = []
            for deep_item in deep_items[:max_anchors_per_bundle]:
                dkey = _deep_key(deep_item)
                if dkey in seen_deep:
                    continue
                ddate = (deep_item.tags.get("_created") or
                         deep_item.tags.get("_updated", ""))[:10]
                ddate_part = f"  [{ddate}]" if ddate else ""
                deep_summary = deep_item.tags.get("_focus_summary", deep_item.summary)
                dl = f"    - {deep_item.id}{ddate_part}  {deep_summary}"
                if not _append_line(block_lines, dl):
                    break
                seen_deep.add(dkey)
                emitted.append(deep_item)

            if emitted:
                _add_deep_window(parent_id, emitted, block_lines)

    # Pass 3: backfill parts + versions on remaining budget.
    # For deep mode this runs after deep items; otherwise after summaries.
    if len(rendered) >= _MIN_ITEMS_FOR_DETAIL and keeper and remaining > 0 and not compact_mode:
        for item, block_lines in rendered:
            if remaining <= 0:
                break

            # User tags
            if show_tags and remaining > 0:
                user_tags = {k: v for k, v in item.tags.items()
                             if not k.startswith(SYSTEM_TAG_PREFIX)}
                if user_tags:
                    pairs = ", ".join(
                        f"{k}: {_tag_display_value(v)}" for k, v in sorted(user_tags.items()))
                    tl = f"  {{{pairs}}}"
                    block_lines.append(tl)
                    remaining -= _tok(tl)

            # Part summaries (skip the focused part — already shown above)
            if remaining > 30:
                focus_part = item.tags.get("_focus_part")
                parts = keeper.list_parts(item.id)
                other_parts = [
                    p for p in parts
                    if not focus_part
                    or str(p.part_num) != str(focus_part)
                ]
                if other_parts:
                    block_lines.append("  Key topics:")
                    remaining -= 4
                    for p in other_parts:
                        if remaining <= 0:
                            break
                        pl = f"  - {p.summary}"
                        block_lines.append(pl)
                        remaining -= _tok(pl)

            # Version summaries (surrounding hit or recent)
            if remaining > 30:
                focus_version = item.tags.get("_focus_version")
                if focus_version and focus_version.isdigit():
                    versions = keeper.list_versions_around(
                        item.id, int(focus_version), radius=2,
                    )
                else:
                    versions = keeper.list_versions(item.id, limit=5)
                    versions = list(reversed(versions))
                if versions:
                    block_lines.append("  Context:")
                    remaining -= 4
                    for v in versions:
                        if remaining <= 0:
                            break
                        vdate = (v.tags.get("_created") or
                                v.tags.get("_updated", ""))[:10]
                        date_part = f"  [{vdate}]" if vdate else ""
                        vl = f"  - @V{{{v.version}}}{date_part}  {v.summary}"
                        block_lines.append(vl)
                        remaining -= _tok(vl)

    return "\n".join("\n".join(lines) for _, lines in rendered)


def _render_frontmatter(ctx: ItemContext) -> str:
    """Render ItemContext as YAML frontmatter with summary."""
    cols = _output_width()
    item = ctx.item

    def _truncate(summary: str, prefix_len: int) -> str:
        max_width = max(cols - prefix_len, 20)
        s = summary.replace("\n", " ")
        if len(s) > max_width:
            s = s[:max_width - 3].rsplit(" ", 1)[0] + "..."
        return s

    version_suffix = f"@V{{{ctx.viewing_offset}}}" if ctx.viewing_offset > 0 else ""
    lines = ["---", f"id: {_shell_quote_id(item.id)}{version_suffix}"]

    display_tags = _filter_display_tags(item.tags)
    lines.extend(_render_tags_frontmatter(display_tags, ctx.edges))

    if item.score is not None:
        lines.append(f"score: {item.score:.2f}")

    # Similar items
    if ctx.similar:
        sim_ids = []
        for s in ctx.similar:
            sid = _shell_quote_id(s.id)
            if s.offset > 0:
                sid += f"@V{{{s.offset}}}"
            sim_ids.append(sid)
        id_width = min(max(len(s) for s in sim_ids), 20)
        lines.append("similar:")
        for s, sid in zip(ctx.similar, sim_ids):
            score_str = f"({s.score:.2f})" if s.score else ""
            actual_id_len = max(len(sid), id_width)
            prefix_len = 4 + actual_id_len + 1 + len(score_str) + 1 + len(s.date) + 1
            summary_preview = _truncate(s.summary, prefix_len)
            lines.append(f"  - {sid.ljust(id_width)} {score_str} {s.date} {summary_preview}")

    # Meta sections
    if ctx.meta:
        for name, refs in ctx.meta.items():
            meta_ids = [_shell_quote_id(r.id) for r in refs]
            id_width = min(max(len(s) for s in meta_ids), 20)
            lines.append(f"meta/{name}:")
            for ref, mid in zip(refs, meta_ids):
                actual_id_len = max(len(mid), id_width)
                prefix_len = 4 + actual_id_len + 1
                summary_preview = _truncate(ref.summary, prefix_len)
                lines.append(f"  - {mid.ljust(id_width)} {summary_preview}")

    # Parts manifest
    if ctx.parts:
        total = len(ctx.parts)
        if ctx.expand_parts:
            visible = ctx.parts  # --parts: show all
        elif ctx.focus_part is not None:
            visible = [p for p in ctx.parts if abs(p.part_num - ctx.focus_part) <= 1]
        elif total <= 3:
            visible = ctx.parts
        else:
            visible = [ctx.parts[0], ctx.parts[-1]]  # first + last
        part_ids = [f"@P{{{p.part_num}}}" for p in visible]
        id_width = max(len(s) for s in part_ids)
        label = "parts:" if ctx.focus_part is None else f"parts: # {total} total, showing around @P{{{ctx.focus_part}}}"
        lines.append(label)
        if ctx.expand_parts or ctx.focus_part is not None or total <= 3:
            for part, pid in zip(visible, part_ids):
                marker = " *" if ctx.focus_part is not None and part.part_num == ctx.focus_part else ""
                prefix_len = 4 + id_width + 1 + len(marker)
                summary_preview = _truncate(part.summary, prefix_len)
                lines.append(f"  - {pid.ljust(id_width)} {summary_preview}{marker}")
        else:
            # Compact: first, "N more...", last
            first, last = visible[0], visible[1]
            first_pid, last_pid = part_ids[0], part_ids[1]
            prefix_len = 4 + id_width + 1
            lines.append(f"  - {first_pid.ljust(id_width)} {_truncate(first.summary, prefix_len)}")
            lines.append(f"  # (...{total - 2} more...)")
            lines.append(f"  - {last_pid.ljust(id_width)} {_truncate(last.summary, prefix_len)}")

    # Version navigation
    if ctx.prev:
        prev_ids = [f"@V{{{v.offset}}}" for v in ctx.prev]
        id_width = max(len(s) for s in prev_ids)
        lines.append("prev:")
        for vid, v in zip(prev_ids, ctx.prev):
            prefix_len = 4 + id_width + 1 + len(v.date) + 1
            summary_preview = _truncate(v.summary, prefix_len)
            lines.append(f"  - {vid.ljust(id_width)} {v.date} {summary_preview}")
    if ctx.next:
        next_ids = [f"@V{{{v.offset}}}" for v in ctx.next]
        id_width = max(len(s) for s in next_ids)
        lines.append("next:")
        for vid, v in zip(next_ids, ctx.next):
            prefix_len = 4 + id_width + 1 + len(v.date) + 1
            summary_preview = _truncate(v.summary, prefix_len)
            lines.append(f"  - {vid.ljust(id_width)} {v.date} {summary_preview}")
    elif ctx.viewing_offset > 0:
        lines.append("next:")
        lines.append(f"  - @V{{0}}")

    lines.append("---")
    lines.append(item.summary)
    return "\n".join(lines)


def _format_summary_line(item: Item, id_width: int = 0, show_tags: bool = False) -> str:
    """Format item as single summary line: id date summary (with @V{N} only for old versions).

    Args:
        item: The item to format
        id_width: Minimum width for ID column (for alignment across items)
        show_tags: Show non-system tags between date and summary
    """
    # Get version/part-scoped ID
    base_id = item.tags.get("_base_id", item.id)
    part_num = item.tags.get("_part_num")
    version = item.tags.get("_version", "0")
    if part_num:
        suffix = f"@P{{{part_num}}}"
    elif version != "0":
        suffix = f"@V{{{version}}}"
    else:
        suffix = ""
    versioned_id = f"{_shell_quote_id(base_id)}{suffix}"

    # Pad ID for column alignment
    padded_id = versioned_id.ljust(id_width) if id_width else versioned_id

    # Score (when available from find results)
    score_str = f" ({item.score:.2f})" if item.score is not None else ""

    # Get date in local timezone
    date = local_date(item.tags.get("_created") or item.tags.get("_updated", ""))

    # Truncate summary to fit terminal width, collapse newlines
    cols = _output_width()
    prefix_len = len(padded_id) + len(score_str) + 1 + len(date) + 1  # "id (score) date "
    max_summary = max(cols - prefix_len, 20)
    summary = item.summary.replace("\n", " ")
    if len(summary) > max_summary:
        summary = summary[:max_summary - 3].rsplit(" ", 1)[0] + "..."

    line = f"{padded_id}{score_str} {date} {summary}"

    # Show matched part summary when item was uplifted from a part hit
    focus_part = item.tags.get("_focus_part")
    focus_summary = item.tags.get("_focus_summary")
    if focus_part and focus_summary:
        focus_text = focus_summary.replace("\n", " ")
        max_focus = max(cols - 4, 20)  # "  > " prefix
        if len(focus_text) > max_focus:
            focus_text = focus_text[:max_focus - 3].rsplit(" ", 1)[0] + "..."
        line += f"\n  > {focus_text}"

    # Optional non-system tags on a separate line
    if show_tags:
        from .types import SYSTEM_TAG_PREFIX
        user_tags = {k: v for k, v in item.tags.items() if not k.startswith(SYSTEM_TAG_PREFIX)}
        if user_tags:
            pairs = ", ".join(
                f"{k}: {_tag_display_value(v)}" for k, v in sorted(user_tags.items())
            )
            line += f"\n  {{{pairs}}}"

    return line


def _format_versioned_id(item: Item) -> str:
    """Format item ID with version suffix only for old versions: id or id@V{N}."""
    base_id = item.tags.get("_base_id", item.id)
    version = item.tags.get("_version", "0")
    version_suffix = f"@V{{{version}}}" if version != "0" else ""
    return f"{_shell_quote_id(base_id)}{version_suffix}"


@app.callback(invoke_without_command=True)
def main_callback(
    ctx: typer.Context,
    verbose: Annotated[bool, typer.Option(
        "--verbose", "-v",
        help="Enable debug-level logging to stderr",
        callback=_verbose_callback,
        is_eager=True,
    )] = False,
    output_json: Annotated[bool, typer.Option(
        "--json", "-j",
        help="Output as JSON",
        callback=_json_callback,
        is_eager=True,
    )] = False,
    ids_only: Annotated[bool, typer.Option(
        "--ids", "-I",
        help="Output only IDs (for piping to xargs)",
        callback=_ids_callback,
        is_eager=True,
    )] = False,
    full_output: Annotated[bool, typer.Option(
        "--full", "-F",
        help="Output full notes (overrides --ids)",
        callback=_full_callback,
        is_eager=True,
    )] = False,
    version: Annotated[Optional[bool], typer.Option(
        "--version",
        help="Show version and exit",
        callback=_version_callback,
        is_eager=True,
    )] = None,
    store: Annotated[Optional[Path], typer.Option(
        "--store", "-s",
        envvar="KEEP_STORE_PATH",
        help="Path to the store directory",
        callback=_store_callback,
        is_eager=True,
    )] = None,
):
    """Reflective memory with semantic search."""
    # If no subcommand provided, show the current intentions (now)
    if ctx.invoked_subcommand is None:
        # On first run, the wizard handles everything — exit after setup
        from .paths import get_config_dir
        from .setup_wizard import needs_wizard
        override = _get_store_override()
        if os.environ.get("KEEP_CONFIG"):
            config_dir = get_config_dir()
        elif override:
            config_dir = Path(override).resolve()
        else:
            config_dir = get_config_dir()
        if needs_wizard(config_dir):
            _get_keeper(None)  # triggers wizard
            return

        from .api import NOWDOC_ID
        kp = _get_keeper(None)
        ctx_item = kp.get_context(NOWDOC_ID, similar_limit=3, meta_limit=3)
        if ctx_item is None:
            kp.get_now()  # force-create nowdoc
            ctx_item = kp.get_context(NOWDOC_ID, similar_limit=3, meta_limit=3)
        typer.echo(render_context(ctx_item, as_json=_get_json_output()))


# -----------------------------------------------------------------------------
# Common Options
# -----------------------------------------------------------------------------

StoreOption = Annotated[
    Optional[Path],
    typer.Option(
        "--store", "-s",
        envvar="KEEP_STORE_PATH",
        help="Path to the store directory (default: ~/.keep/)"
    )
]


LimitOption = Annotated[
    int,
    typer.Option(
        "--limit", "-n",
        help="Maximum results to return"
    )
]


SinceOption = Annotated[
    Optional[str],
    typer.Option(
        "--since",
        help="Only notes updated since (ISO duration: P3D, P1W, PT1H; or date: 2026-01-15)"
    )
]

UntilOption = Annotated[
    Optional[str],
    typer.Option(
        "--until",
        help="Only notes updated before (ISO duration: P3D, P1W, PT1H; or date: 2026-01-15)"
    )
]



def _versions_to_items(doc_id: str, current: Item | None, versions: list) -> list[Item]:
    """Convert current item + previous VersionInfo list into Items for _format_items."""
    items: list[Item] = []
    if current:
        items.append(current)
    for i, v in enumerate(versions, start=1):
        tags = dict(v.tags)
        tags["_version"] = str(i)
        tags["_updated"] = v.created_at or ""
        tags["_updated_date"] = (v.created_at or "")[:10]
        items.append(Item(id=doc_id, summary=v.summary, tags=tags))
    return items


def _parts_to_items(doc_id: str, current: Item | None, parts: list) -> list[Item]:
    """Convert current item + PartInfo list into Items for _format_items."""
    items: list[Item] = []
    if current:
        items.append(current)
    for p in parts:
        tags = dict(p.tags)
        tags["_part_num"] = str(p.part_num)
        tags["_base_id"] = doc_id
        tags["_updated"] = p.created_at or ""
        items.append(Item(id=doc_id, summary=p.summary, tags=tags))
    return items


def _format_items(items: list[Item], as_json: bool = False, keeper=None, show_tags: bool = False) -> str:
    """Format multiple items for display.

    Args:
        items: List of items to format.
        as_json: Output as JSON instead of text.
        keeper: Optional Keeper instance. When provided, items with
                _focus_part (uplifted from part hits) get their parts
                manifest loaded and windowed around the hit.
        show_tags: Show non-system tags in summary lines (used by --deep).
    """
    if _get_ids_output():
        ids = [_format_versioned_id(item) for item in items]
        return json.dumps(ids) if as_json else "\n".join(ids)

    if as_json:
        deep_groups = getattr(items, "deep_groups", {})

        def _item_dict(item):
            from .types import SYSTEM_TAG_PREFIX
            user_tags = {k: v for k, v in item.tags.items()
                         if not k.startswith(SYSTEM_TAG_PREFIX)}
            return {
                "id": item.id,
                "summary": item.summary,
                "tags": user_tags,
                "score": item.score,
                "created": item.created,
                "updated": item.updated,
            }

        result = []
        seen_deep: set[str] = set()
        for item in items:
            d = _item_dict(item)
            parent_id = item.id.split("@")[0] if "@" in item.id else item.id
            group = deep_groups.get(parent_id, []) or deep_groups.get(item.id, [])
            unseen = [di for di in group if di.id not in seen_deep]
            if unseen:
                d["deep"] = [_item_dict(di) for di in unseen]
                seen_deep.update(di.id for di in unseen)
            result.append(d)
        return json.dumps(result, indent=2)

    if not items:
        return "No results."

    # Full format: YAML frontmatter with double-newline separator
    # Default: summary lines with single-newline separator
    if _get_full_output():
        parts = []
        for item in items:
            focus = item.tags.get("_focus_part")
            focus_int = int(focus) if focus else None
            manifest = None
            if focus_int and keeper:
                manifest = keeper.list_parts(item.id) or None
            ctx = ItemContext(
                item=item,
                parts=[PartRef(part_num=p.part_num, summary=p.summary) for p in manifest] if manifest else [],
                focus_part=focus_int,
            )
            parts.append(_render_frontmatter(ctx))
        return "\n\n".join(parts)

    # Compute ID column width for alignment (capped to avoid long URIs dominating)
    all_items = list(items)
    deep_groups = getattr(items, "deep_groups", {})
    for group in deep_groups.values():
        all_items.extend(group)
    max_id = max(len(_format_versioned_id(item)) for item in all_items)
    id_width = min(max_id, 20)

    # Render with nested deep groups (dedup across items sharing a parent)
    if deep_groups:
        lines = []
        seen_deep: set[str] = set()
        for item in items:
            lines.append(_format_summary_line(item, id_width, show_tags=show_tags))
            parent_id = item.id.split("@")[0] if "@" in item.id else item.id
            for deep_item in deep_groups.get(parent_id, []) or deep_groups.get(item.id, []):
                if deep_item.id in seen_deep:
                    continue
                seen_deep.add(deep_item.id)
                deep_line = _format_summary_line(deep_item, id_width, show_tags=show_tags)
                lines.append("  " + deep_line.replace("\n", "\n  "))
        return "\n".join(lines)

    return "\n".join(_format_summary_line(item, id_width, show_tags=show_tags) for item in items)


NO_PROVIDER_ERROR = """
No embedding provider configured.

To use keep, configure a provider:

  Hosted (simplest — no local setup):
    export KEEPNOTES_API_KEY=...   # Sign up at https://keepnotes.ai

  API-based:
    export VOYAGE_API_KEY=...      # Get at dash.voyageai.com
    export ANTHROPIC_API_KEY=...   # Optional: for better summaries

  Local (macOS Apple Silicon):
    pip install 'keep-skill[local]'

See: https://github.com/keepnotes-ai/keep#installation
"""


def _get_keeper(store: Optional[Path]) -> Keeper:
    """Initialize memory, handling errors gracefully.

    Returns a local Keeper or RemoteKeeper depending on config.
    Both satisfy the same protocol — the CLI doesn't distinguish.
    """
    import atexit

    # Check for remote backend config (env vars or TOML [remote] section)
    api_url = os.environ.get("KEEPNOTES_API_URL", "https://api.keepnotes.ai")
    api_key = os.environ.get("KEEPNOTES_API_KEY")
    if api_url and api_key:
        from .config import get_config_dir, load_or_create_config
        from .remote import RemoteKeeper
        try:
            config_dir = get_config_dir()
            config = load_or_create_config(config_dir)
            kp = RemoteKeeper(api_url, api_key, config)
            atexit.register(kp.close)
            return kp
        except Exception as e:
            typer.echo(f"Error connecting to remote: {e}", err=True)
            raise typer.Exit(1)

    # Check global override from --store on main command
    actual_store = store if store is not None else _get_store_override()
    try:
        # Run setup wizard on first use (no existing config)
        from .paths import get_config_dir
        from .setup_wizard import needs_wizard, run_wizard
        if os.environ.get("KEEP_CONFIG"):
            wizard_config_dir = get_config_dir()
        elif actual_store is not None:
            wizard_config_dir = Path(actual_store).resolve()
        else:
            wizard_config_dir = get_config_dir()
        wizard_config = None
        if needs_wizard(wizard_config_dir):
            store_path = Path(actual_store).resolve() if actual_store else None
            wizard_config = run_wizard(wizard_config_dir, store_path)

        kp = Keeper(actual_store, config=wizard_config)
        # Ensure close() runs before interpreter shutdown to release model locks
        atexit.register(kp.close)

        # After first-time setup: populate store with system docs immediately
        # so prompts, tags, state docs, etc. are available right away.
        if wizard_config and kp._needs_sysdoc_migration:
            is_tty = sys.stderr.isatty()
            try:
                def _setup_progress(current, total, label):
                    if is_tty:
                        _progress_bar(current, total, label, err=True)
                result = kp._migrate_system_documents(progress=_setup_progress)
                kp._needs_sysdoc_migration = False
                n_loaded = result.get("created", 0) + result.get("migrated", 0)
                if is_tty:
                    # Replace progress bar with final summary on same line
                    cols = shutil.get_terminal_size((80, 24)).columns
                    msg = f"  Loaded {n_loaded} system docs." if n_loaded else ""
                    sys.stderr.write("\r" + msg.ljust(cols - 1) + "\n")
                    sys.stderr.flush()
                elif n_loaded:
                    typer.echo(f"  Loaded {n_loaded} system docs.", err=True)
            except Exception as e:
                logger.warning("System doc setup deferred: %s", e)

        # Check for remote config in TOML (loaded during Keeper init)
        if kp.config and kp.config.remote:
            from .remote import RemoteKeeper
            remote = RemoteKeeper(
                kp.config.remote.api_url,
                kp.config.remote.api_key,
                kp.config,
            )
            atexit.register(remote.close)
            kp.close()  # Don't need the local Keeper
            return remote

        # Warn (don't exit) if no embedding provider — read-only ops still work
        if kp.config and kp.config.embedding is None:
            typer.echo(NO_PROVIDER_ERROR.strip(), err=True)
        # Check tool integrations (fast path: dict lookup, no I/O if wizard ran)
        if kp.config and not wizard_config:
            from .integrations import check_and_install
            try:
                check_and_install(kp.config)
            except (OSError, ValueError) as e:
                pass  # Never block normal operation
        return kp
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


def _parse_tags(tags: Optional[list[str]]) -> dict:
    """Parse key=value tag list to multivalue dict."""
    if not tags:
        return {}
    parsed: dict[str, str | list[str]] = {}
    for tag in tags:
        if "=" not in tag:
            hint = f"Error: Invalid tag format '{tag}'."
            if ":" in tag:
                k, v = tag.split(":", 1)
                hint += f" Did you mean: {k}={v}?"
            else:
                hint += " Use key=value"
            typer.echo(hint, err=True)
            raise typer.Exit(1)
        k, v = tag.split("=", 1)
        key = k.casefold()
        existing = parsed.get(key)
        if existing is None:
            parsed[key] = v
        elif isinstance(existing, list):
            if v not in existing:
                existing.append(v)
        elif existing != v:
            parsed[key] = [existing, v]
    return parsed


def _filter_by_tags(items: list, tags: list[str]) -> list:
    """Filter items by tag specifications (AND logic).

    Each tag can be:
    - "key" - item must have this tag key (any value)
    - "key=value" - item must have this exact tag
    """
    if not tags:
        return items

    result = items
    for t in tags:
        if "=" in t:
            key, value = t.split("=", 1)
            key = key.casefold()
            result = [item for item in result
                      if value in tag_values(item.tags, key)]
        elif ":" in t:
            # Colon separator — treat as key=value (common mistake)
            key, value = t.split(":", 1)
            key = key.casefold()
            result = [item for item in result
                      if value in tag_values(item.tags, key)]
        else:
            # Key only - check if key exists
            result = [item for item in result if t.casefold() in item.tags]
    return result


def _parse_frontmatter(text: str) -> tuple[str, dict[str, str]]:
    """Parse YAML frontmatter from text, return (content, tags).

    Extracts all scalar frontmatter values as tags, plus values from
    a ``tags`` dict.  Keys starting with ``_`` are skipped (system reserved).
    Non-scalar values (lists, nested dicts) are dropped except ``tags`` dict.
    """
    from .utils import _extract_markdown_frontmatter
    return _extract_markdown_frontmatter(text)


# -----------------------------------------------------------------------------
# Commands
# -----------------------------------------------------------------------------

@app.command()
def find(
    query: Annotated[Optional[str], typer.Argument(help="Search query text")] = None,
    id: Annotated[Optional[str], typer.Option(
        "--id",
        help="Find notes similar to this ID (instead of text search)"
    )] = None,
    include_self: Annotated[bool, typer.Option(
        help="Include the queried note (only with --id)"
    )] = False,
    tag: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t",
        help="Filter by tag (key or key=value, repeatable)"
    )] = None,
    store: StoreOption = None,
    limit: LimitOption = 10,
    since: SinceOption = None,
    until: UntilOption = None,
    history: Annotated[bool, typer.Option(
        "--history", "-H",
        help="Include versions of matching notes"
    )] = False,
    deep: Annotated[bool, typer.Option(
        "--deep", "-D",
        help="Follow tags from results to discover related items"
    )] = False,
    show_tags: Annotated[bool, typer.Option(
        "--tags",
        help="Show non-system tags for each result"
    )] = False,
    show_all: Annotated[bool, typer.Option(
        "--all", "-a",
        help="Include hidden system notes (IDs starting with '.')"
    )] = False,
    scope: Annotated[Optional[str], typer.Option(
        "--scope", "-S",
        help="ID glob to constrain results (e.g. 'file:///path/to/dir*')"
    )] = None,
    token_budget: Annotated[Optional[int], typer.Option(
        "--tokens",
        help="Token budget for rich context output (includes parts and versions)"
    )] = None,
):
    """Find notes by hybrid search (semantic + full-text) or similarity.

    \b
    Examples:
        keep find "authentication"              # Hybrid search
        keep find --id file:///path/to/doc.md   # Find similar notes
        keep find "auth" -t project=myapp       # Search + filter by tag
        keep find "auth" --history              # Include versions
    """
    if id and query:
        typer.echo("Error: Specify either a query or --id, not both", err=True)
        raise typer.Exit(1)
    if not id and not query:
        typer.echo("Error: Specify a query or --id", err=True)
        raise typer.Exit(1)

    kp = _get_keeper(store)

    # --deep is incompatible with --history (versions replace deep groups)
    if history and deep:
        deep = False

    # Search with higher limit if filtering, then post-filter
    search_limit = limit * 5 if tag else limit

    if id:
        results = kp.find(similar_to=id, limit=search_limit, since=since, until=until, include_self=include_self, include_hidden=show_all, deep=deep, scope=scope)
    else:
        results = kp.find(query, limit=search_limit, since=since, until=until, include_hidden=show_all, deep=deep, scope=scope)

    # Post-filter by tags if specified
    deep_groups = getattr(results, "deep_groups", {})
    if tag:
        results = _filter_by_tags(results, tag)

    from .api import FindResults
    results = FindResults(results[:limit], deep_groups=deep_groups)

    # Expand with versions if requested (--deep is not supported with --history)
    if history:
        expanded: list[Item] = []
        for item in results:
            versions = kp.list_versions(item.id, limit=limit)
            expanded.extend(_versions_to_items(item.id, item, versions))
        results = FindResults(expanded, deep_groups={})

    if _get_json_output():
        typer.echo(_format_items(results, as_json=True, keeper=kp, show_tags=show_tags))
    elif token_budget is not None:
        # When deep is active, cap primaries to leave budget for deep items
        cap = 3 if deep else None
        typer.echo(render_find_context(results, keeper=kp, token_budget=token_budget, show_tags=show_tags, deep_primary_cap=cap))
    else:
        typer.echo(_format_items(results, keeper=kp, show_tags=show_tags))


@app.command("list")
def list_recent(
    store: StoreOption = None,
    limit: LimitOption = 10,
    prefix: Annotated[Optional[str], typer.Argument(
        help="ID filter — prefix (e.g. '.tag') or glob (e.g. 'session-*', '*auth*')"
    )] = None,
    tag: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t",
        help="Filter by tag (key or key=value, repeatable)"
    )] = None,
    tags: Annotated[Optional[str], typer.Option(
        "--tags", "-T",
        help="List tag keys (--tags=), or values for KEY (--tags=KEY)"
    )] = None,
    sort: Annotated[str, typer.Option(
        "--sort",
        help="Sort order: 'updated' (default), 'accessed', 'created', or 'id'"
    )] = "updated",
    since: SinceOption = None,
    until: UntilOption = None,
    history: Annotated[bool, typer.Option(
        "--history", "-H",
        help="Include versions in output"
    )] = False,
    parts: Annotated[bool, typer.Option(
        "--parts", "-P",
        help="Include structural parts (from analyze)"
    )] = False,
    with_parts: Annotated[bool, typer.Option(
        "--with-parts",
        help="Only show notes that have been analyzed into parts"
    )] = False,
    ids_only: Annotated[bool, typer.Option(
        "--ids", "-I",
        help="Output only IDs (for piping to xargs)",
        callback=_ids_callback,
        is_eager=True,
    )] = False,
    show_all: Annotated[bool, typer.Option(
        "--all", "-a",
        help="Include hidden system notes (IDs starting with '.')"
    )] = False,
):
    """List recent notes, filter by tags, or list tag keys/values.

    \b
    Examples:
        keep list                      # Recent notes (by update time)
        keep list .tag                 # All .tag/* system docs
        keep list .meta                # All .meta/* system docs
        keep list session-*            # All session-* items (glob)
        keep list *auth*               # Items with 'auth' anywhere in ID
        keep list --sort accessed      # Recent notes (by access time)
        keep list --sort created       # Sort by creation time
        keep list --sort id            # Sort alphabetically by ID
        keep list --tag foo            # Notes with tag 'foo' (any value)
        keep list --tag foo=bar        # Notes with tag foo=bar
        keep list --tag foo --tag bar  # Notes with both tags
        keep list --tags=              # List all tag keys
        keep list --tags=foo           # List values for tag 'foo'
        keep list --since P3D          # Notes updated in last 3 days
        keep list --until 2026-01-15   # Notes updated before date
        keep list --history            # Include versions
        keep list --parts              # Include analyzed parts
    """
    kp = _get_keeper(store)

    # --tags mode: list keys or values
    if tags is not None:
        # Empty string means list all keys, otherwise list values for key
        key = tags if tags else None
        values = kp.list_tags(key)
        if _get_json_output():
            typer.echo(json.dumps(values))
        else:
            if not values:
                if key:
                    typer.echo(f"No values for tag '{key}'.")
                else:
                    typer.echo("No tags found.")
            else:
                for v in values:
                    typer.echo(v)
        return

    # Build unified filter kwargs
    kwargs: dict = {
        "limit": limit, "order_by": sort,
        "since": since, "until": until,
        "include_hidden": show_all, "include_history": history,
    }

    if prefix is not None:
        kwargs["prefix"] = prefix
        kwargs["include_hidden"] = True  # prefix queries always include hidden

    if tag:
        tag_dict: dict[str, str] = {}
        tag_key_list: list[str] = []
        for t in tag:
            if "=" in t:
                k, v = t.split("=", 1)
                tag_dict[k] = v
            else:
                tag_key_list.append(t)
        if tag_dict:
            kwargs["tags"] = tag_dict
        if tag_key_list:
            kwargs["tag_keys"] = tag_key_list

    results = kp.list_items(**kwargs)

    # Filter to only items with parts
    if with_parts:
        doc_coll = kp._resolve_doc_collection()
        results = [item for item in results
                   if kp._document_store.part_count(doc_coll, item.id) > 0]

    # Expand with parts if requested
    if parts:
        expanded: list[Item] = []
        for item in results:
            part_list = kp.list_parts(item.id)
            if part_list:
                expanded.extend(_parts_to_items(item.id, item, part_list))
            else:
                expanded.append(item)
        results = expanded

    typer.echo(_format_items(results, as_json=_get_json_output()))


@app.command("tag")
def tag(
    ids: Annotated[list[str], typer.Argument(default=..., help="Note IDs to tag")],
    tags: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t",
        help="Tag to add/update as key=value"
    )] = None,
    remove: Annotated[Optional[list[str]], typer.Option(
        "--remove", "-r",
        help="Tag keys to remove"
    )] = None,
    remove_values: Annotated[Optional[list[str]], typer.Option(
        "--remove-value", "-R",
        help="Tag values to remove as key=value"
    )] = None,
    store: StoreOption = None,
):
    """Add, update, or remove tags on existing notes.

    Does not re-process the note - only updates tags.

    \b
    Examples:
        keep tag doc:1 --tag project=myapp
        keep tag doc:1 doc:2 --tag status=reviewed
        keep tag doc:1 --remove obsolete_tag
        keep tag doc:1 --remove-value speaker=Bob
    """
    kp = _get_keeper(store)

    # Parse tags from key=value format
    tag_changes = _parse_tags(tags)
    remove_value_changes = _parse_tags(remove_values)

    # Explicit deletion path: --remove for keys, --remove-value for values.
    # Keep --tag key= for backwards compatibility, but steer callers to --remove.
    for key in tag_changes:
        if "" in tag_values(tag_changes, key):
            typer.echo(
                f"Error: Empty --tag value for '{key}'. Use --remove {key} instead.",
                err=True,
            )
            raise typer.Exit(1)
    for key in remove_value_changes:
        if "" in tag_values(remove_value_changes, key):
            typer.echo(
                f"Error: Empty --remove-value for '{key}'. Use --remove {key} instead.",
                err=True,
            )
            raise typer.Exit(1)

    if not tag_changes and not remove and not remove_value_changes:
        typer.echo("Error: Specify at least one --tag, --remove, or --remove-value", err=True)
        raise typer.Exit(1)

    # Process each document (route parts to tag_part)
    results = []
    for doc_id in ids:
        match = PART_SUFFIX_PATTERN.search(doc_id)
        try:
            if match:
                part_num = int(match.group(1))
                base_id = doc_id[:match.start()]
                part = kp.tag_part(
                    base_id,
                    part_num,
                    tags=tag_changes or None,
                    remove=remove,
                    remove_values=remove_value_changes or None,
                )
                if part is None:
                    typer.echo(f"Part not found: {doc_id}", err=True)
                else:
                    typer.echo(f"Updated {doc_id}")
            else:
                item = kp.tag(
                    doc_id,
                    tags=tag_changes or None,
                    remove=remove,
                    remove_values=remove_value_changes or None,
                )
                if item is None:
                    typer.echo(f"Not found: {doc_id}", err=True)
                else:
                    results.append(item)
        except ValueError as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(1)

    if results:
        typer.echo(_format_items(results, as_json=_get_json_output()))


@app.command("tag-update", hidden=True)
def tag_update(
    ids: Annotated[list[str], typer.Argument(default=..., help="Note IDs to tag")],
    tags: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t",
        help="Tag to add/update as key=value"
    )] = None,
    remove: Annotated[Optional[list[str]], typer.Option(
        "--remove", "-r",
        help="Tag keys to remove"
    )] = None,
    remove_values: Annotated[Optional[list[str]], typer.Option(
        "--remove-value", "-R",
        help="Tag values to remove as key=value"
    )] = None,
    store: StoreOption = None,
):
    """Hidden compatibility alias for 'tag'."""
    tag(
        ids=ids,
        tags=tags,
        remove=remove,
        remove_values=remove_values,
        store=store,
    )


def _handle_watch(
    kp: "Keeper",
    watch: bool,
    unwatch: bool,
    source: str,
    kind: str,
    parsed_tags: dict,
    *,
    recurse: bool = False,
    exclude: list[str] | None = None,
    interval: str | None = None,
) -> None:
    """Add or remove a watch entry if --watch or --unwatch was given."""
    if not watch and not unwatch:
        return
    from .watches import add_watch, remove_watch
    if watch:
        kwargs: dict = {}
        if interval:
            kwargs["interval"] = interval
        try:
            entry = add_watch(
                kp, source, kind,
                tags=parsed_tags or {},
                recurse=recurse,
                exclude=exclude or [],
                max_watches=kp.config.max_watches,
                **kwargs,
            )
            typer.echo(f"Watching {kind}: {source} (interval {entry.interval})", err=True)
        except ValueError as e:
            typer.echo(f"Watch error: {e}", err=True)
    elif unwatch:
        if remove_watch(kp, source):
            typer.echo(f"Stopped watching: {source}", err=True)
        else:
            typer.echo(f"Not watching: {source}", err=True)


def _put_store(
    kp: "Keeper",
    source: Optional[str],
    resolved_path: Optional[Path],
    parsed_tags: dict,
    id: Optional[str],
    summary: Optional[str],
    force: bool = False,
    recurse: bool = False,
    exclude: list[str] | None = None,
    watch: bool = False,
    unwatch: bool = False,
    interval: str | None = None,
) -> Optional["Item"]:
    """Execute the store operation for put(). Returns Item, or None for directory mode."""
    if source == "-" or (source is None and _has_stdin_data()):
        # Stdin mode: explicit '-' or piped input
        try:
            content = sys.stdin.read()
        except UnicodeDecodeError:
            typer.echo("Error: stdin contains binary data (not valid UTF-8)", err=True)
            typer.echo("Hint: for binary files, use: keep put file:///path/to/file", err=True)
            raise typer.Exit(1)
        content, frontmatter_tags = _parse_frontmatter(content)
        parsed_tags = {**frontmatter_tags, **parsed_tags}  # CLI tags override
        if summary is not None:
            typer.echo("Error: --summary cannot be used with stdin input (original content would be lost)", err=True)
            typer.echo("Hint: write to a file first, then: keep put file:///path/to/file --summary '...'", err=True)
            raise typer.Exit(1)
        # Use content-addressed ID for stdin text (enables versioning)
        doc_id = id or _text_content_id(content)
        return kp.put(content, id=doc_id, tags=parsed_tags or None, force=force)
    elif resolved_path is not None and resolved_path.is_dir():
        # Directory mode: index files in directory
        if summary is not None:
            typer.echo("Error: --summary cannot be used with directory mode", err=True)
            raise typer.Exit(1)
        if id is not None:
            typer.echo("Error: --id cannot be used with directory mode", err=True)
            raise typer.Exit(1)
        from .ignore import merge_excludes
        combined_exclude = merge_excludes(kp._load_ignore_patterns(), exclude)
        files = _list_directory_files(resolved_path, recurse=recurse, exclude=combined_exclude or None)
        if not files:
            typer.echo(f"Error: no eligible files in {resolved_path}/", err=True)
            hint = "hidden files and symlinks are skipped"
            if not recurse:
                hint += "; use -r to recurse into subdirectories"
            typer.echo(f"Hint: {hint}", err=True)
            raise typer.Exit(1)
        max_files = kp.config.max_dir_files
        if len(files) > max_files:
            typer.echo(f"Error: directory has {len(files)} files (max {max_files})", err=True)
            typer.echo("Hint: increase max_dir_files in keep.toml or index files individually", err=True)
            raise typer.Exit(1)
        results: list[Item] = []
        errors: list[str] = []
        total = len(files)
        is_tty = sys.stderr.isatty()
        for i, fpath in enumerate(files, 1):
            file_uri = f"file://{fpath}"
            rel = fpath.relative_to(resolved_path) if recurse else fpath.name
            try:
                item = kp.put(uri=file_uri, tags=parsed_tags or None, force=force)
                results.append(item)
                if is_tty:
                    _progress_bar(i, total, str(rel), err=True)
                else:
                    typer.echo(f"[{i}/{total}] {rel} ok", err=True)
            except Exception as e:
                errors.append(f"{rel}: {e}")
                if is_tty:
                    _progress_bar(i, total, f"{rel} ERROR", err=True)
                else:
                    typer.echo(f"[{i}/{total}] {rel} error: {e}", err=True)
        if is_tty:
            typer.echo("", err=True)  # newline after progress bar
        indexed = len(results)
        skipped = len(errors)
        typer.echo(f"{indexed} indexed, {skipped} errors from {resolved_path.name}/", err=True)
        if errors:
            for e in errors:
                typer.echo(f"  error: {e}", err=True)
        if results:
            typer.echo(_format_items(results, as_json=_get_json_output()))

        # Git changelog ingest: enqueue for background processing
        from .git_ingest import is_git_repo
        if is_git_repo(resolved_path):
            try:
                kp._get_work_queue().enqueue(
                    "ingest_git",
                    {"item_id": f"file://{resolved_path}", "directory": str(resolved_path)},
                    supersede_key=f"git:{resolved_path}",
                    priority=8,  # lower priority than summarize/analyze
                )
                typer.echo("git: changelog ingest queued", err=True)
            except Exception as e:
                logger.warning("Failed to queue git ingest: %s", e)

        _handle_watch(kp, watch, unwatch, str(resolved_path), "directory",
                      parsed_tags, recurse=recurse, exclude=exclude, interval=interval)
        return None
    elif resolved_path is not None and resolved_path.is_file():
        # File mode: bare file path → normalize to file:// URI
        file_uri = f"file://{resolved_path}"
        item = kp.put(uri=file_uri, id=id or None, tags=parsed_tags or None, summary=summary, force=force)
        _handle_watch(kp, watch, unwatch, file_uri, "file", parsed_tags, interval=interval)
        return item
    elif source and _URI_SCHEME_PATTERN.match(source):
        # URI mode: fetch from URI (--id overrides the document ID)
        item = kp.put(uri=source, id=id or None, tags=parsed_tags or None, summary=summary, force=force)
        _handle_watch(kp, watch, unwatch, source, "url", parsed_tags, interval=interval)
        return item
    elif source:
        # Text mode: inline content (no :// in source)
        if summary is not None:
            typer.echo("Error: --summary cannot be used with inline text (original content would be lost)", err=True)
            typer.echo("Hint: write to a file first, then: keep put file:///path/to/file --summary '...'", err=True)
            raise typer.Exit(1)
        # Use content-addressed ID for text (enables versioning)
        doc_id = id or _text_content_id(source)
        return kp.put(source, id=doc_id, tags=parsed_tags or None, force=force)
    else:
        typer.echo("Error: Provide content, URI, or '-' for stdin", err=True)
        raise typer.Exit(1)


@app.command("put")
def put(
    source: Annotated[Optional[str], typer.Argument(
        help="URI to fetch, text content, or '-' for stdin"
    )] = None,
    id: Annotated[Optional[str], typer.Option(
        "--id", "-i",
        help="Note ID (auto-generated for text/stdin modes)"
    )] = None,
    store: StoreOption = None,
    tags: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t",
        help="Tag as key=value (can be repeated)"
    )] = None,
    summary: Annotated[Optional[str], typer.Option(
        "--summary",
        help="User-provided summary (skips auto-summarization)"
    )] = None,
    suggest_tags: Annotated[bool, typer.Option(
        "--suggest-tags",
        help="Show tag suggestions from similar notes"
    )] = False,
    recurse: Annotated[bool, typer.Option(
        "--recurse", "-r",
        help="Recurse into subdirectories (directory mode)"
    )] = False,
    exclude: Annotated[Optional[list[str]], typer.Option(
        "--exclude", "-x",
        help="Glob pattern to exclude files (directory mode, repeatable)"
    )] = None,
    watch: Annotated[bool, typer.Option(
        "--watch",
        help="Watch this source for changes (daemon re-imports on change)"
    )] = False,
    unwatch: Annotated[bool, typer.Option(
        "--unwatch",
        help="Stop watching this source for changes"
    )] = False,
    interval: Annotated[Optional[str], typer.Option(
        "--interval",
        help="Watch poll interval as ISO 8601 duration (default PT30S, e.g. PT5M, P1D)"
    )] = None,
    _analyze: Annotated[bool, typer.Option(
        "--analyze", hidden=True, help="(deprecated, no-op)"
    )] = False,
    force: Annotated[bool, typer.Option(
        "--force",
        help="Re-process even if content is unchanged"
    )] = False,
):
    """Add or update a note in the store.

    \b
    Input modes (auto-detected):
      keep put /path/to/folder/      # Directory mode: index top-level files
      keep put /path/to/folder/ -r   # Directory mode: recurse into subdirs
      keep put /path/to/file.pdf     # File mode: index single file
      keep put file:///path          # URI mode: has ://
      keep put "my note"             # Text mode: content-addressed ID
      keep put -                     # Stdin mode: explicit -
      echo "pipe" | keep put         # Stdin mode: piped input

    \b
    Directory mode skips hidden files/dirs and symlinks.
    Use --exclude/-x to skip additional patterns (repeatable, fnmatch globs):
      keep put ./src/ -r -x "*.pyc" -x "test_*"

    \b
    Text mode uses content-addressed IDs for versioning:
      keep put "my note"           # Creates %{hash}
      keep put "my note" -t done   # Same ID, new version (tag change)
      keep put "different note"    # Different ID (new doc)
    """
    if watch and unwatch:
        typer.echo("Error: --watch and --unwatch are mutually exclusive", err=True)
        raise typer.Exit(1)
    if interval and not watch:
        typer.echo("Error: --interval requires --watch", err=True)
        raise typer.Exit(1)
    if interval:
        from .watches import parse_duration
        try:
            parse_duration(interval)
        except ValueError:
            typer.echo(f"Error: invalid interval {interval!r} (use ISO 8601: PT30S, PT5M, P1D)", err=True)
            raise typer.Exit(1)

    kp = _get_keeper(store)
    parsed_tags = _parse_tags(tags)

    # Determine mode based on source content
    # Check for filesystem path (directory or file) before other modes
    resolved_path = _is_filesystem_path(source) if source and source != "-" else None

    try:
        item = _put_store(
            kp, source, resolved_path, parsed_tags, id, summary, force,
            recurse=recurse, exclude=exclude, watch=watch, unwatch=unwatch,
            interval=interval,
        )
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)
    if item is None:
        return  # directory mode already printed output

    # Surface similar items (occasion for reflection)
    suggest_limit = 10 if suggest_tags else 3
    ctx = kp.get_context(
        item.id, similar_limit=min(suggest_limit, 3),
        include_meta=False, include_parts=False, include_versions=False,
    )
    typer.echo(render_context(ctx, as_json=_get_json_output()))

    # Show tag suggestions from similar items (needs more than 3)
    if suggest_tags:
        similar_items = kp.get_similar_for_display(item.id, limit=suggest_limit) if suggest_limit > 3 else []
        tag_counts: dict[str, int] = {}
        for si in similar_items:
            for k, v in si.tags.items():
                if k.startswith("_"):
                    continue
                tag = f"{k}={v}" if v else k
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
        if tag_counts:
            # Sort by frequency (descending), then alphabetically
            sorted_tags = sorted(tag_counts.items(), key=lambda x: (-x[1], x[0]))
            typer.echo("\nsuggested tags:")
            for tag, count in sorted_tags:
                typer.echo(f"  -t {tag}  ({count})")
            typer.echo(f"\napply with: keep tag {_shell_quote_id(item.id)} -t TAG")


@app.command("update", hidden=True)
def update(
    source: Annotated[Optional[str], typer.Argument(help="URI to fetch, text content, or '-' for stdin")] = None,
    id: Annotated[Optional[str], typer.Option("--id", "-i")] = None,
    store: StoreOption = None,
    tags: Annotated[Optional[list[str]], typer.Option("--tag", "-t")] = None,
    summary: Annotated[Optional[str], typer.Option("--summary")] = None,
):
    """Add or update a note (alias for 'put')."""
    put(source=source, id=id, store=store, tags=tags, summary=summary)


@app.command("add", hidden=True)
def add(
    source: Annotated[Optional[str], typer.Argument(help="URI to fetch, text content, or '-' for stdin")] = None,
    id: Annotated[Optional[str], typer.Option("--id", "-i")] = None,
    store: StoreOption = None,
    tags: Annotated[Optional[list[str]], typer.Option("--tag", "-t")] = None,
    summary: Annotated[Optional[str], typer.Option("--summary")] = None,
):
    """Add a note (alias for 'put')."""
    put(source=source, id=id, store=store, tags=tags, summary=summary)


@app.command()
def now(
    content: Annotated[Optional[str], typer.Argument(
        help="Content to set (omit to show current)"
    )] = None,
    reset: Annotated[bool, typer.Option(
        "--reset",
        help="Reset to default from system"
    )] = False,
    version: Annotated[Optional[int], typer.Option(
        "--version", "-V",
        help="Version selector (>=0 from current, <0 from oldest: -1 oldest)"
    )] = None,
    history: Annotated[bool, typer.Option(
        "--history", "-H",
        help="List all versions"
    )] = False,
    scope: Annotated[Optional[str], typer.Option(
        "--scope",
        help="Scope for multi-user isolation (e.g. user ID)"
    )] = None,
    store: StoreOption = None,
    tags: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t",
        help="Set tag (with content) or filter (without content)"
    )] = None,
    limit: Annotated[int, typer.Option(
        "--limit", "-n",
        help="Max similar/meta notes to show (default 3)"
    )] = 3,
):
    """Get or set the current working intentions.

    With no arguments, displays the current intentions.
    With content, replaces it.

    \b
    Tags behave differently based on mode:
    - With content: -t sets tags on the update
    - Without content: -t filters version history

    \b
    Examples:
        keep now                         # Show current intentions
        keep now "What's important now"  # Update intentions
        keep now "Auth work" -t project=myapp  # Update with tag
        keep now -t project=myapp        # Find version with tag
        keep now -n 10                   # Show with more similar/meta items
        keep now --reset                 # Reset to default from system
        keep now -V 1                    # Previous version
        keep now --history               # List all versions
    """
    from .api import NOWDOC_ID

    # Expand ${.field} templates from stdin JSON (hooks support)
    has_tpl = _has_templates(content) or (tags and any(_has_templates(t) for t in tags))
    if has_tpl:
        (content,) = _expand_stdin_templates(content)
        tags = _expand_stdin_tag_list(tags)

    kp = _get_keeper(store)
    doc_id = f"now:{scope}" if scope else NOWDOC_ID

    # Handle history listing
    if history:
        # --ids: flat list for piping
        if _get_ids_output():
            versions = kp.list_versions(doc_id, limit=limit)
            current = kp.get(doc_id)
            items = _versions_to_items(doc_id, current, versions)
            typer.echo(_format_items(items))
            return
        # Default: expanded frontmatter with all versions
        ctx = kp.get_context(doc_id)
        if ctx is None:
            typer.echo("Not found", err=True)
            raise typer.Exit(1)
        all_versions = kp.list_versions(doc_id, limit=limit)
        ctx.prev = [
            VersionRef(
                offset=i + 1,
                date=local_date(v.tags.get("_created") or v.created_at or ""),
                summary=v.summary,
            )
            for i, v in enumerate(all_versions)
        ]
        ctx.next = []
        typer.echo(render_context(ctx, as_json=_get_json_output()))
        return

    # Handle version retrieval
    if version is not None:
        ctx = kp.get_context(
            doc_id, version=version,
            include_similar=False, include_meta=False, include_parts=False,
        )
        if ctx is None:
            typer.echo(f"Version not found: {doc_id}@V{{{version}}}", err=True)
            raise typer.Exit(1)
        typer.echo(render_context(ctx, as_json=_get_json_output()))
        return

    # Read from stdin if piped and no content argument
    if content is None and not reset and _has_stdin_data():
        try:
            content = sys.stdin.read().strip() or None
        except UnicodeDecodeError:
            typer.echo("Error: stdin contains binary data (not valid UTF-8)", err=True)
            raise typer.Exit(1)

    # Determine if we're getting or setting
    setting = content is not None or reset

    if setting:
        if reset:
            # Reset to default from system (delete first to clear old tags)
            from .system_docs import _load_frontmatter, SYSTEM_DOC_DIR
            kp.delete(doc_id)
            try:
                new_content, default_tags = _load_frontmatter(SYSTEM_DOC_DIR / "now.md")
                parsed_tags = default_tags
            except FileNotFoundError:
                typer.echo("Error: Builtin now.md not found", err=True)
                raise typer.Exit(1)
        else:
            new_content = content
            parsed_tags = {}

        # Parse user-provided tags (merge with default if reset)
        parsed_tags.update(_parse_tags(tags))

        kp.set_now(new_content, scope=scope, tags=parsed_tags or None)

        # Surface context (occasion for reflection)
        ctx = kp.get_context(doc_id, similar_limit=limit, meta_limit=limit)
        typer.echo(render_context(ctx, as_json=_get_json_output()))
    else:
        # Get current intentions (or search version history if tags specified)
        if tags:
            # Search version history for most recent version with matching tags
            item = _find_now_version_by_tags(kp, tags, scope=scope)
            if item is None:
                typer.echo("No version found matching tags", err=True)
                raise typer.Exit(1)
            # No version nav or similar items for filtered results
            typer.echo(render_context(ItemContext(item=item), as_json=_get_json_output()))
        else:
            # Standard: get current with version navigation and similar items
            ctx = kp.get_context(doc_id, similar_limit=limit, meta_limit=limit)
            if ctx is None:
                kp.get_now(scope=scope)  # force-create
                ctx = kp.get_context(doc_id, similar_limit=limit, meta_limit=limit)
            typer.echo(render_context(ctx, as_json=_get_json_output()))


def _find_now_version_by_tags(kp, tags: list[str], *, scope: Optional[str] = None):
    """Search nowdoc version history for most recent version matching all tags.

    Checks current version first, then scans previous versions.
    """
    from .api import NOWDOC_ID
    doc_id = f"now:{scope}" if scope else NOWDOC_ID

    # Parse tag filters
    tag_filters = []
    for t in tags:
        if "=" in t:
            key, value = t.split("=", 1)
            tag_filters.append((key, value))
        else:
            tag_filters.append((t, None))  # Key only

    def matches_tags(item_tags: dict) -> bool:
        for key, value in tag_filters:
            if value is not None:
                if value not in tag_values(item_tags, key):
                    return False
            else:
                if key not in item_tags:
                    return False
        return True

    # Check current version first
    current = kp.get_now(scope=scope)
    if current and matches_tags(current.tags):
        return current

    # Scan previous versions (newest first)
    versions = kp.list_versions(doc_id, limit=100)
    for i, v in enumerate(versions):
        if matches_tags(v.tags):
            # Found match - get full item at this version offset
            return kp.get_version(doc_id, i + 1)

    return None


def _render_binding(name: str, binding: dict, kp=None, token_budget: int = 4000) -> str:
    """Render a single flow binding to text by dispatching on shape."""
    from .types import Item

    # Find-action results: {"results": [...], "count": N}
    results = binding.get("results")
    if isinstance(results, list) and results:
        items = _dicts_to_items(results, summary_limit=500)
        if items:
            return render_find_context(items, keeper=kp, token_budget=token_budget)

    # Get-action result: {"id": ..., "summary": ..., "tags": ...}
    if "id" in binding and "summary" in binding:
        item = Item(
            id=str(binding["id"]),
            summary=str(binding.get("summary", "")),
            tags=dict(binding.get("tags") or {}),
        )
        from ._context_resolution import ItemContext
        ctx = ItemContext(item=item, viewing_offset=0, similar=[], meta={}, edges={}, parts=[], prev=[], next=[])
        return render_context(ctx)

    # Meta sections: {"sections": {"learnings": [...], "todo": [...]}}
    sections = binding.get("sections")
    if isinstance(sections, dict):
        lines = []
        for section_name, section_items in sections.items():
            if isinstance(section_items, list) and section_items:
                items = _dicts_to_items(section_items, summary_limit=300)
                if items:
                    lines.append(f"meta/{section_name}:")
                    for item in items:
                        sid = item.id[:20]
                        summary = item.summary.replace("\n", " ")[:120]
                        lines.append(f"  - {sid} {summary}")
        return "\n".join(lines)

    # Edges: {"edges": {...}}
    edges = binding.get("edges")
    if isinstance(edges, dict) and edges:
        lines = []
        for tag_key, edge_list in edges.items():
            if isinstance(edge_list, list):
                for e in edge_list:
                    if isinstance(e, dict):
                        lines.append(f"  {tag_key}: {e.get('id', '')} {str(e.get('summary', ''))[:100]}")
        return "\n".join(lines)

    return ""


def expand_prompt(result: "PromptResult", kp=None) -> str:
    """Expand placeholders in a prompt template.

    Supports:
      {get}                 — rendered item context
      {find[:deep][:budget]} — search results
      {text}, {since}, {until} — raw filter values
      {binding_name}        — flow binding (when state doc is used)
    """
    output = result.prompt

    # Expand {get} with rendered context
    if result.context:
        get_rendered = render_context(result.context)
    else:
        get_rendered = ""
    output = output.replace("{get}", get_rendered)

    # Expand {find} variants with token-budgeted context.
    # Syntax: {find[:deep][:budget]}
    # When :deep is present, primary cap is applied automatically.
    _find_re = re.compile(r'\{find(?::(deep))?(?::(\d+))?\}')
    def _expand_find(m):
        if not result.search_results:
            return ""
        is_deep = m.group(1) is not None
        budget_str = m.group(2)
        # Explicit CLI --tokens always wins over template budget
        if result.token_budget is not None:
            budget = result.token_budget
        elif budget_str:
            budget = int(budget_str)
        else:
            budget = 4000
        cap = 3 if is_deep else None
        return render_find_context(
            result.search_results, keeper=kp,
            token_budget=budget,
            deep_primary_cap=cap,
        )
    output = _find_re.sub(_expand_find, output)

    # Expand flow bindings: {binding_name} -> rendered binding
    if result.flow_bindings:
        budget = result.token_budget or 4000
        # Distribute budget across bindings
        n_bindings = sum(1 for b in result.flow_bindings.values() if b)
        per_binding = budget // max(n_bindings, 1)
        for name, binding in result.flow_bindings.items():
            placeholder = "{" + name + "}"
            if placeholder in output:
                rendered = _render_binding(name, binding, kp=kp, token_budget=per_binding)
                output = output.replace(placeholder, rendered)

    # Expand {text}, {since}, {until} with raw filter values
    output = output.replace("{text}", result.text or "")
    output = output.replace("{since}", result.since or "")
    output = output.replace("{until}", result.until or "")

    # Clean up blank lines from empty expansions
    while "\n\n\n" in output:
        output = output.replace("\n\n\n", "\n\n")

    return output.strip()


@app.command()
def prompt(
    name: Annotated[str, typer.Argument(
        help="Prompt name (e.g. 'reflect')"
    )] = "",
    text: Annotated[Optional[str], typer.Argument(
        help="Optional text for context search"
    )] = None,
    list_prompts: Annotated[bool, typer.Option(
        "--list", "-l",
        help="List available agent prompts"
    )] = False,
    id: Annotated[Optional[str], typer.Option(
        "--id",
        help="Item ID for {get} context (default: 'now')"
    )] = None,
    tag: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t",
        help="Filter context by tag (key=value, repeatable)"
    )] = None,
    since: SinceOption = None,
    until: UntilOption = None,
    deep: Annotated[bool, typer.Option(
        "--deep", "-D",
        help="Follow tags from results to discover related items"
    )] = False,
    token_budget: Annotated[Optional[int], typer.Option(
        "--tokens",
        help="Token budget for {find} context (template default if not set)"
    )] = None,
    store: StoreOption = None,
):
    """Render an agent prompt with injected context.

    \b
    The prompt doc may contain {get} and {find} placeholders:
      {get}  — expanded with context for --id (default: now)
      {find} — expanded with search results for the text argument

    \b
    Examples:
        keep prompt --list                        # List available prompts
        keep prompt reflect                       # Reflect on current work
        keep prompt reflect "auth flow"           # Reflect with search context
        keep prompt reflect --id %abc123          # Context from specific item
        keep prompt reflect --since P7D           # Recent context only
        keep prompt reflect --tag project=myapp   # Scoped to project
    """
    # Expand ${.field} templates from stdin JSON (hooks support)
    tag = _expand_stdin_tag_list(tag)

    kp = _get_keeper(store)

    if list_prompts or not name:
        prompts = kp.list_prompts()
        if not prompts:
            typer.echo("No agent prompts available.", err=True)
            raise typer.Exit(1)
        for p in prompts:
            typer.echo(f"{p.name:20s} {p.summary}")
        return

    tags_dict = _parse_tags(tag) if tag else None
    result = kp.render_prompt(
        name, text, id=id, since=since, until=until, tags=tags_dict,
        deep=deep, token_budget=token_budget,
    )
    if result is None:
        typer.echo(f"Prompt not found: {name}", err=True)
        raise typer.Exit(1)

    if _get_json_output():
        items = result.search_results or []
        out = {
            "prompt": expand_prompt(result, kp),
            "context": result.context.to_dict() if result.context else None,
            "results": [
                {
                    "id": item.id,
                    "summary": item.summary,
                    "tags": _filter_display_tags(item.tags),
                    "score": item.score,
                    "created": item.created,
                    "updated": item.updated,
                }
                for item in items
            ],
        }
        typer.echo(json.dumps(out, indent=2))
    else:
        typer.echo(expand_prompt(result, kp))


@app.command(hidden=True)
def reflect(
    text: Annotated[Optional[str], typer.Argument(
        help="Optional text for context search"
    )] = None,
    id: Annotated[Optional[str], typer.Option(
        "--id",
        help="Item ID for {get} context (default: 'now')"
    )] = None,
    store: StoreOption = None,
):
    """Reflect on current actions (alias for 'keep prompt reflect')."""
    kp = _get_keeper(store)
    result = kp.render_prompt("reflect", text, id=id)
    if result is None:
        typer.echo("Prompt 'reflect' not found. Is the store initialized?", err=True)
        raise typer.Exit(1)

    typer.echo(expand_prompt(result, kp))


@app.command()
def move(
    name: Annotated[str, typer.Argument(help="Target note name")],
    tags: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t",
        help="Only extract versions matching these tags (key=value)"
    )] = None,
    from_source: Annotated[Optional[str], typer.Option(
        "--from",
        help="Source note to extract from (default: now)"
    )] = None,
    only: Annotated[bool, typer.Option(
        "--only",
        help="Move only the current (tip) version"
    )] = False,
    _analyze: Annotated[bool, typer.Option(
        "--analyze", hidden=True, help="(deprecated, no-op)"
    )] = False,
    store: StoreOption = None,
):
    """Move versions from now (or another item) into a named item.

    Requires either -t (tag filter) or --only (tip only).
    With -t, matching versions are extracted from the source.
    With --only, just the current version is moved.
    With --from, extract from a specific item instead of now.
    """
    # Expand ${.field} templates from stdin JSON (hooks support)
    (name,) = _expand_stdin_templates(name)
    tags = _expand_stdin_tag_list(tags)

    if not tags and not only:
        typer.echo(
            "Error: use -t to filter by tags, or --only to move just the current version",
            err=True,
        )
        raise typer.Exit(1)

    kp = _get_keeper(store)
    tag_filter = _parse_tags(tags) if tags else None
    source_id = from_source if from_source else None

    try:
        kwargs: dict = {"tags": tag_filter, "only_current": only}
        if source_id:
            kwargs["source_id"] = source_id
        saved = kp.move(name, **kwargs)
    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    as_json = _get_json_output()
    versions = kp.list_versions(name, limit=100)
    items = _versions_to_items(name, saved, versions)
    typer.echo(_format_items(items, as_json=as_json))


@app.command()
def get(
    id: Annotated[list[str], typer.Argument(help="URI(s) of note(s) (append @V{N} for version)")],
    version: Annotated[Optional[int], typer.Option(
        "--version", "-V",
        help="Version selector (>=0 from current, <0 from oldest: -1 oldest)"
    )] = None,
    history: Annotated[bool, typer.Option(
        "--history", "-H",
        help="List all versions"
    )] = False,
    similar: Annotated[bool, typer.Option(
        "--similar", "-S",
        help="List similar notes"
    )] = False,
    meta: Annotated[bool, typer.Option(
        "--meta", "-M",
        help="List meta notes"
    )] = False,
    resolve: Annotated[Optional[list[str]], typer.Option(
        "--resolve", "-R",
        help="Inline meta query (metadoc syntax, repeatable)"
    )] = None,
    parts: Annotated[bool, typer.Option(
        "--parts", "-P",
        help="List structural parts (from analyze)"
    )] = False,
    tag: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t",
        help="Require tag (key or key=value, repeatable)"
    )] = None,
    limit: Annotated[int, typer.Option(
        "--limit", "-n",
        help="Max notes for --history, --similar, or --meta (default: 10)"
    )] = 10,
    store: StoreOption = None,
):
    """Retrieve note(s) by ID.

    Accepts one or more IDs. Version identifiers: Append @V{N} to get a specific version.
    N>=0 selects from current; N<0 selects from oldest archived (-1 oldest).
    Part identifiers: Append @P{N} to get a specific part.

    \b
    Examples:
        keep get doc:1                  # Current version with similar notes
        keep get doc:1 doc:2 doc:3      # Multiple notes
        keep get doc:1 -V 1             # Previous version with prev/next nav
        keep get doc:1 -V -1            # Oldest archived version
        keep get "doc:1@V{1}"           # Same as -V 1
        keep get "doc:1@V{-1}"          # Same as -V -1
        keep get "doc:1@P{1}"           # Part 1 of analyzed note
        keep get doc:1 --history        # List all versions
        keep get doc:1 --parts          # List structural parts
        keep get doc:1 --similar        # List similar items
        keep get doc:1 --meta           # List meta items
        keep get doc:1 -t project=myapp # Only if tag matches
    """
    kp = _get_keeper(store)
    outputs = []
    errors = []

    for one_id in id:
        result = _get_one(kp, one_id, version, history, similar, meta, resolve, tag, limit, parts)
        if result is None:
            errors.append(one_id)
        else:
            outputs.append(result)

    if outputs:
        separator = "\n" if _get_ids_output() else "\n---\n" if len(outputs) > 1 else ""
        typer.echo(separator.join(outputs))

    if errors:
        raise typer.Exit(1)


def _get_part_direct(kp: Keeper, actual_id: str, part_num: int) -> Optional[str]:
    """Get a single part by ID@P{N} and return formatted output."""
    item = kp.get_part(actual_id, part_num)
    if item is None:
        typer.echo(f"Part not found: {actual_id}@P{{{part_num}}}", err=True)
        return None

    if _get_ids_output():
        return f"{_shell_quote_id(actual_id)}@P{{{part_num}}}"
    if _get_json_output():
        return json.dumps({
            "id": actual_id,
            "part": part_num,
            "total_parts": int(item.tags.get("_total_parts", 0)),
            "summary": item.summary,
            "tags": _filter_display_tags(item.tags),
        }, indent=2)

    total = int(item.tags.get("_total_parts", 0))
    lines = ["---", f"id: {_shell_quote_id(actual_id)}@P{{{part_num}}}"]
    display_tags = _filter_display_tags(item.tags)
    lines.extend(_render_tags_frontmatter(display_tags))
    if part_num > 1:
        lines.append("prev:")
        lines.append(f"  - @P{{{part_num - 1}}}")
    if part_num < total:
        lines.append("next:")
        lines.append(f"  - @P{{{part_num + 1}}}")
    lines.append("---")
    lines.append(item.summary)
    return "\n".join(lines)


def _get_parts_list(kp: Keeper, actual_id: str) -> str:
    """List all parts of a document (same format as 'keep list')."""
    part_list = kp.list_parts(actual_id)
    if not part_list:
        return f"No parts for {actual_id}. Use 'keep analyze {actual_id}' to create parts."
    items = _parts_to_items(actual_id, None, part_list)
    return _format_items(items, as_json=_get_json_output())


def _get_similar_list(kp: Keeper, actual_id: str, limit: int) -> str:
    """List similar items for a document."""
    similar_items = kp.get_similar_for_display(actual_id, limit=limit)
    similar_offsets = {s.id: kp.get_version_offset(s) for s in similar_items}

    if _get_ids_output():
        lines = []
        for item in similar_items:
            base_id = item.tags.get("_base_id", item.id)
            offset = similar_offsets.get(item.id, 0)
            lines.append(f"{base_id}@V{{{offset}}}")
        return "\n".join(lines)
    if _get_json_output():
        result = {
            "id": actual_id,
            "similar": [
                {
                    "id": f"{item.tags.get('_base_id', item.id)}@V{{{similar_offsets.get(item.id, 0)}}}",
                    "score": item.score,
                    "date": local_date(item.tags.get("_updated") or item.tags.get("_created", "")),
                    "summary": item.summary[:60],
                }
                for item in similar_items
            ],
        }
        return json.dumps(result, indent=2)
    lines = [f"Similar to {actual_id}:"]
    if similar_items:
        for item in similar_items:
            base_id = item.tags.get("_base_id", item.id)
            offset = similar_offsets.get(item.id, 0)
            score_str = f"({item.score:.2f})" if item.score else ""
            date_part = local_date(item.tags.get("_updated") or item.tags.get("_created", ""))
            summary_preview = item.summary[:50].replace("\n", " ")
            if len(item.summary) > 50:
                summary_preview += "..."
            lines.append(f"  {base_id}@V{{{offset}}} {score_str} {date_part} {summary_preview}")
    else:
        lines.append("  No similar notes found.")
    return "\n".join(lines)


def _get_meta_list(kp: Keeper, actual_id: str, limit: int) -> str:
    """List meta items for a document."""
    meta_sections = kp.resolve_meta(actual_id, limit_per_doc=limit)
    if _get_ids_output():
        lines = []
        for name, items in meta_sections.items():
            for item in items:
                lines.append(_shell_quote_id(item.id))
        return "\n".join(lines)
    if _get_json_output():
        result = {
            "id": actual_id,
            "meta": {
                name: [{"id": item.id, "summary": item.summary[:60]} for item in items]
                for name, items in meta_sections.items()
            },
        }
        return json.dumps(result, indent=2)
    lines = [f"Meta for {actual_id}:"]
    for name, items in meta_sections.items():
        lines.append(f"  {name}:")
        for item in items:
            summary_preview = item.summary[:50].replace("\n", " ")
            if len(item.summary) > 50:
                summary_preview += "..."
            lines.append(f"    {_shell_quote_id(item.id)}  {summary_preview}")
    if len(lines) == 1:
        lines.append("  No meta notes found.")
    return "\n".join(lines)


def _get_resolve_list(kp: Keeper, actual_id: str, resolve: list[str], limit: int) -> str:
    """Resolve inline meta-doc syntax strings."""
    from .utils import _parse_meta_doc
    all_queries: list[dict[str, str]] = []
    all_context: list[str] = []
    all_prereqs: list[str] = []
    for r in resolve:
        q, c, p = _parse_meta_doc(r)
        all_queries.extend(q)
        all_context.extend(c)
        all_prereqs.extend(p)
    all_context = list(dict.fromkeys(all_context))
    all_prereqs = list(dict.fromkeys(all_prereqs))
    items = kp.resolve_inline_meta(
        actual_id, all_queries, all_context, all_prereqs, limit=limit,
    )
    if _get_ids_output():
        return "\n".join(_shell_quote_id(item.id) for item in items)
    if _get_json_output():
        result = {
            "id": actual_id,
            "resolve": [{"id": item.id, "summary": item.summary[:60]} for item in items],
        }
        return json.dumps(result, indent=2)
    lines = [f"Resolve for {actual_id}:"]
    for item in items:
        summary_preview = item.summary[:50].replace("\n", " ")
        if len(item.summary) > 50:
            summary_preview += "..."
        lines.append(f"  {_shell_quote_id(item.id)}  {summary_preview}")
    if len(lines) == 1:
        lines.append("  No matching notes found.")
    return "\n".join(lines)


def _get_one(
    kp: Keeper,
    one_id: str,
    version: Optional[int],
    history: bool,
    similar: bool,
    meta: bool,
    resolve: Optional[list[str]],
    tag: Optional[list[str]],
    limit: int,
    show_parts: bool = False,
    focus_part: Optional[int] = None,
) -> Optional[str]:
    """Get a single item and return its formatted output, or None on error."""
    # Parse @V{N} (signed) or @P{N} identifier from ID (security: check literal first)
    actual_id = one_id
    version_from_id = None
    part_from_id = None

    if kp.exists(one_id):
        actual_id = one_id
    else:
        match = PART_SUFFIX_PATTERN.search(one_id)
        if match:
            part_from_id = int(match.group(1))
            actual_id = one_id[:match.start()]
        else:
            match = VERSION_SUFFIX_PATTERN.search(one_id)
            if match:
                version_from_id = int(match.group(1))
                actual_id = one_id[:match.start()]

    effective_version = version
    if version is None and version_from_id is not None:
        effective_version = version_from_id

    # Dispatch to sub-mode handlers
    if part_from_id is not None:
        return _get_part_direct(kp, actual_id, part_from_id)

    # --history / --parts with --ids: flat list for piping
    if _get_ids_output() and history:
        versions = kp.list_versions(actual_id, limit=limit)
        current = kp.get(actual_id)
        return _format_items(_versions_to_items(actual_id, current, versions))
    if _get_ids_output() and show_parts:
        part_list = kp.list_parts(actual_id)
        if not part_list:
            return f"No parts for {actual_id}. Use 'keep analyze {actual_id}' to create parts."
        return _format_items(_parts_to_items(actual_id, None, part_list))

    if similar:
        return _get_similar_list(kp, actual_id, limit)
    if meta:
        return _get_meta_list(kp, actual_id, limit)
    if resolve:
        return _get_resolve_list(kp, actual_id, resolve, limit)

    # Default + --history + --parts: frontmatter with expanded sections
    selector = effective_version if effective_version is not None else None
    ctx = kp.get_context(actual_id, version=selector)
    if ctx is None:
        if selector is not None:
            typer.echo(f"Version not found: {actual_id}@V{{{selector}}}", err=True)
        else:
            typer.echo(f"Not found: {actual_id}", err=True)
        return None

    # Expand parts section: show all parts without windowing
    if show_parts:
        all_parts = kp.list_parts(actual_id)
        if not all_parts:
            typer.echo(f"No parts for {actual_id}. Use 'keep analyze {actual_id}' to create parts.", err=True)
            return None
        ctx.parts = [PartRef(part_num=p.part_num, summary=p.summary, tags=dict(p.tags)) for p in all_parts]
        ctx.expand_parts = True

    # Expand history: show all versions in prev section
    if history:
        all_versions = kp.list_versions(actual_id, limit=limit)
        ctx.prev = [
            VersionRef(
                offset=i + 1,
                date=local_date(v.tags.get("_created") or v.created_at or ""),
                summary=v.summary,
            )
            for i, v in enumerate(all_versions)
        ]
        ctx.next = []  # full history replaces navigation

    if tag:
        filtered = _filter_by_tags([ctx.item], tag)
        if not filtered:
            typer.echo(f"Tag filter not matched: {actual_id}", err=True)
            return None

    if _get_ids_output():
        return _format_versioned_id(ctx.item)
    return render_context(ctx, as_json=_get_json_output())


@app.command("del")
def del_cmd(
    id: Annotated[list[str], typer.Argument(help="ID(s) of note(s) to delete")],
    store: StoreOption = None,
):
    """Delete the current version of note(s), or a specific version.

    Without @V{N}: reverts to the previous version (or fully deletes if no history).
    With @V{N}: deletes that specific archived version; other versions remain.
      N>=0 selects by offset from current; N<0 selects by oldest-ordinal.

    \b
    Examples:
        keep del %abc123def456        # Remove a text note
        keep del %abc123 %def456      # Remove multiple notes
        keep del now                  # Revert now to previous
        keep del 'now@V{3}'          # Delete version 3 only
    """
    kp = _get_keeper(store)
    had_errors = False

    for one_id in id:
        # Parts cannot be individually deleted
        if PART_SUFFIX_PATTERN.search(one_id):
            typer.echo(f"Error: cannot delete individual parts. Re-analyze or delete the parent.", err=True)
            had_errors = True
            continue

        # Parse @V{N} suffix (signed selectors allowed)
        version_offset = None
        actual_id = one_id
        match = VERSION_SUFFIX_PATTERN.search(one_id)
        if match:
            version_offset = int(match.group(1))
            actual_id = one_id[:match.start()]

        if version_offset is not None:
            if version_offset == 0:
                typer.echo(
                    f"Error: @V{{0}} is current. Use 'keep del {_shell_quote_id(actual_id)}' to revert.",
                    err=True,
                )
                had_errors = True
                continue
            # Delete a specific archived version (offset or oldest-ordinal selector)
            # Fetch the version before deleting so we can show what was removed
            version_item = kp.get_version(actual_id, version_offset)
            deleted = kp.delete_version(actual_id, version_offset)
            if not deleted:
                typer.echo(f"Version not found: {one_id}", err=True)
                had_errors = True
            elif version_item:
                from .types import ItemContext
                typer.echo(render_context(ItemContext(item=version_item), as_json=_get_json_output()))
        else:
            # Original behavior: revert current (or delete if no history)
            item = kp.get(actual_id)
            if item is None:
                typer.echo(f"Not found: {actual_id}", err=True)
                had_errors = True
                continue

            # Show the deleted version (fetched above before deletion)
            ctx = kp.get_context(
                actual_id, include_meta=False, include_parts=False,
                include_similar=False,
            )
            restored = kp.revert(actual_id)

            if ctx:
                typer.echo(render_context(ctx, as_json=_get_json_output()))
            else:
                typer.echo(_format_summary_line(item))

    if had_errors:
        raise typer.Exit(1)


@app.command("delete", hidden=True)
def delete(
    id: Annotated[list[str], typer.Argument(help="ID(s) of note(s) to delete")],
    store: StoreOption = None,
):
    """Delete the current version of note(s) (alias for 'del')."""
    del_cmd(id=id, store=store)


@app.command()
def analyze(
    id: Annotated[str, typer.Argument(help="ID of note to analyze into parts")],
    tag: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t",
        help="Guidance tag keys for decomposition (e.g., -t topic -t type)",
    )] = None,
    foreground: Annotated[bool, typer.Option(
        "--foreground", "--fg",
        help="Run in foreground (default: background)"
    )] = False,
    force: Annotated[bool, typer.Option(
        "--force",
        help="Re-analyze even if parts are already current"
    )] = False,
    store: StoreOption = None,
):
    """Decompose a note or string into meaningful parts.

    For documents (URI sources): decomposes content structurally.
    For inline notes (strings): assembles version history and decomposes
    the temporal sequence into episodic parts.

    Uses an LLM to identify sections, each with its own summary, tags,
    and embedding. Parts appear in 'find' results and can be accessed
    with @P{N} syntax.

    Skips analysis if parts are already current (content unchanged since
    last analysis). Use --force to re-analyze regardless.

    Runs in the background by default (serialized with other ML work);
    use --fg to wait for results.
    """
    kp = _get_keeper(store)

    # Background mode (default): enqueue for serial processing
    if not foreground:
        try:
            enqueued = kp.enqueue_analyze(id, tags=tag, force=force)
        except ValueError as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(1)

        if not enqueued:
            if _get_json_output():
                typer.echo(json.dumps({"id": id, "status": "skipped"}))
            else:
                typer.echo(f"Already analyzed, skipping {id}.", err=True)
            kp.close()
            return

        if _get_json_output():
            typer.echo(json.dumps({"id": id, "status": "queued"}))
        else:
            typer.echo(f"Queued {id} for background analysis.", err=True)
        kp.close()
        return

    try:
        parts = kp.analyze(id, tags=tag, force=force)
    except ValueError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"Analysis failed: {e}", err=True)
        raise typer.Exit(1)

    if not parts:
        if _get_json_output():
            typer.echo(json.dumps({"id": id, "parts": []}))
        else:
            typer.echo(f"Content not decomposable into multiple parts: {id}")
        return

    if _get_json_output():
        result = {
            "id": id,
            "parts": [
                {
                    "part": p.part_num,
                    "pid": f"{id}@P{{{p.part_num}}}",
                    "summary": p.summary[:100],
                    "tags": {k: v for k, v in p.tags.items() if not k.startswith("_")},
                }
                for p in parts
            ],
        }
        typer.echo(json.dumps(result, indent=2))
    else:
        typer.echo(f"Analyzed {id} into {len(parts)} parts:")
        for p in parts:
            summary_preview = p.summary[:60].replace("\n", " ")
            if len(p.summary) > 60:
                summary_preview += "..."
            typer.echo(f"  @P{{{p.part_num}}} {summary_preview}")





def _get_config_value(cfg, store_path: Path, path: str):
    """Get config value by dotted path.

    Special paths (not in TOML):
        file - config file location
        tool - package directory (SKILL.md location)
        openclaw-plugin - OpenClaw plugin directory
        mcpb - generate .mcpb bundle for Claude Desktop
        store - store path

    Dotted paths into config:
        providers - all provider config
        providers.embedding - embedding provider name
        providers.summarization - summarization provider name
        embedding.* - embedding config details
        summarization.* - summarization config details
        tags - default tags
    """
    # Special built-in paths (not in TOML)
    if path == "file":
        return str(cfg.config_path) if cfg else None
    if path == "tool":
        return str(get_tool_directory())
    if path == "openclaw-plugin":
        return str(Path(str(importlib.resources.files("keep"))) / "data" / "openclaw-plugin")
    if path == "mcpb":
        from .mcpb import generate_mcpb
        out = generate_mcpb(store_path=store_path)
        if platform.system() == "Darwin":
            subprocess.Popen(["open", str(out)])
        elif platform.system() == "Windows":
            os.startfile(str(out))
        else:
            subprocess.Popen(["xdg-open", str(out)])
        return (
            'After installation, just say to Claude:\n'
            'Please read all the keep_help documentation, and then use keep_prompt(name="reflect") to save some notes about what you learn.'
        )
    if path == "docs":
        return str(get_tool_directory() / "docs")
    if path == "store":
        return str(store_path)
    # Provider shortcuts
    if path == "providers":
        if cfg:
            return {
                "embedding": cfg.embedding.name if cfg.embedding else None,
                "summarization": cfg.summarization.name,
                "document": cfg.document.name,
            }
        return None
    if path == "providers.embedding":
        return cfg.embedding.name if cfg and cfg.embedding else None
    if path == "providers.summarization":
        return cfg.summarization.name if cfg else None
    if path == "providers.document":
        return cfg.document.name if cfg else None

    # Tags shortcut
    if path == "tags":
        return cfg.default_tags if cfg else {}

    # Dotted path into config attributes
    if not cfg:
        raise typer.BadParameter(f"No config loaded, cannot access: {path}")

    parts = path.split(".")
    value = cfg
    for part in parts:
        if hasattr(value, part):
            value = getattr(value, part)
        elif hasattr(value, "params") and part in value.params:
            # Provider config params
            value = value.params[part]
        elif isinstance(value, dict) and part in value:
            value = value[part]
        else:
            raise typer.BadParameter(f"Unknown config path: {path}")

    # Return name for provider objects
    if hasattr(value, "name") and hasattr(value, "params"):
        return value.name
    return value


def _format_config_with_defaults(cfg, store_path: Path) -> str:
    """Format config output with commented defaults for unused settings."""
    config_path = cfg.config_path if cfg else None
    lines = []

    # Show paths
    lines.append(f"file: {config_path}")
    lines.append(f"tool: {get_tool_directory()}")
    lines.append(f"docs: {get_tool_directory() / 'docs'}")
    lines.append(f"store: {store_path}")

    lines.append(f"openclaw-plugin: {Path(str(importlib.resources.files('keep'))) / 'data' / 'openclaw-plugin'}")

    if cfg:
        lines.append("")
        lines.append("providers:")
        lines.append(f"  embedding: {cfg.embedding.name if cfg.embedding else 'none'}")
        if cfg.embedding and cfg.embedding.params.get("model"):
            lines.append(f"    model: {cfg.embedding.params['model']}")
        lines.append(f"  summarization: {cfg.summarization.name if cfg.summarization else 'none'}")
        if cfg.summarization and cfg.summarization.params.get("model"):
            lines.append(f"    model: {cfg.summarization.params['model']}")

        # Show limits
        lines.append("")
        lines.append("limits:")
        lines.append(f"  max_summary_length: {cfg.max_summary_length}")
        lines.append(f"  max_inline_length: {cfg.max_inline_length}")

        # Show configured tags or example
        if cfg.default_tags:
            lines.append("")
            lines.append("tags:")
            for key, value in cfg.default_tags.items():
                lines.append(f"  {key}: {value}")
        else:
            lines.append("")
            lines.append("# tags:")
            lines.append("#   project: myproject")

        # Show integrations status
        from .integrations import TOOL_CONFIGS
        if cfg.integrations:
            lines.append("")
            lines.append("integrations:")
            for tool_key in TOOL_CONFIGS:
                if tool_key in cfg.integrations:
                    status = cfg.integrations[tool_key]
                    lines.append(f"  {tool_key}: {status}")
            for tool_key in TOOL_CONFIGS:
                if tool_key not in cfg.integrations:
                    lines.append(f"  # {tool_key}: false")
        else:
            lines.append("")
            lines.append("# integrations:")
            for tool_key in TOOL_CONFIGS:
                lines.append(f"#   {tool_key}: false")

        # Show available options as comments
        lines.append("")
        lines.append("# --- Configuration Options ---")
        lines.append("#")
        lines.append("# API Keys (set in environment):")
        lines.append("#   VOYAGE_API_KEY     → embedding: voyage (Anthropic's partner)")
        lines.append("#   ANTHROPIC_API_KEY  → summarization: anthropic")
        lines.append("#   OPENAI_API_KEY     → embedding: openai, summarization: openai")
        lines.append("#   GEMINI_API_KEY     → embedding: gemini, summarization: gemini")
        lines.append("#   GOOGLE_CLOUD_PROJECT → Vertex AI (uses Workload Identity / ADC)")
        lines.append("#")
        lines.append("# Models (configure in keep.toml):")
        lines.append("#   voyage: voyage-3.5-lite (default), voyage-3-large, voyage-code-3")
        lines.append("#   anthropic: claude-3-haiku-20240307 (default), claude-3-5-haiku-20241022")
        lines.append("#   openai embedding: text-embedding-3-small (default), text-embedding-3-large")
        lines.append("#   openai summarization: gpt-4o-mini (default)")
        lines.append("#   gemini embedding: text-embedding-004 (default)")
        lines.append("#   gemini summarization: gemini-2.5-flash (default)")
        lines.append("#")
        lines.append("# Ollama (auto-detected if running, no API key needed):")
        lines.append("#   OLLAMA_HOST        → default: http://localhost:11434")
        lines.append("#   ollama embedding: any model (prefer nomic-embed-text, mxbai-embed-large)")
        lines.append("#   ollama summarization: any generative model (e.g. llama3.2, mistral)")

    return "\n".join(lines)


@app.command(hidden=True, deprecated=True)
def validate(
    id: Annotated[Optional[list[str]], typer.Argument(
        help="System doc ID(s) to validate (e.g. '.tag/act', '.meta/related')"
    )] = None,
    all_docs: Annotated[bool, typer.Option(
        "--all", "-a",
        help="Validate all system docs"
    )] = False,
    diagram: Annotated[bool, typer.Option(
        "--diagram",
        help="Print Mermaid state-transition diagram for .state/* docs"
    )] = False,
    store: StoreOption = None,
):
    """Validate system documents with parser-based semantics.

    Checks .tag/*, .meta/*, .prompt/*, and .state/* documents for
    structural correctness. Reports errors (will cause runtime failures)
    and warnings (may cause unexpected behavior).

    \b
    Examples:
        keep validate .tag/act               # Validate one doc
        keep validate .tag/act .meta/related  # Validate several
        keep validate --all                   # Validate all system docs
        keep validate --diagram              # Mermaid state diagram
    """
    from .validate import validate_system_doc

    if diagram:
        from .validate import state_doc_diagram
        from .system_docs import SYSTEM_DOC_DIR, _filename_to_id, _load_frontmatter
        state_docs: dict[str, str] = {}
        # Load from store if available, else from disk
        try:
            kp = _get_keeper(store)
            doc_coll = kp._resolve_doc_collection()
            for rec in kp._document_store.query_by_id_prefix(doc_coll, ".state/"):
                name = str(getattr(rec, "id", "")).removeprefix(".state/")
                body = str(getattr(rec, "summary", "") or "").strip()
                if name and body:
                    state_docs[name] = body
        except Exception:
            pass
        # Fall back to / supplement with bundled files
        if not state_docs:
            for path in sorted(SYSTEM_DOC_DIR.glob("state-*.md")):
                doc_id = _filename_to_id(path.name)
                name = doc_id.removeprefix(".state/")
                content, _ = _load_frontmatter(path)
                if content.strip():
                    state_docs[name] = content
        typer.echo(state_doc_diagram(state_docs))
        return

    kp = _get_keeper(store)

    # Collect docs to validate: either from --all prefix scan or explicit IDs.
    # The prefix scan returns full records (id, summary, tags) — use them
    # directly to avoid N redundant kp.get() calls and accessed_at writes.
    docs_by_id: dict[str, tuple[str, dict]] = {}  # id -> (summary, tags)
    if all_docs:
        doc_coll = kp._resolve_doc_collection()
        for prefix in (".tag/", ".meta/", ".prompt/", ".state/"):
            for rec in kp._document_store.query_by_id_prefix(doc_coll, prefix):
                doc_id = str(getattr(rec, "id", ""))
                if doc_id:
                    summary = str(getattr(rec, "summary", "") or "")
                    raw_tags = getattr(rec, "tags", None)
                    tags = dict(raw_tags) if isinstance(raw_tags, dict) else {}
                    docs_by_id[doc_id] = (summary, tags)
    elif id:
        for doc_id in id:
            item = kp.get(doc_id)
            if item is None:
                typer.echo(f"{doc_id}: not found", err=True)
                continue
            summary = str(getattr(item, "summary", "") or "")
            raw_tags = getattr(item, "tags", None)
            tags = dict(raw_tags) if isinstance(raw_tags, dict) else {}
            docs_by_id[doc_id] = (summary, tags)
    else:
        typer.echo("Provide doc IDs or use --all. See: keep validate --help", err=True)
        raise typer.Exit(1)

    if not docs_by_id:
        typer.echo("No system docs found.")
        return

    total_errors = 0
    total_warnings = 0
    for doc_id in sorted(docs_by_id):
        content, tags = docs_by_id[doc_id]
        result = validate_system_doc(doc_id, content, tags)

        if result.diagnostics:
            for d in result.diagnostics:
                typer.echo(f"{doc_id}: {d}")
            total_errors += len(result.errors)
            total_warnings += len(result.warnings)
        else:
            typer.echo(f"{doc_id}: ok")

    if total_errors:
        typer.echo(f"\n{total_errors} error(s), {total_warnings} warning(s)")
        raise typer.Exit(1)
    elif total_warnings:
        typer.echo(f"\n{total_warnings} warning(s)")
    else:
        typer.echo(f"\n{len(docs_by_id)} doc(s) ok")


@app.command()
def config(
    path: Annotated[Optional[str], typer.Argument(
        help="Config path to get (e.g., 'file', 'tool', 'store', 'providers.embedding')"
    )] = None,
    reset_system_docs: Annotated[bool, typer.Option(
        "--reset-system-docs",
        help="Force reload system documents from bundled content (overwrites modifications)"
    )] = False,
    setup: Annotated[bool, typer.Option(
        "--setup",
        help="Run interactive setup wizard (provider and tool selection)"
    )] = False,
    state_diagram: Annotated[bool, typer.Option(
        "--state-diagram",
        help="Print Mermaid state-transition diagram for .state/* docs"
    )] = False,
    store: StoreOption = None,
):
    """Show configuration. Optionally get a specific value by path.

    \b
    Examples:
        keep config              # Show all config
        keep config file         # Config file location
        keep config tool         # Package directory (SKILL.md location)
        keep config docs         # Documentation directory
        keep config openclaw-plugin  # OpenClaw plugin directory
        keep config mcpb         # Generate .mcpb for Claude Desktop
        keep config store        # Store path
        keep config providers    # All provider config
        keep config providers.embedding  # Embedding provider name
        keep config --setup      # Re-run interactive setup wizard
        keep config --reset-system-docs  # Reset bundled system docs
        keep config --state-diagram  # Mermaid state-transition diagram
    """
    # Handle setup wizard
    if setup:
        from .paths import get_config_dir
        from .setup_wizard import run_wizard
        actual_store = store if store is not None else _get_store_override()
        if os.environ.get("KEEP_CONFIG"):
            config_dir = get_config_dir()
        elif actual_store:
            config_dir = Path(actual_store).resolve()
        else:
            config_dir = get_config_dir()
        store_path = Path(actual_store).resolve() if actual_store else None
        run_wizard(config_dir, store_path, restart_command="keep config --setup")
        return

    # Handle state diagram
    if state_diagram:
        from .validate import state_doc_diagram
        from .system_docs import SYSTEM_DOC_DIR, _filename_to_id, _load_frontmatter
        state_docs: dict[str, str] = {}
        # Load from store if available, else from disk
        try:
            kp = _get_keeper(store)
            doc_coll = kp._resolve_doc_collection()
            for rec in kp._document_store.query_by_id_prefix(doc_coll, ".state/"):
                name = str(getattr(rec, "id", "")).removeprefix(".state/")
                body = str(getattr(rec, "summary", "") or "").strip()
                if name and body:
                    state_docs[name] = body
        except Exception:
            pass
        # Fall back to / supplement with bundled files
        if not state_docs:
            for p in sorted(SYSTEM_DOC_DIR.glob("state-*.md")):
                doc_id = _filename_to_id(p.name)
                name = doc_id.removeprefix(".state/")
                content, _ = _load_frontmatter(p)
                if content.strip():
                    state_docs[name] = content
        typer.echo(state_doc_diagram(state_docs))
        return

    # Handle system docs reset - requires full Keeper initialization
    if reset_system_docs:
        kp = _get_keeper(store)
        stats = kp.reset_system_documents()
        typer.echo(f"Reset {stats['reset']} system documents")
        return

    # For config display, use lightweight path (no API calls)
    from .config import load_or_create_config
    from .paths import get_config_dir, get_default_store_path

    actual_store = store if store is not None else _get_store_override()
    if actual_store is not None:
        config_dir = Path(actual_store).resolve()
    else:
        config_dir = get_config_dir()

    cfg = load_or_create_config(config_dir)
    config_path = cfg.config_path if cfg else None
    store_path = get_default_store_path(cfg) if actual_store is None else actual_store

    # If a specific path is requested, return just that value
    if path:
        try:
            value = _get_config_value(cfg, store_path, path)
        except typer.BadParameter as e:
            typer.echo(str(e), err=True)
            raise typer.Exit(1)

        if _get_json_output():
            typer.echo(json.dumps({path: value}, indent=2))
        else:
            # Raw output for shell scripting
            if isinstance(value, (list, dict)):
                typer.echo(json.dumps(value))
            else:
                typer.echo(value)
        return

    # Full config output
    if _get_json_output():

        result = {
            "file": str(config_path) if config_path else None,
            "tool": str(get_tool_directory()),
            "docs": str(get_tool_directory() / "docs"),
            "store": str(store_path),
            "openclaw-plugin": str(Path(str(importlib.resources.files("keep"))) / "data" / "openclaw-plugin"),
            "providers": {
                "embedding": cfg.embedding.name if cfg and cfg.embedding else None,
                "summarization": cfg.summarization.name if cfg else None,
                "document": cfg.document.name if cfg else None,
            },
        }
        if cfg and cfg.default_tags:
            result["tags"] = cfg.default_tags
        typer.echo(json.dumps(result, indent=2))
    else:
        typer.echo(_format_config_with_defaults(cfg, store_path))


@app.command("help")
def help_cmd(
    topic: Annotated[Optional[str], typer.Argument(
        help="Documentation topic (e.g. 'quickstart', 'keep-put', 'tagging'). Omit for index."
    )] = None,
):
    """Browse keep documentation.

    \b
    Examples:
        keep help              # Documentation index
        keep help quickstart   # CLI Quick Start guide
        keep help keep-put     # keep put reference
        keep help tagging      # Tagging guide
    """
    from .help import get_help_topic
    typer.echo(get_help_topic(topic or "index", link_style="cli"))


@app.command("pending")
def pending_cmd(
    store: StoreOption = None,
    reindex: Annotated[bool, typer.Option(
        "--reindex",
        help="Enqueue all items for re-embedding, then process"
    )] = False,
    retry: Annotated[bool, typer.Option(
        "--retry",
        help="Reset failed items back to pending for retry"
    )] = False,
    list_items: Annotated[bool, typer.Option(
        "--list", "-l",
        help="List pending work items"
    )] = False,
    purge: Annotated[bool, typer.Option(
        "--purge",
        help="Delete all pending work items from the queue"
    )] = False,
    stop: Annotated[bool, typer.Option(
        "--stop",
        help="Stop the background processor"
    )] = False,
    daemon: Annotated[bool, typer.Option(
        "--daemon",
        hidden=True,
        help="Run as background daemon (used internally)"
    )] = False,
):
    """Process pending background tasks.

    Starts a background processor (if not already running) and shows
    progress. Ctrl-C detaches without stopping the processor.
    Use --reindex to re-embed all items with the current embedding provider.
    """
    # --stop: send SIGTERM to the daemon (lightweight — no Keeper needed)
    if stop:
        from .model_lock import ModelLock
        from .paths import get_default_store_path, get_config_dir
        from .config import load_or_create_config
        if store is not None:
            store_path = Path(store).resolve()
        else:
            override = _get_store_override()
            if override is not None:
                store_path = Path(override).resolve()
            else:
                config_dir = get_config_dir()
                cfg = load_or_create_config(config_dir)
                store_path = get_default_store_path(cfg)
        pid_path = store_path / "processor.pid"
        lock = ModelLock(store_path / ".processor.lock")
        if not lock.is_locked():
            typer.echo("No processor running.")
            pid_path.unlink(missing_ok=True)
            return
        if pid_path.exists():
            try:
                pid = int(pid_path.read_text().strip())
                os.kill(pid, signal.SIGTERM)
                typer.echo(f"Sent stop signal to processor (pid {pid}).", err=True)
            except (ProcessLookupError, ValueError):
                typer.echo("Processor not running (stale PID file).", err=True)
                pid_path.unlink(missing_ok=True)
        else:
            typer.echo("Processor running but no PID file found.", err=True)
        return

    kp = _get_keeper(store)

    # --daemon: run as the actual background processor
    if daemon:
        import logging
        from .model_lock import ModelLock

        _daemon_logger = logging.getLogger("keep.cli.daemon")
        pid_path = kp._processor_pid_path
        processor_lock = ModelLock(kp._store_path / ".processor.lock")
        flow_worker_id = f"pending-daemon:{os.getpid()}"
        shutdown_requested = False

        if not processor_lock.acquire(blocking=False):
            _daemon_logger.info("Daemon: another processor already running, exiting")
            kp.close()
            return

        # Startup banner: version, paths, queue depth
        try:
            from importlib.metadata import version as _pkg_version
            _ver = _pkg_version("keep-skill")
        except Exception:
            _ver = "unknown"
        _daemon_logger.info(
            "Daemon started (pid=%d) keep-skill=%s python=%s store=%s",
            os.getpid(), _ver, sys.executable, kp._store_path,
        )
        # Release leases held by previous daemon instances that exited
        # without completing their work items.
        wq = kp._get_work_queue()
        released = wq.release_stale_leases(flow_worker_id)
        if released:
            _daemon_logger.info("Released %d stale leases from previous daemon", released)

        _daemon_logger.info(
            "Queue: %d pending, %d flow, %d failed",
            kp._pending_queue.count(),
            kp.pending_work_count() if hasattr(kp, "pending_work_count") else 0,
            0,  # failed count not cheaply available
        )
        _daemon_logger.info(
            "Embedding: %s/%s",
            kp._config.embedding.name if kp._config.embedding else "none",
            kp._config.embedding.params.get("model", "") if kp._config.embedding else "",
        )

        def handle_signal(signum, frame):
            nonlocal shutdown_requested
            shutdown_requested = True
            _daemon_logger.info("Received signal %d, shutting down after current item", signum)

        def _is_shutdown():
            return shutdown_requested

        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

        # TTL cleanup for temp files (email attachments, etc.)
        _last_cleanup_ts = 0.0
        _CLEANUP_INTERVAL = 86400  # 24 hours
        _CLEANUP_MAX_AGE = 86400   # delete files older than 24 hours

        def _cleanup_temp_files():
            nonlocal _last_cleanup_ts
            now = time.time()
            if now - _last_cleanup_ts < _CLEANUP_INTERVAL:
                return
            _last_cleanup_ts = now
            cache_dirs = [
                Path.home() / ".cache" / "keep" / "email-att",
            ]
            total_removed = 0
            for cache_dir in cache_dirs:
                if not cache_dir.is_dir():
                    continue
                for entry in cache_dir.iterdir():
                    if not entry.is_dir():
                        continue
                    try:
                        age = now - entry.stat().st_mtime
                        if age > _CLEANUP_MAX_AGE:
                            shutil.rmtree(entry)
                            total_removed += 1
                    except OSError:
                        pass
            if total_removed:
                _daemon_logger.info("Cleaned up %d temp directories", total_removed)

        try:
            pid_path.write_text(str(os.getpid()))
            while not shutdown_requested:
                flow_result = kp.process_pending_work(
                    limit=1,
                    worker_id=flow_worker_id,
                    lease_seconds=180,
                    shutdown_check=_is_shutdown,
                )
                if shutdown_requested:
                    break
                result = kp.process_pending(limit=1, shutdown_check=_is_shutdown)
                delegated = result.get("delegated", 0)

                # Poll watched sources for changes
                from .watches import poll_watches as _poll_watches, has_active_watches, next_check_delay, load_watches
                watch_result = _poll_watches(kp)
                if watch_result["checked"] > 0:
                    _daemon_logger.info(
                        "Watches: checked=%d changed=%d stale=%d errors=%d",
                        watch_result["checked"], watch_result["changed"],
                        watch_result["stale"], watch_result["errors"],
                    )

                # TTL cleanup (self-throttled to once per day)
                _cleanup_temp_files()

                _daemon_logger.info(
                    "Daemon batch: processed=%d failed=%d delegated=%d flow_processed=%d flow_failed=%d",
                    result["processed"], result["failed"], delegated,
                    int(flow_result.get("processed", 0)),
                    int(flow_result.get("failed", 0)) + int(flow_result.get("dead_lettered", 0)),
                )
                flow_activity = (
                    int(flow_result.get("claimed", 0)) > 0
                    or int(flow_result.get("processed", 0)) > 0
                    or int(flow_result.get("failed", 0)) > 0
                    or int(flow_result.get("dead_lettered", 0)) > 0
                )
                if result["processed"] == 0 and result["failed"] == 0 and delegated == 0 and not flow_activity:
                    # Check for outstanding delegated tasks before exiting
                    delegated_remaining = kp._pending_queue.count_delegated() if hasattr(kp._pending_queue, "count_delegated") else 0
                    flow_remaining = kp.pending_work_count() if hasattr(kp, "pending_work_count") else 0
                    pending_remaining = kp._pending_queue.count()
                    if delegated_remaining > 0:
                        _daemon_logger.info("Waiting for %d delegated tasks", delegated_remaining)
                        time.sleep(5)
                        continue
                    if flow_remaining > 0:
                        _daemon_logger.info("Waiting for %d flow work items", flow_remaining)
                        time.sleep(1)
                        continue
                    if pending_remaining > 0:
                        _daemon_logger.info("Waiting for %d pending items (retry backoff)", pending_remaining)
                        time.sleep(5)
                        continue

                    # Active watches keep the daemon alive
                    if has_active_watches(kp):
                        delay = next_check_delay(load_watches(kp))
                        delay = max(1.0, min(delay, 30.0))
                        _daemon_logger.debug("Sleeping %.1fs (next watch check)", delay)
                        time.sleep(delay)
                        continue

                    # Items may have been enqueued after our last dequeue
                    # (e.g. OCR enqueued while we were processing a summarize).
                    # Wait briefly and check once more before exiting.
                    time.sleep(1)
                    if shutdown_requested:
                        break
                    flow_result = kp.process_pending_work(
                        limit=1,
                        worker_id=flow_worker_id,
                        lease_seconds=180,
                        shutdown_check=_is_shutdown,
                    )
                    result = kp.process_pending(limit=1, shutdown_check=_is_shutdown)
                    flow_activity = (
                        int(flow_result.get("claimed", 0)) > 0
                        or int(flow_result.get("processed", 0)) > 0
                        or int(flow_result.get("failed", 0)) > 0
                        or int(flow_result.get("dead_lettered", 0)) > 0
                    )
                    if (
                        result["processed"] == 0
                        and result["failed"] == 0
                        and result.get("delegated", 0) == 0
                        and not flow_activity
                    ):
                        break
                    _daemon_logger.info(
                        "Daemon batch (drain): processed=%d failed=%d flow_processed=%d flow_failed=%d",
                        result["processed"], result["failed"],
                        int(flow_result.get("processed", 0)),
                        int(flow_result.get("failed", 0)) + int(flow_result.get("dead_lettered", 0)),
                    )
        finally:
            _daemon_logger.info("Daemon shutting down")
            try:
                pid_path.unlink()
            except OSError:
                pass
            kp.close()
            processor_lock.release()
        return

    # --purge: delete all pending work items
    if purge:
        wq = kp._get_work_queue()
        n = wq.purge()
        typer.echo(f"Purged {n} pending work items.", err=True)
        kp.close()
        return

    # --retry: reset failed items back to pending
    if retry:
        n = kp._pending_queue.retry_failed()
        if n:
            typer.echo(f"Reset {n} failed items back to pending.", err=True)
        else:
            typer.echo("No failed items to retry.", err=True)
            kp.close()
            return

    # --reindex: enqueue all items for re-embedding
    if reindex:
        count = kp.count()
        if count == 0:
            typer.echo("No notes to reindex.")
            kp.close()
            raise typer.Exit(0)
        typer.echo(f"Enqueuing {count} notes for reindex...", err=True)
        stats = kp.enqueue_reindex()
        typer.echo(
            f"Enqueued {stats['enqueued']} items + {stats['versions']} versions",
            err=True,
        )

    # --list: show pending items and exit
    if list_items:
        items = kp._pending_queue.list_pending()
        failed = kp._pending_queue.list_failed()
        # Also query the work queue (flow items)
        try:
            wq = kp._get_work_queue()
            flow_items = wq.list_pending(limit=100)
        except Exception:
            flow_items = []
        try:
            from .watches import list_watches as _lw
            _watches = _lw(kp)
        except Exception:
            _watches = []
        if not items and not failed and not flow_items and not _watches:
            typer.echo("Nothing pending.")
        else:
            if items:
                for item in items:
                    retry = f" (retry after {item['retry_after']})" if item.get("retry_after") else ""
                    typer.echo(f"  {item['task_type']:15s} {item['supersede_key'] or item['work_id']}{retry}")
            if flow_items:
                if items:
                    typer.echo()
                # Full breakdown by kind (counts all items, not just first page)
                wq = kp._get_work_queue()
                by_kind = wq.count_by_kind()
                for kind, count in sorted(by_kind.items(), key=lambda x: -x[1]):
                    typer.echo(f"  {kind:20s} {count}")
            if failed:
                typer.echo(f"\nFailed ({len(failed)}):")
                for item in failed[:10]:
                    error = item.get("last_error", "unknown")
                    typer.echo(f"  {item['task_type']:15s} {item['id']}: {error}")
        # Show watches
        try:
            from .watches import list_watches
            watches = list_watches(kp)
            if watches:
                active = [w for w in watches if not w.stale]
                stale = [w for w in watches if w.stale]
                label = f"{len(active)} active"
                if stale:
                    label += f", {len(stale)} stale"
                typer.echo(f"\nWatches ({label}):")
                for w in watches:
                    suffix = " [stale]" if w.stale else ""
                    detail = ""
                    if w.kind == "directory":
                        parts = []
                        if w.recurse:
                            parts.append("recurse")
                        if w.exclude:
                            parts.append(f"{len(w.exclude)} excludes")
                        if parts:
                            detail = f" ({', '.join(parts)})"
                    typer.echo(f"  {w.kind:12s} {w.source}{detail}{suffix}")
        except Exception:
            pass
        kp.close()
        return

    # Interactive mode: show status, ensure daemon running, tail log
    pending_count = kp.pending_count()
    flow_pending_count = kp.pending_work_count() if hasattr(kp, "pending_work_count") else 0

    # Show failed and processing items
    queue_stats = kp._pending_queue.stats()
    failed_count = queue_stats.get("failed", 0)
    processing_count = queue_stats.get("processing", 0)

    if pending_count == 0 and processing_count == 0 and flow_pending_count == 0:
        if failed_count:
            typer.echo(f"Nothing pending. {failed_count} failed (use --retry to requeue).", err=True)
            # Show first few failed items
            failed_items = kp._pending_queue.list_failed()
            for item in failed_items[:5]:
                error = item.get("last_error", "unknown")
                typer.echo(f"  {item['id']} ({item['task_type']}): {error}", err=True)
            if len(failed_items) > 5:
                typer.echo(f"  ... and {len(failed_items) - 5} more", err=True)
        else:
            typer.echo("Nothing pending.")
        kp.close()
        return

    typer.echo(_queue_status_line(kp, queue_stats), err=True)

    # Ensure daemon is running
    if not kp._is_processor_running():
        kp._spawn_processor()
        typer.echo("Started background processor.", err=True)
    else:
        typer.echo("Background processor already running.", err=True)

    # Tail the ops log until daemon finishes or user Ctrl-C's
    log_path = kp._store_path / "keep-ops.log"
    _tail_ops_log(log_path, kp)
    kp.close()


def _queue_status_line(kp, queue_stats: dict) -> str:
    """Build a consistent queue status string like '2 queued, 1 processing (1 ocr)'."""
    pending = queue_stats.get("pending", 0)
    processing = queue_stats.get("processing", 0)
    delegated = queue_stats.get("delegated", 0)
    failed = queue_stats.get("failed", 0)
    flow_pending = 0
    if hasattr(kp, "pending_work_count"):
        try:
            flow_pending = int(kp.pending_work_count())
        except Exception:
            flow_pending = 0

    by_type = kp.pending_stats_by_type()
    type_parts = ", ".join(f"{c} {t}" for t, c in sorted(by_type.items()))

    # Flow work queue breakdown by kind
    flow_by_kind = ""
    if flow_pending:
        try:
            wq = kp._get_work_queue()
            by_kind = wq.count_by_kind()
            if by_kind:
                flow_by_kind = ", ".join(f"{c} {k}" for k, c in sorted(by_kind.items(), key=lambda x: -x[1]))
        except Exception:
            pass

    parts = [f"{pending} queued"]
    if processing:
        parts.append(f"{processing} processing")
    if delegated:
        parts.append(f"{delegated} delegated")
    if flow_pending:
        parts.append(f"{flow_pending} flow")

    line = ", ".join(parts)
    if type_parts:
        line += f" ({type_parts})"
    elif flow_by_kind:
        line += f" ({flow_by_kind})"
    if failed:
        line += f" + {failed} failed"
    return line


def _tail_ops_log(log_path: Path, kp) -> None:
    """Tail the ops log, showing new lines until daemon finishes or Ctrl-C."""
    # Grace period for daemon startup (takes a moment to acquire lock)
    time.sleep(1.0)

    try:
        # Ensure log file exists (may not on fresh stores)
        log_path.touch(exist_ok=True)
        with open(log_path) as f:
            f.seek(0, 2)  # Seek to end
            idle_checks = 0
            while True:
                line = f.readline()
                if line:
                    typer.echo(line.rstrip(), err=True)
                    idle_checks = 0
                else:
                    # No new line — check if daemon is still running
                    idle_checks += 1
                    if idle_checks >= 5 and not kp._is_processor_running():
                        stats = kp._pending_queue.stats()
                        flow_pending = kp.pending_work_count() if hasattr(kp, "pending_work_count") else 0
                        if (
                            stats.get("pending", 0) == 0
                            and stats.get("processing", 0) == 0
                            and flow_pending == 0
                        ):
                            typer.echo("Done.", err=True)
                        else:
                            typer.echo(_queue_status_line(kp, stats), err=True)
                        break
                    time.sleep(0.5)
    except (KeyboardInterrupt, EOFError):
        stats = kp._pending_queue.stats()
        typer.echo(
            f"\nDetached. Daemon still running. {_queue_status_line(kp, stats)}",
            err=True,
        )


@app.command("flow")
def flow_cmd(
    state: Annotated[Optional[str], typer.Argument(
        help="State doc name (e.g. 'after-write', 'query-resolve')",
    )] = None,
    target: Annotated[Optional[str], typer.Option(
        "--target", "-t", help="Target note ID to operate on",
    )] = None,
    file: Annotated[Optional[str], typer.Option(
        "--file", "-f", help="YAML state doc file path, or '-' for stdin",
    )] = None,
    budget: Annotated[Optional[int], typer.Option(
        "--budget", "-b", help="Max ticks for this invocation (default: from config)",
    )] = None,
    cursor: Annotated[Optional[str], typer.Option(
        "--cursor", "-c", help="Cursor from a previous stopped flow to resume",
    )] = None,
    param: Annotated[Optional[list[str]], typer.Option(
        "--param", "-p", help="Flow parameter as key=value",
    )] = None,
    store: StoreOption = None,
):
    """Run a state-doc flow synchronously.

    \b
    Examples:
        keep flow after-write --target %abc123
        keep flow query-resolve -p query="auth patterns"
        keep flow --file review.yaml --target myproject
        echo 'match: all' | keep flow --file - --target myproject
        keep flow --cursor <token> --budget 5
    """
    if state is None and file is None and cursor is None:
        typer.echo("Error: provide a state name, --file, or --cursor", err=True)
        raise typer.Exit(1)

    # Parse params
    flow_params: dict[str, Any] = {}
    if target:
        flow_params["id"] = target
    for p in (param or []):
        if "=" not in p:
            typer.echo(f"Error: param must be key=value, got: {p!r}", err=True)
            raise typer.Exit(1)
        k, v = p.split("=", 1)
        # Try to parse as number/bool for convenience
        try:
            flow_params[k] = json.loads(v)
        except (json.JSONDecodeError, ValueError):
            flow_params[k] = v

    # Load inline state doc YAML if --file provided
    state_doc_yaml: Optional[str] = None
    if file is not None:
        if file == "-":
            import sys
            state_doc_yaml = sys.stdin.read()
        else:
            try:
                state_doc_yaml = Path(file).read_text()
            except FileNotFoundError:
                typer.echo(f"Error: file not found: {file}", err=True)
                raise typer.Exit(1)
        if state is None:
            state = "inline"

    # When resuming, state comes from the cursor
    if cursor and state is None:
        state = "__cursor__"  # placeholder; run_flow uses cursor.state

    kp = _get_keeper(store)
    try:
        result = kp.run_flow_command(
            state,
            params=flow_params,
            budget=budget,
            cursor_token=cursor,
            state_doc_yaml=state_doc_yaml,
        )
    except Exception as e:
        typer.echo(f"Error: {e}", err=True)
        kp.close()
        raise typer.Exit(1)
    finally:
        kp.close()

    output: dict[str, Any] = {
        "status": result.status,
        "ticks": result.ticks,
    }
    if result.data:
        output["data"] = result.data
    if result.cursor:
        output["cursor"] = result.cursor
    if result.tried_queries:
        output["tried_queries"] = result.tried_queries

    if _get_json_output():
        typer.echo(json.dumps(output, ensure_ascii=False))
    else:
        typer.echo(json.dumps(output, ensure_ascii=False, indent=2))


# -----------------------------------------------------------------------------
# Data Management
# -----------------------------------------------------------------------------

data_app = typer.Typer(
    name="data",
    help="Data management — export, import.",
    rich_markup_mode=None,
)
app.add_typer(data_app)


@data_app.command("export")
def data_export(
    output: Annotated[str, typer.Argument(
        help="Output file path (use '-' for stdout)"
    )],
    exclude_system: Annotated[bool, typer.Option(
        "--exclude-system", help="Exclude system documents (dot-prefix IDs)"
    )] = False,
    store: StoreOption = None,
):
    """Export the store to JSON for backup or migration."""
    kp = _get_keeper(store)
    it = kp.export_iter(include_system=not exclude_system)
    header = next(it)

    dest = sys.stdout if output == "-" else open(output, "w", encoding="utf-8")
    try:
        # Write streaming JSON: header fields, then documents array
        dest.write("{\n")
        for key in ("format", "version", "exported_at", "store_info"):
            dest.write(f"  {json.dumps(key)}: {json.dumps(header[key], ensure_ascii=False)},\n")
        dest.write('  "documents": [\n')
        first = True
        for doc in it:
            if not first:
                dest.write(",\n")
            dest.write("    " + json.dumps(doc, ensure_ascii=False))
            first = False
        dest.write("\n  ]\n}\n")
    finally:
        if dest is not sys.stdout:
            dest.close()
    kp.close()

    if output != "-":
        info = header["store_info"]
        typer.echo(
            f"Exported {info['document_count']} documents "
            f"({info['version_count']} versions, {info['part_count']} parts) "
            f"to {output}",
            err=True,
        )


@data_app.command("import")
def data_import(
    file: Annotated[str, typer.Argument(help="JSON export file to import")],
    mode: Annotated[str, typer.Option(
        "--mode", "-m", help="Import mode: merge (skip existing) or replace (clear first)"
    )] = "merge",
    store: StoreOption = None,
):
    """Import documents from a JSON export file."""
    if mode not in ("merge", "replace"):
        typer.echo(f"Error: --mode must be 'merge' or 'replace', got '{mode}'", err=True)
        raise SystemExit(1)

    if file == "-":
        data = json.loads(sys.stdin.read())
    else:
        path = Path(file)
        if not path.exists():
            typer.echo(f"Error: file not found: {file}", err=True)
            raise SystemExit(1)
        data = json.loads(path.read_text(encoding="utf-8"))

    if mode == "replace":
        doc_count = len(data.get("documents", []))
        if not typer.confirm(
            f"This will delete all existing documents and import {doc_count} from {file}. Continue?"
        ):
            raise SystemExit(0)

    kp = _get_keeper(store)
    stats = kp.import_data(data, mode=mode)
    kp.close()

    typer.echo(
        f"Imported {stats['imported']} documents "
        f"({stats['versions']} versions, {stats['parts']} parts), "
        f"skipped {stats['skipped']}. "
        f"Queued {stats['queued']} for embedding.",
        err=True,
    )
    if stats["queued"] > 0:
        typer.echo("Run 'keep pending' to process embeddings.", err=True)


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------


@app.command(hidden=True)
def doctor(
    store: StoreOption = None,
    log: Annotated[bool, typer.Option(
        "--log", "-l", help="Tail the ops log (Ctrl-C to stop)"
    )] = False,
    use_faulthandler: Annotated[bool, typer.Option(
        "--faulthandler", help="Enable faulthandler for native crash traces"
    )] = False,
):
    """Diagnostic checks for debugging setup and crash issues."""
    if log:
        from .paths import get_config_dir, get_default_store_path
        from .config import load_or_create_config
        actual_store = store if store is not None else _get_store_override()
        config_dir = Path(actual_store).resolve() if actual_store else get_config_dir()
        cfg = load_or_create_config(config_dir)
        sp = Path(get_default_store_path(cfg) if actual_store is None else actual_store)
        log_path = sp / "keep-ops.log"
        if not log_path.exists():
            typer.echo(f"No ops log at {log_path}", err=True)
            raise typer.Exit(1)
        typer.echo(f"Tailing {log_path} (Ctrl-C to stop)\n", err=True)
        try:
            with open(log_path) as f:
                f.seek(0, 2)  # seek to end
                while True:
                    line = f.readline()
                    if line:
                        typer.echo(line.rstrip())
                    else:
                        time.sleep(0.3)
        except (KeyboardInterrupt, EOFError):
            pass
        return

    if use_faulthandler:
        import faulthandler
        faulthandler.enable()
        typer.echo("faulthandler enabled (native crash traces will print to stderr)\n")

    def ok(msg):
        typer.echo(f"  [ok]   {msg}")

    def fail(msg):
        typer.echo(f"  [FAIL] {msg}")

    def warn(msg):
        typer.echo(f"  [WARN] {msg}")

    # 1. Environment
    try:
        kv = importlib.metadata.version("keep-skill")
    except Exception:
        kv = "?"
    py_ver = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    plat = f"{platform.system()} {platform.release()} ({platform.machine()})"
    ok(f"Environment: Python {py_ver}, {plat}, keep-skill {kv}")

    # 2. Key packages
    pkg_names = ["chromadb", "sentence_transformers", "torch", "pydantic", "sqlite3"]
    pkg_versions = {}
    for pkg in pkg_names:
        try:
            mod = __import__(pkg)
            pkg_versions[pkg] = getattr(mod, "__version__", getattr(mod, "version", "imported"))
        except ImportError:
            pkg_versions[pkg] = "not installed"
    pkg_str = ", ".join(f"{k} {v}" for k, v in pkg_versions.items())
    ok(f"Packages: {pkg_str}")

    # 3. Store config
    from .config import load_or_create_config
    from .paths import get_config_dir, get_default_store_path
    try:
        actual_store = store if store is not None else _get_store_override()
        config_dir = Path(actual_store).resolve() if actual_store else get_config_dir()
        cfg = load_or_create_config(config_dir)
        store_path = Path(get_default_store_path(cfg) if actual_store is None else actual_store)
        emb_name = cfg.embedding.name if cfg and cfg.embedding else "none"
        sum_name = cfg.summarization.name if cfg and cfg.summarization else "none"
        ok(f"Store config: {store_path} (embedding: {emb_name}, summarization: {sum_name})")
    except Exception as e:
        fail(f"Store config: {e}")
        store_path = None

    # 4. SQLite (DocumentStore)
    db_path = store_path / "documents.db" if store_path else None
    if db_path and db_path.exists():
        try:
            from .document_store import DocumentStore
            ds = DocumentStore(db_path)
            doc_count = ds.count("default")
            ver_count = ds.count_versions("default")
            ok(f"SQLite: {doc_count} documents, {ver_count} versions")
            ds.close()
        except Exception as e:
            fail(f"SQLite: {e}")
    elif store_path:
        ok("SQLite: no documents.db yet (new store)")
    else:
        fail("SQLite: skipped (no store path)")

    # 5. Embedding provider
    if cfg and cfg.embedding:
        try:
            from .providers.base import get_registry
            registry = get_registry()
            provider = registry.create_embedding(cfg.embedding.name, cfg.embedding.params)
            t0 = time.perf_counter()
            vec = provider.embed("keep doctor test")
            elapsed_ms = (time.perf_counter() - t0) * 1000
            dim = len(vec)
            model = getattr(provider, "model_name", "?")
            ok(f"Embedding: {model}, dim={dim}, {elapsed_ms:.0f}ms")
        except Exception as e:
            fail(f"Embedding: {e}")
    else:
        ok("Embedding: no provider configured")

    # 6. Summarization provider
    if cfg and cfg.summarization and cfg.summarization.name != "passthrough":
        try:
            from .providers.base import get_registry
            registry = get_registry()
            provider = registry.create_summarization(cfg.summarization.name, cfg.summarization.params)
            t0 = time.perf_counter()
            result = provider.summarize("The quick brown fox jumps over the lazy dog.")
            elapsed_ms = (time.perf_counter() - t0) * 1000
            model = getattr(provider, "model_name", cfg.summarization.name)
            ok(f"Summarization: {model}, {elapsed_ms:.0f}ms")
        except Exception as e:
            fail(f"Summarization: {e}")
    else:
        ok(f"Summarization: {'passthrough' if cfg and cfg.summarization else 'none'}")

    # 7. Media describer
    if cfg and cfg.media:
        try:
            from .providers.base import get_registry
            registry = get_registry()
            provider = registry.create_media(cfg.media.name, cfg.media.params)
            model = getattr(provider, "model", getattr(provider, "model_name", cfg.media.name))
            ok(f"Media: {cfg.media.name} ({model})")
        except Exception as e:
            fail(f"Media: {e}")
    elif cfg:
        from .config import (
            _detect_ollama, _ollama_vision_models, ollama_pull,
            OLLAMA_DEFAULT_VISION_MODEL, OLLAMA_DEFAULT_OCR_MODEL,
            save_config, ProviderConfig,
        )
        ollama = _detect_ollama()
        if ollama:
            vision = _ollama_vision_models(ollama["models"])
            if vision:
                # Vision model available but not configured — auto-configure
                model_name = vision[0]
                params: dict[str, Any] = {"model": model_name}
                if ollama["base_url"] != "http://localhost:11434":
                    params["base_url"] = ollama["base_url"]
                cfg.media = ProviderConfig("ollama", params)
                save_config(cfg)
                ok(f"Media: auto-configured ollama ({model_name})")
            else:
                # Ollama running but no vision model — pull one
                target = OLLAMA_DEFAULT_VISION_MODEL
                typer.echo(f"         Pulling {target}...", nl=False)
                last_status = [""]
                def _progress(s: str) -> None:
                    if s != last_status[0]:
                        last_status[0] = s
                        typer.echo(f"\r         Pulling {target}... {s}    ", nl=False)
                if ollama_pull(target, ollama["base_url"], on_progress=_progress):
                    typer.echo(f"\r         Pulling {target}... done.           ")
                    params = {"model": target}
                    if ollama["base_url"] != "http://localhost:11434":
                        params["base_url"] = ollama["base_url"]
                    cfg.media = ProviderConfig("ollama", params)
                    save_config(cfg)
                    ok(f"Media: pulled and configured ollama ({target})")
                else:
                    typer.echo()
                    warn(f"Media: failed to pull {target}")
        else:
            ok("Media: none available (metadata-only indexing)")

    # 7b. Content extractor (OCR for PDFs/images)
    if cfg and cfg.content_extractor:
        # Verify the model is actually available if it's Ollama
        if cfg.content_extractor.name == "ollama":
            from .config import _detect_ollama, _ollama_has_model, ollama_pull, OLLAMA_DEFAULT_OCR_MODEL, save_config
            ce_model = cfg.content_extractor.params.get("model", OLLAMA_DEFAULT_OCR_MODEL)
            if _ollama_has_model(ce_model):
                ok(f"Content extractor: {cfg.content_extractor.name} ({ce_model})")
            else:
                typer.echo(f"         Pulling {ce_model}...", nl=False)
                base_url = cfg.content_extractor.params.get("base_url")
                last_status = [""]
                def _ce_progress(s: str) -> None:
                    if s != last_status[0]:
                        last_status[0] = s
                        typer.echo(f"\r         Pulling {ce_model}... {s}    ", nl=False)
                if ollama_pull(ce_model, base_url, on_progress=_ce_progress):
                    typer.echo(f"\r         Pulling {ce_model}... done.           ")
                    ok(f"Content extractor: pulled {ce_model}")
                else:
                    typer.echo()
                    warn(f"Content extractor: {ce_model} not available, pull it: ollama pull {ce_model}")
        else:
            ok(f"Content extractor: {cfg.content_extractor.name}")
    elif cfg:
        from .config import _detect_ollama, ollama_pull, OLLAMA_DEFAULT_OCR_MODEL, save_config, ProviderConfig
        ollama = _detect_ollama()
        if ollama:
            # Ollama running — pull OCR model and configure
            target = OLLAMA_DEFAULT_OCR_MODEL
            if any(m.split(":")[0] == target.split(":")[0] for m in ollama["models"]):
                # Model already present, just configure
                params: dict[str, Any] = {"model": target}
                if ollama["base_url"] != "http://localhost:11434":
                    params["base_url"] = ollama["base_url"]
                cfg.content_extractor = ProviderConfig("ollama", params)
                save_config(cfg)
                ok(f"Content extractor: auto-configured ollama ({target})")
            else:
                typer.echo(f"         Pulling {target}...", nl=False)
                last_status = [""]
                def _ocr_progress(s: str) -> None:
                    if s != last_status[0]:
                        last_status[0] = s
                        typer.echo(f"\r         Pulling {target}... {s}    ", nl=False)
                if ollama_pull(target, ollama["base_url"], on_progress=_ocr_progress):
                    typer.echo(f"\r         Pulling {target}... done.           ")
                    params = {"model": target}
                    if ollama["base_url"] != "http://localhost:11434":
                        params["base_url"] = ollama["base_url"]
                    cfg.content_extractor = ProviderConfig("ollama", params)
                    save_config(cfg)
                    ok(f"Content extractor: pulled and configured ollama ({target})")
                else:
                    typer.echo()
                    warn(f"Content extractor: failed to pull {target}")
        else:
            ok("Content extractor: none (PDFs use text extraction only)")

    # 8. Analyzer
    if cfg and cfg.analyzer:
        try:
            from .providers.base import get_registry
            registry = get_registry()
            provider = registry.create_analyzer(cfg.analyzer.name, cfg.analyzer.params)
            ok(f"Analyzer: {cfg.analyzer.name}")
        except Exception as e:
            fail(f"Analyzer: {e}")
    else:
        ok("Analyzer: default (uses summarization provider)")

    # 9. ChromaDB
    if store_path and (store_path / "chroma").exists():
        try:
            from .store import ChromaStore
            cs = ChromaStore(store_path)
            collections = cs.list_collections()
            counts = {c: cs.count(c) for c in collections}
            parts = [f"{c} ({n})" for c, n in counts.items()]
            ok(f"ChromaDB: {', '.join(parts) if parts else 'no collections'}")
        except Exception as e:
            fail(f"ChromaDB: {e}")
    elif store_path:
        ok("ChromaDB: no chroma/ yet (new store)")
    else:
        fail("ChromaDB: skipped (no store path)")

    # 10. Model locks
    if store_path:
        from .model_lock import ModelLock
        for lock_name in [".embedding.lock", ".summarization.lock"]:
            lock_file = store_path / lock_name
            if lock_file.exists():
                probe = ModelLock(lock_file)
                if probe.is_locked():
                    fail(f"Lock: {lock_name} is held by another process (lsof {lock_file})")
                else:
                    ok(f"Lock: {lock_name} available")
            else:
                ok(f"Lock: {lock_name} (not yet created)")
    else:
        ok("Lock: skipped (no store path)")

    # 11. Round-trip (temp store, isolates stack from store data)
    from .config import StoreConfig, ProviderConfig
    tmp_dir = None
    try:
        tmp_dir = tempfile.mkdtemp(prefix="keep_doctor_")
        tmp_path = Path(tmp_dir)
        # Minimal config: use store's embedding provider, passthrough summarization
        test_config = StoreConfig(
            path=tmp_path,
            embedding=cfg.embedding if cfg else None,
            summarization=ProviderConfig("passthrough", {"max_chars": 10000}),
            max_summary_length=10000,
        )
        t0 = time.perf_counter()
        kp = Keeper(tmp_dir, config=test_config)
        kp.put("The quick brown fox jumps over the lazy dog", id="doctor_test")
        item = kp.get("doctor_test")
        elapsed_ms = (time.perf_counter() - t0) * 1000
        if item and "fox" in item.summary:
            ok(f"Round-trip: put + get in {elapsed_ms:.0f}ms")
        else:
            fail("Round-trip: put succeeded but get returned unexpected result")
    except Exception as e:
        fail(f"Round-trip: {e}")
    finally:
        if tmp_dir:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    # 12. System doc validation
    if store_path:
        try:
            from .validate import validate_system_doc
            kp = _get_keeper(store)
            doc_coll = kp._resolve_doc_collection()
            docs_by_id: dict[str, tuple[str, dict]] = {}
            for sys_prefix in (".tag/", ".meta/", ".prompt/", ".state/"):
                for rec in kp._document_store.query_by_id_prefix(doc_coll, sys_prefix):
                    doc_id = str(getattr(rec, "id", ""))
                    if doc_id:
                        summary = str(getattr(rec, "summary", "") or "")
                        raw_tags = getattr(rec, "tags", None)
                        tags_dict = dict(raw_tags) if isinstance(raw_tags, dict) else {}
                        docs_by_id[doc_id] = (summary, tags_dict)
            if docs_by_id:
                errors = 0
                warnings = 0
                bad_docs = []
                for doc_id in sorted(docs_by_id):
                    content, tags_dict = docs_by_id[doc_id]
                    result = validate_system_doc(doc_id, content, tags_dict)
                    errors += len(result.errors)
                    warnings += len(result.warnings)
                    if result.errors:
                        bad_docs.append(doc_id)
                if errors:
                    fail(f"System docs: {len(docs_by_id)} docs, {errors} error(s) in {', '.join(bad_docs)}")
                elif warnings:
                    ok(f"System docs: {len(docs_by_id)} docs, {warnings} warning(s)")
                else:
                    ok(f"System docs: {len(docs_by_id)} docs validated")
            else:
                ok("System docs: none found (new store)")
        except Exception as e:
            fail(f"System docs: {e}")
    else:
        ok("System docs: skipped (no store path)")

    typer.echo()


@app.command()
def mcp(
    store: StoreOption = None,
):
    """Start MCP stdio server for AI agent integration."""
    if store is not None:
        os.environ["KEEP_STORE_PATH"] = str(store)
    elif _get_store_override() is not None:
        os.environ["KEEP_STORE_PATH"] = str(_get_store_override())
    from .mcp import main as mcp_main
    mcp_main()


# -----------------------------------------------------------------------------

def main():
    try:
        app()
    except SystemExit:
        raise  # Let typer handle exit codes
    except KeyboardInterrupt:
        raise SystemExit(130)  # Standard exit code for Ctrl+C
    except Exception as e:
        # Log full traceback to file, show clean message to user
        from .errors import log_exception
        log_path = log_exception(e, context="keep CLI")
        typer.echo(f"Error: {e}", err=True)
        typer.echo(f"Details logged to {log_path}", err=True)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
