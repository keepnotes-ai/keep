"""CLI interface for reflective memory.

Usage:
    keep find "query text"
    keep put file:///path/to/doc.md
    keep get file:///path/to/doc.md
"""

import importlib.metadata
import importlib.resources
import json
import logging
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

logger = logging.getLogger(__name__)

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


from .utils import _list_directory_files  # noqa: E402


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
    """Reflective memory with semantic search (delegated commands)."""
    pass


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


def _get_keeper(store: Optional[Path], *, _force_local: bool = False) -> Keeper:
    """Initialize memory, handling errors gracefully.

    Returns a local Keeper or RemoteKeeper (cloud) depending on config.
    Local daemon commands (put directory, pending, etc.)
    always use a local Keeper — the thin CLI handles the daemon HTTP path.

    When ``_force_local`` is True, skips the remote backend check
    (used by ``keep pending`` which manages the daemon itself).
    """
    import atexit

    # Check for remote backend config (env vars or TOML [remote] section)
    api_url = os.environ.get("KEEPNOTES_API_URL", "https://api.keepnotes.ai")
    api_key = os.environ.get("KEEPNOTES_API_KEY")
    if api_url and api_key and not _force_local:
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
        try:
            ignore_patterns = kp._load_ignore_patterns()
        except AttributeError:
            ignore_patterns = []
        combined_exclude = merge_excludes(ignore_patterns, exclude)
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

        # Git changelog ingest: find all git repos in the tree
        if hasattr(kp, "_get_work_queue"):
            from .git_ingest import discover_git_roots
            git_roots = discover_git_roots(files)
            for root_str in sorted(git_roots):
                try:
                    kp._get_work_queue().enqueue(
                        "ingest_git",
                        {"item_id": f"file://{root_str}", "directory": root_str},
                        supersede_key=f"git:{root_str}",
                        priority=1,
                    )
                except Exception as e:
                    logger.warning("Failed to queue git ingest for %s: %s", root_str, e)
            if git_roots:
                typer.echo(f"git: {len(git_roots)} repo(s) queued for changelog ingest", err=True)

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
    ctx = kp.get_context(
        item.id, similar_limit=3,
        include_meta=False, include_parts=False, include_versions=False,
    )
    typer.echo(render_context(ctx, as_json=_get_json_output()))



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



@app.command()
def config(
    path: Annotated[Optional[str], typer.Argument(
        help="Config path to get (e.g., 'file', 'tool', 'store', 'providers.embedding')"
    )] = None,
    setup: Annotated[bool, typer.Option(
        "--setup",
        help="Run interactive setup wizard (provider and tool selection)"
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
        from ._daemon_client import resolve_store_path
        store_path = resolve_store_path(str(store) if store else None)
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
        # Clean up stale port file
        (store_path / ".daemon.port").unlink(missing_ok=True)
        return

    # pending always needs a local Keeper (manages daemon, queues, etc.)
    kp = _get_keeper(store, _force_local=True)

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
        # Write version file so future callers can compare
        try:
            (kp._store_path / ".processor.version").write_text(_ver)
        except Exception:
            pass
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

        # Start HTTP query server
        from .daemon_server import DaemonServer, DEFAULT_PORT
        _daemon_port = int(os.environ.get("KEEP_DAEMON_PORT", "0")) or DEFAULT_PORT
        _daemon_server = DaemonServer(kp, port=_daemon_port)
        _actual_port = _daemon_server.start()
        _port_path = kp._store_path / ".daemon.port"
        _token_path = kp._store_path / ".daemon.token"
        _port_path.write_text(str(_actual_port))
        _token_path.write_text(_daemon_server.auth_token)
        _daemon_logger.info("Query server on 127.0.0.1:%d", _actual_port)

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

        _REPLENISH_INTERVAL = 1800  # 30 minutes

        # Initialize timer timestamps from persisted state so the daemon
        # resumes schedules after restart instead of starting from zero.
        try:
            from .timer_state import read_timer_state
            _saved_timers = read_timer_state(kp._store_path)
            _last_replenish_ts = _saved_timers.get("supernode-replenish", {}).get("last_run", 0.0)
        except Exception:
            _last_replenish_ts = 0.0

        def _replenish_supernodes():
            nonlocal _last_replenish_ts
            now = time.time()
            if now - _last_replenish_ts < _REPLENISH_INTERVAL:
                return
            _last_replenish_ts = now
            try:
                enqueued = kp.replenish_supernode_queue()
                detail = f"{enqueued} enqueued" if enqueued else "no candidates"
                if enqueued > 0:
                    _daemon_logger.info("Replenished %d supernode review(s)", enqueued)
            except Exception as exc:
                detail = f"error: {exc}"
                _daemon_logger.debug("Supernode replenishment error: %s", exc)
            try:
                from .timer_state import write_timer_event
                write_timer_event(
                    kp._store_path, "supernode-replenish",
                    interval=_REPLENISH_INTERVAL, detail=detail,
                )
            except Exception as _te:
                _daemon_logger.warning("Failed to write timer state: %s", _te)

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

        # Version-aware restart: if a newer version of keep-skill wrote
        # .processor.version, exec-restart to pick up new code.
        _version_file = kp._store_path / ".processor.version"

        def _check_version_restart():
            """Exec-restart if a newer version is expected."""
            try:
                if not _version_file.exists():
                    return
                requested = _version_file.read_text().strip()
                if requested and requested != _ver:
                    _daemon_logger.info(
                        "Version changed (%s → %s), restarting daemon",
                        _ver, requested,
                    )
                    # Clean up before exec
                    try:
                        kp.close()
                    except Exception:
                        pass
                    os.execv(sys.executable, [sys.executable] + sys.argv)
            except Exception as e:
                _daemon_logger.debug("Version check failed: %s", e)

        try:
            pid_path.write_text(str(os.getpid()))
            while not shutdown_requested:
                _check_version_restart()
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

                # Supernode queue replenishment (self-throttled)
                _replenish_supernodes()

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

                    # Timer events keep the daemon alive: watches, supernodes, etc.
                    _has_timers = has_active_watches(kp)
                    if not _has_timers:
                        # Keep alive if supernode replenishment hasn't had a chance
                        # to run yet (never run, or next run is within 2× interval).
                        # After replenishment runs and finds nothing, it sets detail
                        # to "no candidates" and the daemon can eventually exit.
                        _has_timers = (
                            _last_replenish_ts == 0  # never run — give it a chance
                            or (time.time() - _last_replenish_ts) < _REPLENISH_INTERVAL
                        )

                    if _has_timers:
                        if has_active_watches(kp):
                            delay = next_check_delay(load_watches(kp))
                        else:
                            # Next timer event: check how long until replenish fires
                            _time_to_replenish = max(0, _REPLENISH_INTERVAL - (time.time() - _last_replenish_ts))
                            delay = min(_time_to_replenish, 60.0)
                        delay = max(1.0, min(delay, 60.0))
                        _daemon_logger.debug("Sleeping %.1fs (timer events pending)", delay)
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
                _daemon_server.stop()
            except Exception:
                pass
            try:
                _port_path.unlink()
            except OSError:
                pass
            try:
                _token_path.unlink()
            except OSError:
                pass
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
            flow_items = wq.list_pending(limit=-1)
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
                # Break down flow items by state name for visibility
                wq = kp._get_work_queue()
                by_kind = wq.count_by_kind()
                for kind, count in by_kind.items():
                    if kind != "flow":
                        typer.echo(f"  {kind:20s} {count}")
                # Show flow items grouped by state
                flow_by_state: dict[str, list] = {}
                for fi in flow_items:
                    if fi.get("kind") != "flow":
                        continue
                    inp = fi.get("input") or {}
                    state = inp.get("state", "unknown")
                    flow_by_state.setdefault(state, []).append(fi)
                for state, state_items in sorted(flow_by_state.items()):
                    if len(state_items) <= 3:
                        for si in state_items:
                            inp = si.get("input") or {}
                            target = inp.get("item_id") or inp.get("params", {}).get("item_id", "")
                            if target:
                                typer.echo(f"  flow:{state:16s} {target}")
                            else:
                                typer.echo(f"  flow:{state}")
                    else:
                        typer.echo(f"  flow:{state:16s} ({len(state_items)} items)")
            if failed:
                typer.echo(f"\nFailed ({len(failed)}):")
                for item in failed[:10]:
                    error = item.get("last_error", "unknown")
                    typer.echo(f"  {item['task_type']:15s} {item['id']}: {error}")
        # Show timer events (watchers, replenishment, cleanup)
        try:
            from .watches import list_watches
            watches = list_watches(kp)
        except Exception:
            watches = []
        try:
            from .timer_state import format_timer_events, read_timer_state
            timer_state = read_timer_state(kp._store_path)
        except Exception:
            timer_state = {}
        # Also check for known timers (even if never fired)
        from .timer_state import KNOWN_TIMERS
        has_timer_info = watches or timer_state or KNOWN_TIMERS
        if has_timer_info:
            typer.echo("\nTimer events:")
            # Show watches with schedule info
            if watches:
                from .timer_state import _format_ago, _format_until
                from datetime import datetime, timezone
                _now_ts = time.time()
                for w in watches:
                    suffix = " [stale]" if w.stale else ""
                    name = f"watch:{w.kind}"
                    # Parse last_checked and interval
                    ago_str = "never"
                    next_str = ""
                    if w.last_checked:
                        try:
                            _lc = datetime.fromisoformat(w.last_checked)
                            _lc_ts = _lc.replace(tzinfo=timezone.utc).timestamp()
                            ago_str = _format_ago(_now_ts - _lc_ts)
                            # Parse ISO duration for interval
                            from .watches import parse_duration
                            _dur = parse_duration(w.interval)
                            _next_ts = _lc_ts + _dur.total_seconds() - _now_ts
                            next_str = _format_until(_next_ts)
                        except Exception:
                            pass
                    source = w.source
                    if len(source) > 40:
                        source = "..." + source[-37:]
                    parts_line = f"  {name:24s} last: {ago_str:>8s}"
                    if next_str:
                        parts_line += f"  next: {next_str}"
                    parts_line += f"  {source}{suffix}"
                    typer.echo(parts_line)
            # Show persisted + known timer events
            try:
                lines = format_timer_events(kp._store_path)
                for line in lines:
                    typer.echo(line)
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

    # Check for timer events that need a daemon (watches, supernodes)
    _has_timer_events = False
    try:
        from .watches import has_active_watches
        _has_timer_events = has_active_watches(kp)
    except Exception:
        pass
    if not _has_timer_events:
        try:
            from .timer_state import KNOWN_TIMERS, read_timer_state
            _timer_state = read_timer_state(kp._store_path)
            # Timer events need a daemon if any haven't run recently
            import time as _time
            for name, info in KNOWN_TIMERS.items():
                saved = _timer_state.get(name, {})
                last_run = saved.get("last_run", 0)
                interval = info.get("interval", 0)
                if interval and (_time.time() - last_run) >= interval:
                    _has_timer_events = True
                    break
        except Exception:
            pass

    if pending_count == 0 and processing_count == 0 and flow_pending_count == 0 and not _has_timer_events:
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

    if pending_count > 0 or processing_count > 0 or flow_pending_count > 0:
        typer.echo(_queue_status_line(kp, queue_stats), err=True)
    elif _has_timer_events:
        typer.echo("No work queued, but timer events need servicing.", err=True)

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

    # Flow work queue breakdown by kind, sorted by processing priority
    flow_by_kind = ""
    if flow_pending:
        try:
            wq = kp._get_work_queue()
            by_kind = wq.count_by_kind()  # pre-sorted by priority
            if by_kind:
                flow_by_kind = ", ".join(f"{c} {k}" for k, c in by_kind.items())
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


# ---------------------------------------------------------------------------
# Entry point (used by daemon subprocess: python -m keep.cli pending --daemon)
# ---------------------------------------------------------------------------

def main():
    app()


if __name__ == "__main__":
    main()
