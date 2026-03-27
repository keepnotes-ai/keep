"""Thin CLI — pure display layer over the daemon HTTP API.

Parse args → one HTTP call → render JSON → exit.
No keep internals, no models, no database. ~50ms startup.
"""

import json
import os
import shutil
import sys
from pathlib import Path
from typing import Annotated, Optional
from urllib.parse import quote

import typer

from ._daemon_client import http_request as _http, get_port as _daemon_get_port

app = typer.Typer(
    name="keep",
    no_args_is_help=False,
    invoke_without_command=True,
    add_completion=False,
    rich_markup_mode=None,
)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _q(id: str) -> str:
    """URL-encode an ID for path segments."""
    return quote(id, safe="")



def _get(port: int, path: str) -> dict:
    status, body = _http("GET", port, path)
    if status == 404:
        typer.echo(f"Not found", err=True)
        raise typer.Exit(1)
    if status != 200:
        typer.echo(f"Error: {body.get('error', 'unknown')}", err=True)
        raise typer.Exit(1)
    return body


def _post(port: int, path: str, body: dict) -> dict:
    status, result = _http("POST", port, path, body)
    if status != 200:
        typer.echo(f"Error: {result.get('error', 'unknown')}", err=True)
        raise typer.Exit(1)
    return result


def _patch(port: int, path: str, body: dict) -> dict:
    status, result = _http("PATCH", port, path, body)
    if status != 200:
        typer.echo(f"Error: {result.get('error', 'unknown')}", err=True)
        raise typer.Exit(1)
    return result


def _delete(port: int, path: str) -> dict:
    status, result = _http("DELETE", port, path)
    if status != 200:
        typer.echo(f"Error: {result.get('error', 'unknown')}", err=True)
        raise typer.Exit(1)
    return result


# ---------------------------------------------------------------------------
# Daemon port resolution (delegates to _daemon_client)
# ---------------------------------------------------------------------------

def _get_port() -> int:
    """Get daemon port, auto-starting if needed."""
    return _daemon_get_port(_global_store)


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------

def _width() -> int:
    return shutil.get_terminal_size((200, 24)).columns


def _truncate(text: str, max_len: int) -> str:
    """Replace newlines with spaces and truncate at word boundary."""
    text = text.replace("\n", " ")
    if len(text) > max_len:
        return text[:max_len - 3].rsplit(" ", 1)[0] + "..."
    return text


def _parse_tag_args(raw: list[str] | None) -> tuple[dict[str, str], list[str]]:
    """Parse --tag args into (key=value dict, key-only list)."""
    tag_dict: dict[str, str] = {}
    tag_keys: list[str] = []
    for t in (raw or []):
        if "=" in t:
            k, v = t.split("=", 1)
            tag_dict[k] = v
        else:
            tag_keys.append(t)
    return tag_dict, tag_keys


def _date(tags: dict) -> str:
    """Extract display date from tags."""
    from keep.types import local_date
    for key in ("_updated", "_created"):
        val = tags.get(key, "")
        if val:
            return local_date(val)
    return ""


def _display_tags(tags: dict) -> dict:
    """Filter to user-visible tags (matches keep/types.py:INTERNAL_TAGS)."""
    from keep.types import INTERNAL_TAGS
    return {k: v for k, v in tags.items()
            if k not in INTERNAL_TAGS
            and not k.startswith("_tk::")
            and not k.startswith("_tv::")}


def _yaml_quote(v: str) -> str:
    """Quote a YAML scalar value (matches cli.py's _quote_scalar_tag_value)."""
    return json.dumps(str(v))


def _render_tags_block(tags: dict) -> str:
    """Render tags as YAML-style indented block matching old CLI output."""
    display = _display_tags(tags)
    if not display:
        return ""
    lines = []
    for k, v in sorted(display.items()):
        if isinstance(v, list):
            lines.append(f"  {k}: [{', '.join(str(x) for x in v)}]")
        else:
            lines.append(f"  {k}: {_yaml_quote(str(v))}")
    return "\n".join(lines)


def _render_item_line(item: dict, w: int, id_width: int = 0) -> str:
    """Format a single item as a summary line."""
    id = item.get("id", "")
    score = item.get("score")
    tags = item.get("tags", {})
    date = _date(tags)
    padded = id.ljust(id_width) if id_width else id
    score_str = f" ({score:.2f})" if score is not None else ""
    date_str = f" {date}" if date else ""
    prefix_len = len(padded) + len(score_str) + len(date_str) + 2
    summary = _truncate(item.get("summary", ""), w - prefix_len)
    return f"{padded}{score_str}{date_str} {summary}"


def _render_context(data: dict) -> str:
    """Render ItemContext JSON as YAML frontmatter."""
    item = data.get("item", {})
    w = _width()
    lines = ["---"]

    # ID + version
    id_str = item.get("id", "")
    offset = data.get("viewing_offset", 0)
    if offset > 0:
        id_str += f"@V{{{offset}}}"
    lines.append(f"id: {id_str}")

    # Tags
    tags_block = _render_tags_block(item.get("tags", {}))
    if tags_block:
        lines.append("tags:")
        lines.append(tags_block)

    # Similar
    similar = data.get("similar", [])
    if similar:
        sim_ids = [s.get("id", "") for s in similar]
        id_width = min(max(len(s) for s in sim_ids), 20) if sim_ids else 0
        lines.append("similar:")
        for s in similar:
            sid = s.get("id", "")
            score = s.get("score")
            date = s.get("date", "")
            padded = sid.ljust(id_width)
            score_str = f"({score:.2f})" if score is not None else ""
            summary = _truncate(s.get("summary", ""), w - id_width - 25)
            lines.append(f"  - {padded} {score_str} {date} {summary}")

    # Meta sections
    meta = data.get("meta", {})
    for section, items in sorted(meta.items()):
        if items:
            meta_ids = [m.get("id", "") for m in items]
            id_width = min(max(len(s) for s in meta_ids), 20) if meta_ids else 0
            lines.append(f"meta/{section}:")
            for m in items:
                mid = m.get("id", "")
                padded = mid.ljust(id_width)
                msummary = _truncate(m.get("summary", ""), w - id_width - 10)
                lines.append(f"  - {padded} {msummary}")

    # Edges
    edges = data.get("edges", {})
    for pred, refs in sorted(edges.items()):
        if refs:
            lines.append(f"edges/{pred}:")
            for e in refs:
                eid = e.get("source_id", "")
                edate = e.get("date", "")
                esummary = _truncate(e.get("summary", ""), w - len(eid) - 15)
                lines.append(f"  - {eid} {edate} {esummary}")

    # Parts (compact: first + "N more" + last when many)
    parts = data.get("parts", [])
    if parts:
        lines.append("parts:")
        if len(parts) <= 3:
            for p in parts:
                pnum = p.get("part_num", 0)
                psummary = _truncate(p.get("summary", ""), w - 15)
                lines.append(f"  - @P{{{pnum}}} {psummary}")
        else:
            first, last = parts[0], parts[-1]
            lines.append(f"  - @P{{{first.get('part_num', 0)}}} {_truncate(first.get('summary', ''), w - 15)}")
            lines.append(f"  # (...{len(parts) - 2} more...)")
            lines.append(f"  - @P{{{last.get('part_num', 0)}}} {_truncate(last.get('summary', ''), w - 15)}")

    # Version navigation
    prev = data.get("prev", [])
    if prev:
        lines.append("prev:")
        for v in prev:
            voff = v.get("offset", 0)
            vdate = v.get("date", "")
            vsummary = _truncate(v.get("summary", ""), w - 20)
            lines.append(f"  - @V{{{voff}}} {vdate} {vsummary}")

    nxt = data.get("next", [])
    if nxt:
        lines.append("next:")
        for v in nxt:
            voff = v.get("offset", 0)
            vdate = v.get("date", "")
            vsummary = _truncate(v.get("summary", ""), w - 20)
            lines.append(f"  - @V{{{voff}}} {vdate} {vsummary}")

    lines.append("---")

    # Summary body
    summary = item.get("summary", "")
    if summary:
        lines.append(summary)

    return "\n".join(lines)


def _render_find(data: dict, port: int = 0) -> str:
    """Render search results. Respects --ids and --full global flags."""
    notes = data.get("notes", [])

    if _is_ids():
        return "\n".join(n.get("id", "") for n in notes) if notes else "No results."

    if _is_full() and port:
        # Full context for each result
        parts = []
        for n in notes:
            nid = n.get("id", "")
            status, ctx = _http("GET", port, f"/v1/notes/{_q(nid)}/context")
            if status == 200:
                parts.append(_render_context(ctx))
        return "\n\n".join(parts) if parts else "No results."

    w = _width()
    deep_groups = {g["id"]: g["items"] for g in data.get("deep_groups", []) if g.get("id")}

    # Compute aligned ID width
    all_ids = [n.get("id", "") for n in notes]
    id_width = min(max((len(i) for i in all_ids), default=0), 20)

    lines = []
    for item in notes:
        lines.append(_render_item_line(item, w, id_width))
        # Deep group items
        item_id = item.get("id", "")
        base_id = item_id.split("@")[0] if "@" in item_id else item_id
        group = deep_groups.get(base_id, deep_groups.get(item_id, []))
        for deep in group:
            deep_line = _render_item_line(deep, w - 2)
            lines.append(f"  {deep_line}")

    if not lines:
        return "No results."
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

StoreOption = Annotated[Optional[str], typer.Option("--store", "-s", envvar="KEEP_STORE_PATH", help="Store path")]
JsonFlag = Annotated[bool, typer.Option("--json", "-j", help="JSON output")]
LimitOption = Annotated[int, typer.Option("--limit", "-l", help="Result limit")]

# Global flags — set by app callback, read by helpers.
_global_json: bool = False
_global_ids: bool = False
_global_full: bool = False
_global_store: str | None = None


def _is_json(local_flag: bool = False) -> bool:
    """Check if JSON output is requested (local --json or global --json)."""
    return local_flag or _global_json


def _is_ids() -> bool:
    return _global_ids


def _is_full() -> bool:
    return _global_full


@app.callback(invoke_without_command=True)
def default(
    ctx: typer.Context,
    store: StoreOption = None,
    json_output: JsonFlag = False,
    ids_only: Annotated[bool, typer.Option(
        "--ids", "-I", help="Output only IDs (for piping)"
    )] = False,
    full_output: Annotated[bool, typer.Option(
        "--full", "-F", help="Output full notes with context"
    )] = False,
):
    """keep — reflective memory for AI agents."""
    global _global_json, _global_ids, _global_full, _global_store
    _global_json = json_output
    _global_ids = ids_only
    _global_full = full_output
    if store is not None:
        _global_store = store
    if ctx.invoked_subcommand is None:
        # No subcommand → show "now"
        port = _get_port()
        data = _get(port, f"/v1/notes/{_q('now')}/context")
        if _is_json(json_output):
            typer.echo(json.dumps(data, indent=2))
        else:
            typer.echo(_render_context(data))


@app.command()
def get(
    id: Annotated[list[str], typer.Argument(help="Item ID(s)")],
    version: Annotated[Optional[int], typer.Option(
        "--version", "-V", help="Version selector (>=0 from current, <0 from oldest)"
    )] = None,
    history: Annotated[bool, typer.Option(
        "--history", "-H", help="List all versions"
    )] = False,
    similar: Annotated[bool, typer.Option(
        "--similar", "-S", help="List similar notes"
    )] = False,
    meta: Annotated[bool, typer.Option(
        "--meta", "-M", help="List meta notes"
    )] = False,
    parts: Annotated[bool, typer.Option(
        "--parts", "-P", help="List structural parts (from analyze)"
    )] = False,
    tag: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t", help="Require tag (key or key=value, repeatable)"
    )] = None,
    limit: Annotated[int, typer.Option(
        "--limit", "-n", help="Max items per section (default: 10)"
    )] = 10,
    json_output: JsonFlag = False,
):
    """Retrieve note(s) by ID.

    \b
    Accepts one or more IDs. Version: append @V{N}. Part: append @P{N}.
    Examples:
        keep get doc:1                  # Current version with context
        keep get doc:1 doc:2 doc:3      # Multiple notes
        keep get doc:1 -V 1             # Previous version
        keep get "doc:1@P{1}"           # Part 1
        keep get doc:1 --history        # List all versions
        keep get doc:1 --similar        # List similar items
        keep get doc:1 --meta           # List meta items
        keep get doc:1 --parts          # List structural parts
        keep get doc:1 -t project=myapp # Only if tag matches
    """
    port = _get_port()
    outputs = []
    errors = 0
    for item_id in id:
        result = _get_one_item(
            port, item_id, version=version, limit=limit,
            similar=similar, meta=meta, parts=parts, history=history,
            tag=tag, json_output=json_output,
        )
        if result is None:
            errors += 1
        else:
            outputs.append(result)
    if outputs:
        sep = "\n" if _is_ids() else "\n---\n" if len(outputs) > 1 else ""
        typer.echo(sep.join(outputs))
    if errors and not outputs:
        raise typer.Exit(1)


def _get_one_item(
    port: int, item_id: str, *, version: Optional[int], limit: int,
    similar: bool, meta: bool, parts: bool, history: bool,
    tag: Optional[list[str]], json_output: bool,
) -> Optional[str]:
    """Fetch and render a single item. Returns formatted string or None on error."""

    # --similar: flat list via search endpoint
    if similar:
        data = _post(port, "/v1/search", {
            "similar_to": item_id, "limit": limit,
        })
        if _is_json(json_output):
            return json.dumps(data, indent=2)
        return _render_find(data, port)

    # Section-specific context modes
    _SECTION_QS = {
        "parts": "include_similar=false&include_meta=false&include_versions=false&parts_limit={}",
        "meta": "include_similar=false&include_parts=false&include_versions=false&meta_limit={}",
        "history": "include_similar=false&include_meta=false&include_parts=false&versions_limit={}",
    }
    section = "parts" if parts else "meta" if meta else "history" if history else None
    if section:
        qs = "?" + _SECTION_QS[section].format(limit)
    else:
        qs_parts = [f"similar_limit={limit}", f"meta_limit={limit}", f"edges_limit={limit}"]
        if version is not None:
            qs_parts.append(f"version={version}")
        qs = "?" + "&".join(qs_parts)

    status, data = _http("GET", port, f"/v1/notes/{_q(item_id)}/context{qs}")
    if status == 404:
        typer.echo(f"Not found: {item_id}", err=True)
        return None
    if status != 200:
        typer.echo(f"Error: {data.get('error', 'unknown')}", err=True)
        return None
    if _is_json(json_output):
        return json.dumps(data, indent=2)

    # Render section-specific output
    if parts:
        ctx_parts = data.get("parts", [])
        if not ctx_parts:
            return f"No parts for {item_id}. Use 'keep analyze {item_id}' to create parts."
        lines = [f"Parts of {item_id}:"]
        for p in ctx_parts:
            lines.append(f"  @P{{{p.get('part_num', 0)}}} {p.get('summary', '')[:80]}")
        return "\n".join(lines)
    if meta:
        meta_sections = data.get("meta", {})
        lines = [f"Meta for {item_id}:"]
        for sec, items in sorted(meta_sections.items()):
            if items:
                lines.append(f"  {sec}:")
                for m in items:
                    lines.append(f"    {m.get('id', '')}  {m.get('summary', '')[:50]}")
        if len(lines) == 1:
            lines.append("  No meta notes found.")
        return "\n".join(lines)
    if history:
        prev = data.get("prev", [])
        lines = [f"Versions of {item_id}:"]
        lines.append(f"  @V{{0}} (current)")
        for v in prev:
            lines.append(f"  @V{{{v.get('offset', 0)}}} {v.get('date', '')} {v.get('summary', '')[:60]}")
        if not prev:
            lines.append("  No previous versions.")
        return "\n".join(lines)

    # --tag: post-filter
    if tag:
        item_tags = data.get("item", {}).get("tags", {})
        for t in tag:
            if "=" in t:
                k, v = t.split("=", 1)
                if str(item_tags.get(k, "")).lower() != v.lower():
                    typer.echo(f"Tag filter not matched: {item_id}", err=True)
                    return None
            else:
                if t not in item_tags:
                    typer.echo(f"Tag filter not matched: {item_id}", err=True)
                    return None

    if _is_ids():
        return data.get("item", {}).get("id", item_id)
    if _is_json(json_output):
        return json.dumps(data, indent=2)
    return _render_context(data)


@app.command()
def find(
    query: Annotated[Optional[str], typer.Argument(help="Search query text")] = None,
    id: Annotated[Optional[str], typer.Option(
        "--id", help="Find notes similar to this ID (instead of text search)"
    )] = None,
    tag: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t", help="Filter by tag (key or key=value, repeatable)"
    )] = None,
    limit: LimitOption = 10,
    since: Annotated[Optional[str], typer.Option("--since", help="Updated since")] = None,
    until: Annotated[Optional[str], typer.Option("--until", help="Updated before")] = None,
    deep: Annotated[bool, typer.Option("--deep", "-D", help="Follow tags for related items")] = False,
    show_all: Annotated[bool, typer.Option(
        "--all", "-a", help="Include hidden system notes (IDs starting with '.')"
    )] = False,
    scope: Annotated[Optional[str], typer.Option(
        "--scope", "-S", help="ID glob to constrain results (e.g. 'file:///path/to/dir*')"
    )] = None,
    json_output: JsonFlag = False,
):
    """Find notes by hybrid search (semantic + full-text) or similarity.

    \b
    Examples:
        keep find "auth patterns"           # Semantic + full-text search
        keep find --id %abc123              # Find similar to item
        keep find "auth" -t project=myapp   # Search + filter by tag
        keep find "auth" --deep             # Follow tags for related items
        keep find "auth" --since P7D        # Last 7 days only
        keep find "auth" -S "file://.*"     # Scope to file items
    """
    if id and query:
        typer.echo("Error: Specify either a query or --id, not both", err=True)
        raise typer.Exit(1)
    if not id and not query:
        typer.echo("Error: Specify a query or --id", err=True)
        raise typer.Exit(1)

    port = _get_port()
    tag_filter, _ = _parse_tag_args(tag)

    body: dict = {
        "query": query if not id else None,
        "similar_to": id,
        "tags": tag_filter or None,
        "limit": limit,
        "deep": deep or None,
        "since": since,
        "until": until,
        "scope": scope,
        "include_hidden": show_all or None,
    }
    data = _post(port, "/v1/search", body)
    if _is_json(json_output):
        typer.echo(json.dumps(data, indent=2))
    else:
        typer.echo(_render_find(data, port))


def _put_directory(
    port: int, resolved_path: Path, parsed_tags: dict, *,
    recurse: bool, exclude: list[str] | None,
    watch: bool, unwatch: bool, interval: str | None,
    force: bool, json_output: bool,
) -> None:
    """Index files from a directory via the daemon HTTP API."""
    from .utils import _list_directory_files

    # Get ignore patterns from the store's .ignore doc (if any)
    ignore_patterns: list[str] = []
    try:
        status, data = _http("GET", port, f"/v1/notes/{_q('.ignore')}")
        if status == 200 and data.get("summary"):
            from .ignore import parse_ignore_patterns
            ignore_patterns = parse_ignore_patterns(data["summary"])
    except Exception:
        pass

    from .ignore import merge_excludes
    combined_exclude = merge_excludes(ignore_patterns, exclude)
    files = _list_directory_files(resolved_path, recurse=recurse, exclude=combined_exclude or None)
    if not files:
        typer.echo(f"Error: no eligible files in {resolved_path}/", err=True)
        hint = "hidden files and symlinks are skipped"
        if not recurse:
            hint += "; use -r to recurse into subdirectories"
        typer.echo(f"Hint: {hint}", err=True)
        raise typer.Exit(1)

    MAX_DIR_FILES = 1000
    if len(files) > MAX_DIR_FILES:
        typer.echo(f"Error: directory has {len(files)} files (max {MAX_DIR_FILES})", err=True)
        typer.echo("Hint: increase max_dir_files in keep.toml or index files individually", err=True)
        raise typer.Exit(1)

    indexed = 0
    errors: list[str] = []
    total = len(files)
    is_tty = sys.stderr.isatty()
    for i, fpath in enumerate(files, 1):
        file_uri = f"file://{fpath}"
        rel = str(fpath.relative_to(resolved_path) if recurse else fpath.name)
        body: dict = {"uri": file_uri, "tags": parsed_tags or None, "force": force or None}
        try:
            status, data = _http("POST", port, "/v1/notes", body)
            if status == 200:
                indexed += 1
                if is_tty:
                    pct = i * 100 // total
                    typer.echo(f"\r[{pct:3d}%] {rel[:60]}", err=True, nl=False)
                else:
                    typer.echo(f"[{i}/{total}] {rel} ok", err=True)
            else:
                errors.append(f"{rel}: {data.get('error', 'unknown')}")
                if not is_tty:
                    typer.echo(f"[{i}/{total}] {rel} error", err=True)
        except Exception as e:
            errors.append(f"{rel}: {e}")

    if is_tty:
        typer.echo("", err=True)
    typer.echo(f"{indexed} indexed, {len(errors)} errors from {resolved_path.name}/", err=True)
    for e in errors:
        typer.echo(f"  error: {e}", err=True)

    # Watch management
    if watch or unwatch:
        watch_body: dict = {
            "uri": f"file://{resolved_path}",
            "tags": parsed_tags or None,
            "force": None,
        }
        if watch:
            watch_body["watch"] = True
            watch_body["watch_kind"] = "directory"
            watch_body["recurse"] = recurse
            watch_body["exclude"] = exclude
            if interval:
                watch_body["interval"] = interval
        elif unwatch:
            watch_body["unwatch"] = True
        try:
            _http("POST", port, "/v1/notes", watch_body)
        except Exception:
            pass


@app.command()
def put(
    source: Annotated[Optional[str], typer.Argument(help="Content, file path, URI, or '-' for stdin")] = None,
    id: Annotated[Optional[str], typer.Option("--id", "-i", help="Item ID")] = None,
    tags: Annotated[Optional[list[str]], typer.Option("-t", "--tag", help="Tags (key=value)")] = None,
    summary: Annotated[Optional[str], typer.Option("--summary", help="Explicit summary")] = None,
    recurse: Annotated[bool, typer.Option("--recurse", "-r", help="Recurse into subdirectories (directory mode)")] = False,
    exclude: Annotated[Optional[list[str]], typer.Option(
        "--exclude", "-x", help="Glob pattern to exclude files (directory mode, repeatable)"
    )] = None,
    watch: Annotated[bool, typer.Option("--watch", help="Watch source for changes (daemon re-imports on change)")] = False,
    unwatch: Annotated[bool, typer.Option("--unwatch", help="Stop watching source for changes")] = False,
    interval: Annotated[Optional[str], typer.Option(
        "--interval", help="Watch poll interval as ISO 8601 duration (default PT30S)"
    )] = None,
    force: Annotated[bool, typer.Option("--force", "-f", help="Force update")] = False,
    json_output: JsonFlag = False,
):
    """Add or update a note in the store.

    \b
    Input modes (auto-detected):
      keep put /path/to/file.pdf     # File mode
      keep put https://example.com   # URI mode
      keep put /path/to/folder/      # Directory mode
      keep put /path/to/folder/ -r   # Directory mode (recurse)
      keep put "my note"             # Text mode (content-addressed ID)
      keep put -                     # Stdin mode
    """
    if watch and unwatch:
        typer.echo("Error: --watch and --unwatch are mutually exclusive", err=True)
        raise typer.Exit(1)
    if interval and not watch:
        typer.echo("Error: --interval requires --watch", err=True)
        raise typer.Exit(1)

    port = _get_port()
    parsed_tags = {}
    for t in (tags or []):
        if "=" not in t:
            typer.echo(f"Invalid tag format: {t!r} (expected key=value)", err=True)
            raise typer.Exit(1)
        k, v = t.split("=", 1)
        parsed_tags[k] = v

    # Stdin mode
    if source == "-" or (source is None and not sys.stdin.isatty()):
        content = sys.stdin.read()
        if summary is not None:
            typer.echo("Error: --summary cannot be used with stdin (original content would be lost)", err=True)
            raise typer.Exit(1)
        body: dict = {"content": content, "id": id, "tags": parsed_tags or None, "force": force or None}
        data = _post(port, "/v1/notes", body)
    elif source is None:
        typer.echo("Error: provide content, URI, or '-' for stdin", err=True)
        raise typer.Exit(1)
    else:
        # Detect file/URL/directory
        content = source
        uri = None
        watch_kind = "file"
        if source.startswith(("file://", "http://", "https://")):
            uri = source
            content = None
            watch_kind = "url" if source.startswith(("http://", "https://")) else "file"
        elif Path(source).is_dir():
            if summary is not None:
                typer.echo("Error: --summary cannot be used with directory mode", err=True)
                raise typer.Exit(1)
            if id is not None:
                typer.echo("Error: --id cannot be used with directory mode", err=True)
                raise typer.Exit(1)
            # Directory mode — iterate locally, put each file via HTTP
            _put_directory(
                port, Path(source).resolve(), parsed_tags,
                recurse=recurse, exclude=exclude,
                watch=watch, unwatch=unwatch, interval=interval,
                force=force, json_output=json_output,
            )
            return
        elif Path(source).exists() and not source.startswith("%"):
            uri = f"file://{Path(source).resolve()}"
            content = None

        # Inline text + --summary rejected (original content would be lost)
        if content is not None and uri is None and summary is not None:
            typer.echo("Error: --summary cannot be used with inline text (original content would be lost)", err=True)
            typer.echo("Hint: write to a file first, then: keep put file:///path/to/file --summary '...'", err=True)
            raise typer.Exit(1)

        body = {
            "content": content,
            "uri": uri,
            "id": id,
            "tags": parsed_tags or None,
            "summary": summary,
            "force": force or None,
        }
        # Watch params → daemon handles the timer
        if watch:
            body["watch"] = True
            body["watch_kind"] = watch_kind
            if interval:
                body["interval"] = interval
        elif unwatch:
            body["unwatch"] = True
        data = _post(port, "/v1/notes", body)

    if _is_json(json_output):
        typer.echo(json.dumps(data, indent=2))
    else:
        msg = f"{data.get('id', '')} stored."
        if data.get("watch"):
            msg += f" watching (interval {data['watch']['interval']})"
        elif data.get("unwatch"):
            msg += " watch removed."
        typer.echo(msg)


# Hidden aliases for put
@app.command("update", hidden=True)
def update_alias(
    source: Annotated[Optional[str], typer.Argument()] = None,
    id: Annotated[Optional[str], typer.Option("--id", "-i")] = None,
    tags: Annotated[Optional[list[str]], typer.Option("-t", "--tag")] = None,
    summary: Annotated[Optional[str], typer.Option("--summary")] = None,
    force: Annotated[bool, typer.Option("--force", "-f")] = False,
    json_output: JsonFlag = False,
):
    """Alias for 'put'."""
    put(source=source, id=id, tags=tags, summary=summary, force=force, json_output=json_output)


@app.command("add", hidden=True)
def add_alias(
    source: Annotated[Optional[str], typer.Argument()] = None,
    id: Annotated[Optional[str], typer.Option("--id", "-i")] = None,
    tags: Annotated[Optional[list[str]], typer.Option("-t", "--tag")] = None,
    summary: Annotated[Optional[str], typer.Option("--summary")] = None,
    force: Annotated[bool, typer.Option("--force", "-f")] = False,
    json_output: JsonFlag = False,
):
    """Alias for 'put'."""
    put(source=source, id=id, tags=tags, summary=summary, force=force, json_output=json_output)


@app.command()
def tag(
    ids: Annotated[list[str], typer.Argument(help="Note ID(s) to tag")],
    tags: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t", help="Tag to add/update as key=value"
    )] = None,
    remove: Annotated[Optional[list[str]], typer.Option(
        "--remove", "-r", help="Tag keys to remove"
    )] = None,
    remove_values: Annotated[Optional[list[str]], typer.Option(
        "--remove-value", "-R", help="Tag values to remove as key=value"
    )] = None,
    json_output: JsonFlag = False,
):
    """Add, update, or remove tags on existing notes.

    \b
    Does not re-process the note — only updates tags.
    Examples:
        keep tag doc:1 -t project=myapp
        keep tag doc:1 doc:2 -t status=reviewed
        keep tag doc:1 --remove obsolete_tag
        keep tag doc:1 --remove-value speaker=Bob
    """
    set_tags, _ = _parse_tag_args(tags)
    rv_dict, _ = _parse_tag_args(remove_values)

    if not set_tags and not remove and not rv_dict:
        typer.echo("Error: Specify at least one --tag, --remove, or --remove-value", err=True)
        raise typer.Exit(1)

    port = _get_port()
    body: dict = {}
    if set_tags:
        body["set"] = set_tags
    if remove:
        body["remove"] = remove
    if rv_dict:
        body["remove_values"] = rv_dict

    for doc_id in ids:
        data = _patch(port, f"/v1/notes/{_q(doc_id)}/tags", body)
        if _is_json(json_output):
            typer.echo(json.dumps(data, indent=2))
        else:
            typer.echo(f"{data.get('id', doc_id)} tagged.")


@app.command("tag-update", hidden=True)
def tag_update_alias(
    ids: Annotated[list[str], typer.Argument(help="Note ID(s)")],
    tags: Annotated[Optional[list[str]], typer.Option("-t", "--tag")] = None,
    remove: Annotated[Optional[list[str]], typer.Option("--remove", "-r")] = None,
    json_output: JsonFlag = False,
):
    """Alias for 'tag'."""
    tag(ids=ids, tags=tags, remove=remove, json_output=json_output)


@app.command("del")
def delete_cmd(
    id: Annotated[list[str], typer.Argument(help="Item ID(s)")],
):
    """Delete the current version of note(s), or a specific version."""
    port = _get_port()
    for item_id in id:
        data = _delete(port, f"/v1/notes/{_q(item_id)}")
        if data.get("deleted"):
            typer.echo(f"{item_id} deleted.")
        else:
            typer.echo(f"{item_id} not found.", err=True)


@app.command("delete", hidden=True)
def delete_alias(id: Annotated[list[str], typer.Argument(help="Item ID(s)")]):
    """Alias for 'del'."""
    delete_cmd(id=id)


@app.command()
def now(
    content: Annotated[Optional[str], typer.Argument(help="New content")] = None,
    tags: Annotated[Optional[list[str]], typer.Option("-t", "--tag", help="Tags")] = None,
    json_output: JsonFlag = False,
):
    """Get or set the current working intentions.

    \b
    With no arguments, displays the current intentions.
    With content, replaces them (previous version is preserved).
    """
    port = _get_port()
    if content:
        parsed_tags, _ = _parse_tag_args(tags)
        _post(port, "/v1/notes", {"content": content, "id": "now", "tags": parsed_tags or None})
    data = _get(port, f"/v1/notes/{_q('now')}/context")
    if _is_json(json_output):
        typer.echo(json.dumps(data, indent=2))
    else:
        typer.echo(_render_context(data))


@app.command("list")
def list_cmd(
    prefix: Annotated[Optional[str], typer.Argument(
        help="ID filter — prefix (e.g. '.tag') or glob (e.g. 'session-*', '*auth*')"
    )] = None,
    tag: Annotated[Optional[list[str]], typer.Option(
        "--tag", "-t", help="Filter by tag (key or key=value, repeatable)"
    )] = None,
    sort: Annotated[str, typer.Option(
        "--sort", help="Sort order: 'updated' (default), 'accessed', 'created', or 'id'"
    )] = "updated",
    limit: LimitOption = 20,
    since: Annotated[Optional[str], typer.Option("--since")] = None,
    until: Annotated[Optional[str], typer.Option("--until")] = None,
    with_parts: Annotated[bool, typer.Option(
        "--with-parts", help="Only show notes that have been analyzed into parts"
    )] = False,
    show_all: Annotated[bool, typer.Option(
        "--all", "-a", help="Include hidden system notes (IDs starting with '.')"
    )] = False,
    json_output: JsonFlag = False,
):
    """List recent notes, filter by tags, or browse by prefix.

    \b
    Examples:
        keep list                      # Recent notes (by update time)
        keep list .tag                 # All .tag/* system docs
        keep list session-*            # Glob match
        keep list --sort accessed      # By access time
        keep list -t project=myapp     # Filter by tag
        keep list --since P7D          # Last 7 days
        keep list --ids                # IDs only (for piping)
    """
    port = _get_port()
    tag_dict, tag_keys = _parse_tag_args(tag)

    flow_params: dict = {
        "limit": limit,
        "order_by": sort,
        "since": since,
        "until": until,
        "include_hidden": show_all or (prefix.startswith(".") if prefix else False),
    }
    if prefix:
        flow_params["prefix"] = prefix
    if tag_dict:
        flow_params["tags"] = tag_dict
    if tag_keys:
        flow_params["tag_keys"] = tag_keys

    resp = _post(port, "/v1/flow", {"state": "list", "params": flow_params})
    results = resp.get("data", {}).get("results", {}).get("results", [])
    data: dict = {"notes": results}

    # Filter to notes with parts if requested
    if with_parts:
        notes = [n for n in results if n.get("tags", {}).get("_has_parts")]
        data = {"notes": notes}

    if _is_json(json_output):
        typer.echo(json.dumps(data, indent=2))
    else:
        typer.echo(_render_find(data, port))


@app.command()
def move(
    name: Annotated[str, typer.Argument(help="Target collection name")],
    source: Annotated[str, typer.Option("--source", help="Source item ID")] = "now",
    json_output: JsonFlag = False,
):
    """Move versions from now (or another item) into a named item."""
    port = _get_port()
    data = _post(port, "/v1/flow", {
        "state": "move", "params": {"name": name, "source": source},
    })
    typer.echo(f"Moved to {name}.")


@app.command()
def prompt(
    name: Annotated[str, typer.Argument(help="Prompt name (e.g. 'reflect')")] = "",
    text: Annotated[Optional[str], typer.Argument(help="Text for context search")] = None,
    list_prompts: Annotated[bool, typer.Option("--list", "-l", help="List available prompts")] = False,
    id: Annotated[Optional[str], typer.Option("--id", help="Item ID for {get} context")] = None,
    tag: Annotated[Optional[list[str]], typer.Option("--tag", "-t", help="Filter by tag (key=value)")] = None,
    since: Annotated[Optional[str], typer.Option("--since", help="Updated since")] = None,
    until: Annotated[Optional[str], typer.Option("--until", help="Updated before")] = None,
    deep: Annotated[bool, typer.Option("--deep", "-D", help="Follow tags to discover related items")] = False,
    scope: Annotated[Optional[str], typer.Option("--scope", "-S", help="ID glob scope")] = None,
    token_budget: Annotated[Optional[int], typer.Option("--tokens", help="Token budget for {find}")] = None,
    json_output: JsonFlag = False,
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
        keep prompt reflect "auth flow"           # With search context
        keep prompt reflect --since P7D           # Recent context only
    """
    port = _get_port()

    if list_prompts or not name:
        # List prompts via flow
        data = _post(port, "/v1/flow", {"state": "prompt", "params": {"list": True}})
        prompts = data.get("data", {}).get("prompts", [])
        if not prompts:
            typer.echo("No agent prompts available.", err=True)
            raise typer.Exit(1)
        if _is_json(json_output):
            typer.echo(json.dumps({"prompts": prompts}, indent=2))
        else:
            for p in prompts:
                typer.echo(f"{p['name']:20s} {p.get('summary', '')}")
        return

    tags_dict = {}
    for t in (tag or []):
        if "=" in t:
            k, v = t.split("=", 1)
            tags_dict[k] = v

    # Render prompt via flow
    data = _post(port, "/v1/flow", {"state": "prompt", "params": {
        "name": name,
        "text": text,
        "id": id,
        "tags": tags_dict or None,
        "since": since,
        "until": until,
        "deep": deep or None,
        "scope": scope,
        "token_budget": token_budget,
    }})
    flow_data = data.get("data", {})
    if data.get("status") == "error":
        typer.echo(f"Error: {flow_data.get('error', 'unknown')}", err=True)
        raise typer.Exit(1)
    if _is_json(json_output):
        typer.echo(json.dumps(flow_data, indent=2))
    else:
        typer.echo(flow_data.get("text", ""))


@app.command(hidden=True)
def reflect(
    text: Annotated[Optional[str], typer.Argument(help="Text for context search")] = None,
    id: Annotated[Optional[str], typer.Option("--id", help="Item ID for {get} context")] = None,
    json_output: JsonFlag = False,
):
    """Reflect on current actions (alias for 'keep prompt reflect')."""
    prompt(name="reflect", text=text, id=id, json_output=json_output)


@app.command("flow")
def flow_cmd(
    state: Annotated[Optional[str], typer.Argument(help="State doc name")] = None,
    target: Annotated[Optional[str], typer.Option("--target", "-t", help="Target note ID")] = None,
    file: Annotated[Optional[str], typer.Option("--file", "-f", help="YAML state doc file or '-' for stdin")] = None,
    budget: Annotated[Optional[int], typer.Option("--budget", "-b", help="Max ticks")] = None,
    cursor: Annotated[Optional[str], typer.Option("--cursor", "-c", help="Resume cursor")] = None,
    param: Annotated[Optional[list[str]], typer.Option("--param", "-p", help="Parameter as key=value")] = None,
    json_output: JsonFlag = False,
):
    """Run a state-doc flow synchronously.

    \b
    Examples:
        keep flow after-write --target %abc123
        keep flow query-resolve -p query="auth patterns"
        keep flow --file review.yaml --target myproject
        keep flow --cursor <token> --budget 5
    """
    if state is None and file is None and cursor is None:
        typer.echo("Error: provide a state name, --file, or --cursor", err=True)
        raise typer.Exit(1)

    flow_params: dict = {}
    if target:
        flow_params["id"] = target
    for p in (param or []):
        if "=" not in p:
            typer.echo(f"Error: param must be key=value, got: {p!r}", err=True)
            raise typer.Exit(1)
        k, v = p.split("=", 1)
        try:
            flow_params[k] = json.loads(v)
        except (json.JSONDecodeError, ValueError):
            flow_params[k] = v

    state_doc_yaml: Optional[str] = None
    if file is not None:
        if file == "-":
            state_doc_yaml = sys.stdin.read()
        else:
            try:
                state_doc_yaml = Path(file).read_text()
            except FileNotFoundError:
                typer.echo(f"Error: file not found: {file}", err=True)
                raise typer.Exit(1)
        if state is None:
            state = "inline"

    if cursor and state is None:
        state = "__cursor__"

    port = _get_port()
    body: dict = {"state": state, "params": flow_params}
    if budget is not None:
        body["budget"] = budget
    if cursor:
        body["cursor_token"] = cursor
    if state_doc_yaml:
        body["state_doc_yaml"] = state_doc_yaml

    data = _post(port, "/v1/flow", body)
    if _is_json(json_output):
        typer.echo(json.dumps(data, ensure_ascii=False))
    else:
        typer.echo(json.dumps(data, ensure_ascii=False, indent=2))


@app.command("edit")
def edit_cmd(
    id: Annotated[str, typer.Argument(help="ID of note to edit")],
):
    """Edit a note's content in $EDITOR.

    \b
    Opens the current content in your editor. On save, updates the note
    if the content changed.
    Examples:
        keep edit .ignore                    # Edit ignore patterns
        keep edit .prompt/agent/reflect      # Edit a prompt template
        keep edit now                        # Edit current intentions
    """
    import tempfile
    import subprocess as sp

    port = _get_port()
    data = _get(port, f"/v1/notes/{_q(id)}")
    content = data.get("summary", "")

    editor = os.environ.get("EDITOR") or os.environ.get("VISUAL") or "vi"
    suffix = ".md" if not id.endswith((".py", ".js", ".ts", ".json", ".yaml", ".yml", ".toml")) else ""
    with tempfile.NamedTemporaryFile(suffix=suffix or Path(id).suffix, mode="w", delete=False, prefix="keep-edit-") as f:
        f.write(content)
        tmp = f.name

    try:
        sp.run([editor, tmp], check=True)
        new_content = Path(tmp).read_text()
    except (sp.CalledProcessError, KeyboardInterrupt):
        typer.echo("Editor exited abnormally, no changes saved", err=True)
        raise typer.Exit(1)
    finally:
        Path(tmp).unlink(missing_ok=True)

    if new_content == content:
        typer.echo("No changes", err=True)
        return

    _post(port, "/v1/notes", {"content": new_content, "id": id})
    typer.echo(f"Updated {id}", err=True)


@app.command()
def analyze(
    id: Annotated[str, typer.Argument(help="ID of note to analyze")],
    tag: Annotated[Optional[list[str]], typer.Option("--tag", "-t", help="Guidance tag keys")] = None,
    foreground: Annotated[bool, typer.Option("--foreground", "--fg", help="Run in foreground")] = False,
    force: Annotated[bool, typer.Option("--force", help="Re-analyze even if current")] = False,
    json_output: JsonFlag = False,
):
    """Decompose a note or string into meaningful parts.

    \b
    Uses an LLM to identify sections, each with its own summary, tags,
    and embedding. Runs in background by default; use --fg to wait.
    """
    port = _get_port()
    body: dict = {"id": id, "foreground": foreground, "force": force}
    if tag:
        body["tags"] = tag
    data = _post(port, "/v1/analyze", body)

    if _is_json(json_output):
        typer.echo(json.dumps(data, indent=2))
    else:
        if data.get("parts") is not None:
            parts = data["parts"]
            if parts:
                typer.echo(f"Analyzed {id} into {len(parts)} parts:")
                for p in parts:
                    summary = str(p.get("summary", ""))[:60].replace("\n", " ")
                    typer.echo(f"  @P{{{p.get('part_num', '?')}}} {summary}")
            else:
                typer.echo(f"Content not decomposable into multiple parts: {id}")
        elif data.get("queued"):
            typer.echo(f"Queued {id} for background analysis.", err=True)
        elif data.get("skipped"):
            typer.echo(f"Already analyzed, skipping {id}.", err=True)


@app.command("help")
def help_cmd(
    topic: Annotated[Optional[str], typer.Argument(help="Documentation topic")] = None,
):
    """Browse keep documentation.

    \b
    Examples:
        keep help              # Documentation index
        keep help quickstart   # CLI Quick Start guide
        keep help keep-put     # keep put reference
    """
    from keep.help import get_help_topic
    typer.echo(get_help_topic(topic or "index", link_style="cli"))


# ---------------------------------------------------------------------------
# Delegate commands — these stay with the full CLI
# ---------------------------------------------------------------------------

@app.command(context_settings={"allow_extra_args": True, "allow_interspersed_args": True})
def pending(ctx: typer.Context):
    """Process pending background tasks."""
    from keep.cli import app as full_app
    full_app(["pending"] + ctx.args, standalone_mode=False)


@app.command()
def config(
    ctx: typer.Context,
    path: Annotated[Optional[str], typer.Argument(
        help="Config path (e.g., 'file', 'tool', 'store', 'providers.embedding')"
    )] = None,
    setup: Annotated[bool, typer.Option("--setup", help="Run setup wizard")] = False,
    reset_system_docs: Annotated[bool, typer.Option(
        "--reset-system-docs", help="Force reload system documents from bundled content"
    )] = False,
    state_diagram: Annotated[bool, typer.Option(
        "--state-diagram", help="Print Mermaid state-transition diagram for .state/* docs"
    )] = False,
    json_output: JsonFlag = False,
):
    """Show configuration. Optionally get a specific value by path.

    \b
    Special paths: file, tool, store, docs, mcpb, providers
    Dotted paths: providers.embedding, tags, etc.
    """
    # --reset-system-docs: server-side operation
    if reset_system_docs:
        port = _get_port()
        data = _post(port, "/v1/admin/reset-system-docs", {})
        count = data.get("reset", 0)
        typer.echo(f"Reset {count} system documents")
        return

    # --state-diagram: query server for .state/* docs, render locally
    if state_diagram:
        port = _get_port()
        resp = _post(port, "/v1/flow", {
            "state": "list",
            "params": {"prefix": ".state/", "include_hidden": True, "limit": 100},
        })
        results = resp.get("data", {}).get("results", {}).get("results", [])
        state_docs: dict[str, str] = {}
        for note in results:
            nid = note.get("id", "")
            body = (note.get("summary") or "").strip()
            if nid.startswith(".state/") and body:
                state_docs[nid.removeprefix(".state/")] = body
        if not state_docs:
            typer.echo("No .state/* documents found.", err=True)
            raise typer.Exit(1)
        from keep.validate import state_doc_diagram
        typer.echo(state_doc_diagram(state_docs))
        return

    # Everything else delegates to the full CLI
    from keep.cli import app as full_app
    is_json = json_output or (ctx.parent and ctx.parent.params.get("json_output", False))
    args = []
    if is_json:
        args.append("--json")
    args.append("config")
    if path:
        args.append(path)
    if setup:
        args.append("--setup")
    try:
        full_app(args, standalone_mode=True)
    except SystemExit as e:
        if e.code:
            raise typer.Exit(e.code)


@app.command(hidden=True)
def doctor(ctx: typer.Context):
    """Diagnostic checks (delegates to full CLI)."""
    from keep.cli import app as full_app
    full_app(["doctor"] + ctx.args, standalone_mode=False)



@app.command()
def mcp(ctx: typer.Context):
    """Start MCP stdio server."""
    if _global_store:
        os.environ["KEEP_STORE_PATH"] = _global_store
    from keep.mcp import main as mcp_main
    mcp_main()


# ---------------------------------------------------------------------------
# Data management subcommands — delegate to full CLI for file I/O
# ---------------------------------------------------------------------------

data_app = typer.Typer(
    name="data",
    help="Data management — export, import.",
    context_settings={"allow_extra_args": True, "allow_interspersed_args": True},
    rich_markup_mode=None,
)
app.add_typer(data_app)


@data_app.callback(invoke_without_command=True)
def data_callback(ctx: typer.Context):
    """Delegates to full CLI."""
    from keep.cli import app as full_app
    args = ["data"] + (ctx.invoked_subcommand or "").split() + ctx.args
    full_app([a for a in args if a], standalone_mode=False)


def main():
    app()


if __name__ == "__main__":
    main()
