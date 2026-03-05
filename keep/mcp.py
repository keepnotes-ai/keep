"""
MCP stdio server for keep — reflective memory tools for AI agents.

Exposes Keeper operations as MCP tools so local AI agents (Claude Code, etc.)
get full reflective memory capability without HTTP infrastructure.

Usage:
    keep mcp                        # stdio server (via CLI)
    claude --mcp-server keep="keep mcp"   # Claude Code integration

All Keeper calls are serialized through a single asyncio.Lock.
ChromaDB cross-process safety is handled at the store layer.
"""

import asyncio
from pathlib import Path
from typing import Annotated, Optional

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from .api import Keeper, _text_content_id
from .cli import render_context, render_find_context, expand_prompt

# ---------------------------------------------------------------------------
# Server setup
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "keep",
    instructions=(
        "Reflective memory with semantic search. "
        "Store facts, preferences, decisions, and documents. "
        "Search by meaning. Persist context across sessions."
    ),
)

_keeper: Optional[Keeper] = None
_lock = asyncio.Lock()


def _get_keeper() -> Keeper:
    """Lazy-init Keeper with default config (respects KEEP_STORE_PATH env).

    Must be called inside ``async with _lock`` — Keeper init is not
    thread-safe and we rely on the caller holding the lock to avoid
    racing on the global.
    """
    global _keeper
    if _keeper is None:
        import os
        store_path = os.environ.get("KEEP_STORE_PATH")
        _keeper = Keeper(store_path=Path(store_path) if store_path else None)
    return _keeper


# ---------------------------------------------------------------------------
# Tool annotations
# ---------------------------------------------------------------------------

_READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False)
_IDEMPOTENT = ToolAnnotations(idempotentHint=True, destructiveHint=False)
_DESTRUCTIVE = ToolAnnotations(destructiveHint=True, idempotentHint=False)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool(
    description=(
        "Store a fact, preference, decision, URL, or document in long-term memory. "
        "For URLs, fetches and indexes the content. "
        "Set analyze=true to decompose into searchable parts."
    ),
    annotations=_IDEMPOTENT,
)
async def keep_put(
    content: Annotated[str, Field(
        description="Text to store, or a URI (http://, https://, file://) to fetch and index.",
    )],
    id: Annotated[Optional[str], Field(
        description="Custom ID. Auto-generated if omitted for inline text; URI used as ID for URIs.",
    )] = None,
    summary: Annotated[Optional[str], Field(
        description="User-provided summary (skips auto-summarization).",
    )] = None,
    tags: Annotated[Optional[dict[str, str | list[str]]], Field(
        description='Tags to categorize. Example: {"topic": "preferences", "project": "myapp"}',
    )] = None,
    analyze: Annotated[bool, Field(
        description="If true, decompose the stored content into searchable parts after storing.",
    )] = False,
) -> str:
    """Store content in memory."""
    async with _lock:
        keeper = _get_keeper()
        is_uri = content.startswith(("http://", "https://", "file://"))
        try:
            if is_uri:
                item = keeper.put(uri=content, id=id, summary=summary, tags=tags)
            else:
                # Match CLI behavior: inline text defaults to content-addressed IDs.
                doc_id = id or _text_content_id(content)
                item = keeper.put(content, id=doc_id, summary=summary, tags=tags)
        except (ValueError, OSError) as e:
            return f"Error: {e}"

        status = "Unchanged" if item.changed is False else "Stored"
        result = f"{status}: {item.id}"

        if analyze:
            try:
                parts = keeper.analyze(item.id)
                result += f" ({len(parts)} parts)"
            except ValueError as e:
                result += f" (analyze failed: {e})"

    return result


@mcp.tool(
    description=(
        "Search long-term memory by natural language query. "
        "Returns matching items ranked by relevance with similarity scores."
    ),
    annotations=_READ_ONLY,
)
async def keep_find(
    query: Annotated[str, Field(
        description="Natural language search query.",
    )],
    tags: Annotated[Optional[dict[str, str | list[str]]], Field(
        description="Filter results by tags (all must match).",
    )] = None,
    since: Annotated[Optional[str], Field(
        description="Only items updated since this value (ISO duration like P3D, or date like 2026-01-15).",
    )] = None,
    until: Annotated[Optional[str], Field(
        description="Only items updated before this value (ISO duration or date).",
    )] = None,
    deep: Annotated[bool, Field(
        description="Follow tags from results to discover related items.",
    )] = False,
    show_tags: Annotated[bool, Field(
        description="Show non-system tags for each result.",
    )] = False,
    token_budget: Annotated[int, Field(
        description="Token budget for results context (default: 4000).",
    )] = 4000,
) -> str:
    """Search memory."""
    async with _lock:
        keeper = _get_keeper()
        # Derive retrieval limit from token budget
        tokens_per_item = 200 if deep else 50
        limit = min(200, max(10, token_budget // tokens_per_item))
        items = keeper.find(query, tags=tags, limit=limit, since=since, until=until, deep=deep)

        if not items:
            return "No results found."

        return render_find_context(items, keeper=keeper, token_budget=token_budget, show_tags=show_tags)


@mcp.tool(
    description=(
        "Retrieve a specific item by ID with full context: "
        "similar items, meta-doc sections, parts, and version history. "
        'Use id="now" to read current working context.'
    ),
    annotations=_READ_ONLY,
)
async def keep_get(
    id: Annotated[str, Field(description="Item ID to retrieve.")],
) -> str:
    """Retrieve item with full context."""
    async with _lock:
        keeper = _get_keeper()
        ctx = keeper.get_context(id)

    if ctx is None:
        return f"Not found: {id}"
    return render_context(ctx)


@mcp.tool(
    description=(
        "Update the current working context with new state, goals, or decisions. "
        "This persists across sessions. "
        'To read context, use keep_get with id="now".'
    ),
    annotations=_IDEMPOTENT,
)
async def keep_now(
    content: Annotated[str, Field(
        description="New working context — describe current state, active goals, recent decisions.",
    )],
    tags: Annotated[Optional[dict[str, str | list[str]]], Field(
        description="Optional tags.",
    )] = None,
) -> str:
    """Update current working context."""
    async with _lock:
        keeper = _get_keeper()
        item = keeper.set_now(content, tags=tags)
    return f"Context updated: {item.id}"


@mcp.tool(
    description=(
        "Add, update, or remove tags on an existing item without re-processing it. "
        "Use empty string value to delete a tag."
    ),
    annotations=_IDEMPOTENT,
)
async def keep_tag(
    id: Annotated[str, Field(description="Item ID.")],
    tags: Annotated[dict[str, str | list[str]], Field(
        description='Tags to add/update. Use empty string value "" to delete a tag.',
    )],
) -> str:
    """Update tags on an existing item."""
    async with _lock:
        keeper = _get_keeper()
        item = keeper.tag(id, tags)

    if item is None:
        return f"Not found: {id}"

    def _fmt(k: str, v: str | list[str]) -> str:
        if isinstance(v, list):
            return ", ".join(f"{k}={x}" for x in v)
        return f"{k}={v}"
    display = ", ".join(_fmt(k, v) for k, v in tags.items() if v)
    removed = [k for k, v in tags.items() if not v]
    parts = []
    if display:
        parts.append(f"set {display}")
    if removed:
        parts.append(f"removed {', '.join(removed)}")
    return f"Tagged {id}: {'; '.join(parts)}"


@mcp.tool(
    description="Permanently delete an item and its version history from memory.",
    annotations=_DESTRUCTIVE,
)
async def keep_delete(
    id: Annotated[str, Field(description="Item ID to delete.")],
) -> str:
    """Delete an item."""
    async with _lock:
        keeper = _get_keeper()
        deleted = keeper.delete(id)
    return f"Deleted: {id}" if deleted else f"Not found: {id}"


@mcp.tool(
    description=(
        "List recent items, optionally filtered by ID prefix/glob, tags, or date range."
    ),
    annotations=_READ_ONLY,
)
async def keep_list(
    prefix: Annotated[Optional[str], Field(
        description='Filter by ID prefix or glob pattern (e.g. ".tag/*").',
    )] = None,
    tags: Annotated[Optional[dict[str, str | list[str]]], Field(
        description="Filter by tag key=value pairs.",
    )] = None,
    since: Annotated[Optional[str], Field(
        description="Only items updated since this value (ISO duration or date).",
    )] = None,
    until: Annotated[Optional[str], Field(
        description="Only items updated before this value (ISO duration or date).",
    )] = None,
    limit: Annotated[int, Field(
        description="Max results to return.",
    )] = 10,
) -> str:
    """List recent items."""
    async with _lock:
        keeper = _get_keeper()
        items = keeper.list_items(
            prefix=prefix, tags=tags, since=since, until=until, limit=limit,
        )

    if not items:
        return "No items found."

    lines = []
    for item in items:
        date = item.tags.get("_updated_date", "")
        lines.append(f"- {item.id}  {date}  {item.summary}")
    return "\n".join(lines)


@mcp.tool(
    description=(
        "Move versions from a source item (default: now) into a named target item. "
        "Useful for extracting topics from working context into named notes."
    ),
    annotations=ToolAnnotations(destructiveHint=False, idempotentHint=False),
)
async def keep_move(
    name: Annotated[str, Field(
        description="Target item ID (created if new, extended if exists).",
    )],
    source_id: Annotated[str, Field(
        description='Source item to extract from (default: "now").',
    )] = "now",
    tags: Annotated[Optional[dict[str, str | list[str]]], Field(
        description="If provided, only extract versions whose tags match all specified key=value pairs.",
    )] = None,
    only_current: Annotated[bool, Field(
        description="If true, only move the current (tip) version, not history.",
    )] = False,
) -> str:
    """Move versions from one item to another."""
    async with _lock:
        keeper = _get_keeper()
        try:
            item = keeper.move(name, source_id=source_id, tags=tags, only_current=only_current)
        except ValueError as e:
            return f"Error: {e}"
    return f"Moved to: {item.id}"


@mcp.tool(
    description=(
        "Render an agent prompt with context injected from memory. "
        "Returns actionable instructions for reflection, session start, etc. "
        "Call with no name to list available prompts."
    ),
    annotations=_READ_ONLY,
)
async def keep_prompt(
    name: Annotated[Optional[str], Field(
        description='Prompt name (e.g. "reflect", "session-start"). Omit to list available prompts.',
    )] = None,
    text: Annotated[Optional[str], Field(
        description="Optional search query for additional context injection.",
    )] = None,
    id: Annotated[Optional[str], Field(
        description='Item ID for context (default: "now").',
    )] = None,
    tags: Annotated[Optional[dict[str, str | list[str]]], Field(
        description="Filter search context by tags.",
    )] = None,
    since: Annotated[Optional[str], Field(
        description="Only include items updated since this value (ISO duration or date).",
    )] = None,
    until: Annotated[Optional[str], Field(
        description="Only include items updated before this value (ISO duration or date).",
    )] = None,
    deep: Annotated[bool, Field(
        description="Follow tags from results to discover related items.",
    )] = False,
    token_budget: Annotated[Optional[int], Field(
        description="Token budget for search results context (template default if not set).",
    )] = None,
) -> str:
    """Render an agent prompt with injected context."""
    async with _lock:
        keeper = _get_keeper()

        if not name:
            prompts = keeper.list_prompts()
            if not prompts:
                return "No agent prompts available."
            lines = [f"- {p.name:20s} {p.summary}" for p in prompts]
            return "\n".join(lines)

        result = keeper.render_prompt(
            name, text, id=id, since=since, until=until, tags=tags,
            deep=deep, token_budget=token_budget,
        )

        if result is None:
            return f"Prompt not found: {name}"

        return expand_prompt(result, kp=keeper)


# ---------------------------------------------------------------------------
# Startup hints
# ---------------------------------------------------------------------------

def _check_mcp_setup():
    """Print setup hints for detected tools missing keep MCP config."""
    import json
    import sys

    home = Path.home()
    hints: list[str] = []

    # Claude Code: MCP servers live in ~/.claude.json (NOT ~/.claude/settings.json)
    claude_dir = home / ".claude"
    if claude_dir.is_dir():
        configured = False
        claude_json = home / ".claude.json"
        if claude_json.exists():
            try:
                data = json.loads(claude_json.read_text(encoding="utf-8"))
                configured = "keep" in data.get("mcpServers", {})
            except (json.JSONDecodeError, OSError):
                pass
        if not configured:
            hints.append(
                'Claude Code:\n'
                '    claude mcp add --scope user keep -- keep mcp'
            )

    # Kiro: ~/.kiro/settings/mcp.json → mcpServers.keep
    kiro_dir = home / ".kiro"
    if kiro_dir.is_dir():
        configured = False
        mcp_json = kiro_dir / "settings" / "mcp.json"
        if mcp_json.exists():
            try:
                data = json.loads(mcp_json.read_text(encoding="utf-8"))
                configured = "keep" in data.get("mcpServers", {})
            except (json.JSONDecodeError, OSError):
                pass
        if not configured:
            hints.append(
                'Kiro:\n'
                '    kiro-cli mcp add --name keep --scope global -- keep mcp'
            )

    # Codex: ~/.codex/config.toml → [mcp_servers.keep]
    codex_dir = home / ".codex"
    if codex_dir.is_dir():
        configured = False
        config_toml = codex_dir / "config.toml"
        if config_toml.exists():
            try:
                content = config_toml.read_text(encoding="utf-8")
                configured = "mcp_servers.keep" in content
            except OSError:
                pass
        if not configured:
            hints.append(
                'Codex:\n'
                '    codex mcp add keep -- keep mcp'
            )

    # VS Code: user-level mcp.json → servers.keep
    import platform
    if platform.system() == "Darwin":
        vscode_dir = home / "Library" / "Application Support" / "Code"
    else:
        vscode_dir = home / ".config" / "Code"
    if vscode_dir.is_dir():
        configured = False
        vscode_mcp = vscode_dir / "User" / "mcp.json"
        if vscode_mcp.exists():
            try:
                data = json.loads(vscode_mcp.read_text(encoding="utf-8"))
                configured = "keep" in data.get("servers", {})
            except (json.JSONDecodeError, OSError):
                pass
        if not configured:
            hints.append(
                "VS Code:\n"
                """    code --add-mcp '{"name":"keep","command":"keep","args":["mcp"]}'"""
            )

    if hints:
        print("keep: MCP server not configured for:", file=sys.stderr)
        for hint in hints:
            print(f"  {hint}", file=sys.stderr)
        print(file=sys.stderr)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _check_integrations():
    """Run hook/integration upgrades (same as other CLI commands)."""
    try:
        keeper = _get_keeper()
        if keeper.config:
            from .integrations import check_and_install
            check_and_install(keeper.config)
    except Exception:
        pass  # Never block MCP startup


def main():
    """Run the MCP stdio server."""
    import os
    import signal
    # anyio's stdin reader uses abandon_on_cancel=False, which shields the
    # blocking readline from task cancellation.  The first Ctrl+C only cancels
    # the task (which can't take effect), so install our own handler.
    # Use os._exit to avoid SystemExit during interpreter shutdown, which
    # can deadlock on the stdin buffer lock held by the reader thread.
    signal.signal(signal.SIGINT, lambda *_: os._exit(130))

    _check_integrations()
    _check_mcp_setup()
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
