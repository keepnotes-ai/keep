"""MCP stdio server for keep — reflective memory for AI agents.

Three tools: keep_flow (all operations), keep_help (documentation),
keep_prompt (practice prompts).

Usage:
    keep mcp                        # stdio server (via CLI)
    claude --mcp-server keep="keep mcp"   # Claude Code integration

All Keeper calls are serialized through a single asyncio.Lock.
ChromaDB cross-process safety is handled at the store layer.
"""

import asyncio
import json
import os
import platform
import signal
import sys
from pathlib import Path
from typing import Annotated, Any, Optional

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from .api import Keeper
from .cli import expand_prompt

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

    On first init: runs system-doc migration (so prompts, tags, etc.
    are available immediately) and validates the embedding provider.
    """
    global _keeper
    if _keeper is None:
        import os
        store_path = os.environ.get("KEEP_STORE_PATH")
        keeper = Keeper(store_path=Path(store_path) if store_path else None)

        # Load system docs into a fresh store so the agent has prompts,
        # tag definitions, etc. from the first request.
        if keeper._needs_sysdoc_migration:
            try:
                keeper._migrate_system_documents()
                keeper._needs_sysdoc_migration = False
            except Exception as e:
                print(f"keep: system doc setup deferred: {e}", file=sys.stderr)

        # Validate embedding provider — quit early with a clear message
        # rather than serving from a broken store.
        if keeper._config.embedding is None:
            print(
                "keep: no embedding provider configured.\n"
                "\n"
                "Run the setup wizard:  keep config --setup\n"
                "Or set an API key:     export OPENAI_API_KEY=...\n"
                "Or install Ollama:     https://ollama.com/",
                file=sys.stderr,
            )
            sys.exit(1)

        _keeper = keeper
    return _keeper


# ---------------------------------------------------------------------------
# Tool annotations
# ---------------------------------------------------------------------------

_READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False)
_IDEMPOTENT = ToolAnnotations(idempotentHint=True, destructiveHint=False)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool(
    description=(
        "Run a state-doc flow synchronously. Flows evaluate state doc rules, "
        "execute actions (find, get, tag, summarize, etc.), and follow transitions. "
        "Returns when done, on error, or when budget is exhausted (with a resumable cursor). "
        "See: keep_help(topic='keep-flow') for usage."
    ),
    annotations=_IDEMPOTENT,
)
async def keep_flow(
    state: Annotated[str, Field(
        description="State doc name (e.g. 'query-resolve', 'get-context', 'put', 'tag', 'delete', 'move', 'stats').",
    )],
    params: Annotated[Optional[dict[str, Any]], Field(
        description="Flow parameters. Use 'id' key to target a specific note.",
    )] = None,
    budget: Annotated[Optional[int], Field(
        description="Max ticks for this invocation (default: from config).",
    )] = None,
    cursor: Annotated[Optional[str], Field(
        description="Cursor from a previous stopped flow to resume.",
    )] = None,
    state_doc_yaml: Annotated[Optional[str], Field(
        description="Inline YAML state doc (instead of loading from store).",
    )] = None,
    token_budget: Annotated[Optional[int], Field(
        description="Token budget for rendering results (default: raw JSON).",
    )] = None,
) -> str:
    """Run a state-doc flow."""
    async with _lock:
        keeper = _get_keeper()
        try:
            result = keeper.run_flow_command(
                state,
                params=params,
                budget=budget,
                cursor_token=cursor,
                state_doc_yaml=state_doc_yaml,
            )
        except (ValueError, OSError) as e:
            return f"Error: {e}"
    if token_budget and token_budget > 0:
        from .cli import render_flow_response
        return render_flow_response(result, token_budget=token_budget, keeper=_get_keeper())
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
    return json.dumps(output, indent=2)


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

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
    scope: Annotated[Optional[str], Field(
        description="ID glob to constrain search results (e.g. 'file:///path/to/dir*').",
    )] = None,
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
            deep=deep, scope=scope, token_budget=token_budget,
        )

        if result is None:
            return f"Prompt not found: {name}"

        return expand_prompt(result, kp=keeper)


# ---------------------------------------------------------------------------
# Help / documentation
# ---------------------------------------------------------------------------

@mcp.tool(
    description=(
        "Browse keep documentation. Returns the full text of a guide. "
        "Call with no arguments (or topic=\"index\") to see the documentation index with all available topics. "
        "Each topic in the index links to a detailed guide you can retrieve by name."
    ),
    annotations=_READ_ONLY,
)
async def keep_help(
    topic: Annotated[str, Field(
        description='Documentation topic, e.g. "index", "quickstart", "keep-put", "tagging". '
                    'Use "index" to see all available topics.',
    )] = "index",
) -> str:
    from .help import get_help_topic
    return get_help_topic(topic, link_style="mcp")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """Run the MCP stdio server."""
    # anyio's stdin reader uses abandon_on_cancel=False, which shields the
    # blocking readline from task cancellation.  The first Ctrl+C only cancels
    # the task (which can't take effect), so install our own handler.
    # Use os._exit to avoid SystemExit during interpreter shutdown, which
    # can deadlock on the stdin buffer lock held by the reader thread.
    signal.signal(signal.SIGINT, lambda *_: os._exit(130))

    # Eagerly init the store so system docs are loaded and provider
    # issues surface immediately (not on the first tool call).
    _get_keeper()

    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
