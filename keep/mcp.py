"""MCP stdio server for keep — reflective memory for AI agents.

Thin HTTP wrapper over the daemon. No local Keeper, no models, no database.

Three tools: keep_flow (all operations), keep_help (documentation),
keep_prompt (practice prompts).

Usage:
    keep mcp                        # stdio server (via CLI)
    claude --mcp-server keep="keep mcp"   # Claude Code integration
"""

import json
import os
import signal
import sys
import http.client
from typing import Annotated, Any, Optional

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from .daemon_client import get_port, http_request

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

_port: Optional[int] = None


def _ensure_daemon() -> int:
    """Connect to (or auto-start) the daemon. Returns port."""
    global _port
    if _port is None:
        _port = get_port(os.environ.get("KEEP_STORE_PATH"))
    return _port


def _post(path: str, body: dict) -> tuple[int, dict]:
    """POST to the daemon. Returns (status, json_body)."""
    global _port
    try:
        status, result = http_request("POST", _ensure_daemon(), path, body)
    except (ConnectionError, TimeoutError, http.client.RemoteDisconnected, OSError):
        _port = None
        status, result = http_request("POST", _ensure_daemon(), path, body)
    if status == 401:
        # Daemon may have restarted on a new port. Re-resolve.
        _port = None
        status, result = http_request("POST", _ensure_daemon(), path, body)
    return status, result


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
        "Execute a keep operation via state-doc flow. "
        "Examples:\n"
        '  Search: state="query-resolve", params={"query": "auth patterns"}\n'
        '  Get context: state="get", params={"item_id": "now"}\n'
        '  Store text: state="put", params={"content": "decision: use JWT", "tags": {"project": "auth"}}\n'
        '  Store with ID: state="put", params={"id": "meeting-notes", "content": "..."}\n'
        '  Store file: state="put", params={"uri": "file:///path/to/doc.md"}\n'
        '  Store URL: state="put", params={"uri": "https://example.com/article"}\n'
        '  List items: state="list", params={"prefix": ".tag/", "include_hidden": true}\n'
        '  Resume stopped search: state="query-resolve", cursor="<cursor from previous call>"\n'
        "When status is 'stopped', pass the returned cursor to continue. "
        "Set token_budget for rendered text output instead of raw JSON. "
        'List available flows: keep_help(topic="flow_state_docs").'
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
    body: dict = {
        "state": state,
        "params": params,
        "budget": budget,
        "cursor": cursor,
        "state_doc_yaml": state_doc_yaml,
    }
    if token_budget and token_budget > 0:
        body["token_budget"] = token_budget
    status, resp = _post("/v1/flow", body)
    if status != 200:
        return f"Error: {resp.get('error', 'unknown')}"

    # Server-side rendered output (when token_budget was provided)
    if resp.get("rendered"):
        return resp["rendered"]

    # Build selective JSON (same shape as before — no bindings/history)
    output: dict[str, Any] = {
        "status": resp.get("status"),
        "ticks": resp.get("ticks"),
    }
    data = resp.get("data")
    if data:
        output["data"] = data
    cursor_val = resp.get("cursor")
    if cursor_val:
        output["cursor"] = cursor_val
    tried = resp.get("tried_queries")
    if tried:
        output["tried_queries"] = tried
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
    flow_params: dict[str, Any] = {}
    if not name:
        flow_params["list"] = True
    else:
        flow_params["name"] = name
        if text:
            flow_params["text"] = text
        if id:
            flow_params["id"] = id
        if tags:
            flow_params["tags"] = tags
        if since:
            flow_params["since"] = since
        if until:
            flow_params["until"] = until
        if deep:
            flow_params["deep"] = deep
        if scope:
            flow_params["scope"] = scope
        if token_budget:
            flow_params["token_budget"] = token_budget

    status, resp = _post("/v1/flow", {"state": "prompt", "params": flow_params})
    if status != 200:
        return f"Error: {resp.get('error', 'unknown')}"

    flow_data = resp.get("data", {})
    flow_status = resp.get("status")

    # List mode
    if not name:
        prompts = flow_data.get("prompts", [])
        if not prompts:
            return "No agent prompts available."
        lines = [f"- {p['name']:20s} {p.get('summary', '')}" for p in prompts]
        return "\n".join(lines)

    # Error
    if flow_status == "error":
        return f"Prompt not found: {name}"

    # Render mode — daemon already expanded the prompt
    return flow_data.get("text", f"Prompt not found: {name}")


# ---------------------------------------------------------------------------
# Help / documentation
# ---------------------------------------------------------------------------

@mcp.tool(
    description=(
        "Comprehensive keep documentation with examples for all commands, "
        "flows, tagging, prompts, and architecture. "
        "Call with topic=\"index\" to see all available guides."
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

    # Connect to daemon eagerly so setup issues surface immediately.
    _ensure_daemon()

    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
