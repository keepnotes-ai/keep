"""Bundled documentation browser for keep.

Shared by both MCP (keep_help tool) and CLI (keep help command).
Reads markdown docs from the bundled docs/ directory and rewrites
inter-doc links for the target interface.
"""

import re
from pathlib import Path

_LINK_RE = re.compile(r'\[([^\]]+)\]\(([A-Za-z0-9_-]+)\.md\)')


def _docs_dir() -> Path:
    """Return the bundled docs directory."""
    from .config import get_tool_directory
    return get_tool_directory() / "docs"


def doc_topics() -> dict[str, Path]:
    """Build topic -> file mapping from docs/, excluding subdirectories."""
    docs = _docs_dir()
    if not docs.is_dir():
        return {}
    return {
        f.stem.lower(): f
        for f in docs.iterdir()
        if f.is_file() and f.suffix == ".md"
    }


def _rewrite_links(content: str, topics: dict[str, Path], style: str) -> str:
    """Rewrite markdown links to local .md files for the target interface."""
    def _replace(m):
        text, filename = m.group(1), m.group(2)
        topic = filename.lower()
        if topic not in topics:
            return m.group(0)
        if style == "cli":
            return f"[{text}](run: keep help {topic})"
        else:
            return f'[{text}](use keep_help with topic="{topic}")'

    return _LINK_RE.sub(_replace, content)


def get_help_topic(topic: str = "index", link_style: str = "mcp") -> str:
    """Return documentation for a topic with links rewritten.

    Args:
        topic: Topic name (lowercase, no extension). Default "index".
        link_style: "mcp" or "cli" — controls how inter-doc links are rendered.

    Returns:
        Markdown content, or an error/listing message.
    """
    topics = doc_topics()
    if not topics:
        return "Documentation not found — package may not include bundled docs."

    key = topic.lower().strip()
    if key not in topics:
        available = sorted(topics.keys())
        return f"Unknown topic: {topic!r}. Available topics: {', '.join(available)}"

    content = topics[key].read_text(encoding="utf-8")
    return _rewrite_links(content, topics, link_style)
