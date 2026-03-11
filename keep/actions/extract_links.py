from __future__ import annotations

"""Extract wiki-style and markdown-style links from markdown content."""

import re
from pathlib import PurePosixPath
from typing import Any

from . import action
from ._item_scope import resolve_item_content

# ---------------------------------------------------------------------------
# Link parsing
# ---------------------------------------------------------------------------

# [[target]] or [[target|display text]]
_WIKI_LINK_RE = re.compile(r"\[\[([^\]|]+)(?:\|[^\]]+)?\]\]")

# [display](target) — but not images: ![alt](src)
_MD_LINK_RE = re.compile(r"(?<!!)\[([^\]]*)\]\(([^)]+)\)")


def _parse_links(content: str) -> list[dict[str, str]]:
    """Extract links from markdown content.

    Returns a list of dicts with 'target' and 'style' keys.
    """
    links: list[dict[str, str]] = []
    seen: set[str] = set()

    for m in _WIKI_LINK_RE.finditer(content):
        target = m.group(1).strip()
        if target and target not in seen:
            seen.add(target)
            links.append({"target": target, "style": "wiki"})

    for m in _MD_LINK_RE.finditer(content):
        target = m.group(2).strip()
        # Skip anchors, mailto, and empty
        if not target or target.startswith("#") or target.startswith("mailto:"):
            continue
        if target not in seen:
            seen.add(target)
            links.append({"target": target, "style": "markdown"})

    return links


# ---------------------------------------------------------------------------
# Link resolution
# ---------------------------------------------------------------------------

def _is_url(target: str) -> bool:
    return target.startswith("http://") or target.startswith("https://")


def _resolve_internal_link(
    target: str,
    source_id: str,
    context: Any,
) -> str | None:
    """Resolve an internal link target to a keep item ID.

    Uses path-segment matching against file:// URIs. Tries with and
    without .md extension.
    """
    # Normalize: strip .md suffix for matching
    bare = target.removesuffix(".md")

    # If the source is a file:// URI, resolve relative paths
    if source_id.startswith("file://"):
        source_path = PurePosixPath(source_id.removeprefix("file://"))
        source_dir = source_path.parent

        # Build candidate file:// URIs
        candidates = []
        resolved = source_dir / target
        candidates.append(f"file://{resolved}")
        if not target.endswith(".md"):
            candidates.append(f"file://{resolved}.md")
        # Also try bare name resolve
        resolved_bare = source_dir / bare
        candidates.append(f"file://{resolved_bare}")
        candidates.append(f"file://{resolved_bare}.md")

        # Deduplicate while preserving order
        seen: set[str] = set()
        unique: list[str] = []
        for c in candidates:
            if c not in seen:
                seen.add(c)
                unique.append(c)

        for candidate in unique:
            item = context.get(candidate)
            if item is not None:
                return candidate

    # Fallback: try the target directly as an ID
    item = context.get(target)
    if item is not None:
        return target
    item = context.get(bare)
    if item is not None:
        return bare

    return None


# ---------------------------------------------------------------------------
# Action
# ---------------------------------------------------------------------------

@action(id="extract_links")
class ExtractLinks:
    """Extract links from markdown content and create reference edges."""

    def run(self, params: dict[str, Any], context: Any) -> dict[str, Any]:
        item_id, item, content = resolve_item_content(params, context)

        tag_key = str(params.get("tag", "references"))
        create_targets = str(params.get("create_targets", "true")).lower() == "true"

        links = _parse_links(content)
        if not links:
            return {"skipped": True}

        mutations: list[dict[str, Any]] = []
        resolved_targets: list[str] = []

        for link in links:
            target = link["target"]

            if _is_url(target):
                # External URL — use as-is
                resolved_targets.append(target)
                if create_targets:
                    # Auto-vivify: create a stub item for the URL if missing
                    existing = context.get(target)
                    if existing is None:
                        mutations.append({
                            "op": "put_item",
                            "id": target,
                            "content": target,
                            "summary": target,
                            "tags": {"_source": "link"},
                            "queue_background_tasks": True,
                        })
            else:
                # Internal link — resolve via path matching
                resolved = _resolve_internal_link(target, item_id, context)
                if resolved:
                    resolved_targets.append(resolved)
                elif create_targets:
                    # Auto-vivify: create from relative path if source is file://
                    vivified = _vivify_id(target, item_id)
                    if vivified:
                        resolved_targets.append(vivified)
                        mutations.append({
                            "op": "put_item",
                            "id": vivified,
                            "content": f"[{target}]",
                            "summary": f"[{target}]",
                            "tags": {"_source": "link"},
                            "queue_background_tasks": False,
                        })

        if not resolved_targets:
            return {"skipped": True, "links_found": len(links)}

        # Build edge tags — merge with existing
        existing_tags = dict(getattr(item, "tags", {}) or {})
        existing_refs = existing_tags.get(tag_key)
        if isinstance(existing_refs, list):
            # Merge, preserving order, dedup
            seen = set(existing_refs)
            merged = list(existing_refs)
            for t in resolved_targets:
                if t not in seen:
                    seen.add(t)
                    merged.append(t)
            existing_tags[tag_key] = merged
        elif isinstance(existing_refs, str) and existing_refs:
            seen = {existing_refs}
            merged = [existing_refs]
            for t in resolved_targets:
                if t not in seen:
                    seen.add(t)
                    merged.append(t)
            existing_tags[tag_key] = merged
        else:
            existing_tags[tag_key] = resolved_targets

        mutations.append({
            "op": "set_tags",
            "target": item_id,
            "tags": existing_tags,
        })

        return {
            "links": [l["target"] for l in links],
            "resolved": resolved_targets,
            "mutations": mutations,
        }


def _vivify_id(target: str, source_id: str) -> str | None:
    """Build a keep ID for auto-vivification of an internal link."""
    bare = target.removesuffix(".md")
    if source_id.startswith("file://"):
        source_path = PurePosixPath(source_id.removeprefix("file://"))
        resolved = source_path.parent / bare
        return f"file://{resolved}.md"
    # Non-file source: use the target as-is
    return bare or None
