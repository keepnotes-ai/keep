from __future__ import annotations

"""Extract wiki-style and markdown-style links from markdown content."""

import logging
import re
from pathlib import Path, PurePosixPath
from typing import Any

from . import action
from ._item_scope import resolve_item_content

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Link parsing
# ---------------------------------------------------------------------------

# [[target]] or [[target|display text]]
_WIKI_LINK_RE = re.compile(r"\[\[([^\]|]+)(?:\|[^\]]+)?\]\]")

# [display](target) or [display](target "title") — but not images
_MD_LINK_RE = re.compile(r'(?<!!)\[([^\]]*)\]\(([^)\s]+)(?:\s+"[^"]*")?\)')

# ![alt](src) or ![alt](src "title")
_MD_IMAGE_RE = re.compile(r'!\[([^\]]*)\]\(([^)\s]+)(?:\s+"[^"]*")?\)')

# Bare URLs (http/https) — used for non-markdown content
_URL_RE = re.compile(r'https?://[^\s<>\"\')]+')

# Directories that mark a vault root
_VAULT_MARKERS = (".obsidian", ".logseq", ".git")


def _parse_links(content: str, *, content_type: str = "text/markdown") -> list[dict[str, str]]:
    """Extract links from content.

    For markdown: extracts wiki-links, markdown links, and images.
    For other types (HTML, email, etc.): extracts bare URLs.

    Returns a list of dicts with 'target' and 'style' keys.
    """
    links: list[dict[str, str]] = []
    seen: set[str] = set()

    is_markdown = content_type in ("text/markdown", "text/x-markdown")

    if is_markdown:
        for m in _WIKI_LINK_RE.finditer(content):
            target = m.group(1).strip()
            if target and target not in seen:
                seen.add(target)
                links.append({"target": target, "style": "wiki"})

        for m in _MD_IMAGE_RE.finditer(content):
            target = m.group(2).strip()
            if target and target not in seen:
                seen.add(target)
                links.append({"target": target, "style": "markdown"})

        for m in _MD_LINK_RE.finditer(content):
            target = m.group(2).strip()
            # Skip anchors, mailto, and empty
            if not target or target.startswith("#") or target.startswith("mailto:"):
                continue
            if target not in seen:
                seen.add(target)
                links.append({"target": target, "style": "markdown"})
    else:
        # Non-markdown: extract bare URLs
        for m in _URL_RE.finditer(content):
            target = m.group(0).rstrip(".,;:!?)")
            if target and target not in seen:
                seen.add(target)
                links.append({"target": target, "style": "url"})

    return links


# ---------------------------------------------------------------------------
# Vault detection
# ---------------------------------------------------------------------------

def _detect_vault_root(file_path: str) -> str | None:
    """Walk parent dirs of a file:// path looking for vault markers.

    Returns the vault root as a ``file://`` URI, or None.
    """
    p = Path(file_path)
    for parent in p.parents:
        for marker in _VAULT_MARKERS:
            if (parent / marker).is_dir():
                return f"file://{parent}"
    return None


def _get_vault_root(source_id: str, context: Any) -> str | None:
    """Resolve the vault root for a file:// source item.

    Checks cached ``.vault/*`` items first, falls back to filesystem detection.
    Returns the vault root URI or None.
    """
    if not source_id.startswith("file://"):
        return None

    file_path = source_id.removeprefix("file://")

    # Fast path: check known vault roots from store
    try:
        vault_items = context.list_items(
            prefix=".vault/", include_hidden=True, limit=100,
        )
    except Exception:
        vault_items = []

    if isinstance(vault_items, list):
        for vi in vault_items:
            vault_uri = str(getattr(vi, "id", "")).removeprefix(".vault/")
            if vault_uri and source_id.startswith(vault_uri):
                return vault_uri

    # Slow path: walk filesystem (once per vault)
    vault_root = _detect_vault_root(file_path)
    return vault_root


# ---------------------------------------------------------------------------
# Link resolution
# ---------------------------------------------------------------------------

def _is_url(target: str) -> bool:
    return target.startswith("http://") or target.startswith("https://")


def _resolve_internal_link(
    target: str,
    source_id: str,
    context: Any,
    *,
    style: str = "wiki",
    vault_root: str | None = None,
) -> str | None:
    """Resolve an internal link target to a keep item ID.

    For markdown-style links, uses folder-relative path matching only.
    For wiki-style links, tries folder-relative first then vault-wide
    name search as a fallback.
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

    # Wiki-style fallback: vault-wide name search
    if style == "wiki" and hasattr(context, "find_by_name"):
        found = context.find_by_name(bare, vault=vault_root)
        if found is not None:
            found_id = str(getattr(found, "id", ""))
            if found_id:
                return found_id

    return None


# ---------------------------------------------------------------------------
# Action
# ---------------------------------------------------------------------------

@action(id="extract_links", priority=1)
class ExtractLinks:
    """Extract links from content and create reference edges.

    For markdown: wiki-links, markdown links, and images.
    For HTML/email/other: bare URLs (http/https).
    """

    def run(self, params: dict[str, Any], context: Any) -> dict[str, Any]:
        item_id, item, content = resolve_item_content(params, context)

        tag_key = str(params.get("tag", "references"))
        create_targets = str(params.get("create_targets", "true")).lower() == "true"

        # Determine content type for link parsing strategy
        item_tags = getattr(item, "tags", {}) or {}
        ct = item_tags.get("_content_type", "text/markdown")

        links = _parse_links(content, content_type=ct)
        if not links:
            return {"skipped": True}

        # Detect vault root for wiki-style resolution
        vault_root = _get_vault_root(item_id, context)

        mutations: list[dict[str, Any]] = []
        resolved_targets: list[str] = []

        # If we discovered a new vault root, register it
        if vault_root:
            vault_item_id = f".vault/{vault_root}"
            existing_vault = context.get(vault_item_id)
            if existing_vault is None:
                mutations.append({
                    "op": "put_item",
                    "id": vault_item_id,
                    "content": f"Vault root: {vault_root}",
                    "summary": f"Vault root: {vault_root}",
                    "tags": {"_source": "vault_detect", "category": "system"},
                    "queue_background_tasks": False,
                })

        for link in links:
            target = link["target"]
            style = link["style"]

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
                resolved = _resolve_internal_link(
                    target, item_id, context,
                    style=style, vault_root=vault_root,
                )
                if resolved:
                    # For wiki links, encode the alias for re-resolution
                    ref_value = resolved
                    if style == "wiki":
                        bare = target.removesuffix(".md")
                        ref_value = f"{resolved}[[{bare}]]"
                    resolved_targets.append(ref_value)
                elif create_targets:
                    # Auto-vivify: create from relative path if source is file://
                    vivified = _vivify_id(target, item_id)
                    if vivified:
                        bare = target.removesuffix(".md")
                        ref_value = vivified
                        if style == "wiki":
                            ref_value = f"{vivified}[[{bare}]]"
                        resolved_targets.append(ref_value)
                        mutations.append({
                            "op": "put_item",
                            "id": vivified,
                            "content": f"[{target}]",
                            "summary": f"[{target}]",
                            "tags": {"_source": "link", "_link_stem": bare},
                            "queue_background_tasks": False,
                        })

        if not resolved_targets:
            return {"skipped": True, "links_found": len(links)}

        # Build edge tags — merge with existing
        existing_tags = dict(getattr(item, "tags", {}) or {})
        existing_refs = existing_tags.get(tag_key)
        if isinstance(existing_refs, list):
            # Merge, preserving order, dedup by ID
            from ..types import parse_ref
            seen_ids = {parse_ref(r)[0] for r in existing_refs}
            merged = list(existing_refs)
            for t in resolved_targets:
                tid = parse_ref(t)[0]
                if tid not in seen_ids:
                    seen_ids.add(tid)
                    merged.append(t)
            existing_tags[tag_key] = merged
        elif isinstance(existing_refs, str) and existing_refs:
            from ..types import parse_ref
            seen_ids = {parse_ref(existing_refs)[0]}
            merged = [existing_refs]
            for t in resolved_targets:
                tid = parse_ref(t)[0]
                if tid not in seen_ids:
                    seen_ids.add(tid)
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
