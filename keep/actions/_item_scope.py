from __future__ import annotations

"""Shared helpers for item-scoped state actions."""

from typing import Any


def resolve_item_id(params: dict[str, Any], context: Any) -> str | None:
    """Resolve the target item ID from action params or current context."""
    raw = params.get("item_id")
    if raw is not None:
        item_id = str(raw).strip()
        if item_id:
            return item_id
    current = getattr(context, "item_id", None)
    if current is None:
        return None
    item_id = str(current).strip()
    return item_id or None


def resolve_item(params: dict[str, Any], context: Any) -> tuple[str, Any]:
    """Load the target item for an item-scoped action."""
    item_id = resolve_item_id(params, context)
    if not item_id:
        raise ValueError("item-scoped action requires item_id")
    item = context.get(item_id)
    if item is None:
        raise ValueError(f"target item not found: {item_id}")
    return item_id, item


def check_content_hash(
    params: dict[str, Any], context: Any, item_id: str, hash_tag: str
) -> bool:
    """Return True if the item's content is unchanged since the last run.

    Compares ``content_hash`` on the document with the stored ``hash_tag``
    value.  Returns False (do not skip) when:
    - ``params["force"]`` is truthy
    - the document or content_hash is unavailable
    - hashes differ or the tag has never been set
    """
    if params.get("force"):
        return False
    get_doc = getattr(context, "get_document", None)
    if get_doc is None:
        return False
    doc = get_doc(item_id)
    if doc is None:
        return False
    content_hash = getattr(doc, "content_hash", None)
    if not content_hash:
        return False
    tags = getattr(doc, "tags", None) or {}
    return tags.get(hash_tag) == content_hash


def resolve_item_content(params: dict[str, Any], context: Any) -> tuple[str, Any, str]:
    """Resolve non-empty content text for an item-scoped action."""
    item_id, item = resolve_item(params, context)
    content = getattr(item, "content", None)
    if content is None and getattr(context, "item_id", None) == item_id:
        payload_content = getattr(context, "item_content", None)
        if payload_content is not None:
            content = payload_content
    if content is None:
        content = getattr(item, "summary", "")
    text = str(content or "")
    if not text:
        raise ValueError(f"item content unavailable: {item_id}")
    return item_id, item, text
