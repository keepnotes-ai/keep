from __future__ import annotations

"""Spec-driven tagging helpers shared by state actions."""

from typing import Any

from ..analyzers import TagClassifier, extract_prompt_section


def _truthy(value: Any) -> bool:
    """Return True for conventional truthy textual values."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "on"}


def _item_id(item: Any) -> str:
    """Return a normalized item ID string."""
    return str(getattr(item, "id", "")).strip()


def _item_summary(item: Any) -> str:
    """Return an item's summary text."""
    return str(getattr(item, "summary", "") or "")


def _item_tags(item: Any) -> dict[str, Any]:
    """Return an item's tag mapping."""
    raw = getattr(item, "tags", None)
    return dict(raw) if isinstance(raw, dict) else {}


def load_tag_specs(context: Any, *, limit: int = 5000) -> list[dict[str, Any]]:
    """Load constrained `.tag/*` specs through action context adapters."""
    fetch_limit = max(int(limit), 1)
    try:
        docs = context.list_items(
            prefix=".tag/",
            include_hidden=True,
            limit=fetch_limit,
        )
    except Exception:
        return []
    if not isinstance(docs, list) or not docs:
        return []

    parents: dict[str, Any] = {}
    children: dict[str, list[Any]] = {}
    for item in docs:
        doc_id = _item_id(item)
        if not doc_id.startswith(".tag/"):
            continue
        parts = doc_id.split("/")
        if len(parts) == 2:
            key = parts[1].strip()
            if key:
                parents[key] = item
            continue
        if len(parts) == 3:
            key = parts[1].strip()
            if key:
                children.setdefault(key, []).append(item)

    specs: list[dict[str, Any]] = []
    for key in sorted(parents.keys()):
        parent = parents[key]
        if not _truthy(_item_tags(parent).get("_constrained")):
            continue
        parent_summary = _item_summary(parent)
        values: list[dict[str, Any]] = []
        for child in sorted(children.get(key, []), key=_item_id):
            child_id = _item_id(child)
            value = child_id.split("/")[-1].strip()
            if not value:
                continue
            child_summary = _item_summary(child)
            values.append(
                {
                    "value": value,
                    "description": child_summary,
                    "prompt": extract_prompt_section(child_summary),
                }
            )
        specs.append(
            {
                "key": key,
                "description": parent_summary,
                "prompt": extract_prompt_section(parent_summary),
                "values": values,
            }
        )

    return specs


def classify_parts_with_specs(parts: list[dict[str, Any]], context: Any) -> list[dict[str, Any]]:
    """Classify part summaries with constrained tag specs when available."""
    if not parts:
        return parts
    specs = load_tag_specs(context)
    if not specs:
        return parts
    provider = context.resolve_provider("summarization")
    classifier = TagClassifier(provider=provider)
    return classifier.classify(parts, specs=specs)
