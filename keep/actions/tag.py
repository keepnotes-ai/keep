from __future__ import annotations

"""Apply explicit tags to one or more items."""

from typing import Any

from . import action


@action(id="tag")
class Tag:
    """Apply public tag semantics to one or more items."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        tags = params.get("tags")
        remove = params.get("remove") if isinstance(params.get("remove"), list) else None
        remove_values = (
            params.get("remove_values")
            if isinstance(params.get("remove_values"), dict)
            else None
        )
        if not isinstance(tags, dict):
            tags = None
        if not tags and not remove and not remove_values:
            return {"skipped": True, "reason": "no tag mutations provided"}

        # Accept a single item ID or a list of result dicts
        items = params.get("items")
        item_id = params.get("id") or params.get("item_id")

        target_ids: list[str] = []
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and "id" in item:
                    target_ids.append(str(item["id"]))
                elif isinstance(item, str):
                    target_ids.append(item)
        elif item_id:
            target_ids.append(str(item_id))

        if not target_ids:
            raise ValueError("tag requires items (list) or id")

        updated_items: list[dict[str, Any]] = []
        for target_id in target_ids:
            item = context.tag(
                target_id,
                tags,
                remove=remove,
                remove_values=remove_values,
            )
            if item is None:
                raise ValueError(f"Item not found: {target_id}")
            updated_items.append({
                "id": getattr(item, "id", target_id),
                "summary": getattr(item, "summary", ""),
                "tags": dict(getattr(item, "tags", None) or {}),
            })

        first = updated_items[0]
        return {
            "count": len(target_ids),
            "ids": target_ids,
            "id": first["id"],
            "summary": first["summary"],
            "tags": first["tags"],
            "items": updated_items,
        }
