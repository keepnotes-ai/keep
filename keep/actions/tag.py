from __future__ import annotations

"""Apply explicit tags to one or more items."""

from typing import Any

from . import action


@action(id="tag")
class Tag:
    """Set explicit tags on one or more items."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        tags = params.get("tags")
        if not isinstance(tags, dict) or not tags:
            # When called as a background task without explicit tags,
            # skip gracefully instead of failing repeatedly.
            return {"skipped": True, "reason": "no tags provided"}

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

        for tid in target_ids:
            context.tag(tid, tags)

        return {
            "count": len(target_ids),
            "ids": target_ids,
        }
