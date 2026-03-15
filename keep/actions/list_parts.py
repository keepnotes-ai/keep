from __future__ import annotations

from typing import Any

from . import action, item_to_result


@action(id="list_parts")
class ListParts:
    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        item_id = params.get("id") or params.get("item_id")
        if not item_id:
            raise ValueError("list_parts requires id")
        limit = int(params.get("limit") or 5)
        rows = context.list_items(
            prefix=f"{item_id}@p",
            include_hidden=True,
            limit=limit,
        )
        results = [item_to_result(row) for row in rows]
        return {
            "results": results,
            "count": len(results),
        }
