from __future__ import annotations

from typing import Any

from . import action


@action(id="resolve_edges")
class ResolveEdges:
    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        item_id = params.get("id") or params.get("item_id")
        if not item_id:
            raise ValueError("resolve_edges requires id")
        limit = int(params.get("limit") or 5)
        edges = context.resolve_edges(str(item_id), limit=limit)
        return edges
