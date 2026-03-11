from __future__ import annotations

from typing import Any

from . import action


@action(id="get")
class Get:
    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        raw_id = params.get("id")
        if raw_id is None:
            raise ValueError("get requires id")
        item = context.get(str(raw_id))
        if item is None:
            return {}
        tags = getattr(item, "tags", None)
        return {
            "id": str(getattr(item, "id", "")),
            "summary": str(getattr(item, "summary", "")),
            "tags": dict(tags) if isinstance(tags, dict) else {},
        }
