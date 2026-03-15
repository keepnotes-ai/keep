from __future__ import annotations

from typing import Any

from . import action


@action(id="list_versions")
class ListVersions:
    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        item_id = params.get("id") or params.get("item_id")
        if not item_id:
            raise ValueError("list_versions requires id")
        limit = int(params.get("limit") or 3)
        versions = context.list_versions(str(item_id), limit=limit)
        return {
            "versions": [
                {
                    "offset": i + 1,
                    "summary": str(getattr(v, "summary", "")),
                    "date": (getattr(v, "tags", {}) or {}).get("_created")
                            or getattr(v, "created_at", ""),
                }
                for i, v in enumerate(versions)
            ],
            "count": len(versions),
        }
