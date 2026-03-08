from __future__ import annotations

from typing import Any

from . import action, item_to_result


@action(name="resolve_meta")
class ResolveMeta:
    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        raw_item_id = params.get("item_id")
        if raw_item_id is None:
            raise ValueError("resolve_meta requires item_id")
        limit = max(int(params.get("limit", 3)), 1)
        sections_raw = context.resolve_meta(str(raw_item_id), limit_per_doc=limit)
        sections: dict[str, list[dict[str, Any]]] = {}
        total = 0
        for key, values in (sections_raw or {}).items():
            if not isinstance(values, list):
                continue
            rows = [item_to_result(v) for v in values]
            if not rows:
                continue
            sections[str(key)] = rows
            total += len(rows)
        return {
            "sections": sections,
            "count": total,
        }
