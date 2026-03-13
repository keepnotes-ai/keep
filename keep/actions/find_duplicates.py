from __future__ import annotations

"""Find documents with identical content and link them via an edge tag."""

from typing import Any

from . import action
from ._item_scope import resolve_item_id


@action(id="find_duplicates", priority=2)
class FindDuplicates:
    """Detect exact-content duplicates and emit edge-tag mutations."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        item_id = resolve_item_id(params, context)
        if not item_id:
            raise ValueError("find_duplicates requires item_id")

        tag_key = str(params.get("tag", "duplicates"))

        doc = context.get_document(item_id)
        if doc is None:
            return {"skipped": True, "reason": "document not found"}

        content_hash = getattr(doc, "content_hash", None)
        if not content_hash:
            return {"skipped": True, "reason": "no content hash"}

        content_hash_full = getattr(doc, "content_hash_full", "") or ""

        matches = context.find_by_content_hash(
            content_hash,
            content_hash_full=content_hash_full,
            exclude_id=item_id,
            limit=20,
        )
        if not matches:
            return {"duplicates": []}

        match_ids = [getattr(m, "id", str(m)) for m in matches]

        # Merge with any existing edge-tag values to avoid clobbering
        existing_tags = getattr(doc, "tags", None) or {}
        existing_vals = existing_tags.get(tag_key)
        if isinstance(existing_vals, list):
            current = set(existing_vals)
        elif existing_vals:
            current = {str(existing_vals)}
        else:
            current = set()

        new_ids = [mid for mid in match_ids if mid not in current]
        if not new_ids:
            return {"skipped": True, "reason": "already linked", "duplicates": match_ids}

        merged = list(current | set(new_ids))
        tag_value = merged if len(merged) > 1 else merged[0]

        mutations = [
            {
                "op": "set_tags",
                "target": item_id,
                "tags": {tag_key: tag_value},
            }
        ]

        return {"duplicates": match_ids, "mutations": mutations}
