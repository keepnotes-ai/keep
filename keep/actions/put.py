from __future__ import annotations

from typing import Any

from . import action


@action(id="put")
class Put:
    """Store content or index a URI in memory."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        content = params.get("content")
        uri = params.get("uri")
        if content is None and uri is None:
            raise ValueError("put requires content or uri")

        tags = params.get("tags")
        normalized_tags = {str(k): v for k, v in tags.items()} if isinstance(tags, dict) else None
        summary = params.get("summary")
        item_id = params.get("id")
        created_at = params.get("created_at")
        force = bool(params.get("force", False))

        item = context.put(
            content=str(content) if content is not None else None,
            uri=str(uri) if uri is not None else None,
            id=str(item_id) if item_id is not None else None,
            tags=normalized_tags,
            summary=str(summary) if summary is not None else None,
            created_at=str(created_at) if created_at is not None else None,
            force=force,
        )
        return {
            "id": getattr(item, "id", None),
            "summary": getattr(item, "summary", None),
            "tags": dict(getattr(item, "tags", None) or {}),
            "changed": getattr(item, "changed", None),
        }
