from __future__ import annotations

import hashlib
from typing import Any

from . import action


def _content_id(content: str) -> str:
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()[:12]
    return f"%{digest}"


@action
class Put:
    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        del context
        content = params.get("content")
        uri = params.get("uri")
        if (content is None and uri is None) or (content is not None and uri is not None):
            raise ValueError("put requires exactly one of content or uri")

        item_id = params.get("id")
        resolved_id = str(item_id).strip() if item_id is not None else ""
        if not resolved_id and uri is not None:
            resolved_id = str(uri).strip()
        if not resolved_id and content is not None:
            resolved_id = _content_id(str(content))

        tags = params.get("tags")
        normalized_tags = {str(k): v for k, v in tags.items()} if isinstance(tags, dict) else None
        summary = params.get("summary")

        op: dict[str, Any] = {"op": "put_item"}
        if content is not None:
            op["content"] = str(content)
        if uri is not None:
            op["uri"] = str(uri)
        if item_id is not None:
            op["id"] = str(item_id)
        if normalized_tags is not None:
            op["tags"] = normalized_tags
        if summary is not None:
            op["summary"] = str(summary)
        if "created_at" in params:
            op["created_at"] = params.get("created_at")
        if "force" in params:
            op["force"] = bool(params.get("force"))
        if "queue_background_tasks" in params:
            op["queue_background_tasks"] = bool(params.get("queue_background_tasks"))
        if "capture_write_context" in params:
            op["capture_write_context"] = bool(params.get("capture_write_context"))

        return {
            "id": resolved_id,
            "mutations": [op],
        }
