from __future__ import annotations

"""Media description action for URI-backed non-text content."""

import logging
from pathlib import Path
from typing import Any

from ..paths import validate_path_within_home
from . import action
from ._item_scope import resolve_item

logger = logging.getLogger(__name__)


@action(id="describe")
class Describe:
    """Generate a text description of media content (images, audio, video)."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        """Describe media content and emit a summary mutation."""
        item_id, item = resolve_item(params, context)

        tags = getattr(item, "tags", None)
        tags = dict(tags) if isinstance(tags, dict) else {}
        uri = str(
            params.get("uri")
            or getattr(item, "uri", "")
            or tags.get("_source_uri")
            or item_id
        ).strip()
        content_type = str(
            params.get("content_type") or tags.get("_content_type") or ""
        ).strip()

        describer = context.resolve_provider("media")
        describe_fn = getattr(describer, "describe", None)
        if not callable(describe_fn):
            return {"description": "", "skipped": True}

        # Resolve file path
        file_path = uri.removeprefix("file://") if uri.startswith("file://") else uri
        path = Path(file_path).resolve()
        if not path.exists():
            logger.warning("File no longer exists for describe: %s", path)
            return {"description": "", "skipped": True, "reason": "missing_file"}

        try:
            validate_path_within_home(path)
        except ValueError:
            logger.warning("Describe path outside home directory, skipping: %s", path)
            return {"description": "", "skipped": True, "reason": "path_outside_home"}

        try:
            description = describe_fn(str(path), content_type)
        except Exception as e:
            logger.warning("Media description failed for %s: %s", uri, e)
            return {"description": "", "skipped": True, "reason": f"describe_error: {e}"}

        if not description or not description.strip():
            return {"description": "", "skipped": True, "reason": "empty_description"}

        # Append to existing summary
        existing_summary = str(getattr(item, "summary", "") or "")
        if existing_summary:
            enriched = existing_summary.rstrip() + "\n\nDescription:\n" + description
        else:
            enriched = description

        return {
            "description": description,
            "mutations": [
                {
                    "op": "set_summary",
                    "target": item_id,
                    "summary": enriched,
                    "embed": True,
                }
            ],
        }
