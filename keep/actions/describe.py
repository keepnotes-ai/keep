from __future__ import annotations

"""Media description action for URI-backed non-text content."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..paths import validate_path_within_home
from . import action
from ._item_scope import resolve_item

if TYPE_CHECKING:
    from ..api import Keeper
    from ..task_workflows import TaskRequest, TaskRunResult

logger = logging.getLogger(__name__)


@action(id="describe")
class Describe:
    """Generate a text description of media content (images, audio, video)."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        """Describe media content and emit a summary mutation."""
        item_id, item = resolve_item(params, context)

        tags = getattr(item, "tags", None)
        tags = dict(tags) if isinstance(tags, dict) else {}
        uri = str(getattr(item, "uri", "") or tags.get("_source_uri") or item_id).strip()
        content_type = str(tags.get("_content_type") or "").strip()

        describer = context.resolve_provider("media")
        describe_fn = getattr(describer, "describe", None)
        if not callable(describe_fn):
            return {"description": "", "skipped": True}

        description = str(describe_fn(uri, content_type) or "")
        out: dict[str, Any] = {"description": description}
        if description.strip():
            out["mutations"] = [
                {
                    "op": "set_summary",
                    "target": item_id,
                    "summary": "$output.description",
                }
            ]
        return out

    def run_task(self, keeper: "Keeper", req: "TaskRequest") -> "TaskRunResult":
        """Background task workflow for media description."""
        from ..processors import ProcessorResult
        from ..task_workflows import TaskRunResult

        uri = req.metadata.get("uri") or req.id
        content_type = req.metadata.get("content_type", "")

        describer = keeper._get_media_describer()
        if not describer:
            return TaskRunResult(status="skipped", details={"reason": "no_media_provider"})

        file_path = uri.removeprefix("file://") if uri.startswith("file://") else uri
        path = Path(file_path).resolve()
        if not path.exists():
            logger.warning("File no longer exists for describe: %s", path)
            return TaskRunResult(status="skipped", details={"reason": "missing_file"})

        try:
            validate_path_within_home(path)
        except ValueError:
            logger.warning("Describe path outside home directory, skipping: %s", path)
            return TaskRunResult(status="skipped", details={"reason": "path_outside_home"})

        try:
            description = describer.describe(str(path), content_type)
        except Exception as e:
            logger.warning("Media description failed for %s: %s", uri, e)
            return TaskRunResult(status="skipped", details={"reason": f"describe_error: {e}"})

        if not description or not description.strip():
            return TaskRunResult(status="skipped", details={"reason": "empty_description"})

        existing = keeper._document_store.get(req.collection, req.id)
        if not existing:
            return TaskRunResult(status="skipped", details={"reason": "deleted"})

        enriched = existing.summary
        if enriched:
            enriched = enriched.rstrip() + "\n\nDescription:\n" + description
        else:
            enriched = description

        result = ProcessorResult(task_type="describe", summary=enriched)
        keeper.apply_result(req.id, req.collection, result, existing_tags=existing.tags)
        return TaskRunResult(status="applied", details={"chars": len(description)})
