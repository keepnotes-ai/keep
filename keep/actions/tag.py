from __future__ import annotations

"""Item-scoped constrained-tag classification action."""

from typing import TYPE_CHECKING, Any

from . import action
from ._item_scope import resolve_item_content
from ._tagging import classify_parts_with_specs

if TYPE_CHECKING:
    from ..api import Keeper
    from ..task_workflows import TaskRequest, TaskRunResult


def _normalize_tag_value(value: Any) -> str | list[str] | None:
    """Normalize classifier tag values to scalar-or-list strings."""
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        out = [str(v).strip() for v in value if str(v).strip()]
        if not out:
            return None
        return out[0] if len(out) == 1 else out
    text = str(value).strip()
    return text or None


@action(id="tag")
class Tag:
    """Classify an item against `.tag/*` specs and emit tag mutations."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        """Run constrained classification and return normalized tags."""
        item_id, _item, content = resolve_item_content(params, context)

        parts = [{"summary": str(content), "tags": {}}]
        classified = classify_parts_with_specs(parts, context)
        row = classified[0] if classified else {}
        raw_tags = row.get("tags") if isinstance(row, dict) else {}
        if not isinstance(raw_tags, dict):
            raw_tags = {}
        tags: dict[str, Any] = {}
        for key, value in raw_tags.items():
            key_str = str(key).strip()
            if not key_str:
                continue
            normalized = _normalize_tag_value(value)
            if normalized is None:
                continue
            tags[key_str] = normalized
        out: dict[str, Any] = {"tags": tags}
        if tags:
            out["mutations"] = [
                {
                    "op": "set_tags",
                    "target": item_id,
                    "tags": "$output.tags",
                }
            ]
        return out

    def run_task(self, keeper: "Keeper", req: "TaskRequest") -> "TaskRunResult":
        """Background task workflow for tag classification."""
        from ..task_workflows import TaskRunResult

        content = str(req.content or "").strip()
        if not content:
            return TaskRunResult(status="skipped", details={"reason": "no_content"})

        provider_name = str(req.metadata.get("provider") or "noop").strip()
        provider_params = req.metadata.get("provider_params")
        if provider_params is not None and not isinstance(provider_params, dict):
            provider_params = None

        from ..providers.base import get_registry

        registry = get_registry()
        provider = registry.create_tagging(provider_name, provider_params)
        tag_method = getattr(provider, "tag", None)
        if not callable(tag_method):
            return TaskRunResult(status="skipped", details={"reason": "provider_has_no_tag"})

        tags = tag_method(content)
        if not isinstance(tags, dict):
            return TaskRunResult(status="skipped", details={"reason": "invalid_provider_output"})

        normalized = {str(k): str(v) for k, v in tags.items() if str(k).strip() and str(v).strip()}
        if not normalized:
            return TaskRunResult(status="skipped", details={"reason": "empty_tags"})

        keeper.tag(req.id, tags=normalized)
        return TaskRunResult(status="applied", details={"tag_count": len(normalized)})
