from __future__ import annotations

"""Item-scoped decomposition action for generating structured parts."""

from typing import Any

from ..processors import process_analyze
from ..providers.base import AnalysisChunk
from . import action
from ._item_scope import resolve_item_content
from ._tagging import classify_parts_with_specs


def _normalize_part(raw: Any) -> dict[str, Any]:
    """Normalize provider output into a stable part shape."""
    if not isinstance(raw, dict):
        return {"summary": "", "content": "", "tags": {}}
    tags = raw.get("tags")
    return {
        "summary": str(raw.get("summary") or ""),
        "content": str(raw.get("content") or ""),
        "tags": dict(tags) if isinstance(tags, dict) else {},
    }


@action
class Analyze:
    """Decompose item content into parts and emit part `put_item` mutations."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        """Analyze content, classify parts, and build storage mutations."""
        item_id, _item, content = resolve_item_content(params, context)
        guide_context = str(params.get("guide_context") or "")

        raw_parts: list[dict[str, Any]]
        analyzer = context.resolve_provider("analyzer")
        analyze_fn = getattr(analyzer, "analyze", None)
        if callable(analyze_fn):
            chunks = [AnalysisChunk(content=str(content), tags={}, index=0)]
            result = analyze_fn(chunks, guide_context)
            raw_parts = result if isinstance(result, list) else []
        else:
            summarizer = context.resolve_provider("summarization")
            proc = process_analyze(
                [{"content": str(content), "tags": {}, "index": 0}],
                guide_context,
                None,
                analyzer_provider=summarizer,
                classifier_provider=summarizer,
            )
            raw_parts = proc.parts or []

        parts = [_normalize_part(part) for part in raw_parts]
        for idx, part in enumerate(parts, start=1):
            part["part_num"] = idx
        parts = classify_parts_with_specs(parts, context)
        out: dict[str, Any] = {"parts": parts}

        if not parts:
            return out

        mutations: list[dict[str, Any]] = []
        for idx, part in enumerate(parts, start=1):
            part_id = f"{item_id}@p{idx}"
            tags = dict(part.get("tags") or {})
            tags["_base_id"] = item_id
            tags["_part_num"] = str(idx)
            mutations.append(
                {
                    "op": "put_item",
                    "id": part_id,
                    "content": str(part.get("content") or ""),
                    "summary": str(part.get("summary") or ""),
                    "tags": tags,
                    "queue_background_tasks": False,
                }
            )
        out["mutations"] = mutations
        return out
