from __future__ import annotations

"""Item-scoped decomposition action for generating structured parts."""

from typing import Any

from ..processors import process_analyze
from ..providers.base import AnalysisChunk
from ..types import SYSTEM_TAG_PREFIX
from . import action
from ._item_scope import check_content_hash, resolve_item_content
from ._tagging import classify_parts_with_specs
from ._item_scope import resolve_item
from ._tagging import load_tag_specs


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


@action(id="analyze", priority=7, async_action=True)
class Analyze:
    """Decompose item content into parts and emit part `put_item` mutations."""

    def prepare(self, params: dict[str, Any], context) -> dict[str, Any]:
        """Populate analyze inputs shared by local and delegated execution."""
        prepared = dict(params)
        item_id, item = resolve_item(prepared, context)
        item_tags = dict(getattr(item, "tags", None) or {})

        if prepared.get("chunks") is None:
            gather_chunks = getattr(context, "gather_analyze_chunks", None)
            if callable(gather_chunks):
                chunk_data = gather_chunks(item_id, item)
                if isinstance(chunk_data, dict):
                    prepared["chunks"] = list(chunk_data.get("context", [])) + list(chunk_data.get("targets", []))
                elif isinstance(chunk_data, list):
                    prepared["chunks"] = chunk_data

        if prepared.get("guide_context") in (None, ""):
            raw_tags = prepared.get("tags")
            if isinstance(raw_tags, list) and raw_tags:
                gather_guide = getattr(context, "gather_guide_context", None)
                if callable(gather_guide):
                    prepared["guide_context"] = gather_guide(raw_tags)

        if prepared.get("tag_specs") is None:
            specs = load_tag_specs(context)
            if specs:
                prepared["tag_specs"] = specs

        if prepared.get("prompt_override") is None and hasattr(context, "resolve_prompt"):
            prompt_text = context.resolve_prompt("analyze", item_tags)
            if prompt_text is not None:
                prepared["prompt_override"] = prompt_text

        return prepared

    def build_delegated_payload(
        self, params: dict[str, Any], content: str,
    ) -> tuple[str, dict[str, Any] | None]:
        metadata: dict[str, Any] = {}
        for key in ("chunks", "guide_context", "tag_specs", "prompt_override"):
            value = params.get(key)
            if value:
                metadata[key] = value
        if isinstance(params.get("tags"), list):
            metadata["tags"] = list(params["tags"])
        return "", metadata or None

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        """Analyze content, classify parts, and build storage mutations."""
        item_id, _item = resolve_item(params, context)
        item_tags = dict(getattr(_item, "tags", None) or {})

        if check_content_hash(params, context, item_id, "_analyzed_hash"):
            return {"skipped": True, "reason": "content unchanged"}
        prepared = self.prepare(params, context)
        guide_context = str(prepared.get("guide_context") or "")
        prompt_text = prepared.get("prompt_override")
        if prompt_text is None:
            raise ValueError("missing prompt doc for analyze")

        raw_chunks = prepared.get("chunks")
        if isinstance(raw_chunks, list) and raw_chunks:
            chunk_dicts = raw_chunks
        else:
            _item_id, _item_again, content = resolve_item_content(params, context)
            chunk_dicts = [{"content": str(content), "tags": {}, "index": 0}]

        analysis_chunks = [
            AnalysisChunk(
                content=str(chunk.get("content", "")),
                tags=dict(chunk.get("tags") or {}),
                index=int(chunk.get("index", idx)),
            )
            for idx, chunk in enumerate(chunk_dicts)
            if isinstance(chunk, dict)
        ]

        raw_parts: list[dict[str, Any]]
        analyzer = context.resolve_provider("analyzer")
        analyze_fn = getattr(analyzer, "analyze", None)
        if callable(analyze_fn):
            result = analyze_fn(analysis_chunks, guide_context, prompt_override=prompt_text)
            raw_parts = result if isinstance(result, list) else []
        else:
            summarizer = context.resolve_provider("summarization")
            proc = process_analyze(
                chunk_dicts,
                guide_context,
                None,
                analyzer_provider=summarizer,
                classifier_provider=summarizer,
                prompt_override=prompt_text,
            )
            raw_parts = proc.get("parts") or []

        parts = [_normalize_part(part) for part in raw_parts]
        for idx, part in enumerate(parts, start=1):
            part["part_num"] = idx
        tag_specs = prepared.get("tag_specs")
        if isinstance(tag_specs, list) and tag_specs:
            try:
                from ..analyzers import TagClassifier
                provider = context.resolve_provider("summarization")
                classifier = TagClassifier(provider=provider)
                parts = classifier.classify(parts, specs=tag_specs)
            except Exception:
                parts = classify_parts_with_specs(parts, context)
        else:
            parts = classify_parts_with_specs(parts, context)
        out: dict[str, Any] = {"parts": parts}

        if not parts:
            return out

        mutations: list[dict[str, Any]] = []

        # Delete old parts before inserting new ones
        mutations.append({"op": "delete_prefix", "prefix": f"{item_id}@p"})

        doc = context.get_document(item_id) if hasattr(context, "get_document") else None
        existing_tags = dict(getattr(doc, "tags", None) or {}) if doc else {}
        parent_user_tags = {
            key: value for key, value in existing_tags.items()
            if not key.startswith(SYSTEM_TAG_PREFIX)
        }

        for idx, part in enumerate(parts, start=1):
            part_id = f"{item_id}@p{idx}"
            tags = dict(parent_user_tags)
            tags.update(dict(part.get("tags") or {}))
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

        # Record _analyzed_hash so we don't re-analyze unchanged content
        content_hash = getattr(doc, "content_hash", None) if doc else None
        if content_hash:
            existing_tags["_analyzed_hash"] = content_hash
            list_versions = getattr(context, "list_versions", None)
            if callable(list_versions):
                versions = list_versions(item_id, limit=1)
                if versions:
                    version = getattr(versions[0], "version", None)
                    if version is not None:
                        existing_tags["_analyzed_version"] = str(version)
            mutations.append(
                {
                    "op": "set_tags",
                    "target": item_id,
                    "tags": existing_tags,
                }
            )

        out["mutations"] = mutations
        return out
