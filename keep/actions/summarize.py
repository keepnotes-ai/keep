from __future__ import annotations

"""Item-scoped summary generation action."""

from typing import Any

from ..types import filter_non_system_tags
from . import action
from ._item_scope import check_content_hash, resolve_item, resolve_item_content


def _enrich_content_type(item_id: str, tags: dict) -> dict:
    """Infer _content_type from a URI-style item ID if not already set.

    Allows prompt matching on content type for items stored before
    _content_type was tracked as a system tag.
    """
    from keep.providers.documents import FileDocumentProvider

    # Extract suffix from URI or path
    clean_id = item_id.split("#")[0].split("?")[0]
    if "/" in clean_id:
        suffix_part = clean_id.rsplit("/", 1)[-1]
    else:
        suffix_part = clean_id
    if "." in suffix_part:
        ext = "." + suffix_part.rsplit(".", 1)[-1].lower()
        ct = FileDocumentProvider.EXTENSION_TYPES.get(ext)
        if ct:
            tags = dict(tags)
            tags["_content_type"] = ct
    return tags


@action(id="summarize", priority=3, async_action=True)
class Summarize:
    """Generate and return a summary for the target item."""

    def prepare(self, params: dict[str, Any], context) -> dict[str, Any]:
        """Populate summarize inputs shared by local and delegated execution."""
        prepared = dict(params)
        try:
            item_id, item = resolve_item(prepared, context)
        except ValueError:
            return prepared
        item_tags = dict(getattr(item, "tags", None) or {})

        if prepared.get("context") is None:
            user_tags = filter_non_system_tags(item_tags)
            gather = getattr(context, "gather_context", None)
            if user_tags and callable(gather):
                context_text = gather(item_id, user_tags)
                if context_text:
                    prepared["context"] = context_text

        if prepared.get("system_prompt") is None:
            prompt_tags = item_tags
            if "_content_type" not in prompt_tags and item_id:
                prompt_tags = _enrich_content_type(item_id, prompt_tags)
            resolve_prompt = getattr(context, "resolve_prompt", None)
            if resolve_prompt is not None:
                prompt_text = resolve_prompt("summarize", prompt_tags)
                if prompt_text is not None:
                    prepared["system_prompt"] = prompt_text

        return prepared

    def build_delegated_payload(
        self, params: dict[str, Any], content: str,
    ) -> tuple[str, dict[str, Any] | None]:
        metadata: dict[str, Any] = {}
        context_text = params.get("context")
        if context_text:
            metadata["context"] = context_text
        prompt_text = params.get("system_prompt")
        if prompt_text:
            metadata["system_prompt_override"] = prompt_text
        return content, metadata or None

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        """Summarize item content and emit a `set_summary` mutation."""
        item_id, item, content = resolve_item_content(params, context)

        if check_content_hash(params, context, item_id, "_summarized_hash"):
            return {"skipped": True, "reason": "content unchanged"}

        provider = context.resolve_provider("summarization")
        summarize = getattr(provider, "summarize", None)
        if not callable(summarize):
            raise ValueError("summarization provider does not expose summarize(content, ...)")

        max_length = int(params.get("max_length", 500))
        context_text = params.get("context")

        # Resolve prompt doc (.prompt/summarize/*) matching item tags
        prompt_text = params.get("system_prompt")
        if prompt_text is None:
            prepared = self.prepare(params, context)
            prompt_text = prepared.get("system_prompt")
            context_text = prepared.get("context", context_text)
        if prompt_text is None:
            raise ValueError("missing prompt doc for summarize")

        try:
            summary = summarize(
                str(content),
                max_length=max(max_length, 1),
                context=str(context_text) if context_text is not None else None,
                system_prompt=prompt_text,
            )
        except TypeError:
            try:
                summary = summarize(
                    str(content),
                    context=str(context_text) if context_text is not None else None,
                )
            except TypeError:
                summary = summarize(str(content))
        out_summary = "" if summary is None else str(summary)
        mutations: list[dict[str, Any]] = [
            {
                "op": "set_summary",
                "target": item_id,
                "summary": out_summary,
            }
        ]
        # Record _summarized_hash so we skip unchanged content next time
        doc = context.get_document(item_id) if hasattr(context, "get_document") else None
        content_hash = getattr(doc, "content_hash", None) if doc else None
        if content_hash:
            mutations.append(
                {
                    "op": "set_tags",
                    "target": item_id,
                    "tags": {"_summarized_hash": content_hash},
                }
            )
        return {
            "summary": out_summary,
            "mutations": mutations,
        }
