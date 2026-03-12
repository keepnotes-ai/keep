from __future__ import annotations

"""Item-scoped summary generation action."""

from typing import Any

from . import action
from ._item_scope import check_content_hash, resolve_item_content


@action(id="summarize", priority=3)
class Summarize:
    """Generate and return a summary for the target item."""

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
            item_tags = getattr(item, "tags", None) or {}
            resolve_prompt = getattr(context, "resolve_prompt", None)
            if resolve_prompt is not None:
                try:
                    prompt_text = resolve_prompt("summarize", item_tags)
                except Exception:
                    pass

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
