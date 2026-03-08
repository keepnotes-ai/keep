from __future__ import annotations

"""Item-scoped summary generation action."""

from typing import Any

from . import action
from ._item_scope import resolve_item_content


@action
class Summarize:
    """Generate and return a summary for the target item."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        """Summarize item content and emit a `set_summary` mutation."""
        item_id, _item, content = resolve_item_content(params, context)

        provider = context.resolve_provider("summarization")
        summarize = getattr(provider, "summarize", None)
        if not callable(summarize):
            raise ValueError("summarization provider does not expose summarize(content, ...)")

        max_length = int(params.get("max_length", 500))
        context_text = params.get("context")
        try:
            summary = summarize(
                str(content),
                max_length=max(max_length, 1),
                context=str(context_text) if context_text is not None else None,
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
        return {
            "summary": out_summary,
            "mutations": [
                {
                    "op": "set_summary",
                    "target": item_id,
                    "summary": "$output.summary",
                }
            ],
        }
