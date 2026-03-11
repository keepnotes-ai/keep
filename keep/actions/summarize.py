from __future__ import annotations

"""Item-scoped summary generation action."""

import logging
from typing import TYPE_CHECKING, Any

from ..processors import process_summarize
from ..types import filter_non_system_tags
from . import action
from ._item_scope import resolve_item_content

if TYPE_CHECKING:
    from ..api import Keeper
    from ..task_workflows import TaskRequest, TaskRunResult

logger = logging.getLogger(__name__)


@action(id="summarize")
class Summarize:
    """Generate and return a summary for the target item."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        """Summarize item content and emit a `set_summary` mutation."""
        item_id, item, content = resolve_item_content(params, context)

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

    def run_task(self, keeper: "Keeper", req: "TaskRequest") -> "TaskRunResult":
        """Background task workflow for summarization."""
        from ..task_workflows import TaskRunResult

        doc = keeper._document_store.get(req.collection, req.id)
        if doc is None:
            return TaskRunResult(status="skipped", details={"reason": "deleted"})

        summarize_prompt = None
        try:
            summarize_prompt = keeper._resolve_prompt_doc("summarize", doc.tags)
        except Exception as e:
            logger.debug("Summarize prompt doc resolution failed: %s", e)

        context = None
        user_tags = filter_non_system_tags(doc.tags)
        if user_tags:
            context = keeper._gather_context(req.id, user_tags)

        result = process_summarize(
            req.content,
            context=context,
            summarization_provider=keeper._get_summarization_provider(),
            system_prompt_override=summarize_prompt,
        )
        keeper.apply_result(req.id, req.collection, result)
        return TaskRunResult(status="applied")
