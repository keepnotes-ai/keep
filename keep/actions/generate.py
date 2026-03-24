from __future__ import annotations

import json
from typing import Any

from ..providers.base import parse_tag_json
from . import action


@action(id="generate", async_action=True)
class Generate:
    """Generate text via LLM.

    Accepts either raw ``system``/``user`` text or a ``prompt`` reference
    to a ``.prompt/*`` system doc.  When ``prompt`` is provided, the
    matching prompt doc is resolved (most-specific match wins) and used
    as the system prompt.  ``doc_tags`` can be passed to influence prompt
    matching specificity.
    """

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        provider = context.resolve_provider("summarization")
        generate = getattr(provider, "generate", None)
        if not callable(generate):
            raise ValueError("generate action requires provider.generate(system, user, ...)")

        # Resolve system prompt: explicit text or .prompt/* doc
        system = str(params.get("system") or "")
        prompt_prefix = params.get("prompt")
        if prompt_prefix and not system:
            resolve = getattr(context, "resolve_prompt", None)
            if resolve is not None:
                doc_tags = params.get("doc_tags")
                if not isinstance(doc_tags, dict):
                    doc_tags = {}
                item_id = params.get("id") or params.get("item_id")
                resolved = resolve(
                    str(prompt_prefix), doc_tags,
                    item_id=str(item_id) if item_id else None,
                )
                if resolved:
                    system = str(resolved)

        user = str(params.get("user") or "")
        max_tokens = max(int(params.get("max_tokens", 4096)), 1)
        format_hint = str(params.get("format") or "").strip().lower()

        raw = generate(system, user, max_tokens=max_tokens)
        text = "" if raw is None else str(raw)
        out: dict[str, Any] = {"text": text}
        if format_hint != "json":
            return out

        parsed: Any
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = parse_tag_json(text)
        if isinstance(parsed, dict):
            for key, value in parsed.items():
                key_str = str(key)
                if key_str == "text":
                    continue
                out[key_str] = value
        return out
