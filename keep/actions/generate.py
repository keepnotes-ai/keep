from __future__ import annotations

import json
from typing import Any

from ..providers.base import parse_tag_json
from . import action


@action(id="generate")
class Generate:
    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        provider = context.resolve_provider("summarization")
        generate = getattr(provider, "generate", None)
        if not callable(generate):
            raise ValueError("generate action requires provider.generate(system, user, ...)")

        system = str(params.get("system") or "")
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
