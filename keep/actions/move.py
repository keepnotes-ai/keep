from __future__ import annotations

"""Move versions from one item to another."""

from typing import Any

from . import action


@action(id="move")
class Move:
    """Move versions from a source item into a named target.

    Wraps Keeper.move() as a flow action so it can be composed
    with find/tag in state doc pipelines.
    """

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        name = params.get("name") or params.get("target")
        if not name:
            raise ValueError("move requires name (target ID)")
        source_id = str(params.get("source") or params.get("source_id") or "now")
        only_current = bool(params.get("only_current", False))
        tags = params.get("tags") if isinstance(params.get("tags"), dict) else None

        item = context.move(
            str(name),
            source_id=source_id,
            tags=tags,
            only_current=only_current,
        )
        return {
            "id": getattr(item, "id", str(name)),
            "summary": getattr(item, "summary", ""),
        }
