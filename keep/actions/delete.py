from __future__ import annotations

"""Delete an item from the store."""

from typing import Any

from . import action


@action(id="delete")
class Delete:
    """Permanently delete an item and its version history."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        item_id = params.get("id") or params.get("item_id")
        if not item_id:
            raise ValueError("delete requires id")
        context.delete(str(item_id))
        return {"deleted": str(item_id)}
