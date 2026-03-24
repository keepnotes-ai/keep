from __future__ import annotations

"""Find items with high inbound edge counts (supernodes) that need review.

Queries the edge table for items with ``new_refs > 0`` since their
last ``_supernode_reviewed`` timestamp.  Guards against overwriting
real content: only stubs (``_source=link`` or ``_source=auto-vivify``)
or previously-reviewed items are eligible.

Results are scored by ``fan_in × (1 + new_refs)`` and filtered to
items matching a ``.prompt/supernode/*`` scope (most-specific wins).
"""

from typing import Any

from . import action

# Sources that indicate a stub (auto-created, not authored content)
_STUB_SOURCES = frozenset({"link", "auto-vivify"})


@action(id="find_supernodes")
class FindSupernodes:
    """Discover supernodes eligible for review."""

    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        min_fan_in = max(int(params.get("min_fan_in", 5)), 1)
        limit = max(int(params.get("limit", 10)), 1)

        # Query document store for supernode candidates
        ds = context.get_document_store()
        if ds is None:
            return {"results": [], "count": 0, "error": "no document store"}
        collection = context.get_collection()
        candidates = ds.find_supernode_candidates(
            collection, min_fan_in=min_fan_in, limit=limit * 3,
        )

        if not candidates:
            return {"results": [], "count": 0}

        # Filter: only stubs or previously-reviewed items
        eligible = []
        for c in candidates:
            source = c.get("source")
            last_reviewed = c.get("last_reviewed")
            if source in _STUB_SOURCES or last_reviewed:
                eligible.append(c)

        if not eligible:
            return {"results": [], "count": 0}

        # Filter by prompt scope: only items matching a .prompt/supernode/* doc
        resolve_prompt = getattr(context, "resolve_prompt", None)
        if resolve_prompt is not None:
            scoped = []
            for c in eligible:
                prompt = resolve_prompt("supernode", {}, item_id=c["id"])
                if prompt is not None:
                    scoped.append(c)
            eligible = scoped

        results = [
            {
                "id": c["id"],
                "fan_in": c["fan_in"],
                "new_refs": c["new_refs"],
                "score": c["score"],
            }
            for c in eligible[:limit]
        ]

        return {"results": results, "count": len(results)}
