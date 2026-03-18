from __future__ import annotations

from typing import Any

from . import action, item_to_result


def _part_to_result(base_id: str, part) -> dict[str, Any]:
    """Convert a PartInfo to a find-result dict with a part ID."""
    tags = dict(getattr(part, "tags", None) or {})
    tags["_base_id"] = base_id
    tags["_part_num"] = str(part.part_num)
    return {
        "id": f"{base_id}@p{part.part_num}",
        "summary": getattr(part, "summary", "") or "",
        "tags": tags,
        "score": None,
    }


@action(id="find")
class Find:
    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        query = params.get("query")
        similar_to = params.get("similar_to")
        tags = params.get("tags") if isinstance(params.get("tags"), dict) else None
        prefix = params.get("prefix")
        since = params.get("since")
        until = params.get("until")
        offset = int(params.get("offset") or 0)
        include_hidden = bool(params.get("include_hidden", False))
        order_by = str(params.get("order_by") or "updated")
        limit = int(params.get("limit", 10))
        limit = max(limit, 1)

        # Bias: {id: weight} — score multiplier. 0=exclude, <1=demote, 1=neutral, >1=boost
        bias = params.get("bias")
        if isinstance(bias, dict):
            bias = {str(k): float(v) for k, v in bias.items()
                    if isinstance(v, (int, float))}
        else:
            bias = None
        n_excluded = sum(1 for v in (bias or {}).values() if v == 0)

        scope = params.get("scope")
        if isinstance(scope, str) and scope:
            scope = scope
        else:
            scope = None

        has_selector = any([
            bool(query),
            bool(similar_to),
            bool(tags),
            bool(prefix),
            bool(since),
        ])
        if not has_selector:
            raise ValueError("find requires one of query, similar_to, tags, prefix, or since")
        if query and similar_to:
            raise ValueError("find.query and find.similar_to are mutually exclusive")

        fetch_limit = limit + n_excluded + offset

        # Parts prefix query: prefix ending with @p targets the parts
        # table (document_parts), not the documents table.
        _parts_prefix = False
        if prefix and str(prefix).endswith("@p"):
            list_parts = getattr(context, "list_parts", None)
            if callable(list_parts):
                base_id = str(prefix)[:-2]  # strip @p suffix
                all_parts = list_parts(base_id)
                results = [_part_to_result(base_id, p) for p in all_parts]
                if offset > 0:
                    results = results[offset:]
                results = results[:limit]
                return {"results": results, "count": len(results)}

        if query or similar_to:
            rows = context.find(
                str(query) if query is not None else None,
                tags=tags,
                similar_to=str(similar_to) if similar_to is not None else None,
                limit=fetch_limit,
                since=str(since) if since is not None else None,
                until=str(until) if until is not None else None,
                include_hidden=include_hidden,
                scope=scope,
            )
        else:
            rows = context.list_items(
                prefix=str(prefix) if prefix is not None else None,
                tags=tags,
                since=str(since) if since is not None else None,
                until=str(until) if until is not None else None,
                order_by=order_by,
                include_hidden=include_hidden,
                limit=fetch_limit,
            )

        # Apply bias exclusions (weight=0) before converting to result dicts
        if bias:
            rows = [r for r in rows
                    if bias.get(getattr(r, "id", None), 1) != 0]

        # Apply offset
        if offset > 0:
            rows = rows[offset:]

        rows = rows[:limit]
        results = [item_to_result(row) for row in rows]

        # Apply bias score multipliers on result dicts (Items are frozen)
        if bias:
            for r in results:
                rid = r.get("id")
                w = bias.get(rid) if rid else None
                if w is not None and w != 0 and isinstance(r.get("score"), (int, float)):
                    r["score"] = r["score"] * w
            results.sort(key=lambda r: -(r.get("score") or 0))

        return {
            "results": results,
            "count": len(results),
        }
