from __future__ import annotations

from typing import Any

from . import action, item_to_result


@action(id="find")
class Find:
    def run(self, params: dict[str, Any], context) -> dict[str, Any]:
        query = params.get("query")
        similar_to = params.get("similar_to")
        tags = params.get("tags") if isinstance(params.get("tags"), dict) else None
        prefix = params.get("prefix")
        since = params.get("since")
        until = params.get("until")
        include_hidden = bool(params.get("include_hidden", False))
        order_by = str(params.get("order_by") or "updated")
        limit = int(params.get("limit", 10))
        limit = max(limit, 1)
        exclude = params.get("exclude")
        if isinstance(exclude, str):
            exclude = [exclude]
        exclude_set = set(exclude) if isinstance(exclude, list) else set()

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

        fetch_limit = limit + len(exclude_set) if exclude_set else limit

        if query or similar_to:
            rows = context.find(
                str(query) if query is not None else None,
                tags=tags,
                similar_to=str(similar_to) if similar_to is not None else None,
                limit=fetch_limit,
                since=str(since) if since is not None else None,
                until=str(until) if until is not None else None,
                include_hidden=include_hidden,
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

        if exclude_set:
            rows = [r for r in rows if getattr(r, "id", None) not in exclude_set][:limit]

        results = [item_to_result(row) for row in rows]
        return {
            "results": results,
            "count": len(results),
        }
