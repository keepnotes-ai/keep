"""Local task workflow dispatch.

Actions implement a single ``run(params, context)`` method.  This module
provides the ``TaskRequest`` / ``TaskRunResult`` types, a lightweight
adapter that presents a Keeper as an ``ActionContext``, a generic
mutation applier, and the ``run_local_task`` dispatcher that ties them
together.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .api import Keeper

logger = logging.getLogger(__name__)


@dataclass
class TaskRequest:
    """Minimal task request shape shared by queue and flow paths."""

    task_type: str
    id: str
    collection: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskRunResult:
    """Outcome of a local task workflow."""

    status: str  # "applied" | "skipped"
    details: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Adapter: Keeper → ActionContext
# ---------------------------------------------------------------------------

class _KeeperActionContext:
    """Present a Keeper as an ActionContext for background task execution."""

    def __init__(
        self,
        keeper: "Keeper",
        *,
        collection: str,
        item_id: str | None = None,
        item_content: str | None = None,
    ) -> None:
        self._keeper = keeper
        self._collection = collection
        self.item_id = item_id
        self.item_content = item_content

    def get(self, id: str) -> Any:
        return self._keeper.get(id)

    def find(
        self,
        query: str | None = None,
        *,
        tags: dict[str, Any] | None = None,
        similar_to: str | None = None,
        limit: int = 10,
        since: str | None = None,
        until: str | None = None,
        include_hidden: bool = False,
        scope: str | None = None,
    ) -> list[Any]:
        return self._keeper.find(
            query, tags=tags, similar_to=similar_to, limit=limit,
            since=since, until=until, include_hidden=include_hidden,
            scope=scope,
        )

    def list_items(
        self,
        *,
        prefix: str | None = None,
        tags: dict[str, Any] | None = None,
        since: str | None = None,
        until: str | None = None,
        order_by: str = "updated",
        include_hidden: bool = False,
        limit: int = 10,
    ) -> list[Any]:
        return self._keeper.list_items(
            prefix=prefix, tags=tags, since=since, until=until,
            order_by=order_by, include_hidden=include_hidden, limit=limit,
        )

    def list_parts(self, id: str) -> list[Any]:
        return self._keeper.list_parts(id)

    def get_document(self, id: str) -> Any:
        return self._keeper._document_store.get(self._collection, id)

    def find_by_name(self, stem: str, *, vault: str | None = None) -> Any:
        results = self._keeper._document_store.find_by_name(
            self._collection, stem, id_prefix=vault, limit=1,
        )
        if results:
            from .types import Item
            rec = results[0]
            return Item(id=rec.id, summary=rec.summary, tags=rec.tags)
        return None

    def find_referencing(self, target_id: str, tag_key: str = "references", limit: int = 50) -> list[Any]:
        """Find items that reference *target_id* via edge-tag *tag_key*."""
        doc_coll = self._keeper._resolve_doc_collection()
        edges = self._keeper._document_store.get_inverse_edges(doc_coll, target_id)
        # edges: list of (inverse, source_id, created) — we want the sources
        source_ids = list(dict.fromkeys(src for _, src, _ in edges))[:limit]
        if not source_ids:
            return []
        docs = self._keeper._document_store.get_many(doc_coll, source_ids)
        from .types import Item
        return [
            Item(id=rec.id, summary=rec.summary, tags=rec.tags)
            for rec in docs.values()
        ]

    def find_by_content_hash(
        self, content_hash: str, *, content_hash_full: str = "",
        exclude_id: str = "", limit: int = 10,
    ) -> list[Any]:
        """Find documents with matching content hash."""
        results = self._keeper._document_store.find_by_content_hash(
            self._collection, content_hash,
            content_hash_full=content_hash_full,
            exclude_id=exclude_id, limit=limit,
        )
        if isinstance(results, list):
            return results
        return [results] if results else []

    def resolve_meta(self, id: str, limit_per_doc: int = 3) -> dict[str, list[Any]]:
        return self._keeper.resolve_meta(item_id=id, limit_per_doc=limit_per_doc)

    def resolve_provider(self, kind: str, name: str | None = None) -> Any:
        _PROVIDER_MAP = {
            "summarization": self._keeper._get_summarization_provider,
            "content_extractor": self._keeper._get_content_extractor,
            "analyzer": self._keeper._get_analyzer,
            "media": self._keeper._get_media_describer,
        }
        method = _PROVIDER_MAP.get(kind)
        if method is None:
            raise ValueError(f"unknown provider kind: {kind!r}")
        return method()

    def resolve_prompt(self, prefix: str, doc_tags: dict[str, Any] | None = None) -> str | None:
        try:
            return self._keeper._resolve_prompt_doc(prefix, doc_tags or {})
        except Exception:
            return None

    def traverse(self, source_ids: list[str], *, limit: int = 5) -> dict[str, list[Any]]:
        traverse = getattr(self._keeper, "traverse_related", None)
        if callable(traverse):
            return traverse(source_ids, limit_per_source=limit)
        return {}


# ---------------------------------------------------------------------------
# Generic mutation applier
# ---------------------------------------------------------------------------

def _resolve_ref(value: Any, output: dict[str, Any]) -> Any:
    """Resolve ``$output.X`` references to concrete values."""
    if isinstance(value, str) and value.startswith("$output."):
        key = value[len("$output."):]
        return output.get(key, value)
    return value


def _apply_mutations(
    keeper: "Keeper",
    collection: str,
    output: dict[str, Any],
) -> None:
    """Process mutations returned by an action's ``run()`` method."""
    from .types import casefold_tags_for_index

    mutations = output.get("mutations")
    if not isinstance(mutations, list):
        return

    for mut in mutations:
        if not isinstance(mut, dict):
            continue
        op = mut.get("op", "")

        if op == "set_summary":
            target = str(mut["target"])
            summary = str(_resolve_ref(mut["summary"], output))
            # Strip LLM preambles ("Here is a summary...") as a safety net
            from keep.providers.base import strip_summary_preamble
            summary = strip_summary_preamble(summary)
            keeper._document_store.update_summary(collection, target, summary)
            if mut.get("embed"):
                chroma_coll = keeper._resolve_chroma_collection()
                embedding = keeper._get_embedding_provider().embed(summary)
                existing = keeper._document_store.get(collection, target)
                tags = {}
                if existing:
                    tags = casefold_tags_for_index(existing.tags or {})
                keeper._store.upsert(
                    collection=chroma_coll, id=target,
                    embedding=embedding, summary=summary, tags=tags,
                )
            else:
                keeper._store.update_summary(collection, target, summary)

        elif op == "set_content":
            target = str(mut["target"])
            content = str(_resolve_ref(mut["content"], output))
            content_hash = str(mut.get("content_hash", ""))
            content_hash_full = str(mut.get("content_hash_full", ""))
            summary = str(_resolve_ref(mut.get("summary", ""), output))
            keeper._document_store.update_summary(collection, target, summary)
            if content_hash:
                keeper._document_store.update_content_hash(
                    collection, target,
                    content_hash=content_hash,
                    content_hash_full=content_hash_full,
                )
            chroma_coll = keeper._resolve_chroma_collection()
            embedding = keeper._get_embedding_provider().embed(summary)
            existing = keeper._document_store.get(collection, target)
            tags = {}
            if existing:
                tags = casefold_tags_for_index(existing.tags or {})
            keeper._store.upsert(
                collection=chroma_coll, id=target,
                embedding=embedding, summary=summary, tags=tags,
            )

        elif op == "put_item":
            from .types import is_part_id, parse_part_id
            item_id = mut.get("id", "")
            if is_part_id(item_id):
                # Parts go through part-specific storage, not _put_direct
                base_id, part_num = parse_part_id(item_id)
                from .document_store import PartInfo
                from .types import utc_now
                part = PartInfo(
                    part_num=part_num,
                    summary=str(mut.get("summary", "")),
                    content=str(mut.get("content", "")),
                    tags=dict(mut.get("tags") or {}),
                    created_at=utc_now(),
                )
                keeper._document_store.upsert_single_part(collection, base_id, part)
                chroma_coll = keeper._resolve_chroma_collection()
                embedding = keeper._get_embedding_provider().embed(part.summary)
                keeper._store.upsert_part(
                    chroma_coll, base_id, part_num,
                    embedding, part.summary, casefold_tags_for_index(part.tags),
                )
            else:
                keeper._put_direct(
                    content=str(mut.get("content", "")),
                    id=item_id,
                    summary=str(mut.get("summary", "")),
                    tags=mut.get("tags"),
                    queue_background_tasks=mut.get("queue_background_tasks", False),
                )

        elif op == "set_tags":
            target = str(mut["target"])
            tags = _resolve_ref(mut["tags"], output)
            if isinstance(tags, dict):
                # Fetch existing tags for edge diff
                existing_doc = keeper._document_store.get(collection, target)
                existing_tags = existing_doc.tags if existing_doc else {}

                # Merge into existing tags (don't replace — actions only
                # return the tags they want to set, not the full tag map)
                merged = dict(existing_tags)
                merged.update(tags)

                keeper._document_store.update_tags(collection, target, merged)
                chroma_coll = keeper._resolve_chroma_collection()
                keeper._store.update_tags(
                    chroma_coll, target, casefold_tags_for_index(merged),
                )

                # Sync edge table for any edge-tag changes
                keeper._process_edge_tags(target, merged, existing_tags, collection)

        elif op == "delete_prefix":
            prefix = str(mut["prefix"])
            chroma_coll = keeper._resolve_chroma_collection()
            keeper._store.delete_parts(chroma_coll, prefix.rstrip("@p"))
            keeper._document_store.delete_parts(collection, prefix.rstrip("@p"))

        else:
            logger.warning("Unknown mutation op: %r", op)


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def run_local_task(keeper: "Keeper", req: TaskRequest) -> TaskRunResult:
    """Run a background task by calling the action's ``run()`` method."""
    from .actions import get_action
    from .perf_stats import perf

    task_type = str(req.task_type or "").strip()
    action = get_action(task_type)

    ctx = _KeeperActionContext(
        keeper,
        collection=req.collection,
        item_id=req.id,
        item_content=req.content or None,
    )

    params: dict[str, Any] = {"item_id": req.id}
    params.update(req.metadata)

    with perf.timer("action", task_type, context_id=req.id):
        output = action.run(params, ctx)

    if not isinstance(output, dict):
        return TaskRunResult(status="skipped", details={"reason": "no_output"})

    if output.get("skipped"):
        return TaskRunResult(status="skipped", details=output)

    mutations = output.get("mutations")
    if not mutations:
        return TaskRunResult(status="skipped", details=output)

    _apply_mutations(keeper, req.collection, output)
    return TaskRunResult(status="applied", details=output)
