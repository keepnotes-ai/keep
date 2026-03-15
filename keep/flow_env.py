"""Flow runtime environment adapters.

This module provides a stable dependency surface for flow runtime
logic so the runtime can be shared across local and hosted implementations.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional, Protocol

from .actions import coerce_item_id
from .processors import ProcessorResult

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .api import Keeper


class FlowRuntimeEnv(Protocol):
    """Environment contract consumed by flow runtime and executors."""

    def get(self, id: str) -> Any | None: ...

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
        deep: bool = False,
    ) -> list[Any]: ...

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
    ) -> list[Any]: ...

    def resolve_meta(self, id: str, *, limit_per_doc: int = 3) -> dict[str, list[Any]]: ...
    def traverse_related(
        self,
        source_ids: list[str],
        *,
        limit_per_source: int = 5,
    ) -> dict[str, list[Any]]: ...

    def get_context(
        self,
        id: str,
        *,
        include_similar: bool = True,
        include_meta: bool = True,
        include_parts: bool = True,
        include_versions: bool = True,
    ) -> Any | None: ...

    def resolve_doc_collection(self) -> str: ...

    def get_document(self, id: str, *, collection: Optional[str] = None) -> Any | None: ...

    def upsert_item(
        self,
        *,
        target: str,
        content: str,
        tags: dict[str, Any] | None = None,
        summary: str | None = None,
    ) -> None: ...

    def put_item(
        self,
        *,
        content: str | None = None,
        uri: str | None = None,
        id: str | None = None,
        summary: str | None = None,
        tags: dict[str, Any] | None = None,
        created_at: str | None = None,
        force: bool = False,
        queue_background_tasks: bool = True,
        capture_write_context: bool = False,
    ) -> Any: ...

    def enqueue_task(
        self,
        *,
        task_type: str,
        item_id: str,
        collection: str,
        content: str,
        metadata: dict[str, Any] | None = None,
        tags: dict[str, Any] | None = None,
    ) -> None: ...

    def consume_write_context(self, item_id: str) -> dict[str, Any] | None: ...

    def set_tags(self, target: str, tags: dict[str, Any]) -> None: ...

    def set_summary(self, target: str, summary: str) -> None: ...

    def get_planner_priors(
        self,
        *,
        scope_key: str | None,
        candidates: list[str] | None = None,
    ) -> dict[str, Any]: ...

    def resolve_prompt(self, prefix: str, doc_tags: dict[str, Any]) -> str | None: ...

    def get_default_summarization_provider(self) -> Any: ...
    def get_default_document_provider(self) -> Any: ...
    def get_default_tagging_provider(self) -> Any: ...
    def get_default_analyzer_provider(self) -> Any: ...
    def get_default_content_extractor_provider(self) -> Any: ...

    def run_local_task_workflow(
        self,
        *,
        task_type: str,
        item_id: str,
        collection: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...


class LocalFlowEnvironment:
    """Adapter from local Keeper APIs/private internals to runtime env contract."""

    def __init__(self, keeper: "Keeper") -> None:
        self._keeper = keeper
        self._query_embedding: Any = None  # set by caller for deep-find flows

    def get(self, id: str) -> Any | None:
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
        deep: bool = False,
    ) -> list[Any]:
        return self._keeper.find(
            query=query,
            tags=tags,
            similar_to=similar_to,
            limit=limit,
            since=since,
            until=until,
            include_hidden=include_hidden,
            deep=deep,
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
            prefix=prefix,
            tags=tags,
            since=since,
            until=until,
            order_by=order_by,
            include_hidden=include_hidden,
            limit=limit,
        )

    def list_versions(self, id: str, *, limit: int = 3) -> list[Any]:
        return self._keeper.list_versions(id, limit=limit)

    def resolve_edges(self, id: str, *, limit: int = 5) -> dict[str, Any]:
        """Resolve forward and inverse edges for an item."""
        from .types import EdgeRef

        item = self._keeper.get(id)
        if item is None:
            return {"edges": {}, "count": 0}
        edge_refs = self._keeper._resolve_edge_refs(item, id)
        result: dict[str, list[dict]] = {}
        total = 0
        for key, refs in edge_refs.items():
            entries = []
            for ref in refs[:limit]:
                entries.append({
                    "id": ref.source_id,
                    "summary": ref.summary or "",
                    "predicate": ref.predicate or "",
                    "date": ref.date or "",
                })
                total += 1
            if entries:
                result[key] = entries
        return {"edges": result, "count": total}

    def resolve_meta(self, id: str, *, limit_per_doc: int = 3) -> dict[str, list[Any]]:
        return self._keeper.resolve_meta(id, limit_per_doc=limit_per_doc)

    def traverse_related(
        self,
        source_ids: list[str],
        *,
        limit_per_source: int = 5,
    ) -> dict[str, list[Any]]:
        from .utils import _record_to_item

        limit = max(int(limit_per_source), 1)
        doc_coll = self._keeper._resolve_doc_collection()
        ds = self._keeper._document_store

        # Batch-fetch source items (no touch — internal traversal)
        clean_ids = [s for s in (str(sid).strip() for sid in source_ids) if s]
        if not clean_ids:
            return {}
        source_records = ds.get_many(doc_coll, clean_ids)
        source_items = [
            _record_to_item(source_records[sid])
            for sid in clean_ids if sid in source_records
        ]
        if not source_items:
            return {}

        source_set = {str(item.id) for item in source_items}

        groups: dict[str, list[Any]] = {}
        tagfollow_items: list[Any] = []  # items needing Tier 2 batch

        # Tier 1: Direct edge follow (forward + inverse)
        # Collect all related IDs first, then batch-fetch.
        per_source_related: dict[str, list[str]] = {}
        all_related_ids: list[str] = []
        for item in source_items:
            source_id = str(item.id)
            try:
                fwd = ds.get_forward_edges(doc_coll, source_id)
                inv = ds.get_inverse_edges(doc_coll, source_id)
                related_ids: list[str] = []
                seen_ids: set[str] = set()
                for _pred, target_id, _created in fwd:
                    if target_id not in seen_ids and target_id not in source_set:
                        seen_ids.add(target_id)
                        related_ids.append(target_id)
                for _inv, edge_source_id, _created in inv:
                    if edge_source_id not in seen_ids and edge_source_id not in source_set:
                        seen_ids.add(edge_source_id)
                        related_ids.append(edge_source_id)
                related_ids = related_ids[:limit * 3]
                per_source_related[source_id] = related_ids
                all_related_ids.extend(related_ids)
            except Exception:
                logger.debug("Edge follow failed for %s", source_id, exc_info=True)
                per_source_related[source_id] = []

        # Batch-fetch all edge targets at once (no touch)
        related_records = ds.get_many(doc_coll, all_related_ids) if all_related_ids else {}

        for item in source_items:
            source_id = str(item.id)
            related_ids = per_source_related.get(source_id, [])
            candidates = [
                _record_to_item(related_records[rid])
                for rid in related_ids if rid in related_records
            ]

            if candidates:
                deduped: list[Any] = []
                seen: set[str] = set()
                for cand in candidates:
                    cand_id = coerce_item_id(cand)
                    if not cand_id or cand_id in source_set or cand_id in seen:
                        continue
                    seen.add(cand_id)
                    deduped.append(cand)
                    if len(deduped) >= limit:
                        break
                groups[source_id] = deduped
            else:
                tagfollow_items.append(item)

        # Tier 2: Batch tag-facet grouping for items without edges.
        # Processing as a batch enables discriminative tag filtering
        # (dropping tags shared by all items) and IDF weighting.
        if tagfollow_items:
            chroma_coll = self._keeper._resolve_chroma_collection()
            try:
                tag_groups = self._keeper._deep_tag_follow(
                    tagfollow_items,
                    chroma_coll,
                    doc_coll,
                    embedding=self._query_embedding,
                    max_per_group=limit,
                )
                if isinstance(tag_groups, dict):
                    for source_id, group in tag_groups.items():
                        if isinstance(group, list) and group:
                            groups[source_id] = group
            except Exception:
                logger.debug("Tag-follow failed for traverse", exc_info=True)

        # Ensure all sources have an entry
        for item in source_items:
            source_id = str(item.id)
            if source_id not in groups:
                groups[source_id] = []

        return groups

    def get_context(
        self,
        id: str,
        *,
        include_similar: bool = True,
        include_meta: bool = True,
        include_parts: bool = True,
        include_versions: bool = True,
    ) -> Any | None:
        return self._keeper.get_context(
            id,
            include_similar=include_similar,
            include_meta=include_meta,
            include_parts=include_parts,
            include_versions=include_versions,
        )

    def resolve_doc_collection(self) -> str:
        return self._keeper._resolve_doc_collection()

    def get_document(self, id: str, *, collection: Optional[str] = None) -> Any | None:
        doc_coll = collection or self.resolve_doc_collection()
        return self._keeper._document_store.get(doc_coll, id)

    def upsert_item(
        self,
        *,
        target: str,
        content: str,
        tags: dict[str, Any] | None = None,
        summary: str | None = None,
    ) -> None:
        self._keeper._put_direct(
            content=content,
            id=target,
            tags=tags,
            summary=summary,
        )

    def put_item(
        self,
        *,
        content: str | None = None,
        uri: str | None = None,
        id: str | None = None,
        summary: str | None = None,
        tags: dict[str, Any] | None = None,
        created_at: str | None = None,
        force: bool = False,
        queue_background_tasks: bool = True,
        capture_write_context: bool = False,
    ) -> Any:
        return self._keeper._put_direct(
            content=content,
            uri=uri,
            id=id,
            summary=summary,
            tags=tags,
            created_at=created_at,
            force=force,
            queue_background_tasks=queue_background_tasks,
            capture_write_context=capture_write_context,
        )

    def enqueue_task(
        self,
        *,
        task_type: str,
        item_id: str,
        collection: str,
        content: str,
        metadata: dict[str, Any] | None = None,
        tags: dict[str, Any] | None = None,
    ) -> None:
        self._keeper._enqueue_task_background(
            task_type=task_type,
            id=item_id,
            doc_coll=collection,
            content=content,
            metadata=metadata,
            tags=tags,
        )

    def consume_write_context(self, item_id: str) -> dict[str, Any] | None:
        return self._keeper._consume_write_context(item_id)

    def set_tags(self, target: str, tags: dict[str, Any]) -> None:
        self._keeper.tag(target, tags=tags)

    def set_summary(self, target: str, summary: str) -> None:
        existing = self._keeper.get(target)
        if existing is None:
            raise ValueError(f"Target note not found: {target}")
        if existing.summary == summary:
            return

        doc_coll = self.resolve_doc_collection()
        result = ProcessorResult(task_type="summarize", summary=summary)
        self._keeper.apply_result(
            target,
            doc_coll,
            result,
            existing_tags=dict(existing.tags),
        )

    def get_planner_priors(
        self,
        *,
        scope_key: str | None,
        candidates: list[str] | None = None,
    ) -> dict[str, Any]:
        return self._keeper.get_planner_priors(
            scope_key=scope_key,
            candidates=candidates,
        )

    def resolve_prompt(self, prefix: str, doc_tags: dict[str, Any]) -> str | None:
        """Resolve a prompt doc (e.g. .prompt/summarize/default) matching tags."""
        try:
            return self._keeper._resolve_prompt_doc(prefix, doc_tags)
        except Exception:
            return None

    def get_default_summarization_provider(self) -> Any:
        return self._keeper._get_summarization_provider()

    def get_default_document_provider(self) -> Any:
        return self._keeper._document_provider

    def get_default_tagging_provider(self) -> Any:
        from .providers.base import get_registry

        registry = get_registry()
        return registry.create_tagging("noop")

    def get_default_analyzer_provider(self) -> Any:
        return self._keeper._get_analyzer()

    def get_default_content_extractor_provider(self) -> Any:
        return self._keeper._get_content_extractor()

    def run_local_task_workflow(
        self,
        *,
        task_type: str,
        item_id: str,
        collection: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._keeper._run_local_task_workflow(
            task_type=task_type,
            item_id=item_id,
            collection=collection,
            content=content,
            metadata=metadata,
        )
