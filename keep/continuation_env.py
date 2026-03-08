"""Continuation runtime environment adapters.

This module provides a stable dependency surface for continuation runtime
logic so the runtime can be shared across local and hosted implementations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Protocol

from .processors import ProcessorResult

if TYPE_CHECKING:
    from .api import Keeper


class ContinuationRuntimeEnv(Protocol):
    """Environment contract consumed by continuation runtime and executors."""

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

    def emit_escalation_note(
        self,
        template_ref: str,
        *,
        context: dict[str, Any] | None = None,
    ) -> str | None: ...

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


class LocalContinuationEnvironment:
    """Adapter from local Keeper APIs/private internals to runtime env contract."""

    def __init__(self, keeper: "Keeper") -> None:
        self._keeper = keeper

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

    def resolve_meta(self, id: str, *, limit_per_doc: int = 3) -> dict[str, list[Any]]:
        return self._keeper.resolve_meta(id, limit_per_doc=limit_per_doc)

    def traverse_related(
        self,
        source_ids: list[str],
        *,
        limit_per_source: int = 5,
    ) -> dict[str, list[Any]]:
        limit = max(int(limit_per_source), 1)
        source_items: list[Any] = []
        for source_id in source_ids:
            sid = str(source_id).strip()
            if not sid:
                continue
            item = self._keeper.get(sid)
            if item is not None:
                source_items.append(item)
        if not source_items:
            return {}

        source_set = {str(item.id) for item in source_items}
        doc_coll = self._keeper._resolve_doc_collection()
        chroma_coll = self._keeper._resolve_chroma_collection()
        try:
            embedder = self._keeper._get_embedding_provider()
        except Exception:
            embedder = None

        groups: dict[str, list[Any]] = {}
        for item in source_items:
            source_id = str(item.id)
            query = str(item.summary or source_id).strip() or source_id
            embedding = None
            if embedder is not None:
                try:
                    embedding = embedder.embed(query)
                except Exception:
                    embedding = None

            candidates: list[Any] = []
            if embedding is not None:
                try:
                    edge_groups = self._keeper._deep_edge_follow(
                        [item],
                        chroma_coll,
                        doc_coll,
                        query=query,
                        embedding=embedding,
                        top_k=1,
                        exclude_ids=source_set,
                    )
                    group = edge_groups.get(source_id) if isinstance(edge_groups, dict) else None
                    if isinstance(group, list):
                        candidates = group
                except Exception:
                    candidates = []

            if not candidates:
                try:
                    tag_groups = self._keeper._deep_tag_follow(
                        [item],
                        chroma_coll,
                        doc_coll,
                        embedding=embedding,
                        top_k=1,
                        max_per_group=limit,
                    )
                    group = tag_groups.get(source_id) if isinstance(tag_groups, dict) else None
                    if isinstance(group, list):
                        candidates = group
                except Exception:
                    candidates = []

            if not candidates:
                try:
                    candidates = self._keeper.find(similar_to=source_id, limit=limit + len(source_set))
                except Exception:
                    candidates = []

            deduped: list[Any] = []
            seen: set[str] = set()
            for cand in candidates:
                cand_id = str(getattr(cand, "id", "")).strip()
                if not cand_id or cand_id in source_set or cand_id in seen:
                    continue
                seen.add(cand_id)
                deduped.append(cand)
                if len(deduped) >= limit:
                    break
            groups[source_id] = deduped

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

    def emit_escalation_note(
        self,
        template_ref: str,
        *,
        context: dict[str, Any] | None = None,
    ) -> str | None:
        return self._keeper.emit_escalation_note(
            template_ref,
            context=context,
        )

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
