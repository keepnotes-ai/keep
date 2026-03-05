"""
Continuation runtime environment adapters.

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
        deep: bool = False,
    ) -> list[Any]: ...

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

    def get_profile_document(self, profile_id: str) -> Any | None: ...

    def upsert_item(
        self,
        *,
        target: str,
        content: str,
        tags: dict[str, Any] | None = None,
        summary: str | None = None,
    ) -> None: ...

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
        deep: bool = False,
    ) -> list[Any]:
        return self._keeper.find(
            query=query,
            tags=tags,
            similar_to=similar_to,
            limit=limit,
            deep=deep,
        )

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

    def get_profile_document(self, profile_id: str) -> Any | None:
        return self._keeper.get(profile_id)

    def upsert_item(
        self,
        *,
        target: str,
        content: str,
        tags: dict[str, Any] | None = None,
        summary: str | None = None,
    ) -> None:
        self._keeper.put(
            content=content,
            id=target,
            tags=tags,
            summary=summary,
        )

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
