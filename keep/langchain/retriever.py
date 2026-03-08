"""KeepNotesRetriever — LangChain BaseRetriever backed by Keep.

Returns Keep notes as LangChain Documents, with optional ``now``
context prepended to results.

Usage::

    from keep.langchain import KeepNotesRetriever

    retriever = KeepNotesRetriever(user_id="alice", include_now=True)

    # Plugs into any RAG chain
    docs = retriever.invoke("What do I know about coffee?")
"""

from __future__ import annotations

from typing import Optional

try:
    from langchain_core.callbacks import CallbackManagerForRetrieverRun
    from langchain_core.documents import Document
    from langchain_core.retrievers import BaseRetriever
except ImportError as e:
    raise ImportError(
        "langchain-core is required for KeepNotesRetriever. "
        "Install with: pip install keep-skill[langchain]"
    ) from e

from keep.api import Keeper


class KeepNotesRetriever(BaseRetriever):
    """LangChain retriever that searches Keep reflective memory.

    Args:
        keeper: An existing Keeper instance. If None, one is created
            from ``store`` path.
        store: Path to the Keep store directory.
        user_id: Optional user scope for multi-user isolation.
        limit: Maximum search results to return.
        include_now: If True, prepend the current ``now`` context as
            the first document in results.
    """

    model_config = {"arbitrary_types_allowed": True}

    keeper: Keeper
    user_id: Optional[str] = None
    limit: int = 5
    include_now: bool = False

    def __init__(
        self,
        keeper: Keeper | None = None,
        store: str | None = None,
        user_id: str | None = None,
        limit: int = 5,
        include_now: bool = False,
        **kwargs,
    ):
        if keeper is None:
            keeper = Keeper(store_path=store)
            keeper._get_embedding_provider()
        super().__init__(
            keeper=keeper,
            user_id=user_id,
            limit=limit,
            include_now=include_now,
            **kwargs,
        )

    def _get_relevant_documents(
        self,
        query: str,
        *,
        run_manager: CallbackManagerForRetrieverRun,
    ) -> list[Document]:
        docs: list[Document] = []

        # Optionally prepend now context
        if self.include_now:
            now_item = self.keeper.get_now(scope=self.user_id)
            if now_item.summary:
                docs.append(Document(
                    page_content=now_item.summary,
                    metadata={"source": now_item.id, "type": "now"},
                ))

        # Search
        tags = {"user": self.user_id} if self.user_id else None
        items = self.keeper.find(query, tags=tags, limit=self.limit)

        for item in items:
            metadata = {"source": item.id}
            if item.score is not None:
                metadata["score"] = item.score
            # Include non-system tags
            for k, v in item.tags.items():
                if not k.startswith("_"):
                    metadata[k] = v
            docs.append(Document(
                page_content=item.summary,
                metadata=metadata,
            ))

        return docs
