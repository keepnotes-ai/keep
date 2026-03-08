"""Pluggable storage backend factory.

Creates storage backends (DocumentStore, VectorStore, PendingQueue) based on
configuration. External backends register via the ``keep.backends`` entry
point group.

External backend packages provide a factory function::

    def create_stores(config: StoreConfig) -> StoreBundle:
        ...

and register it in their pyproject.toml::

    [project.entry-points."keep.backends"]
    my-backend = "my_package.backend:create_stores"
"""

from typing import NamedTuple, Optional

from .config import StoreConfig
from .protocol import DocumentStoreProtocol, PendingQueueProtocol, VectorStoreProtocol


class StoreBundle(NamedTuple):
    """Collection of storage backends returned by the factory."""
    doc_store: DocumentStoreProtocol
    vector_store: VectorStoreProtocol
    pending_queue: PendingQueueProtocol
    is_local: bool  # True for filesystem-backed stores


class NullPendingQueue:
    """No-op pending queue for backends that handle summarization server-side."""

    def enqueue(
        self, id: str, collection: str, content: str,
        *, task_type: str = "summarize", metadata: dict | None = None,
    ) -> None:
        pass

    def dequeue(self, limit: int = 10) -> list:
        return []

    def complete(self, id: str, collection: str, task_type: str = "summarize") -> None:
        pass

    def fail(
        self, id: str, collection: str, task_type: str = "summarize",
        error: str | None = None,
    ) -> None:
        pass

    def abandon(
        self, id: str, collection: str, task_type: str = "summarize",
        error: str | None = None,
    ) -> None:
        pass

    def count(self) -> int:
        return 0

    def stats(self) -> dict:
        return {"pending": 0, "processing": 0, "delegated": 0, "failed": 0,
                "total": 0, "collections": 0, "max_attempts": 0, "oldest": None}

    def stats_by_type(self) -> dict[str, int]:
        return {}

    def list_failed(self) -> list[dict]:
        return []

    def retry_failed(self) -> int:
        return 0

    def clear(self) -> int:
        return 0

    def get_status(self, id: str) -> dict | None:
        return None

    def peek(
        self, id: str, collection: str, task_type: str = "summarize",
    ):
        return None

    def mark_delegated(
        self, id: str, collection: str, task_type: str, remote_task_id: str,
    ) -> None:
        pass

    def list_delegated(self) -> list:
        return []

    def count_delegated(self) -> int:
        return 0

    def close(self) -> None:
        pass


def create_stores(config: StoreConfig) -> StoreBundle:
    """Create storage backends from configuration.

    For ``backend = "local"`` (default), creates local storage backends.
    For other values, loads the backend via the ``keep.backends`` entry
    point group.
    """
    if config.backend == "local":
        return _create_local_stores(config)
    return _load_backend(config.backend, config)


def _create_local_stores(config: StoreConfig) -> StoreBundle:
    """Create the default local storage backends."""
    from .document_store import DocumentStore
    from .pending_summaries import PendingSummaryQueue
    from .store import ChromaStore

    store_path = config.path

    pending_queue = None
    doc_store = None
    try:
        pending_queue = PendingSummaryQueue(store_path / "pending_summaries.db")
        doc_store = DocumentStore(store_path / "documents.db")

        embedding_dim: Optional[int] = None
        if config.embedding_identity:
            embedding_dim = config.embedding_identity.dimension
        vector_store = ChromaStore(store_path, embedding_dimension=embedding_dim)
    except Exception:
        if pending_queue:
            pending_queue.close()
        if doc_store:
            doc_store.close()
        raise

    return StoreBundle(
        doc_store=doc_store,
        vector_store=vector_store,
        pending_queue=pending_queue,
        is_local=True,
    )


def _load_backend(name: str, config: StoreConfig) -> StoreBundle:
    """Load a backend by entry point name."""
    from importlib.metadata import entry_points

    eps = entry_points(group="keep.backends")
    for ep in eps:
        if ep.name == name:
            factory = ep.load()
            bundle = factory(config)
            if not isinstance(bundle, StoreBundle):
                raise TypeError(
                    f"Backend {name!r} factory returned {type(bundle).__name__}, "
                    f"expected StoreBundle"
                )
            return bundle

    available = [ep.name for ep in eps]
    if available:
        raise ValueError(
            f"Unknown backend: {name!r}. Available: {available}"
        )
    raise ValueError(
        f"Unknown backend: {name!r}. No backends registered. "
        f"Install a backend package (e.g. pip install keepnotes-postgres)."
    )
