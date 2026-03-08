"""Keep - Reflective Memory.

A persistent reflective memory with similarity search, full-text search,
and tag-based retrieval. Remember everything, find by meaning.

Quick Start:
    from keep import Keeper

    kp = Keeper()
    kp.put(uri="file:///path/to/document.md", tags={"project": "myproject"})
    results = kp.find("something similar to this query")

CLI Usage:
    keep find "query text"
    keep put file:///path/to/doc.md -t category=docs

The store is initialized automatically on first use.
"""

# Configure quiet mode early (before any library imports)
import os
if not os.environ.get("KEEP_VERBOSE"):
    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
    os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
    os.environ.setdefault("HF_HUB_DISABLE_SYMLINKS_WARNING", "1")

from .api import Keeper, NOWDOC_ID
from .backend import StoreBundle, NullPendingQueue
from .protocol import (
    DocumentStoreProtocol,
    KeeperProtocol,
    PendingQueueProtocol,
    VectorStoreProtocol,
)
from .processors import ProcessorResult, DELEGATABLE_TASK_TYPES
from .types import Item, filter_non_system_tags, SYSTEM_TAG_PREFIX, INTERNAL_TAGS

try:
    from importlib.metadata import version as _pkg_version
    __version__ = _pkg_version("keep-skill")
except Exception:
    __version__ = "0.0.0"  # fallback for editable installs without metadata
__all__ = [
    "Keeper",
    "Item",
    "NOWDOC_ID",
    "filter_non_system_tags",
    "SYSTEM_TAG_PREFIX",
    "INTERNAL_TAGS",
    # Pluggable backend support
    "StoreBundle",
    "NullPendingQueue",
    "DocumentStoreProtocol",
    "VectorStoreProtocol",
    "PendingQueueProtocol",
    "KeeperProtocol",
    # Processing pipeline
    "ProcessorResult",
    "DELEGATABLE_TASK_TYPES",
]
