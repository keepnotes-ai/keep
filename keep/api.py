"""Core API for reflective memory.

This is the minimal working implementation focused on:
- put(): fetch/embed → summarize → store
- find(): embed query → search
- get(): retrieve by ID
"""

import json
import logging
import re
import time
import uuid
from dataclasses import replace
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

logger = logging.getLogger(__name__)

from .utils import (
    _parse_date_param,
    _filter_by_date,
    _enrich_updated_date,
    _is_hidden,
    _record_to_item,
    _extract_markdown_frontmatter,
    _parse_meta_doc,
    _get_env_tags,
    _user_tags_changed,
    _merge_tags_additive,
    _split_tag_additions,
    _normalize_remove_keys,
    _apply_tag_mutations,
    _text_content_id,
    _MARKDOWN_EXTENSIONS,
)

import os
import sys

from .config import load_or_create_config, save_config, StoreConfig, EmbeddingIdentity
from .paths import get_config_dir, get_default_store_path
from .protocol import DocumentStoreProtocol, VectorStoreProtocol, PendingQueueProtocol
from .providers import get_registry
from .providers.base import (
    Document,
    DocumentProvider,
    EmbeddingProvider,
    MediaDescriber,
    SummarizationProvider,
)
from .providers.embedding_cache import CachingEmbeddingProvider
from .document_store import PartInfo, VersionInfo
from .types import (
    Item, ItemContext, EdgeRef, TagMap,
    casefold_tags, casefold_tags_for_index, filter_non_system_tags,
    iter_tag_pairs, set_tag_values, tag_values, parse_ref,
    SYSTEM_TAG_PREFIX, local_date, utc_now,
    parse_utc_timestamp, validate_tag_key, validate_id, normalize_id, is_part_id,
    MAX_TAG_VALUE_LENGTH,
)
from .flow_env import LocalFlowEnvironment
from ._background_processing import BackgroundProcessingMixin
from ._provider_lifecycle import ProviderLifecycleMixin
from ._search_augmentation import SearchAugmentationMixin
from ._context_resolution import ContextResolutionMixin


class FindResults(list):
    """List of Items with optional deep-follow groups.

    Subclasses list for backward compatibility.  When ``deep=True`` is
    used, ``deep_groups`` maps each primary item ID to the bridge items
    discovered via its tags.
    """

    def __init__(self, items, deep_groups=None):
        super().__init__(items)
        self.deep_groups: dict[str, list[Item]] = deep_groups or {}


# Default max length for truncated placeholder summaries
TRUNCATE_LENGTH = 500

# Fixed ID for the current working context (singleton)
NOWDOC_ID = "now"


from .system_docs import (
    SYSTEM_DOC_DIR,
    SYSTEM_DOC_IDS,
    _load_frontmatter,
)

from .processors import _content_hash, _content_hash_full


# -------------------------------------------------------------------------
# Decomposition helpers (module-level, used by Keeper.analyze)
# -------------------------------------------------------------------------

class Keeper(ProviderLifecycleMixin, BackgroundProcessingMixin, SearchAugmentationMixin, ContextResolutionMixin):
    """Reflective memory keeper - persistent storage with similarity search.

    Example:
        kp = Keeper()
        kp.put(uri="file:///path/to/readme.md")
        results = kp.find("installation instructions")
    """
    
    def __init__(
        self,
        store_path: Optional[str | Path] = None,
        decay_half_life_days: float = 30.0,
        *,
        config: Optional[StoreConfig] = None,
        doc_store: Optional["DocumentStoreProtocol"] = None,
        vector_store: Optional["VectorStoreProtocol"] = None,
        pending_queue: Optional["PendingQueueProtocol"] = None,
    ) -> None:
        """Initialize or open an existing reflective memory store.

        Args:
            store_path: Path to store directory. Uses default if not specified.
                       Overrides any store.path setting in config.
            decay_half_life_days: Memory decay half-life in days (ACT-R model).
                After this many days, an item's effective relevance is halved.
                Set to 0 or negative to disable decay.
            config: Pre-loaded StoreConfig (skips filesystem config discovery).
            doc_store: Injected document store (skips default backend creation).
            vector_store: Injected vector store (skips default backend creation).
            pending_queue: Injected summary queue (skips default backend creation).
        """
        self._decay_half_life_days = decay_half_life_days

        # --- Config resolution ---
        if config is not None:
            # Injected config — skip filesystem discovery
            self._config: StoreConfig = config
            self._store_path = config.path if config.path else Path(".")
        else:
            # Resolve config and store paths from filesystem.
            # KEEP_CONFIG takes priority for config dir; otherwise
            # store_path doubles as config dir (backwards compat).
            explicit_config = os.environ.get("KEEP_CONFIG")
            if store_path is not None:
                self._store_path = Path(store_path).resolve()
                config_dir = Path(explicit_config).expanduser().resolve() if explicit_config else self._store_path
            else:
                config_dir = get_config_dir()

            self._config = load_or_create_config(config_dir)

            if store_path is None:
                self._store_path = get_default_store_path(self._config)

        # --- Document provider ---
        registry = get_registry()
        self._document_provider: DocumentProvider = registry.create_document(
            self._config.document.name,
            self._config.document.params,
        )
        self._apply_file_size_limit(self._document_provider)

        # Lazy-loaded providers (created on first use to avoid network access for read-only ops)
        self._embedding_provider: Optional[EmbeddingProvider] = None
        self._summarization_provider: Optional[SummarizationProvider] = None
        self._media_describer: Optional[MediaDescriber] = None
        self._content_extractor = None  # ContentExtractor, lazy-loaded
        self._analyzer = None  # AnalyzerProvider, lazy-loaded

        # Cache and validate config/env tags once per Keeper instance.
        self._default_tags = self._validate_tag_map(
            self._config.default_tags,
            source="Config default tags",
            check_constraints=False,
        )
        self._env_tags = self._validate_tag_map(
            _get_env_tags(),
            source="KEEP_TAG_* environment tags",
            check_constraints=False,
        )

        # --- Persistent operations log ---
        from .logging_config import configure_ops_log
        self._ops_log_handler = configure_ops_log(self._store_path)

        # --- Storage backends (injected or factory-created) ---
        if doc_store is not None and vector_store is not None:
            # Fully injected (tests, custom setups)
            from .backend import NullPendingQueue
            self._document_store = doc_store
            self._store = vector_store
            self._pending_queue = pending_queue or NullPendingQueue()
            self._is_local = False
        else:
            # Factory-based creation from config
            from .backend import create_stores
            bundle = create_stores(self._config)
            self._document_store = bundle.doc_store
            self._store = bundle.vector_store
            self._pending_queue = bundle.pending_queue
            self._is_local = bundle.is_local

        # Guard against concurrent background reconciliation
        import threading
        self._reconcile_lock = threading.Lock()
        self._reconcile_done = threading.Event()
        self._closing = threading.Event()  # signals reconcile to abort
        self._provider_init_lock = threading.RLock()
        self._last_spawn_time: float = 0.0
        self._tagdoc_cache: dict[str, Optional[dict[str, str]]] = {}

        # Check store consistency and reconcile in background if needed
        # (safe for all backends — uses abstract store interface)
        needs_reconcile = self._check_store_consistency() and self._config.embedding is not None

        # If cosine migration fired (L2→cosine), auto-enqueue reindex
        if getattr(self._store, "migrated_to_cosine", False):
            logger.info("Cosine migration detected — enqueuing reindex")
            import sys
            try:
                stats = self.enqueue_reindex()
                print(
                    f"Search index migrated to cosine similarity.\n"
                    f"Search is unavailable until reindex completes.\n"
                    f"Run: keep pending",
                    file=sys.stderr,
                )
            except Exception as e:
                logger.error("Failed to enqueue reindex after cosine migration: %s", e)
                print(
                    f"ERROR: Search index migration failed. Search may not work.\n"
                    f"Try: keep pending --force\n"
                    f"Details: {e}",
                    file=sys.stderr,
                )
            needs_reconcile = False  # reindex will handle everything

        chroma_coll = self._resolve_chroma_collection()
        doc_coll = self._resolve_doc_collection()

        # Legacy metadata migration: old key=value Chroma metadata does not
        # satisfy marker-based tag filters. Rewrite metadata in-place.
        #
        # The detection scan is O(number of indexed rows), so persist a
        # per-store "verified" flag after a successful check/migration.
        if not self._config.chroma_tag_markers_verified:
            marker_migration_state = self._detect_chroma_tag_marker_migration_need(
                chroma_coll, doc_coll,
            )
            if marker_migration_state is True:
                import sys
                try:
                    print(
                        "Migrating search metadata to multivalue tag markers "
                        "(this may take a while on larger stores)...",
                        file=sys.stderr,
                        flush=True,
                    )
                    stats = self._migrate_chroma_tag_markers(chroma_coll, doc_coll)
                    logger.info(
                        "Tag marker migration complete: %d docs, %d versions, %d parts",
                        stats["docs"], stats["versions"], stats["parts"],
                    )
                    print(
                        "Search metadata migrated to multivalue tag markers "
                        f"({stats['docs']} docs, {stats['versions']} versions, {stats['parts']} parts).",
                        file=sys.stderr,
                    )
                    self._mark_chroma_tag_markers_verified()
                except Exception as e:
                    logger.warning("Tag marker migration failed: %s", e)
                    print(
                        "WARNING: tag metadata migration failed; "
                        "tag-filtered semantic search may be incomplete.\n"
                        "Run: keep pending --reindex",
                        file=sys.stderr,
                    )
            elif marker_migration_state is False:
                self._mark_chroma_tag_markers_verified()

        if needs_reconcile:
            self._reconcile_thread = threading.Thread(
                target=self._auto_reconcile_safe, args=(chroma_coll, doc_coll), daemon=True,
            )
            self._reconcile_thread.start()
        else:
            self._reconcile_thread = None
            self._reconcile_done.set()

        # --- Task delegation client (for hosted processing) ---
        self._task_client = None
        if self._config.remote:
            from .task_client import TaskClient
            try:
                self._task_client = TaskClient(
                    self._config.remote.api_url,
                    self._config.remote.api_key,
                    project=self._config.remote.project,
                )
            except Exception as e:
                logger.warning("Failed to initialize TaskClient: %s", e)

        # --- Planner stats (precomputed priors for flow discriminators) ---
        self._planner_stats = None
        if self._is_local:
            from .planner_stats import PlannerStatsStore
            try:
                self._planner_stats = PlannerStatsStore(
                    self._store_path / "planner_stats.db"
                )
                # Bootstrap: enqueue rebuild task if stats have never been built.
                # The daemon will pick this up on its next tick.
                doc_coll = self._resolve_doc_collection()
                if self._planner_stats.needs_rebuild(doc_coll):
                    self._pending_queue.enqueue(
                        ".planner-rebuild", doc_coll, "",
                        task_type="planner-rebuild",
                    )
            except Exception as e:
                logger.warning("Failed to initialize PlannerStatsStore: %s", e)

        # System doc migration deferred to first write (needs embeddings)
        from .system_docs import _bundled_docs_hash
        self._needs_sysdoc_migration = (
            self._config.system_docs_hash != _bundled_docs_hash()
        )

        # Direct work queue (replaces FlowEngine for background tasks).
        self._work_queue = None
        self._work_queue_lock = threading.Lock()
        self._write_context_by_id: dict[str, dict[str, Any]] = {}
        self._write_context_lock = threading.Lock()

    def _apply_file_size_limit(self, provider: DocumentProvider) -> None:
        """Apply max_file_size config to file-based providers."""
        from .providers.documents import FileDocumentProvider, CompositeDocumentProvider
        max_size = self._config.max_file_size
        if isinstance(provider, FileDocumentProvider):
            provider.max_size = max_size
        elif isinstance(provider, CompositeDocumentProvider):
            for p in provider._providers:
                if isinstance(p, FileDocumentProvider):
                    p.max_size = max_size

    def _check_store_consistency(self) -> bool:
        """Check if document store and vector store ID sets match.

        Returns True if reconciliation is needed. Does not fix —
        that is deferred to the first _upsert call when the
        embedding provider is available.
        """
        try:
            result = self.reconcile(fix=False)
            if result["missing_from_index"] or result["orphaned_in_index"]:
                logger.info(
                    "Store inconsistency: %d missing from search index, %d orphaned (will auto-reconcile)",
                    result["missing_from_index"], result["orphaned_in_index"],
                )
                return True
        except Exception as e:
            logger.debug("Store consistency check failed: %s", e)
        return False

    def _mark_chroma_tag_markers_verified(self) -> None:
        """Persist that this store no longer needs startup marker scans."""
        if self._config.chroma_tag_markers_verified:
            return
        self._config.chroma_tag_markers_verified = True
        try:
            save_config(self._config)
        except Exception as e:
            logger.debug("Failed to persist chroma_tag_markers_verified: %s", e)

    def _detect_chroma_tag_marker_migration_need(
        self, chroma_coll: str, doc_coll: str,
    ) -> Optional[bool]:
        """Detect legacy Chroma metadata without multivalue tag markers.

        Returns:
            True: legacy metadata detected, migration needed
            False: scan completed and migration not needed
            None: check failed (caller should avoid persisting "verified")
        """
        has_tag_markers = getattr(self._store, "has_tag_markers", None)
        if not callable(has_tag_markers):
            return False

        def _has_user_tags(tags: dict[str, Any]) -> bool:
            return any(
                not key.startswith(SYSTEM_TAG_PREFIX) and tag_values(tags, key)
                for key in tags
            )

        try:
            indexed_ids = set(self._store.list_ids(chroma_coll))
            if not indexed_ids:
                return False
            for doc_id in self._document_store.list_ids(doc_coll):
                doc = self._document_store.get(doc_coll, doc_id)
                if doc is not None and doc_id in indexed_ids:
                    if _has_user_tags(doc.tags) and not has_tag_markers(chroma_coll, doc_id):
                        return True

                for vi in self._document_store.list_versions(
                    doc_coll, doc_id, limit=1_000_000,
                ):
                    version_id = f"{doc_id}@v{vi.version}"
                    if version_id not in indexed_ids:
                        continue
                    ver_tags = dict(vi.tags)
                    ver_tags["_version"] = str(vi.version)
                    ver_tags["_base_id"] = doc_id
                    if _has_user_tags(ver_tags) and not has_tag_markers(chroma_coll, version_id):
                        return True

                for part in self._document_store.list_parts(doc_coll, doc_id):
                    part_id = f"{doc_id}@p{part.part_num}"
                    if part_id not in indexed_ids:
                        continue
                    part_tags = dict(part.tags)
                    part_tags["_part_num"] = str(part.part_num)
                    part_tags["_base_id"] = doc_id
                    if _has_user_tags(part_tags) and not has_tag_markers(chroma_coll, part_id):
                        return True
        except Exception as e:
            logger.debug("Tag marker migration check failed: %s", e)
            return None
        return False

    def _migrate_chroma_tag_markers(
        self, chroma_coll: str, doc_coll: str,
    ) -> dict[str, int]:
        """Rewrite legacy Chroma metadata to marker-based tag encoding."""
        rewrite_tags = getattr(self._store, "rewrite_tags", None)
        if not callable(rewrite_tags):
            return {"docs": 0, "versions": 0, "parts": 0}

        indexed_ids = set(self._store.list_ids(chroma_coll))
        if not indexed_ids:
            return {"docs": 0, "versions": 0, "parts": 0}

        docs = versions = parts = 0
        for doc_id in self._document_store.list_ids(doc_coll):
            if doc_id in indexed_ids:
                doc = self._document_store.get(doc_coll, doc_id)
                if doc is not None and rewrite_tags(
                    chroma_coll, doc_id, casefold_tags_for_index(doc.tags),
                ):
                    docs += 1

            for vi in self._document_store.list_versions(
                doc_coll, doc_id, limit=1_000_000,
            ):
                version_id = f"{doc_id}@v{vi.version}"
                if version_id not in indexed_ids:
                    continue
                ver_tags = dict(vi.tags)
                ver_tags["_version"] = str(vi.version)
                ver_tags["_base_id"] = doc_id
                if rewrite_tags(
                    chroma_coll, version_id, casefold_tags_for_index(ver_tags),
                ):
                    versions += 1

            for part in self._document_store.list_parts(doc_coll, doc_id):
                part_id = f"{doc_id}@p{part.part_num}"
                if part_id not in indexed_ids:
                    continue
                part_tags = dict(part.tags)
                part_tags["_part_num"] = str(part.part_num)
                part_tags["_base_id"] = doc_id
                if rewrite_tags(
                    chroma_coll, part_id, casefold_tags_for_index(part_tags),
                ):
                    parts += 1

        return {"docs": docs, "versions": versions, "parts": parts}

    def _auto_reconcile_safe(self, chroma_coll: str, doc_coll: str) -> None:
        """Background-safe wrapper for auto-reconcile. Logs failures."""
        if not self._reconcile_lock.acquire(blocking=False):
            logger.info("Reconciliation already in progress, skipping")
            return
        try:
            self._auto_reconcile(chroma_coll, doc_coll)
        except Exception as e:
            logger.warning("Auto-reconcile failed: %s", e)
        finally:
            self._reconcile_lock.release()
            self._reconcile_done.set()

    def _auto_reconcile(self, chroma_coll: str, doc_coll: str) -> None:
        """Fix store divergence using summaries (no content re-fetch needed).

        Validates embedding provider first, then fixes missing/orphaned items.
        Uses its own DocumentStore connection to avoid concurrent-access
        segfaults with the main thread's SQLite connection.
        """
        if self._config.embedding is None:
            logger.info("Skipping reconciliation: no embedding provider configured")
            return

        try:
            self._get_embedding_provider()
        except Exception as e:
            logger.warning("Skipping reconciliation: provider unavailable: %s", e)
            return

        # Open a separate SQLite connection for thread-safe reads.
        # The main thread may be running find() concurrently, and Python's
        # sqlite3 module crashes (segfault) on concurrent access to the
        # same Connection object from multiple threads.
        from .document_store import DocumentStore
        recon_ds = DocumentStore(self._document_store._db_path)
        try:
            result = self.reconcile(fix=True, _doc_store=recon_ds)
            logger.info(
                "Auto-reconcile complete: %d fixed, %d orphans removed, %d missing",
                result["fixed"], result["removed"], result["missing_from_index"],
            )
        finally:
            recon_ds.close()

    def _migrate_system_documents(self, progress=None) -> dict:
        """Migrate system documents to stable IDs and current version."""
        from .system_docs import migrate_system_documents
        result = migrate_system_documents(self, progress=progress)
        self._tagdoc_cache.clear()  # tagdocs may have changed
        self._scan_tagdoc_backfills()
        return result

    def _scan_tagdoc_backfills(self) -> None:
        """Ensure backfill is queued for every tagdoc with _inverse.

        Called once at startup (after system doc migration) to catch
        tagdocs that were created outside the normal put() path or
        from a previous version that didn't support edges.

        Also materializes any missing inverse tagdocs for existing
        stores that were created before inverse materialization existed.
        """
        doc_coll = self._resolve_doc_collection()
        tagdocs = self._document_store.query_by_id_prefix(doc_coll, ".tag/")
        for td in tagdocs:
            # Only top-level tagdocs (.tag/KEY, not .tag/KEY/VALUE)
            if "/" in td.id[5:]:
                continue
            inverse = td.tags.get("_inverse")
            if inverse:
                # Coerce to str in case stored as list (defensive)
                if isinstance(inverse, list):
                    inverse = inverse[0]
                self._check_edge_backfill(td.id[5:], inverse, doc_coll)
                self._ensure_inverse_tagdoc(td.id[5:], inverse, doc_coll)

    # -- Provider lifecycle methods are in ProviderLifecycleMixin --

    def _resolve_prompt_doc(
        self,
        prefix: str,
        doc_tags: dict[str, str],
    ) -> str | None:
        """Find a .prompt/* doc matching the given tags and return its prompt text.

        Scans .prompt/{prefix}/* documents. For each, parses match rules
        from content (same DSL as .meta/* docs) and checks against doc_tags.
        Returns the ## Prompt section of the best match (most specific).

        Args:
            prefix: "analyze" or "summarize"
            doc_tags: Tags of the document being analyzed/summarized

        Returns:
            Prompt text from the best-matching doc, or None.
        """
        from .analyzers import extract_prompt_section

        doc_coll = self._resolve_doc_collection()
        prompt_docs = self._document_store.query_by_id_prefix(
            doc_coll, f".prompt/{prefix}/"
        )
        if not prompt_docs:
            return None

        best_prompt = None
        best_specificity = -1  # more match rules = more specific

        for rec in prompt_docs:
            content = rec.summary if hasattr(rec, 'summary') else ""

            prompt_text = extract_prompt_section(content)
            if not prompt_text:
                continue

            # Parse match rules from body (before ## Prompt)
            query_lines, _, _ = _parse_meta_doc(content)

            if not query_lines:
                # No match rules = default/fallback (specificity 0)
                if best_specificity < 0:
                    best_prompt = prompt_text
                    best_specificity = 0
                continue

            # Check if any query line fully matches doc_tags
            for query in query_lines:
                if all(v in tag_values(doc_tags, k) for k, v in query.items()):
                    specificity = len(query)
                    if specificity > best_specificity:
                        best_specificity = specificity
                        best_prompt = prompt_text
                    break

        return best_prompt

    def _gather_context(
        self,
        id: str,
        tags: dict[str, str],
    ) -> str | None:
        """Gather related item summaries that share any user tag.

        Uses OR union (any tag matches), not AND intersection.
        Boosts score when multiple tags match.

        Args:
            id: ID of the item being summarized (to exclude from results)
            tags: User tags from the item being summarized

        Returns:
            Formatted context string, or None if no related items found
        """
        if not tags:
            return None

        # Get similar items (broader search, we'll filter by tags)
        try:
            similar = self.find(similar_to=id, limit=20)
        except KeyError:
            # Item not found yet (first indexing) - no context available
            return None

        # Score each item: similarity * (1 + matching_tag_count * boost)
        TAG_BOOST = 0.2  # 20% boost per matching tag
        scored: list[tuple[float, int, Item]] = []

        for item in similar:
            if item.id == id:
                continue

            # Count matching tags (OR: at least one must match)
            matching = sum(
                1 for k, v in iter_tag_pairs(tags, include_system=False)
                if v in tag_values(item.tags, k)
            )
            if matching == 0:
                continue  # No tag overlap, skip

            # Boost score by number of matching tags
            base_score = item.score if item.score is not None else 0.5
            boosted_score = base_score * (1 + matching * TAG_BOOST)
            scored.append((boosted_score, matching, item))

        if not scored:
            return None

        # Sort by boosted score, take top 5
        scored.sort(key=lambda x: x[0], reverse=True)
        top = scored[:5]

        # Format context as topic keywords only (not summaries).
        # Including raw summary text causes small models to parrot
        # phrases from context into the new summary (contamination).
        topic_values = set()
        for _, _, item in top:
            for k, v in filter_non_system_tags(item.tags).items():
                topic_values.add(v)

        if not topic_values:
            return None

        return "Related topics: " + ", ".join(sorted(topic_values))

    def _validate_embedding_identity(self, provider: EmbeddingProvider) -> None:
        """Validate embedding provider matches stored identity, or record it.

        On first use, records the embedding identity to config.
        On subsequent uses, if the provider changed, silently updates config
        and enqueues reindex tasks into the pending queue.
        """
        # Get current provider's identity
        current = EmbeddingIdentity(
            provider=self._config.embedding.name,
            model=getattr(provider, "model_name", "unknown"),
            dimension=provider.dimension,
        )

        stored = self._config.embedding_identity

        if stored is None:
            # First use: record the identity and set store dimension
            logger.info(
                "Recording embedding identity: %s/%s (%dd)",
                current.provider, current.model, current.dimension,
            )
            self._config.embedding_identity = current
            save_config(self._config)
            self._store.reset_embedding_dimension(current.dimension)
        else:
            # Check for provider change
            if (stored.provider != current.provider or
                stored.model != current.model or
                stored.dimension != current.dimension):
                logger.info(
                    "Embedding provider changed: %s/%s (%dd) → %s/%s (%dd)",
                    stored.provider, stored.model, stored.dimension,
                    current.provider, current.model, current.dimension,
                )
                self._config.embedding_identity = current
                save_config(self._config)
                # Update store dimension for new model
                self._store.reset_embedding_dimension(current.dimension)
                # If dimension changed, drop ChromaDB collection so it's
                # recreated with the new dimension on first write.
                # Without this, ChromaDB rejects new-dimension vectors.
                if stored.dimension != current.dimension:
                    chroma_coll = self._resolve_chroma_collection()
                    try:
                        self._store.delete_collection(chroma_coll)
                        logger.info(
                            "Dropped search index (dimension %d → %d)",
                            stored.dimension, current.dimension,
                        )
                    except Exception as e:
                        logger.warning("Could not drop search index: %s", e)
                # Enqueue reindex tasks for pending queue
                import sys
                try:
                    stats = self.enqueue_reindex()
                    logger.info(
                        "Enqueued %d items (+%d versions) for reindex",
                        stats["enqueued"], stats["versions"],
                    )
                    dim_msg = (
                        f" Dimension changed ({stored.dimension}→{current.dimension});"
                        f" search index was cleared."
                        if stored.dimension != current.dimension else ""
                    )
                    print(
                        f"Embedding model changed.{dim_msg}\n"
                        f"Enqueued {stats['enqueued']} items for reindex.\n"
                        f"Search is unavailable until reindex completes.\n"
                        f"Run: keep pending",
                        file=sys.stderr,
                    )
                except Exception as e:
                    logger.error("Failed to enqueue reindex after model change: %s", e)
                    print(
                        f"ERROR: Embedding model changed but reindex failed.\n"
                        f"Search will not work until reindex completes.\n"
                        f"Try: keep pending --force\n"
                        f"Details: {e}",
                        file=sys.stderr,
                    )
            else:
                logger.debug(
                    "Embedding identity unchanged: %s/%s (%dd)",
                    stored.provider, stored.model, stored.dimension,
                )

    def enqueue_reindex(self) -> dict:
        """Enqueue embed tasks for all docs (and their versions) into the pending queue.

        Returns:
            Dict with stats: enqueued (int), versions (int)
        """
        doc_coll = self._resolve_doc_collection()
        doc_ids = self._document_store.list_ids(doc_coll)
        enqueued = 0
        versions = 0

        for doc_id in doc_ids:
            record = self._document_store.get(doc_coll, doc_id)
            if record is None:
                continue
            self._pending_queue.enqueue(
                doc_id, doc_coll, record.summary,
                task_type="reindex",
                metadata={"tags": dict(record.tags)},
            )
            enqueued += 1

            # Enqueue version reindex
            for vi in self._document_store.list_versions(doc_coll, doc_id, limit=100):
                version_id = f"{doc_id}@v{vi.version}"
                self._pending_queue.enqueue(
                    version_id, doc_coll, vi.summary,
                    task_type="reindex",
                    metadata={
                        "version": vi.version,
                        "base_id": doc_id,
                        "tags": dict(vi.tags),
                    },
                )
                versions += 1

        logger.info("Enqueue reindex: %d items + %d versions", enqueued, versions)
        return {"enqueued": enqueued, "versions": versions}

    # -------------------------------------------------------------------------
    # Data Export / Import
    # -------------------------------------------------------------------------

    def export_iter(self, *, include_system: bool = True) -> Iterator[dict]:
        """Stream-export all documents as an iterator of dicts.

        Yields dicts one at a time so that arbitrarily large stores can be
        exported without loading everything into memory.

        **First yield** — header dict::

            {"format": "keep-export", "version": 1, "exported_at": "...",
             "store_info": {"document_count": N, "version_count": N,
                            "part_count": N, "collection": "..."}}

        **Subsequent yields** — one dict per document. Each document is
        self-contained: its ``versions`` and ``parts`` lists are included
        inline (not yielded separately)::

            {"id": "...", "summary": "...", "tags": {...},
             "created_at": "...", "updated_at": "...", "accessed_at": "...",
             "versions": [...], "parts": [...]}

        Embeddings are excluded (model-dependent; regenerated on import).

        Args:
            include_system: If False, skip system documents (dot-prefix IDs)
        """
        doc_coll = self._resolve_doc_collection()
        doc_ids = self._document_store.list_ids(doc_coll)

        if not include_system:
            doc_ids = [d for d in doc_ids if not d.startswith(".")]

        # Header with document count; version/part counts filled per-document
        # as we stream (header store_info is best-effort for streaming)
        yield {
            "format": "keep-export",
            "version": 1,
            "exported_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
            "store_info": {
                "document_count": len(doc_ids),
                "version_count": sum(
                    self._document_store.version_count(doc_coll, d)
                    for d in doc_ids
                ),
                "part_count": sum(
                    self._document_store.part_count(doc_coll, d)
                    for d in doc_ids
                ),
                "collection": doc_coll,
            },
        }

        for doc_id in doc_ids:
            record = self._document_store.get(doc_coll, doc_id)
            if record is None:
                continue

            doc_dict: dict = {
                "id": record.id,
                "summary": record.summary,
                "tags": dict(record.tags),
                "content_hash": record.content_hash,
                "content_hash_full": record.content_hash_full,
                "created_at": record.created_at,
                "updated_at": record.updated_at,
                "accessed_at": record.accessed_at,
            }

            # Versions — inline within the document dict
            versions = []
            for vi in self._document_store.list_versions(doc_coll, doc_id, limit=10000):
                versions.append({
                    "version": vi.version,
                    "summary": vi.summary,
                    "tags": dict(vi.tags),
                    "content_hash": vi.content_hash,
                    "created_at": vi.created_at,
                })
            if versions:
                doc_dict["versions"] = versions

            # Parts — inline within the document dict
            parts = []
            for pi in self._document_store.list_parts(doc_coll, doc_id):
                parts.append({
                    "part_num": pi.part_num,
                    "summary": pi.summary,
                    "tags": dict(pi.tags),
                    "content": pi.content,
                    "created_at": pi.created_at,
                })
            if parts:
                doc_dict["parts"] = parts

            yield doc_dict

    def export_data(self, *, include_system: bool = True) -> dict:
        """Export all documents, versions, and parts as a single dict.

        Convenience wrapper around :meth:`export_iter` that collects everything
        into memory.  Fine for small/medium stores; for large stores use
        ``export_iter()`` directly to stream documents.

        Returns:
            Dict in keep-export format (version 1) with a ``documents`` list.
        """
        it = self.export_iter(include_system=include_system)
        header = next(it)
        header["documents"] = list(it)
        return header

    def import_data(self, data: dict, *, mode: str = "merge") -> dict:
        """Import documents from an export dict.

        All SQLite writes happen in a single transaction for speed.
        Re-embedding is queued for background processing (not done inline).

        Args:
            data: Dict in keep-export format
            mode: "merge" (skip existing IDs) or "replace" (clear first)

        Returns:
            Dict with stats: {imported, skipped, versions, parts, queued}
        """
        if data.get("format") != "keep-export":
            raise ValueError("Invalid export format (expected 'keep-export')")
        if data.get("version", 0) > 1:
            raise ValueError(
                f"Export format version {data['version']} is not supported "
                f"(this version supports up to 1)"
            )

        doc_coll = self._resolve_doc_collection()
        documents = data.get("documents", [])

        if mode == "replace":
            self._document_store.delete_collection_all(doc_coll)
            chroma_coll = self._resolve_chroma_collection()
            try:
                self._store.delete_collection(chroma_coll)
            except Exception:
                pass  # ChromaDB collection may not exist yet

        # In merge mode, get existing IDs to skip
        existing_ids: set[str] = set()
        if mode == "merge":
            existing_ids = set(self._document_store.list_ids(doc_coll))

        # Validate imported tag keys and normalize case to match normal writes.
        for doc in documents:
            doc_id = doc.get("id", "<unknown>")
            doc["tags"] = self._validate_tag_map(
                doc.get("tags", {}),
                source=f"Import document tags ({doc_id})",
                check_constraints=False,
            )
            for ver in doc.get("versions", []):
                ver_num = ver.get("version", "?")
                ver["tags"] = self._validate_tag_map(
                    ver.get("tags", {}),
                    source=f"Import version tags ({doc_id}@v{ver_num})",
                    check_constraints=False,
                )
            for part in doc.get("parts", []):
                part_num = part.get("part_num", "?")
                part["tags"] = self._validate_tag_map(
                    part.get("tags", {}),
                    source=f"Import part tags ({doc_id}@p{part_num})",
                    check_constraints=False,
                )

        # Filter to importable documents
        to_import = []
        skipped = 0
        for doc in documents:
            if doc["id"] in existing_ids:
                skipped += 1
            else:
                to_import.append(doc)

        # Batch-insert all documents, versions, parts in one transaction
        stats = self._document_store.import_batch(doc_coll, to_import)

        # Bulk-enqueue reindex tasks for all imported documents
        queued = 0
        for doc in to_import:
            doc_id = doc["id"]
            self._pending_queue.enqueue(
                doc_id, doc_coll, doc.get("summary", ""),
                task_type="reindex",
                metadata={"tags": doc.get("tags", {})},
            )
            queued += 1

            # Enqueue version reindex
            for ver in doc.get("versions", []):
                version_id = f"{doc_id}@v{ver['version']}"
                self._pending_queue.enqueue(
                    version_id, doc_coll, ver.get("summary", ""),
                    task_type="reindex",
                    metadata={
                        "version": ver["version"],
                        "base_id": doc_id,
                        "tags": ver.get("tags", {}),
                    },
                )

        return {
            "imported": stats["documents"],
            "skipped": skipped,
            "versions": stats["versions"],
            "parts": stats["parts"],
            "queued": queued,
        }

    @property
    def embedding_identity(self) -> EmbeddingIdentity | None:
        """Current embedding identity (provider, model, dimension)."""
        return self._config.embedding_identity
    
    def _resolve_chroma_collection(self) -> str:
        """Vector collection name derived from embedding identity."""
        if self._config.embedding_identity:
            return self._config.embedding_identity.key
        return "default"

    def _resolve_doc_collection(self) -> str:
        """DocumentStore collection — always 'default'."""
        return "default"

    def _build_tag_where(self, tags: dict) -> dict | None:
        """Build backend where-clause for tag filters."""
        builder = getattr(self._store, "build_tag_where", None)
        if callable(builder):
            return builder(tags)
        conditions: list[dict[str, str]] = []
        for key in tags:
            for value in tag_values(tags, key):
                conditions.append({key: value})
        if not conditions:
            return None
        return conditions[0] if len(conditions) == 1 else {"$and": conditions}
    
    # -------------------------------------------------------------------------
    # Tag Validation
    # -------------------------------------------------------------------------

    def _validate_tag_map(
        self,
        tags: dict,
        *,
        source: str,
        check_constraints: bool,
    ) -> dict:
        """Casefold and validate tag keys/values for non-system tags."""
        try:
            normalized = casefold_tags(tags)
        except ValueError as e:
            raise ValueError(f"{source}: {e}") from e
        for key in normalized:
            if key.startswith(SYSTEM_TAG_PREFIX):
                continue
            try:
                validate_tag_key(key)
            except ValueError as e:
                raise ValueError(f"{source}: {e}") from e
            for value in tag_values(normalized, key):
                if len(value) > MAX_TAG_VALUE_LENGTH:
                    raise ValueError(
                        f"{source}: Tag value too long (max {MAX_TAG_VALUE_LENGTH}): {key!r}"
                    )
        if check_constraints:
            self._validate_constrained_tags(normalized)
        return normalized

    def _validate_write_tags(self, tags: dict) -> dict:
        """Casefold, validate keys/lengths, and check constrained values.

        Returns the casefolded tags dict.  Used by put(), tag(), tag_part().
        """
        return self._validate_tag_map(tags, source="Tags", check_constraints=True)

    def _validate_constrained_tags(
        self, tags: dict, existing_tags: dict | None = None,
    ) -> None:
        """Check constrained tag values against sub-doc existence.

        For each user tag, looks up `.tag/KEY`. If that doc exists and has
        `_constrained=true`, checks that `.tag/KEY/VALUE` exists. Raises
        ValueError with valid values listed if not.

        If the tag doc has ``_requires``, the constraint only applies when
        the required tag is present in either *tags* or *existing_tags*.
        """
        doc_coll = self._resolve_doc_collection()
        for key in tags:
            if key.startswith(SYSTEM_TAG_PREFIX):
                continue
            values = tag_values(tags, key)
            if not values:
                continue
            parent_id = f".tag/{key}"
            parent = self._document_store.get(doc_coll, parent_id)
            if parent is None:
                continue  # no tag doc → unconstrained
            if parent.tags.get("_constrained") != "true":
                continue  # tag doc exists but not constrained
            # _requires: only enforce constraint when the required tag is present
            requires = parent.tags.get("_requires")
            if requires:
                in_new = requires in tags
                in_existing = bool(existing_tags and requires in existing_tags)
                if not in_new and not in_existing:
                    continue
            for value in values:
                if value == "":
                    continue  # deletion, no validation needed
                value_id = f".tag/{key}/{value}"
                if not self._document_store.get(doc_coll, value_id):
                    valid = self._list_constrained_values(key)
                    raise ValueError(
                        f"Invalid value for constrained tag '{key}': {value!r}. "
                        f"Valid values: {', '.join(sorted(valid))}"
                    )

    def _list_constrained_values(self, key: str) -> list[str]:
        """List valid values for a constrained tag by finding sub-docs."""
        doc_coll = self._resolve_doc_collection()
        prefix = f".tag/{key}/"
        docs = self._document_store.query_by_id_prefix(doc_coll, prefix)
        return [doc.id[len(prefix):] for doc in docs]

    def _get_singular_keys(self, keys: Iterable[str]) -> set[str]:
        """Return the subset of *keys* whose tagdoc has ``_singular=true``.

        For each non-system key, looks up ``.tag/{key}`` in the document
        store.  If the tagdoc exists and carries ``_singular: "true"``,
        the key is included in the result set.
        """
        doc_coll = self._resolve_doc_collection()
        singular: set[str] = set()
        for key in keys:
            if key.startswith(SYSTEM_TAG_PREFIX):
                continue
            parent = self._document_store.get(doc_coll, f".tag/{key}")
            if parent is not None and parent.tags.get("_singular") == "true":
                singular.add(key)
        return singular

    def _validate_singular_tags(self, add_changes: dict, singular_keys: set[str]) -> None:
        """Raise if any singular key has more than one incoming value."""
        for key in singular_keys:
            values = tag_values(add_changes, key)
            if len(values) > 1:
                raise ValueError(
                    f"Tag '{key}' is singular (at most one value allowed), "
                    f"but got {len(values)} values: {values!r}"
                )

    # -------------------------------------------------------------------------
    # Edge processing (tag-driven relationship edges)
    # -------------------------------------------------------------------------

    def _process_edge_tags(
        self,
        id: str,
        merged_tags: dict[str, str],
        existing_tags: dict[str, str],
        doc_coll: str,
    ) -> None:
        """Create/update/delete edges based on tagdoc _inverse declarations.

        For each non-system tag on the document, checks whether the
        corresponding `.tag/{key}` tagdoc has an `_inverse` value.
        If so, the tag represents an edge: source=id, target=value,
        predicate=key, inverse=_inverse value.

        Also handles tag removal: if a key that was an edge-tag is no
        longer present, the corresponding edge row is deleted.
        """
        # System docs never participate as edge sources.
        # Hosted RBAC invariant: non-system writes (writer role) must not
        # trigger writes to dot-prefixed system documents.
        if id.startswith("."):
            return

        # Determine which keys to check: union of current and previous
        current_keys = {
            k for k in merged_tags
            if not k.startswith(SYSTEM_TAG_PREFIX) and tag_values(merged_tags, k)
        }
        previous_keys = {
            k for k in existing_tags
            if not k.startswith(SYSTEM_TAG_PREFIX) and tag_values(existing_tags, k)
        }
        all_keys = current_keys | previous_keys
        if not all_keys:
            return  # no user tags → no edges possible

        def _get_tagdoc_tags(key: str) -> Optional[dict[str, str]]:
            if key not in self._tagdoc_cache:
                parent = self._document_store.get(doc_coll, f".tag/{key}")
                self._tagdoc_cache[key] = parent.tags if parent else None
            return self._tagdoc_cache[key]

        # Collect batch operations across all edge-tag keys
        edges_to_delete: list[tuple[str, str, str]] = []  # (source_id, predicate, target_id)
        edges_to_add: list[tuple[str, str, str, str, str]] = []  # (source_id, predicate, target_id, inverse, created)
        backfill_checks: list[tuple[str, str]] = []  # (predicate, inverse)

        for key in all_keys:
            td_tags = _get_tagdoc_tags(key)
            if td_tags is None:
                continue
            inverse = td_tags.get("_inverse")
            if not inverse:
                continue
            if isinstance(inverse, list):
                inverse = inverse[0]

            current_values = set(tag_values(merged_tags, key))
            previous_values = set(tag_values(existing_tags, key))

            removed_values = previous_values - current_values
            added_values = current_values - previous_values

            # Tag values removed → queue edge deletions.
            for removed in removed_values:
                target_id = parse_ref(removed)[0]
                try:
                    target_id = normalize_id(target_id)
                except ValueError:
                    pass
                edges_to_delete.append((id, key, target_id))

            # Tag present → queue edge upsert + auto-vivify target.
            for current_value in sorted(added_values):
                if not current_value:
                    continue
                try:
                    target_id = normalize_id(parse_ref(current_value)[0])
                except ValueError:
                    logger.debug("Skipping invalid edge target for tag %r: %r", key, current_value)
                    continue
                if target_id.startswith("."):
                    continue
                # Auto-vivify: create target as empty doc if it doesn't exist.
                # Uses atomic INSERT OR IGNORE to avoid TOCTOU race where a
                # concurrent writer creates the real document between check
                # and write (upsert would overwrite it with the empty stub).
                reference_created = (
                    merged_tags.get("_created")
                    or merged_tags.get("_updated")
                    or utc_now()
                )
                now = utc_now()
                inserted = self._document_store.insert_if_absent(
                    doc_coll, target_id,
                    summary="",
                    tags={
                        "_created": reference_created,
                        "_updated": now,
                        "_source": "auto-vivify",
                    },
                    created_at=reference_created,
                )
                if inserted:
                    self._pending_queue.enqueue(
                        target_id, doc_coll, target_id,
                        task_type="reindex",
                        metadata={
                            "tags": {
                                "_created": reference_created,
                                "_updated": now,
                                "_source": "auto-vivify",
                            },
                        },
                    )

                created = merged_tags.get("_created") or merged_tags.get("_updated") or utc_now()
                edges_to_add.append((id, key, target_id, inverse, created))
                backfill_checks.append((key, inverse))

        # Execute batched edge mutations
        if edges_to_delete:
            self._document_store.delete_edges_batch(doc_coll, edges_to_delete)
        if edges_to_add:
            self._document_store.upsert_edges_batch(doc_coll, edges_to_add)

        # Trigger backfill checks (deduplicated by predicate)
        seen_predicates: set[str] = set()
        for predicate, inverse in backfill_checks:
            if predicate not in seen_predicates:
                seen_predicates.add(predicate)
                self._check_edge_backfill(predicate, inverse, doc_coll)

    def _check_edge_backfill(
        self, predicate: str, inverse: str, doc_coll: str,
    ) -> None:
        """Ensure edges are backfilled for a predicate that has _inverse.

        Only enqueues a backfill task if no record exists at all.
        A pending record (completed=NULL) means a task is already queued.
        """
        if self._document_store.backfill_exists(doc_coll, predicate):
            return  # already pending or completed
        self._document_store.upsert_backfill(doc_coll, predicate, inverse)
        self._pending_queue.enqueue(
            id=f".backfill/{predicate}",
            collection=doc_coll,
            content="",
            task_type="backfill-edges",
            metadata={"predicate": predicate, "inverse": inverse},
        )

    def _process_tagdoc_inverse_change(
        self,
        id: str,
        new_tags: dict[str, str],
        old_tags: dict[str, str],
        doc_coll: str,
    ) -> None:
        """Handle _inverse changes on a .tag/* document.

        Called *before* storage so old_tags reflect the previous state.
        Detects whether _inverse was added, changed, or removed, and
        adjusts edges/backfill accordingly.
        """
        # Extract predicate from tagdoc ID: .tag/KEY → KEY.
        # Explicit boundary: only system-tagdoc writes may mutate edge schema
        # and materialize inverse tagdocs. Non-system writes cannot enter here.
        if not id.startswith(".tag/") or "/" in id[5:]:
            return  # not a top-level tagdoc
        predicate = id[5:]

        old_inverse = old_tags.get("_inverse") if old_tags else None
        new_inverse = new_tags.get("_inverse") if new_tags else None

        if old_inverse == new_inverse:
            return  # no change

        if old_inverse and not new_inverse:
            # _inverse removed → clean up all edges and backfill for this predicate
            self._document_store.delete_edges_for_predicate(doc_coll, predicate)
            self._document_store.delete_version_edges_for_predicate(doc_coll, predicate)
            self._document_store.delete_backfill(doc_coll, predicate)
        elif new_inverse and old_inverse != new_inverse:
            # _inverse added or changed → delete old edges, enqueue new backfill
            if old_inverse:
                self._document_store.delete_edges_for_predicate(doc_coll, predicate)
                self._document_store.delete_version_edges_for_predicate(doc_coll, predicate)
                self._document_store.delete_backfill(doc_coll, predicate)
            self._check_edge_backfill(predicate, new_inverse, doc_coll)
            # Materialize the inverse tagdoc synchronously
            self._ensure_inverse_tagdoc(predicate, new_inverse, doc_coll)

    def _ensure_inverse_tagdoc(
        self, predicate: str, inverse: str, doc_coll: str,
    ) -> None:
        """Ensure .tag/{inverse} exists with _inverse={predicate}.

        Called synchronously when .tag/{predicate} declares _inverse={inverse}.
        Creates the inverse tagdoc if missing, or verifies consistency.

        Raises ValueError if .tag/{inverse} already has a different _inverse.
        """
        inverse_tagdoc_id = f".tag/{inverse}"
        existing = self._document_store.get(doc_coll, inverse_tagdoc_id)

        if existing:
            existing_inverse = existing.tags.get("_inverse")
            if existing_inverse == predicate:
                return  # already correct
            if existing_inverse and existing_inverse != predicate:
                raise ValueError(
                    f"Inverse conflict: .tag/{inverse} already declares "
                    f"_inverse={existing_inverse!r}, cannot set to {predicate!r} "
                    f"(required by .tag/{predicate} _inverse={inverse})"
                )
            # Exists without _inverse → add it
            tags = dict(existing.tags)
            tags["_inverse"] = predicate
            tags["_updated"] = utc_now()
            self._document_store.upsert(
                doc_coll, inverse_tagdoc_id,
                summary=existing.summary, tags=tags,
            )
        else:
            # Create minimal inverse tagdoc with a useful summary
            # so it can be embedded and found via search.
            now = utc_now()
            summary = f"Inverse edge tag for `{predicate}` (auto-generated)"
            self._document_store.upsert(
                doc_coll, inverse_tagdoc_id,
                summary=summary,
                tags={
                    "_inverse": predicate,
                    "_created": now,
                    "_updated": now,
                    "_source": "auto-vivify",
                    "category": "system",
                    "context": "tag-description",
                },
            )

        # Backfill edges for the inverse direction too
        self._check_edge_backfill(inverse, predicate, doc_coll)

    # -------------------------------------------------------------------------
    # Write Operations
    # -------------------------------------------------------------------------

    # -- Task dispatch methods are in BackgroundProcessingMixin --
    # (_task_idempotency_key, _enqueue_*_background, _dispatch_after_write_flow,
    #  _load_after_write_state_doc, _store_write_context, _consume_write_context)

    def _upsert(
        self,
        id: str,
        content: str,
        *,
        tags: Optional[dict] = None,
        summary: Optional[str] = None,
        system_tags: dict[str, str],
        created_at: Optional[str] = None,
        force: bool = False,
        queue_summarize: bool = True,
    ) -> Item:
        """Core upsert logic used by put()."""
        # Wait for background reconciliation to finish before writing.
        # The reconcile thread and main thread both access the embedding
        # provider, ChromaDB, and SQLite — concurrent access causes hangs
        # with ChromaDB's Rust bindings and double model loading.
        self._reconcile_done.wait(timeout=120)

        doc_coll = self._resolve_doc_collection()
        chroma_coll = self._resolve_chroma_collection()

        # Deferred init tasks (best-effort — don't block user writes)
        if self._needs_sysdoc_migration:
            self._needs_sysdoc_migration = False  # Clear before call (migration calls remember → _upsert)
            try:
                self._migrate_system_documents()
            except Exception as e:
                logger.warning("System doc migration deferred: %s", e, exc_info=True)

        # Get existing item to preserve tags (check document store first, fall back to ChromaDB)
        existing_tags = {}
        existing_doc = self._document_store.get(doc_coll, id)
        if existing_doc:
            existing_tags = filter_non_system_tags(existing_doc.tags)
        else:
            existing = self._store.get(chroma_coll, id)
            if existing:
                existing_tags = filter_non_system_tags(existing.tags)

        # Preserve analysis watermark across puts (incremental analysis needs
        # the version baseline even when content changes).
        _prev_analyzed_version = None
        if existing_doc:
            _prev_analyzed_version = existing_doc.tags.get("_analyzed_version")

        # Compute content hash for change detection
        new_hash = _content_hash(content)

        # Build tags: existing + config + env + user, then replace system tags.
        merged_tags = {**existing_tags}

        if self._default_tags:
            _merge_tags_additive(merged_tags, self._default_tags)

        _merge_tags_additive(merged_tags, self._env_tags)

        if tags:
            user_tags = casefold_tags(filter_non_system_tags(tags))
            # Validate constrained tags (only user-provided, not existing/env)
            self._validate_constrained_tags(user_tags)
            # Singular enforcement: clear existing values so merge replaces
            singular_keys = self._get_singular_keys(
                k for k in user_tags if tag_values(user_tags, k) != [""]
            )
            if singular_keys:
                self._validate_singular_tags(user_tags, singular_keys)
            for key in user_tags:
                values = tag_values(user_tags, key)
                if values == [""]:
                    merged_tags.pop(key, None)
                else:
                    if key in singular_keys:
                        merged_tags.pop(key, None)
                    _merge_tags_additive(merged_tags, {key: values})

        # Track content length as a system tag (always updated)
        system_tags["_content_length"] = str(len(content))

        _merge_tags_additive(merged_tags, system_tags, replace_system=True)

        # Restore analysis version watermark (dropped by filter_non_system_tags)
        if _prev_analyzed_version and "_analyzed_version" not in merged_tags:
            merged_tags["_analyzed_version"] = _prev_analyzed_version

        # Change detection (before embedding to allow early return)
        content_unchanged = (
            existing_doc is not None
            and existing_doc.content_hash == new_hash
        )
        tags_changed = (
            existing_doc is not None
            and _user_tags_changed(existing_doc.tags, merged_tags)
        )

        # Backfill system tags for items stored before they were tracked
        if existing_doc is not None:
            backfill_tags = {}
            for sys_key in ("_content_type", "_content_length"):
                if sys_key in merged_tags and sys_key not in existing_doc.tags:
                    backfill_tags[sys_key] = merged_tags[sys_key]
            if backfill_tags:
                self._document_store.patch_head_tags(doc_coll, id, backfill_tags)

        # Early return: content and tags unchanged.
        # Still dispatch after-write flow — it will no-op if processing
        # is already complete, but re-enqueues if a prior purge dropped
        # unfinished work.
        if content_unchanged and not tags_changed and summary is None and not force:
            logger.debug("Content and tags unchanged for %s", id)
            if queue_summarize and not id.startswith("."):
                self._dispatch_after_write_flow(
                    item_id=id,
                    content=content,
                    tags=dict(existing_doc.tags),
                )
            return _record_to_item(existing_doc, changed=False)

        # Determine summary
        max_len = self._config.max_summary_length
        is_system_doc = id.startswith(".")
        if summary is not None:
            if not is_system_doc and len(summary) > max_len:
                import warnings
                warnings.warn(
                    f"Summary exceeds max_summary_length ({len(summary)} > {max_len}), truncating",
                    UserWarning,
                    stacklevel=3
                )
                summary = summary[:max_len]
            final_summary = summary
        elif is_system_doc:
            # System docs (.prompt/*, .tag/*, .meta/*) store full content
            # as the summary — they are authored content, not items to summarize.
            final_summary = content
        elif content_unchanged and tags_changed:
            logger.debug("Tags changed for %s", id)
            final_summary = existing_doc.summary
            # Only queue re-summarization if the summary is NOT the full
            # original content (i.e. it was already truncated/summarized).
            # If content_hash == hash(summary), the summary IS the content
            # and must never be overwritten by an LLM summary.
            summary_is_content = (
                existing_doc.content_hash == _content_hash(existing_doc.summary)
            )
            if queue_summarize and len(content) > max_len and not summary_is_content:
                self._enqueue_task_background(
                    task_type="summarize",
                    id=id,
                    doc_coll=doc_coll,
                    content=content,
                    tags=merged_tags,
                )
        elif len(content) <= max_len:
            final_summary = content
            # Content IS the summary — mark as already summarized so no
            # future summarize task can overwrite the original content.
            merged_tags["_summarized_hash"] = new_hash
        else:
            final_summary = content[:max_len] + "..."
            if queue_summarize:
                self._enqueue_task_background(
                    task_type="summarize",
                    id=id,
                    doc_coll=doc_coll,
                    content=content,
                    tags=merged_tags,
                )

        # Cloud mode: defer embedding to background worker for faster response.
        # The doc store write happens immediately; the note is findable by
        # tags/FTS/ID right away. Similarity search works once the
        # background worker computes and stores the embedding.
        if not self._is_local:
            result, content_changed = self._document_store.upsert(
                collection=doc_coll,
                id=id,
                summary=final_summary,
                tags=merged_tags,
                content_hash=new_hash,
                content_hash_full=_content_hash_full(content),
                created_at=created_at,
            )
            # Try embedding dedup before enqueueing (saves network round-trip)
            if not content_unchanged:
                donor_embedding = self._try_dedup_embedding(
                    doc_coll, chroma_coll, new_hash, id, content,
                )
                if donor_embedding is not None:
                    self._store.upsert(
                        collection=chroma_coll, id=id,
                        embedding=donor_embedding,
                        summary=final_summary,
                        tags=casefold_tags_for_index(merged_tags),
                    )
                    return _record_to_item(result, changed=True)
            # Enqueue embedding task (content needed for embedding computation)
            embed_meta = {}
            if existing_doc is not None and not content_unchanged:
                embed_meta["content_changed"] = True
            self._pending_queue.enqueue(
                id, doc_coll, content,
                task_type="embed",
                metadata=embed_meta,
            )
            return _record_to_item(result, changed=not content_unchanged)

        # Local mode: compute embedding synchronously
        # If no embedding provider, write to document store only (data is safe;
        # embeddings are filled in by reconciliation when a provider appears).
        _has_embeddings = self._config.embedding is not None
        embedding = None
        if _has_embeddings:
            if content_unchanged:
                embedding = self._store.get_embedding(chroma_coll, id)
                if embedding is None:
                    embedding = self._try_dedup_embedding(
                        doc_coll, chroma_coll, new_hash, id, content,
                    )
                if embedding is None:
                    embedding = self._get_embedding_provider().embed(final_summary)
            else:
                embedding = self._try_dedup_embedding(doc_coll, chroma_coll, new_hash, id, content)
                if embedding is None:
                    embedding = self._get_embedding_provider().embed(final_summary)

        # Detect _inverse changes on tagdocs BEFORE storage overwrites old state
        if id.startswith(".tag/"):
            old_tagdoc_tags = existing_doc.tags if existing_doc else {}
            self._process_tagdoc_inverse_change(id, merged_tags, old_tagdoc_tags, doc_coll)
            # Invalidate cached tagdoc for this key
            tag_key = id.removeprefix(".tag/").split("/")[0]
            self._tagdoc_cache.pop(tag_key, None)

        # Save old embedding before ChromaDB upsert overwrites it (for version archival)
        old_embedding = None
        if _has_embeddings and existing_doc is not None and not content_unchanged:
            old_embedding = self._store.get_embedding(chroma_coll, id)

        # Dual-write: document store (canonical) + ChromaDB (embedding index)
        result, content_changed = self._document_store.upsert(
            collection=doc_coll,
            id=id,
            summary=final_summary,
            tags=merged_tags,
            content_hash=new_hash,
            content_hash_full=_content_hash_full(content),
            created_at=created_at,
        )

        if _has_embeddings:
            self._store.upsert(
                collection=chroma_coll,
                id=id,
                embedding=embedding,
                summary=final_summary,
                tags=casefold_tags_for_index(merged_tags),
            )

            # If content changed and we archived a version, also store versioned embedding
            if existing_doc is not None and content_changed:
                max_ver = self._document_store.max_version(doc_coll, id)
                if max_ver > 0:
                    if old_embedding is None:
                        old_embedding = self._get_embedding_provider().embed(existing_doc.summary)
                    self._store.upsert_version(
                        collection=chroma_coll,
                        id=id,
                        version=max_ver,
                        embedding=old_embedding,
                        summary=existing_doc.summary,
                        tags=casefold_tags_for_index(existing_doc.tags),
                    )

        # Process tag-driven edges (_inverse on tagdocs)
        self._process_edge_tags(id, merged_tags, existing_tags, doc_coll)

        # Spawn background processor if needed (local only — uses filesystem locks)
        if (
            queue_summarize
            and summary is None
            and len(content) > max_len
            and (not content_unchanged or tags_changed or force)
        ):
            self._spawn_processor()

        return _record_to_item(result, changed=not content_unchanged)

    def _put_direct(
        self,
        content: Optional[str] = None,
        *,
        uri: Optional[str] = None,
        id: Optional[str] = None,
        summary: Optional[str] = None,
        tags: Optional[TagMap] = None,
        created_at: Optional[str] = None,
        force: bool = False,
        queue_background_tasks: bool = True,
        capture_write_context: bool = False,
    ) -> Item:
        """Store content in the memory.

        Provide either inline content or a URI to fetch — not both.

        **Inline mode** (content provided):
        - Stores text directly. Auto-generates an ID if not provided.
        - Short content is used verbatim as summary. Large content gets
          async summarization (truncated placeholder stored immediately).

        **URI mode** (uri provided):
        - Fetches the document, extracts text, generates embeddings.

        Args:
            content: Inline text content to store.
            uri: URI of a document to fetch and index.
            id: Explicit item ID (auto-generated if omitted).
            summary: Pre-computed summary (skips auto-summarization).
            tags: Tag map to attach to the item.
            created_at: Override creation timestamp (ISO 8601).
            force: Re-process even if content is unchanged.
        """
        if content is not None and uri is not None:
            raise ValueError("Provide content or uri, not both")
        if content is None and uri is None:
            raise ValueError("Either content or uri is required")

        if tags:
            tags = self._validate_write_tags(tags)

        # Parts are immutable — block put() with part-like IDs
        effective_id = id or uri or ""
        if is_part_id(effective_id):
            raise ValueError(
                f"Cannot modify part directly: {effective_id!r}. "
                "Parts are managed by analyze()."
            )

        # Enforce required tags (skip for system docs with dot-prefix IDs)
        if self._config.required_tags and not effective_id.startswith("."):
            user_tags = {k: v for k, v in (tags or {}).items()
                         if not k.startswith(SYSTEM_TAG_PREFIX)} if tags else {}
            missing = [t for t in self._config.required_tags if t not in user_tags]
            if missing:
                raise ValueError(f"Required tags missing: {', '.join(missing)}")

        if uri is not None:
            # URI mode: fetch document, extract content, store
            uri = normalize_id(uri)
            # When --id is provided, use it as the document ID; otherwise the URI is the ID
            doc_id = normalize_id(id) if id else uri

            # Fast path for local files: skip expensive read if stat unchanged
            is_file_uri = uri.startswith("file://") or uri.startswith("/")
            if is_file_uri and summary is None and not force:
                try:
                    fpath = Path(uri.removeprefix("file://")).resolve()
                    st = fpath.stat()
                    doc_coll = self._resolve_doc_collection()
                    existing = self._document_store.get(doc_coll, doc_id)
                    if (existing
                            and existing.tags.get("_file_mtime_ns") == str(st.st_mtime_ns)
                            and existing.tags.get("_file_size") == str(st.st_size)):
                        # Backfill _content_type for items stored before it was tracked
                        if "_content_type" not in existing.tags:
                            from .providers.documents import FileDocumentProvider
                            ct = FileDocumentProvider.EXTENSION_TYPES.get(fpath.suffix.lower())
                            if ct:
                                self._document_store.patch_head_tags(
                                    doc_coll, doc_id, {"_content_type": ct},
                                )
                        # File stat unchanged — check if tags would also be unchanged
                        if not tags or not _user_tags_changed(
                                existing.tags,
                                {**filter_non_system_tags(existing.tags),
                                 **casefold_tags(tags)}):
                            logger.debug("File stat unchanged, skipping read for %s", doc_id)
                            return _record_to_item(existing, changed=False)
                except OSError:
                    pass  # Fall through to normal fetch

            doc = self._document_provider.fetch(uri)

            # Extract frontmatter from markdown files
            _uri_lower = uri.lower()
            _is_markdown = any(_uri_lower.endswith(ext) for ext in _MARKDOWN_EXTENSIONS)
            if _is_markdown and doc.content:
                body, fm_tags = _extract_markdown_frontmatter(doc.content)
                if fm_tags:
                    fm_tags = self._validate_tag_map(
                        fm_tags,
                        source=f"Frontmatter tags in {uri}",
                        check_constraints=False,
                    )
                if body != doc.content or fm_tags:
                    doc = Document(
                        uri=doc.uri,
                        content=body,
                        content_type=doc.content_type,
                        metadata=doc.metadata,
                        tags={**(doc.tags or {}), **fm_tags},
                    )

            # Merge provider-extracted tags with user tags (user wins on collision)
            merged_tags: dict[str, str] | None = None
            if doc.tags or tags:
                merged_tags = {}
                if doc.tags:
                    merged_tags.update(
                        self._validate_tag_map(
                            filter_non_system_tags(doc.tags),
                            source=f"Document-derived tags from {uri}",
                            check_constraints=False,
                        )
                    )
                if tags:
                    merged_tags.update(tags)

            system_tags = {"_source": "uri"}
            if doc.content_type:
                system_tags["_content_type"] = doc.content_type

            # Store file stat for fast-path change detection on next put
            if is_file_uri and doc.metadata:
                try:
                    fpath = Path(uri.removeprefix("file://")).resolve()
                    st = fpath.stat()
                    system_tags["_file_mtime_ns"] = str(st.st_mtime_ns)
                    system_tags["_file_size"] = str(st.st_size)
                except OSError:
                    pass

            # Use file birthtime as created_at for new items
            if created_at is None and is_file_uri and doc.metadata:
                birthtime = doc.metadata.get("birthtime")
                if birthtime is not None:
                    created_at = datetime.fromtimestamp(
                        birthtime, tz=timezone.utc
                    ).isoformat()

            # Store source URI as system tag when using custom ID
            if doc_id != uri:
                system_tags["_source_uri"] = uri

            result = self._upsert(
                doc_id, doc.content,
                tags=merged_tags, summary=summary,
                system_tags=system_tags,
                created_at=created_at,
                force=force,
                queue_summarize=queue_background_tasks,
            )

            ocr_pages = (doc.metadata or {}).get("_ocr_pages")

            if capture_write_context:
                self._store_write_context(
                    result.id,
                    {
                        "content": doc.content,
                        "uri": uri,
                        "ocr_pages": list(ocr_pages or []),
                        "content_type": doc.content_type or "",
                    },
                )

            # Post-write background tasks are driven by the after-write
            # state doc — do NOT hardcode task enqueues here.  See
            # _dispatch_after_write_flow() and builtin_state_docs.py.
            if queue_background_tasks:
                self._dispatch_after_write_flow(
                    item_id=result.id,
                    content=doc.content,
                    uri=uri,
                    content_type=doc.content_type or "",
                    tags=merged_tags,
                    summary=summary,
                    ocr_pages=ocr_pages,
                )

            # Process email attachments as child items with edges
            attachments = (doc.metadata or {}).get("_attachments")
            if attachments:
                self._put_email_attachments(
                    parent_id=result.id,
                    attachments=attachments,
                    parent_tags=merged_tags,
                    created_at=created_at,
                    queue_background_tasks=queue_background_tasks,
                )

            return result
        else:
            # Inline mode: store content directly
            # Enforce inline length limit at the API level so all paths
            # (CLI, MCP, direct API) are bounded identically.
            is_system = id and id.startswith(".")
            if not is_system and len(content) > self._config.max_inline_length:
                raise ValueError(
                    f"Inline content too long ({len(content)} chars, "
                    f"max {self._config.max_inline_length}). "
                    "Use a file URI instead: keep put file:///path/to/file"
                )

            if id is None:
                # Match CLI/MCP behavior: default inline IDs are content-addressed.
                id = _text_content_id(content)
            else:
                id = normalize_id(id)

            result = self._upsert(
                id, content,
                tags=tags, summary=summary,
                system_tags={"_source": "inline"},
                created_at=created_at,
                force=force,
                queue_summarize=queue_background_tasks,
            )
            if capture_write_context:
                self._store_write_context(
                    result.id,
                    {
                        "content": str(content or ""),
                        "uri": "",
                        "ocr_pages": [],
                        "content_type": "",
                    },
                )
            # Post-write background tasks are driven by the after-write
            # state doc — do NOT hardcode task enqueues here.  See
            # _dispatch_after_write_flow() and builtin_state_docs.py.
            if queue_background_tasks:
                self._dispatch_after_write_flow(
                    item_id=result.id,
                    content=str(content or ""),
                    tags=tags,
                    summary=summary,
                )
            return result

    def _put_email_attachments(
        self,
        parent_id: str,
        attachments: list[dict],
        parent_tags: dict,
        created_at: Optional[str] = None,
        queue_background_tasks: bool = True,
    ) -> list[Item]:
        """Store email attachments as child items with edges to the parent.

        Each attachment is put via the normal URI path (so it gets text
        extraction, OCR, etc.) with an ``attachment`` edge-tag pointing
        to the parent email.  The child ID uses a fragment suffix:
        ``{parent_id}#att-{N}``.

        Temp files in ~/.cache/keep/email-att/ are NOT cleaned up here
        because background tasks (OCR, describe) need the files later.
        """
        results = []

        for i, att in enumerate(attachments, 1):
            att_path = att.get("path")
            if not att_path:
                continue

            child_id = f"{parent_id}#att-{i}"
            filename = att.get("filename", f"attachment-{i}")

            # Inherit key context tags from the parent email
            child_tags: dict = {}
            for key in ("from", "to", "cc", "bcc", "date", "subject"):
                if key in parent_tags:
                    child_tags[key] = parent_tags[key]
            child_tags["attachment"] = parent_id
            child_tags["filename"] = filename

            try:
                result = self._put_direct(
                    uri=att_path,
                    id=child_id,
                    tags=child_tags,
                    created_at=created_at,
                    queue_background_tasks=queue_background_tasks,
                )
                results.append(result)
                logger.info(
                    "Stored email attachment %s (%s, %s)",
                    child_id, filename, att.get("content_type", ""),
                )
            except Exception as e:
                logger.warning(
                    "Failed to store email attachment %s (%s): %s",
                    child_id, filename, e,
                )

        return results

    def put(
        self,
        content: Optional[str] = None,
        *,
        uri: Optional[str] = None,
        id: Optional[str] = None,
        summary: Optional[str] = None,
        tags: Optional[TagMap] = None,
        created_at: Optional[str] = None,
        force: bool = False,
    ) -> Item:
        """Store content in memory."""
        return self._put_direct(
            content=content,
            uri=uri,
            id=id,
            summary=summary,
            tags=tags,
            created_at=created_at,
            force=force,
        )

    # -------------------------------------------------------------------------
    # Query Operations
    # -------------------------------------------------------------------------
    # _apply_recency_decay, _rrf_fuse, _deep_tag_follow, _deep_edge_follow,
    # _deep_follow_via_flow are provided by SearchAugmentationMixin.

    def find(
        self,
        query: Optional[str] = None,
        *,
        tags: Optional[TagMap] = None,
        similar_to: Optional[str] = None,
        limit: int = 10,
        since: Optional[str] = None,
        until: Optional[str] = None,
        include_self: bool = False,
        include_hidden: bool = False,
        deep: bool = False,
    ) -> list[Item]:
        """Find items by hybrid search (semantic + FTS5) or similarity to an existing note.

        When an embedding provider is configured, runs both semantic and full-text
        search and fuses results with Reciprocal Rank Fusion (RRF). Falls back to
        FTS-only when no embedding provider is available.

        Exactly one of `query` or `similar_to` must be provided.

        Args:
            query: Search query text
            tags: Optional tag filter — only return items matching all specified tags
            similar_to: Find items similar to this note ID
            limit: Maximum results to return
            since: Only include items updated since (ISO duration like P3D, or date)
            until: Only include items updated before (ISO duration like P3D, or date)
            include_self: Include the queried item in results (only with similar_to)
            include_hidden: Include system notes (dot-prefix IDs)
            deep: Follow tags from results to discover related items
        """
        if query and similar_to:
            raise ValueError("Specify either query or similar_to, not both")
        if not query and not similar_to:
            raise ValueError("Specify either query or similar_to")

        chroma_coll = self._resolve_chroma_collection()
        doc_coll = self._resolve_doc_collection()

        # Deep search needs edges (created by tag definitions with
        # _inverse).  Ensure system-doc migration has run and, if it
        # enqueued edge-backfill tasks, process them synchronously so
        # edges are available for this query.
        if deep and self._needs_sysdoc_migration:
            try:
                self._migrate_system_documents()
                # Drain any edge-backfill tasks that migration enqueued
                # so edges are ready for this search.
                self._flush_edge_backfill(doc_coll)
                self._needs_sysdoc_migration = False
            except Exception as e:
                logger.warning("System doc migration deferred: %s", e, exc_info=True)

        embedding = None  # Set in semantic/similar_to branches

        # Build where clause from tags filter
        where = None
        casefolded_tags: Optional[dict] = None
        if tags:
            casefolded_tags = casefold_tags(tags)
            for k in casefolded_tags:
                if not k.startswith(SYSTEM_TAG_PREFIX):
                    validate_tag_key(k)
            where = self._build_tag_where(casefolded_tags)

        if similar_to:
            # Similar-to mode: use stored embedding from existing item
            similar_to = normalize_id(similar_to)
            item = self._store.get(chroma_coll, similar_to)
            if item is None:
                raise KeyError(f"Item not found: {similar_to}")

            embedding = self._store.get_embedding(chroma_coll, similar_to)
            if embedding is None:
                embedding = self._get_embedding_provider().embed(item.summary)
            actual_limit = (limit + 1 if not include_self else limit) * 3
            if deep:
                actual_limit = max(actual_limit, 30)
            results = self._store.query_embedding(chroma_coll, embedding, limit=actual_limit, where=where)

            if not include_self:
                results = [r for r in results if r.id != similar_to]

            items = [r.to_item() for r in results]
            items = self._apply_recency_decay(items)

        elif self._config.embedding is not None:
            # Hybrid search: semantic + FTS5, fused with RRF.
            # Each list over-fetches independently so RRF can discover
            # items that rank well in one signal but poorly in the other.
            embedding = self._get_embedding_provider().embed(query)
            sem_fetch = max(limit * 10, 200)
            fts_fetch = max(limit * 10, 100)
            if deep:
                sem_fetch = max(sem_fetch, 30)

            sem_results = self._store.query_embedding(
                chroma_coll, embedding, limit=sem_fetch, where=where,
            )
            sem_items = [r.to_item() for r in sem_results]
            sem_items = self._apply_recency_decay(sem_items)

            fts_rows = self._document_store.query_fts(
                doc_coll, query, limit=fts_fetch, tags=casefolded_tags,
            )
            fts_items = [Item(id=r[0], summary=r[1]) for r in fts_rows]

            if fts_items:
                items = self._rrf_fuse(sem_items, fts_items)
            else:
                # FTS unavailable or no matches — use semantic results as-is
                items = sem_items

        else:
            # No embedding provider — FTS only
            fetch_limit = limit * 3
            fts_rows = self._document_store.query_fts(
                doc_coll, query, limit=fetch_limit, tags=casefolded_tags,
            )
            items = [Item(id=r[0], summary=r[1]) for r in fts_rows]

        # Hydrate search hits from canonical SQLite tags so user tags remain
        # available even when Chroma metadata stores marker fields only.
        hydrated: list[Item] = []
        for item in items:
            base_id = item.tags.get(
                "_base_id",
                item.id.split("@")[0] if "@" in item.id else item.id,
            )
            head = self._document_store.get(doc_coll, base_id)
            if head is None:
                hydrated.append(item)
                continue
            head_item = _record_to_item(head, score=item.score)
            merged_tags = dict(head_item.tags)
            merged_tags.update(item.tags or {})
            hydrated.append(Item(
                id=item.id,
                summary=item.summary or head_item.summary,
                tags=merged_tags,
                score=item.score,
            ))
        items = hydrated

        # Deep follow: prefer edge-following when edges exist in the store,
        # fall back to tag-following for stores without edges.
        deep_groups: dict[str, list[Item]] = {}
        injected_entity_ids: set[str] = set()
        if deep and embedding is not None:
            if self._document_store.has_edges(doc_coll):
                # For similar_to mode, use the anchor item's summary as FTS query
                deep_query = query if query else ""

                # Entity injection: if query mentions known edge targets
                # by name, inject them as synthetic primaries so their
                # edges get traversed even if they didn't rank in search.
                deep_items = list(items)
                entity_hits: list[str] = []
                if deep_query:
                    # Only consider the top display window as "already present"
                    _ENTITY_WINDOW = 10
                    top_ids = {(i.id.split("@")[0] if "@" in i.id else i.id)
                               for i in items[:_ENTITY_WINDOW]}
                    entity_hits = self._document_store.find_edge_targets(
                        doc_coll, deep_query)
                    # Insert at front so entities are within top_k window
                    inject_pos = 0
                    for eid in entity_hits:
                        if eid not in top_ids:
                            deep_items.insert(inject_pos, Item(id=eid, summary="", tags={}, score=0.5))
                            injected_entity_ids.add(eid)
                            inject_pos += 1
                    # Remove matched entity phrases from deep FTS query so
                    # activity/content terms dominate deep evidence. This
                    # avoids over-blocking standalone content words.
                    if entity_hits:
                        cleaned = deep_query
                        for eid in sorted(entity_hits, key=len, reverse=True):
                            parts = re.findall(r"[a-z0-9]+", eid.lower())
                            if not parts:
                                continue
                            # Match phrase tokens with flexible separators.
                            pattern = r"\b" + r"[^a-z0-9]+".join(
                                re.escape(tok) for tok in parts
                            ) + r"\b"
                            cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
                        kept = re.findall(r"[a-z0-9]+", cleaned.lower())
                        if kept:
                            deep_query = " ".join(kept)

                # Exclude only a few top primaries from deep results.
                # With deep_primary_cap, most primaries get dropped so
                # they should remain available as deep sub-items.
                # Entities are never excluded (they're the group hubs).
                _DEEP_EXCLUDE = 3
                exclude = set()
                for i in items[:_DEEP_EXCLUDE]:
                    pid = i.id.split("@")[0] if "@" in i.id else i.id
                    if pid not in injected_entity_ids:
                        exclude.add(pid)
                deep_groups = self._deep_edge_follow(
                    deep_items, chroma_coll, doc_coll,
                    query=deep_query,
                    embedding=embedding,
                    exclude_ids=exclude,
                )
                # Inject entities that produced deep groups into the
                # primary list so they appear as result items and
                # pass the final_ids filter in the remapping step.
                if deep_groups:
                    item_ids = {(i.id.split("@")[0] if "@" in i.id else i.id)
                                for i in items}
                    for gk in deep_groups:
                        if gk not in item_ids:
                            head = self._document_store.get(doc_coll, gk)
                            if head:
                                items.append(_record_to_item(head, score=0.5))
                            else:
                                items.append(Item(
                                    id=gk, summary="", tags={}, score=0.5,
                                ))
            else:
                # For similar_to mode, use the anchor item's summary
                # as the flow query since the find action requires one.
                flow_query = query or ""
                if not flow_query and similar_to:
                    anchor = self._document_store.get(doc_coll, similar_to)
                    if anchor:
                        flow_query = getattr(anchor, "summary", "") or ""
                deep_groups = self._deep_follow_via_flow(
                    query=flow_query,
                    limit=limit,
                    embedding=embedding,
                )

        # Apply common filters
        if since is not None or until is not None:
            # Enrich _updated_date from SQLite for items missing it
            # (e.g. version refs whose ChromaDB metadata lacks the tag)
            all_items = list(items)
            for g in deep_groups.values():
                all_items.extend(g)
            _enrich_updated_date(all_items, self._document_store, doc_coll)
            items = _filter_by_date(items, since=since, until=until)
            deep_groups = {pid: _filter_by_date(g, since=since, until=until)
                          for pid, g in deep_groups.items()}
        if not include_hidden:
            items = [i for i in items if not _is_hidden(i)]
            deep_groups = {pid: [i for i in g if not _is_hidden(i)]
                          for pid, g in deep_groups.items()}
        deep_groups = {pid: g for pid, g in deep_groups.items() if g}

        # Part-to-parent uplift: replace part hits with their parent
        # documents, carrying _focus_part so the formatter can window
        # the parts manifest around the hit.  Dedup: keep the highest-
        # scoring part when multiple parts of the same parent match.
        uplifted: list[Item] = []
        seen_parents: dict[str, int] = {}  # parent_id -> index in uplifted
        for item in items:
            if is_part_id(item.id):
                parent_id = item.tags.get("_base_id", item.id.split("@")[0])
                part_num = item.tags.get("_part_num")
                # FTS-originated items lack tags — parse part_num from ID
                if not part_num:
                    suffix = item.id.rsplit("@p", 1)
                    if len(suffix) == 2 and suffix[1].isdigit():
                        part_num = suffix[1]
                if parent_id in seen_parents:
                    # Already have this parent — skip (first hit had higher score)
                    continue
                parent_doc = self._document_store.get(doc_coll, parent_id)
                if parent_doc:
                    parent_item = _record_to_item(parent_doc)
                    parent_tags = dict(parent_item.tags)
                    if part_num:
                        parent_tags["_focus_part"] = part_num
                        parent_tags["_focus_summary"] = item.summary
                    uplifted.append(Item(
                        id=parent_id, summary=parent_item.summary,
                        tags=parent_tags, score=item.score,
                    ))
                    seen_parents[parent_id] = len(uplifted) - 1
                else:
                    uplifted.append(item)  # Parent gone — keep raw part
            elif "@v" in item.id:
                # Version hit — uplift to parent, preserving hit version
                parent_id = item.id.rsplit("@v", 1)[0]
                version_str = item.id.rsplit("@v", 1)[1]
                if parent_id in seen_parents:
                    continue
                parent_doc = self._document_store.get(doc_coll, parent_id)
                if parent_doc:
                    parent_item = _record_to_item(parent_doc)
                    parent_tags = dict(parent_item.tags)
                    if version_str.isdigit():
                        parent_tags["_focus_version"] = version_str
                        parent_tags["_focus_summary"] = item.summary
                    uplifted.append(Item(
                        id=parent_id, summary=parent_item.summary,
                        tags=parent_tags, score=item.score,
                    ))
                    seen_parents[parent_id] = len(uplifted) - 1
                else:
                    uplifted.append(item)  # Parent gone — keep raw version
            else:
                # Regular document — dedup against uplifted parents
                if item.id in seen_parents:
                    continue
                uplifted.append(item)
                seen_parents[item.id] = len(uplifted) - 1
        items = uplifted

        # Remap deep_groups: uplift keys AND items (parts → parents), dedup.
        # Only exclude deep items that appear in the top primary results
        # the user will actually see — not the full over-fetched pool
        # (which may swallow all deep candidates when fetch_limit >> display).
        _DEEP_EXCLUDE_WINDOW = 10
        if deep_groups:
            final_ids = set()
            for i in items[:min(limit, _DEEP_EXCLUDE_WINDOW)]:
                final_ids.add(i.id)
                # Include parent ID so version hits match deep group keys
                if "@" in i.id:
                    final_ids.add(i.id.split("@")[0])
            # Ensure injected entities pass the filter even if they're
            # beyond the limit window (they were appended, not ranked)
            final_ids.update(injected_entity_ids & deep_groups.keys())
            remapped: dict[str, list[Item]] = {}
            remap_keys: dict[str, set[str]] = {}
            for source_id, group in deep_groups.items():
                # Uplift the group key (source primary) to parent
                key_parent = source_id.split("@")[0] if "@" in source_id else source_id
                is_entity_group = key_parent in injected_entity_ids
                bucket = remapped.setdefault(key_parent, [])
                seen_keys = remap_keys.setdefault(key_parent, set())
                # Uplift each deep item to its parent too
                for item in group:
                    deep_parent_id = item.id.split("@")[0] if "@" in item.id else item.id
                    # Skip if this deep item IS a final primary result —
                    # UNLESS this is an entity group where overlap is
                    # intentional (deep_primary_cap handles dedup at render)
                    if deep_parent_id in final_ids and not is_entity_group:
                        continue
                    anchor_key = (
                        item.tags.get("_anchor_id")
                        or f"{item.id}|{item.tags.get('_focus_version', '')}|"
                           f"{item.tags.get('_focus_part', '')}|"
                           f"{item.tags.get('_focus_summary', '')}"
                    )
                    if anchor_key in seen_keys:
                        continue
                    seen_keys.add(anchor_key)
                    if "@" in item.id:
                        parent_doc = self._document_store.get(doc_coll, deep_parent_id)
                        if parent_doc:
                            pi = _record_to_item(parent_doc)
                            tags = dict(pi.tags)
                            for k, v in (item.tags or {}).items():
                                if k.startswith("_focus_") or k.startswith("_anchor_") or k == "_lane":
                                    tags[k] = v
                            bucket.append(
                                Item(
                                    id=item.id,
                                    summary=pi.summary,
                                    tags=tags,
                                    score=item.score,
                                )
                            )
                        else:
                            bucket.append(item)
                    else:
                        bucket.append(item)
            deep_groups = {
                pid: sorted(bucket, key=lambda x: x.score or 0, reverse=True)
                for pid, bucket in remapped.items()
                if pid in final_ids
            }

        final = items[:limit]
        # Promote injected entities into the final results so their
        # deep groups can be rendered.  Replace the lowest-scored
        # non-entity item to stay within the limit.
        if deep_groups and injected_entity_ids:
            final_base_ids = {(i.id.split("@")[0] if "@" in i.id else i.id)
                              for i in final}
            for eid in injected_entity_ids:
                if eid in deep_groups and eid not in final_base_ids:
                    entity_item = next(
                        (i for i in items if i.id == eid), None)
                    if entity_item and final:
                        # Mark as entity so renderer can prioritize
                        entity_tags = dict(entity_item.tags) if entity_item.tags else {}
                        entity_tags["_entity"] = "true"
                        entity_item = Item(
                            id=entity_item.id, summary=entity_item.summary,
                            tags=entity_tags, score=entity_item.score,
                        )
                        # Replace the lowest-scored item that has no
                        # deep group (preserve items that do)
                        worst_idx = None
                        worst_score = float('inf')
                        for idx, fi in enumerate(final):
                            fi_base = fi.id.split("@")[0] if "@" in fi.id else fi.id
                            if fi_base not in deep_groups and fi.id not in deep_groups:
                                if (fi.score or 0) < worst_score:
                                    worst_score = fi.score or 0
                                    worst_idx = idx
                        if worst_idx is not None:
                            final[worst_idx] = entity_item
                        # else: all items have deep groups — skip to stay within limit
        # Enrich tags from SQLite (ChromaDB stores casefolded values;
        # SQLite has the canonical original-case values for display)
        def _enrich_from_sqlite(items_to_enrich):
            enriched = []
            for item in items_to_enrich:
                doc = self._document_store.get(doc_coll, item.id)
                if not doc and "@" in item.id:
                    base_id = item.id.split("@")[0]
                    doc = self._document_store.get(doc_coll, base_id)
                if doc:
                    enriched_item = _record_to_item(doc, score=item.score)
                    tags = enriched_item.tags
                    focus = item.tags.get("_focus_part")
                    if focus:
                        tags["_focus_part"] = focus
                    focus_summary = item.tags.get("_focus_summary")
                    if focus_summary:
                        tags["_focus_summary"] = focus_summary
                    focus_version = item.tags.get("_focus_version")
                    if focus_version:
                        tags["_focus_version"] = focus_version
                    entity_marker = item.tags.get("_entity")
                    if entity_marker:
                        tags["_entity"] = entity_marker
                    enriched.append(Item(
                        id=item.id, summary=item.summary,
                        tags=tags, score=item.score,
                    ))
                else:
                    enriched.append(item)
            return enriched

        if final:
            self._document_store.touch_many(doc_coll, [i.id for i in final])
            final = _enrich_from_sqlite(final)
        # Enrich deep group items too (same casefolded-tag issue)
        if deep_groups:
            deep_groups = {
                pid: _enrich_from_sqlite(group)
                for pid, group in deep_groups.items()
            }
        return FindResults(final, deep_groups=deep_groups)

    # -------------------------------------------------------------------------
    # State-doc flow binding mappers for get_context
    # -------------------------------------------------------------------------

    def _resolve_edge_refs(self, item: "Item", item_id: str) -> dict[str, list["EdgeRef"]]:
        """Resolve structural edge references (explicit + inverse) for display.

        Uses direct database queries rather than the generic traverse action,
        because edges require inverse-edge table lookups and explicit edge-tag
        resolution — operations the traverse action doesn't support.
        """
        doc_coll = self._resolve_doc_collection()
        edge_ref_index: dict[str, dict[str, EdgeRef]] = {}

        def _upsert(key: str, ref: EdgeRef, *, prefer_new: bool = False) -> None:
            refs_for_key = edge_ref_index.setdefault(key, {})
            existing = refs_for_key.get(ref.source_id)
            if existing is None:
                refs_for_key[ref.source_id] = ref
                return
            winner_date = (ref.date if prefer_new else existing.date) or \
                          (existing.date if prefer_new else ref.date)
            winner_summary = (ref.summary if prefer_new else existing.summary) or \
                             (existing.summary if prefer_new else ref.summary)
            refs_for_key[ref.source_id] = EdgeRef(
                source_id=ref.source_id, date=winner_date, summary=winner_summary,
            )

        # Explicit edge refs from current item's edge-tag values
        explicit_targets_by_key: dict[str, list[str]] = {}
        user_keys = [
            k for k in item.tags
            if not k.startswith(SYSTEM_TAG_PREFIX) and tag_values(item.tags, k)
        ]
        if user_keys:
            tagdoc_ids = [f".tag/{k}" for k in user_keys]
            tagdocs = self._document_store.get_many(doc_coll, tagdoc_ids)
            for key in user_keys:
                td = tagdocs.get(f".tag/{key}")
                if td is None or not td.tags.get("_inverse"):
                    continue
                seen_targets: set[str] = set()
                resolved_targets: list[str] = []
                for raw_target in tag_values(item.tags, key):
                    try:
                        target_id = normalize_id(parse_ref(raw_target)[0])
                    except ValueError:
                        continue
                    if target_id.startswith(".") or target_id in seen_targets:
                        continue
                    seen_targets.add(target_id)
                    resolved_targets.append(target_id)
                if resolved_targets:
                    explicit_targets_by_key[key] = resolved_targets

        if explicit_targets_by_key:
            target_ids = {
                tid for tids in explicit_targets_by_key.values() for tid in tids
            }
            target_docs = self._document_store.get_many(doc_coll, list(target_ids))
            for key, target_ids_for_key in explicit_targets_by_key.items():
                for target_id in target_ids_for_key:
                    doc = target_docs.get(target_id)
                    created = ""
                    summary = ""
                    if doc is not None:
                        created = local_date(
                            doc.tags.get("_created") or doc.tags.get("_updated") or ""
                        )
                        summary = doc.summary or ""
                    _upsert(key, EdgeRef(
                        source_id=target_id, date=created, summary=summary,
                    ))

        raw_edges = self._document_store.get_inverse_edges(doc_coll, item_id)
        if raw_edges:
            source_ids = list({src for _, src, _ in raw_edges})
            source_docs = self._document_store.get_many(doc_coll, source_ids)
            for inverse, source_id, created in raw_edges:
                doc = source_docs.get(source_id)
                _upsert(inverse, EdgeRef(
                    source_id=source_id,
                    date=local_date(created),
                    summary=doc.summary if doc else "",
                ), prefer_new=True)

        return {
            key: list(refs_by_source.values())
            for key, refs_by_source in edge_ref_index.items()
        }


    # get_context, resolve_version_offset, render_prompt, list_prompts,
    # get_similar_for_display, get_version_offset, resolve_meta,
    # resolve_inline_meta, _resolve_meta_queries, _rank_by_relevance,
    # _map_flow_similar, _map_flow_meta, _map_flow_parts
    # are provided by ContextResolutionMixin.

    def list_tags(
        self,
        key: Optional[str] = None,
    ) -> list[str]:
        """List distinct tag keys or values.

        Args:
            key: If provided, list distinct values for this key.
                 If None, list distinct tag keys.

        Returns:
            Sorted list of distinct keys or values
        """
        if key is not None:
            validate_tag_key(key)
        doc_coll = self._resolve_doc_collection()

        if key is None:
            return self._document_store.list_distinct_tag_keys(doc_coll)
        else:
            return self._document_store.list_distinct_tag_values(doc_coll, key)
    
    # -------------------------------------------------------------------------
    # Direct Access
    # -------------------------------------------------------------------------
    
    def get(self, id: str) -> Optional[Item]:
        """Retrieve a specific item by ID.

        Reads from document store (canonical), falls back to vector store for legacy data.
        Touches accessed_at on successful retrieval.
        """
        id = normalize_id(id)
        doc_coll = self._resolve_doc_collection()
        chroma_coll = self._resolve_chroma_collection()

        # Try document store first (canonical)
        try:
            doc_record = self._document_store.get(doc_coll, id)
        except Exception as e:
            logger.warning("DocumentStore.get(%s) failed: %s", id, e)
            if self._is_local and "malformed" in str(e):
                # SQLite-specific recovery — only for local backends
                if hasattr(self._document_store, '_try_runtime_recover'):
                    self._document_store._try_runtime_recover()
                # Retry once after recovery
                try:
                    doc_record = self._document_store.get(doc_coll, id)
                except Exception:
                    doc_record = None
            else:
                doc_record = None
        if doc_record:
            self._document_store.touch(doc_coll, id)
            return _record_to_item(doc_record)

        # Fall back to ChromaDB for legacy data
        result = self._store.get(chroma_coll, id)
        if result is None:
            return None
        return result.to_item()

    def get_version(
        self,
        id: str,
        offset: int = 0,
    ) -> Optional[Item]:
        """Get a specific version of a document by public selector.

        Selector semantics:
        - 0 = current version
        - 1 = previous version
        - 2 = two versions ago
        - ...
        - -1 = oldest archived version
        - -2 = second oldest archived version
        - ...

        Args:
            id: Document identifier
            offset: Public version selector

        Returns:
            Item if found, None if version doesn't exist
        """
        id = normalize_id(id)
        resolved_offset = self.resolve_version_offset(id, offset)
        if resolved_offset is None:
            return None
        doc_coll = self._resolve_doc_collection()

        if resolved_offset == 0:
            # Current version
            return self.get(id)

        # Get archived version
        version_info = self._document_store.get_version(doc_coll, id, resolved_offset)
        if version_info is None:
            return None

        return Item(
            id=id,
            summary=version_info.summary,
            tags=version_info.tags,
        )

    def list_versions(
        self,
        id: str,
        limit: int = 10,
    ) -> list[VersionInfo]:
        """List version history for a document.

        Returns versions in reverse chronological order (newest archived first).
        Does not include the current version.

        Args:
            id: Document identifier
            limit: Maximum versions to return

        Returns:
            List of VersionInfo, newest archived first
        """
        id = normalize_id(id)
        doc_coll = self._resolve_doc_collection()
        return self._document_store.list_versions(doc_coll, id, limit)

    def list_versions_around(
        self,
        id: str,
        version: int,
        radius: int = 2,
    ) -> list[VersionInfo]:
        """Return versions within `radius` of `version`, in chronological order.

        Useful for showing surrounding context when a specific version was
        matched during search.
        """
        id = normalize_id(id)
        doc_coll = self._resolve_doc_collection()
        return self._document_store.list_versions_around(
            doc_coll, id, version, radius,
        )

    def get_version_nav(
        self,
        id: str,
        current_version: Optional[int] = None,
        limit: int = 3,
    ) -> dict[str, list[VersionInfo]]:
        """Get version navigation info (prev/next) for display.

        Args:
            id: Document identifier
            current_version: The version being viewed (None = current/live version)
            limit: Max previous versions to return when viewing current

        Returns:
            Dict with 'prev' and optionally 'next' lists of VersionInfo.
        """
        doc_coll = self._resolve_doc_collection()
        return self._document_store.get_version_nav(doc_coll, id, current_version, limit)

    def exists(self, id: str) -> bool:
        """Check if an item exists in the store."""
        id = normalize_id(id)
        doc_coll = self._resolve_doc_collection()
        chroma_coll = self._resolve_chroma_collection()
        # Check document store first, then ChromaDB
        return self._document_store.exists(doc_coll, id) or self._store.exists(chroma_coll, id)
    
    def delete(
        self,
        id: str,
        *,
        delete_versions: bool = True,
    ) -> bool:
        """Delete an item from both stores.

        Args:
            id: Document identifier
            delete_versions: If True, also delete version history

        Returns:
            True if item existed and was deleted.
        """
        id = normalize_id(id)
        if is_part_id(id):
            raise ValueError(
                f"Cannot delete part directly: {id!r}. "
                "Use analyze() to replace parts, or delete the parent document."
            )
        doc_coll = self._resolve_doc_collection()
        chroma_coll = self._resolve_chroma_collection()
        # Delete from both stores (including versions)
        doc_deleted = self._document_store.delete(doc_coll, id, delete_versions=delete_versions)
        chroma_deleted = self._store.delete(chroma_coll, id, delete_versions=delete_versions)
        # Clean up edges (both directions)
        self._document_store.delete_edges_for_source(doc_coll, id)
        self._document_store.delete_edges_for_target(doc_coll, id)
        self._document_store.delete_version_edges_for_source(doc_coll, id)
        self._document_store.delete_version_edges_for_target(doc_coll, id)
        return doc_deleted or chroma_deleted

    def revert(self, id: str) -> Optional[Item]:
        """Revert to the previous version, or delete if no versions exist.

        Returns the restored item, or None if the item was fully deleted.
        """
        id = normalize_id(id)
        if is_part_id(id):
            raise ValueError(
                f"Cannot revert part directly: {id!r}. "
                "Use analyze() to replace parts, or delete the parent document."
            )
        doc_coll = self._resolve_doc_collection()
        chroma_coll = self._resolve_chroma_collection()

        max_ver = self._document_store.max_version(doc_coll, id)

        if max_ver == 0:
            # No history — full delete
            self.delete(id)
            return None

        # Get the versioned ChromaDB ID we need to promote
        versioned_chroma_id = f"{id}@v{max_ver}"

        # Get the archived embedding from ChromaDB
        archived_embedding = self._store.get_embedding(chroma_coll, versioned_chroma_id)

        # Restore in DocumentStore (promotes latest version to current)
        restored = self._document_store.restore_latest_version(doc_coll, id)

        if restored is None:
            # Shouldn't happen given version_count > 0, but be safe
            self.delete(id)
            return None

        # Update ChromaDB: replace current with restored version's data
        if archived_embedding:
            self._store.upsert(
                collection=chroma_coll, id=id,
                embedding=archived_embedding,
                summary=restored.summary,
                tags=casefold_tags_for_index(restored.tags),
            )

        # Delete the versioned entry from ChromaDB
        self._store.delete_entries(chroma_coll, [versioned_chroma_id])

        # Clean up stale parts (structural decomposition of old content)
        self._store.delete_parts(chroma_coll, id)
        self._document_store.delete_parts(doc_coll, id)

        return self.get(id)

    def delete_version(self, id: str, offset: int) -> bool:
        """Delete a specific archived version by public selector.

        Selector semantics match get_version:
        - 1=previous, 2=two ago, ...
        - -1=oldest archived, -2=second oldest archived, ...
        - 0=current (not allowed here; use revert()).

        Returns True if the version was found and deleted.
        """
        id = normalize_id(id)
        resolved_offset = self.resolve_version_offset(id, offset)
        if resolved_offset is None:
            return False
        if resolved_offset < 1:
            raise ValueError("Use revert() to delete the current version (offset 0)")
        doc_coll = self._resolve_doc_collection()
        chroma_coll = self._resolve_chroma_collection()

        # Resolve offset → internal version number
        version_info = self._document_store.get_version(doc_coll, id, resolved_offset)
        if version_info is None:
            return False

        # Delete from SQLite
        self._document_store.delete_version(doc_coll, id, version_info.version)

        # Delete from ChromaDB
        versioned_chroma_id = f"{id}@v{version_info.version}"
        self._store.delete_entries(chroma_coll, [versioned_chroma_id])

        return True

    # -------------------------------------------------------------------------
    # Current Working Context (Now)
    # -------------------------------------------------------------------------

    def get_now(self, *, scope: Optional[str] = None) -> Item:
        """Get the current working intentions.

        A singleton document representing what you're currently working on.
        If it doesn't exist, creates one with default content and tags from
        the bundled system now.md file.

        Args:
            scope: Optional scope for multi-user isolation (e.g. user ID).
                   When set, uses ``now:{scope}`` instead of the singleton ``now``.

        Returns:
            The current intentions Item (never None - auto-creates if missing)
        """
        doc_id = f"now:{scope}" if scope else NOWDOC_ID
        item = self.get(doc_id)
        if item is None:
            if scope:
                # Scoped now: initialize with minimal content
                item = self.set_now(f"# Now ({scope})\n\nWorking context.", scope=scope)
            else:
                # Singleton now: use bundled system doc
                try:
                    default_content, default_tags = _load_frontmatter(SYSTEM_DOC_DIR / "now.md")
                except FileNotFoundError:
                    default_content = "# Now\n\nYour working context."
                    default_tags = {}
                item = self.set_now(default_content, tags=default_tags)
        return item

    def set_now(
        self,
        content: str,
        *,
        scope: Optional[str] = None,
        tags: Optional[TagMap] = None,
    ) -> Item:
        """Set the current working intentions.

        Updates the singleton intentions with new content. Uses put()
        internally with the fixed NOWDOC_ID.

        Args:
            content: New content for the current intentions
            scope: Optional scope for multi-user isolation (e.g. user ID).
                   When set, uses ``now:{scope}`` and auto-tags with ``user={scope}``.
            tags: Optional additional tags to apply

        Returns:
            The updated context Item
        """
        doc_id = f"now:{scope}" if scope else NOWDOC_ID
        merged_tags = dict(tags or {})
        if scope:
            merged_tags.setdefault("user", scope)
        return self.put(content, id=doc_id, tags=merged_tags or None)

    def move(
        self,
        name: str,
        *,
        source_id: str = NOWDOC_ID,
        tags: Optional[TagMap] = None,
        only_current: bool = False,
    ) -> Item:
        """Move versions from a source document into a named item.

        Moves matching versions (filtered by tags if provided) from source_id
        to a named item. If the target already exists, extracted versions are
        appended to its history. The source retains non-matching versions;
        if fully emptied and source is 'now', it resets to default.

        Args:
            name: ID for the target item (created if new, extended if exists)
            source_id: Document to extract from (default: now)
            tags: If provided, only extract versions whose tags contain
                  all specified key=value pairs. If None, extract all.
            only_current: If True, only extract the current (tip) version,
                        not any archived history.

        Returns:
            The moved Item.

        Raises:
            ValueError: If name is empty, source doesn't exist,
                        or no versions match the filter.
        """
        if not name:
            raise ValueError("Name cannot be empty")
        name = normalize_id(name)
        source_id = normalize_id(source_id)
        if is_part_id(name):
            raise ValueError(
                f"Cannot move to a part ID: {name!r}. "
                "Parts are managed by analyze()."
            )

        # Casefold tag filters so they match casefolded storage
        if tags:
            tags = casefold_tags(tags)
            for key in tags:
                validate_tag_key(key)

        doc_coll = self._resolve_doc_collection()
        chroma_coll = self._resolve_chroma_collection()

        # Get the source's archived version numbers before extraction
        # (needed to map to ChromaDB versioned IDs)
        source_versions = self._document_store.list_versions(
            doc_coll, source_id, limit=10000
        )
        source_current = self._document_store.get(doc_coll, source_id)
        if source_current is None:
            raise ValueError(f"Source document '{source_id}' not found")

        # Identify which versions will be extracted (for ChromaDB cleanup)
        def _tags_match(item_tags: dict, filt: dict) -> bool:
            for k in filt:
                wanted = set(tag_values(filt, k))
                if not wanted:
                    continue
                stored_values = set(tag_values(item_tags, k))
                if not wanted.issubset(stored_values):
                    return False
            return True

        if only_current:
            # Only extract the tip — no archived versions
            matched_version_nums = []
            if tags:
                current_matches = _tags_match(source_current.tags, tags)
            else:
                current_matches = True
        elif tags:
            matched_version_nums = [
                v.version for v in source_versions
                if _tags_match(v.tags, tags)
            ]
            current_matches = _tags_match(source_current.tags, tags)
        else:
            matched_version_nums = [v.version for v in source_versions]
            current_matches = True

        # Extract in DocumentStore (SQLite side)
        extracted, new_source, base_version = self._document_store.extract_versions(
            doc_coll, source_id, name, tag_filter=tags,
            only_current=only_current,
        )

        # ChromaDB side: move embeddings

        # 1. Collect source ChromaDB IDs for matched versions
        source_chroma_ids = [f"{source_id}@v{n}" for n in matched_version_nums]
        if current_matches:
            source_chroma_ids.append(source_id)

        # 2. If target already exists, archive its current embedding
        # (extract_versions already archived the SQLite side; we mirror in ChromaDB)
        if base_version > 1:
            archive_version = base_version - 1  # version assigned by _archive_current_unlocked
            existing = self._store.get_entries_full(chroma_coll, [name])
            if existing and existing[0].get("embedding") is not None:
                entry = existing[0]
                archived_vid = f"{name}@v{archive_version}"
                archived_tags = dict(entry["tags"])
                archived_tags["_version"] = str(archive_version)
                archived_tags["_base_id"] = name
                self._store.upsert_batch(
                    chroma_coll,
                    [archived_vid],
                    [entry["embedding"]],
                    [entry["summary"] or ""],
                    [casefold_tags_for_index(archived_tags)],
                )

        # 3. Batch-get embeddings from source
        embedding_map: dict[str, list[float]] = {}
        if source_chroma_ids:
            source_entries = self._store.get_entries_full(chroma_coll, source_chroma_ids)
            for entry in source_entries:
                if entry.get("embedding") is not None:
                    embedding_map[entry["id"]] = entry["embedding"]

            # 4. Batch-delete source entries
            found_ids = [entry["id"] for entry in source_entries]
            if found_ids:
                self._store.delete_entries(chroma_coll, found_ids)

        # 5. Insert target entries with new IDs
        # The extracted list is chronological (oldest first),
        # last one is current, rest are history
        target_ids = []
        target_embeddings = []
        target_summaries = []
        target_tags = []

        # History versions (sequential from base_version)
        for seq, vi in enumerate(extracted[:-1], start=base_version):
            target_vid = f"{name}@v{seq}"
            # Find the embedding for this version
            source_vid = f"{source_id}@v{vi.version}" if vi.version > 0 else source_id
            emb = embedding_map.get(source_vid)
            if emb is not None:
                ver_tags = dict(vi.tags)
                ver_tags["_version"] = str(seq)
                ver_tags["_base_id"] = name
                target_ids.append(target_vid)
                target_embeddings.append(emb)
                target_summaries.append(vi.summary)
                target_tags.append(ver_tags)

        # Current (newest extracted)
        newest = extracted[-1]
        source_cur_id = f"{source_id}@v{newest.version}" if newest.version > 0 else source_id
        cur_emb = embedding_map.get(source_cur_id)
        if cur_emb is not None:
            cur_tags = dict(newest.tags)
            cur_tags["_saved_from"] = source_id
            from .types import utc_now as _utc_now
            cur_tags["_saved_at"] = _utc_now()
            target_ids.append(name)
            target_embeddings.append(cur_emb)
            target_summaries.append(newest.summary)
            target_tags.append(cur_tags)

        # 6. Batch-insert/update into ChromaDB
        if target_ids:
            self._store.upsert_batch(
                chroma_coll,
                target_ids,
                target_embeddings,
                target_summaries,
                [casefold_tags_for_index(t) for t in target_tags],
            )

        # Add system tags to the saved document in DocumentStore too
        saved_doc = self._document_store.get(doc_coll, name)
        if saved_doc:
            saved_tags = dict(saved_doc.tags)
            saved_tags["_saved_from"] = source_id
            from .types import utc_now as _utc_now2
            saved_tags["_saved_at"] = _utc_now2()
            self._document_store.update_tags(doc_coll, name, saved_tags)

        # If source was fully emptied and is 'now', recreate with defaults
        if new_source is None and source_id == NOWDOC_ID:
            try:
                default_content, default_tags = _load_frontmatter(
                    SYSTEM_DOC_DIR / "now.md"
                )
            except FileNotFoundError:
                default_content = "# Now\n\nYour working context."
                default_tags = {}
            self.set_now(default_content, tags=default_tags)

        return self.get(name)

    def reset_system_documents(self) -> dict:
        """Force reload all system documents from bundled content."""
        from .system_docs import reset_system_documents
        return reset_system_documents(self)

    def tag(
        self,
        id: str,
        tags: Optional[dict[str, Any]] = None,
        remove: Optional[list[str]] = None,
        remove_values: Optional[dict[str, Any]] = None,
    ) -> Optional[Item]:
        """Update tags on an existing document without re-processing.

        Does NOT re-fetch, re-embed, or re-summarize. Only updates tags.

        Tag behavior:
        - Provided tags are merged with existing user tags
        - `remove=["key"]` removes full keys
        - `remove_values={"key": "value"}` removes specific values
        - Empty string value ("") still deletes that key (legacy compatibility)
        - System tags (_prefixed) cannot be modified via this method

        Args:
            id: Document identifier
            tags: Tags to add/update
            remove: Tag keys to delete
            remove_values: Specific tag values to remove per key

        Returns:
            Updated Item if found, None if document doesn't exist
        """
        doc_coll = self._resolve_doc_collection()
        chroma_coll = self._resolve_chroma_collection()

        # Deferred system doc migration (normally runs on first _upsert)
        if self._needs_sysdoc_migration:
            self._needs_sysdoc_migration = False
            try:
                self._migrate_system_documents()
            except Exception as e:
                logger.warning("System doc migration deferred: %s", e, exc_info=True)

        # Validate inputs
        id = normalize_id(id)
        add_changes: dict[str, Any] = {}
        legacy_delete_keys: set[str] = set()
        if tags:
            # For tag mutations, constrained-tag validation applies only to
            # additive values, not removals.
            tags = self._validate_tag_map(
                tags, source="Tags", check_constraints=False,
            )
            user_changes = {
                k: tags[k] for k in tags if not k.startswith(SYSTEM_TAG_PREFIX)
            }
            legacy_delete_keys, add_changes = _split_tag_additions(user_changes)
        remove_keys = _normalize_remove_keys(remove) | legacy_delete_keys
        remove_value_changes: dict[str, Any] = {}
        if remove_values:
            remove_values = self._validate_tag_map(
                remove_values, source="Tags", check_constraints=False,
            )
            remove_value_changes = {
                k: remove_values[k]
                for k in remove_values
                if not k.startswith(SYSTEM_TAG_PREFIX)
            }

        # Get existing item (prefer document store, fall back to ChromaDB)
        existing = self.get(id)
        if existing is None:
            return None

        # Start with existing tags, separate system from user
        current_tags = dict(existing.tags)
        system_tags = {k: v for k, v in current_tags.items()
                       if k.startswith(SYSTEM_TAG_PREFIX)}
        user_tags = {k: v for k, v in current_tags.items()
                     if not k.startswith(SYSTEM_TAG_PREFIX)}

        # Apply tag changes (filter out system tags from input)
        if add_changes:
            self._validate_constrained_tags(add_changes, existing_tags=current_tags)
            singular_keys = self._get_singular_keys(add_changes)
            if singular_keys:
                self._validate_singular_tags(add_changes, singular_keys)
                for sk in singular_keys:
                    user_tags.pop(sk, None)
        if add_changes or remove_keys or remove_value_changes:
            user_tags = _apply_tag_mutations(
                user_tags,
                add_changes,
                remove_keys=remove_keys,
                remove_by_key=remove_value_changes,
            )

        # Merge back: user tags + system tags
        final_tags = {**user_tags, **system_tags}

        # Dual-write: SQLite gets original values, ChromaDB gets casefolded
        self._document_store.update_tags(doc_coll, id, final_tags)
        self._store.update_tags(chroma_coll, id, casefold_tags_for_index(final_tags))

        # Return updated item
        return self.get(id)

    def tag_part(
        self,
        id: str,
        part_num: int,
        tags: Optional[dict[str, Any]] = None,
        remove: Optional[list[str]] = None,
        remove_values: Optional[dict[str, Any]] = None,
    ) -> Optional[PartInfo]:
        """Update user tags on a part without re-analyzing.

        Parts are machine-generated, so summaries and content are immutable.
        Tags can be edited to correct or override analyzer tagging decisions.

        System tags (_prefixed) cannot be modified.
        `remove=["key"]` removes full keys.
        `remove_values={"key": "value"}` removes specific values.
        Empty string still deletes a key (legacy compatibility).

        Args:
            id: Parent document ID (not the part ID)
            part_num: Part number (1-indexed)
            tags: Tags to add/update
            remove: Tag keys to delete
            remove_values: Specific tag values to remove per key

        Returns:
            Updated PartInfo if found, None if part doesn't exist
        """
        id = normalize_id(id)
        doc_coll = self._resolve_doc_collection()
        chroma_coll = self._resolve_chroma_collection()

        part = self._document_store.get_part(doc_coll, id, part_num)
        if part is None:
            return None

        # Merge: existing tags + new tags, empty string = delete
        merged = dict(part.tags)
        add_changes: dict[str, Any] = {}
        legacy_delete_keys: set[str] = set()
        if tags:
            tags = self._validate_tag_map(
                tags, source="Tags", check_constraints=False,
            )
            user_changes = {
                k: tags[k] for k in tags if not k.startswith(SYSTEM_TAG_PREFIX)
            }
            legacy_delete_keys, add_changes = _split_tag_additions(user_changes)
        remove_keys = _normalize_remove_keys(remove) | legacy_delete_keys
        remove_value_changes: dict[str, Any] = {}
        if remove_values:
            remove_values = self._validate_tag_map(
                remove_values, source="Tags", check_constraints=False,
            )
            remove_value_changes = {
                k: remove_values[k]
                for k in remove_values
                if not k.startswith(SYSTEM_TAG_PREFIX)
            }
        if add_changes:
            self._validate_constrained_tags(add_changes, existing_tags=merged)
            singular_keys = self._get_singular_keys(add_changes)
            if singular_keys:
                self._validate_singular_tags(add_changes, singular_keys)
                for sk in singular_keys:
                    merged.pop(sk, None)
        if add_changes or remove_keys or remove_value_changes:
            merged = _apply_tag_mutations(
                merged,
                add_changes,
                remove_keys=remove_keys,
                remove_by_key=remove_value_changes,
            )

        # Dual-write: SQLite gets original values, ChromaDB gets casefolded
        self._document_store.update_part_tags(doc_coll, id, part_num, merged)
        self._store.update_tags(chroma_coll, f"{id}@p{part_num}",
                                casefold_tags_for_index(merged))

        return self._document_store.get_part(doc_coll, id, part_num)

    # -------------------------------------------------------------------------
    # Parts (structural decomposition)
    # -------------------------------------------------------------------------

    def _gather_analyze_chunks(
        self, id: str, doc_record, *, since_version: int | None = None,
    ) -> list[dict] | dict[str, list[dict]]:
        """Gather content chunks for analysis as serializable dicts.

        For URI sources: re-fetch the document content.
        For inline notes: assemble version history chronologically.

        When *since_version* is set and the item is not URI-sourced, returns
        ``{"context": [...], "targets": [...]}`` with overlap context chunks
        and new-version target chunks.  Returns ``{"context": [], "targets": []}``
        if there are no new versions.
        """
        doc_coll = self._resolve_doc_collection()
        source = doc_record.tags.get("_source")
        parent_user_tags = {
            k: v for k, v in doc_record.tags.items()
            if not k.startswith(SYSTEM_TAG_PREFIX)
        }
        chunks: list[dict] = []

        if source == "uri":
            try:
                doc = self._document_provider.fetch(id)
                chunks = [{"content": doc.content, "tags": parent_user_tags, "index": 0}]
            except Exception as e:
                logger.warning("Could not re-fetch %s: %s, using summary", id, e)
            if chunks:
                return chunks  # URI sources don't support incremental

        versions = self._document_store.list_versions(doc_coll, id, limit=100)

        if since_version is not None and versions:
            # Incremental: split versions into context (already analyzed) and targets (new)
            chronological = list(reversed(versions))
            context_versions = [v for v in chronological if v.version <= since_version]
            new_versions = [v for v in chronological if v.version > since_version]

            if not new_versions:
                return {"context": [], "targets": []}

            # Take last N context versions as overlap
            overlap = context_versions[-self._config.incremental_context:]

            context_chunks: list[dict] = []
            for i, v in enumerate(overlap):
                date_str = v.created_at[:10] if v.created_at else ""
                context_chunks.append({
                    "content": f"[{date_str}]\n{v.summary}",
                    "tags": parent_user_tags,
                    "index": i,
                })

            target_chunks: list[dict] = []
            idx = len(context_chunks)
            for v in new_versions:
                date_str = v.created_at[:10] if v.created_at else ""
                target_chunks.append({
                    "content": f"[{date_str}]\n{v.summary}",
                    "tags": parent_user_tags,
                    "index": idx,
                })
                idx += 1
            # Current doc as final target
            target_chunks.append({
                "content": f"[current]\n{doc_record.summary}",
                "tags": parent_user_tags,
                "index": idx,
            })

            return {"context": context_chunks, "targets": target_chunks}

        # Full analysis (no since_version or no versions)
        if versions:
            for i, v in enumerate(reversed(versions)):
                date_str = v.created_at[:10] if v.created_at else ""
                chunks.append({
                    "content": f"[{date_str}]\n{v.summary}",
                    "tags": parent_user_tags,
                    "index": i,
                })
            chunks.append({
                "content": f"[current]\n{doc_record.summary}",
                "tags": parent_user_tags,
                "index": len(chunks),
            })
        else:
            chunks = [{"content": doc_record.summary, "tags": parent_user_tags, "index": 0}]

        return chunks

    def _gather_guide_context(self, tags: list[str]) -> str:
        """Build guide context string from tag descriptions."""
        doc_coll = self._resolve_doc_collection()
        guide_parts = []
        for tag_key in tags:
            tag_doc = self._document_store.get(doc_coll, f".tag/{tag_key}")
            if tag_doc:
                guide_parts.append(f"## Tag: {tag_key}\n{tag_doc.summary}")
        return "\n\n".join(guide_parts)

    def analyze(
        self,
        id: str,
        *,
        analyzer=None,
        tags: Optional[list[str]] = None,
        force: bool = False,
    ) -> list[PartInfo]:
        """Decompose a note or string into meaningful parts.

        For URI-sourced documents: decomposes the document content structurally.
        For inline notes (strings): assembles the version history and decomposes
        the temporal sequence into episodic parts.

        Uses a pluggable AnalyzerProvider to identify sections with summaries
        and tags. Re-analysis replaces all previous parts atomically.

        Skips analysis if the document's content_hash matches the stored
        _analyzed_hash tag (parts are already current). Use force=True
        to override.

        Args:
            id: Document or string to analyze
            analyzer: Override AnalyzerProvider for decomposition
                (default: use configured analyzer or SlidingWindowAnalyzer)
            tags: Guidance tag keys (e.g., ["topic", "type"]) —
                fetches .tag/xxx descriptions as decomposition context
            force: Skip the _analyzed_hash check and re-analyze regardless

        Returns:
            List of PartInfo for the created parts (empty list if skipped)
        """
        from .processors import ProcessorResult, process_analyze

        id = normalize_id(id)
        doc_coll = self._resolve_doc_collection()

        # Get the document
        doc_record = self._document_store.get(doc_coll, id)
        if doc_record is None:
            raise ValueError(f"Document not found: {id}")

        # Skip if parts are already current (content unchanged since last analysis)
        if not force and doc_record.content_hash:
            analyzed_hash = doc_record.tags.get("_analyzed_hash")
            if analyzed_hash and analyzed_hash == doc_record.content_hash:
                logger.info("Skipping analysis for %s: parts already current", id)
                return self.list_parts(id)

        # Detect incremental vstring analysis
        analyzed_version_str = doc_record.tags.get("_analyzed_version")
        is_vstring = doc_record.tags.get("_source") != "uri"
        incremental = (
            not force
            and bool(analyzed_version_str)
            and is_vstring
            and analyzer is None  # custom analyzers always get full
        )
        since_version: int | None = None
        if incremental:
            try:
                since_version = int(analyzed_version_str)
            except (ValueError, TypeError):
                incremental = False

        # Phase 1: Gather — assemble chunks, guide context, tag specs (local)
        gather_result = self._gather_analyze_chunks(
            id, doc_record, since_version=since_version if incremental else None,
        )

        # Handle incremental gather result
        if isinstance(gather_result, dict):
            context_chunks = gather_result["context"]
            target_chunks = gather_result["targets"]
            if not target_chunks:
                logger.info("Skipping incremental analysis for %s: no new versions", id)
                self._record_analyzed_tags(doc_coll, id, doc_record)
                return self.list_parts(id)
            chunk_dicts = context_chunks + target_chunks
        else:
            context_chunks = None
            target_chunks = None
            chunk_dicts = gather_result
            incremental = False  # gather returned flat list (URI or full)

        total_content = "".join(c["content"] for c in chunk_dicts)
        if len(total_content.strip()) < 50:
            logger.info("Skipping analysis for %s: content too short", id)
            return []

        guide_context = self._gather_guide_context(tags) if tags else ""

        tag_specs = None
        try:
            from .analyzers import TagClassifier
            classifier = TagClassifier(
                provider=self._get_summarization_provider(),
            )
            tag_specs = classifier.load_specs(self) or None
        except Exception as e:
            logger.warning("Could not load tag specs: %s", e)

        # Resolve analysis prompt from .prompt/analyze/* docs
        analysis_prompt = None
        try:
            analysis_prompt = self._resolve_prompt_doc("analyze", doc_record.tags)
        except Exception as e:
            logger.debug("Prompt doc resolution failed: %s", e)

        # Phase 2: Compute — pure processor (local or remote)
        # Wait for any background reconciliation to finish first — both
        # sentence-transformers (embedding) and mlx-lm (summarization)
        # import the `transformers` package, and concurrent imports
        # corrupt module state (Python import lock is per-module).
        if analyzer is None:
            self._reconcile_done.wait(timeout=30)
            analyzer_provider = self._get_summarization_provider()
        else:
            analyzer_provider = None  # custom analyzer passed directly

        if incremental and context_chunks is not None and target_chunks is not None:
            # Incremental path: single-window LLM call with context + targets
            from .analyzers import (
                INCREMENTAL_ANALYSIS_PROMPT,
                _estimate_tokens,
                _parse_parts,
                extract_prompt_section,
            )

            # Resolve incremental prompt from .prompt/analyze/incremental system doc
            incremental_prompt = None
            try:
                doc_coll_prompt = self._resolve_doc_collection()
                inc_doc = self._document_store.get(doc_coll_prompt, ".prompt/analyze/incremental")
                if inc_doc and inc_doc.summary:
                    extracted = extract_prompt_section(inc_doc.summary)
                    if extracted:
                        incremental_prompt = extracted
            except Exception as e:
                logger.debug("Incremental prompt doc resolution failed: %s", e)

            total_tokens = sum(
                _estimate_tokens(c["content"])
                for c in context_chunks + target_chunks
            )
            # If too large for single window, fall back to full analysis
            if total_tokens > 12000:
                logger.info(
                    "Incremental content too large (%d tokens), falling back to full analysis: %s",
                    total_tokens, id,
                )
                incremental = False
                # Re-gather as full
                chunk_dicts = self._gather_analyze_chunks(id, doc_record)
                # Fall through to full path below
            else:
                # Build single-window prompt with <analyze> marking
                prompt_parts = ["<content>"]
                for c in context_chunks:
                    prompt_parts.append(c["content"])
                prompt_parts.append("<analyze>")
                for c in target_chunks:
                    prompt_parts.append(c["content"])
                prompt_parts.append("</analyze>")
                prompt_parts.append("</content>")
                user_prompt = "\n\n".join(prompt_parts)

                if guide_context:
                    user_prompt = f"{guide_context}\n\n---\n\n{user_prompt}"

                provider = analyzer_provider or self._get_summarization_provider()
                # Unwrap caching wrapper if present
                raw_provider = provider
                if hasattr(raw_provider, '_provider') and raw_provider._provider is not None:
                    raw_provider = raw_provider._provider

                prompt_text = incremental_prompt or INCREMENTAL_ANALYSIS_PROMPT
                try:
                    result_text = raw_provider.generate(
                        prompt_text, user_prompt, max_tokens=4096,
                    )
                    new_parts = _parse_parts(result_text) if result_text else []
                except Exception as e:
                    logger.warning("Incremental analysis LLM call failed: %s", e)
                    new_parts = []

                # Classify new parts with tag specs
                if tag_specs and new_parts:
                    try:
                        classifier.classify(new_parts, tag_specs)
                    except Exception as e:
                        logger.warning("Tag classification skipped: %s", e)

                # Append new parts (don't delete old ones)
                if new_parts:
                    self._append_incremental_parts(
                        id, doc_coll, new_parts, doc_record, analyzer_provider,
                    )

                self._record_analyzed_tags(doc_coll, id, doc_record)
                return self.list_parts(id)

        # Full analysis path
        if analyzer is not None:
            # Custom analyzer: call directly (not through process_analyze)
            from .providers.base import AnalysisChunk
            analysis_chunks = [
                AnalysisChunk(
                    content=c["content"], tags=c.get("tags", {}),
                    index=c.get("index", i),
                )
                for i, c in enumerate(chunk_dicts)
            ]
            raw_parts = analyzer.analyze(
                analysis_chunks, guide_context,
                prompt_override=analysis_prompt,
            )
            if tag_specs and raw_parts:
                try:
                    classifier.classify(raw_parts, tag_specs)
                except Exception as e:
                    logger.warning("Tag classification skipped: %s", e)
            proc_result = ProcessorResult(task_type="analyze", parts=raw_parts)
        else:
            proc_result = process_analyze(
                chunk_dicts, guide_context, tag_specs,
                analyzer_provider=analyzer_provider,
                prompt_override=analysis_prompt,
                classifier_provider=analyzer_provider,
            )

        # Content not decomposable — single section is redundant with the note
        if not proc_result.parts or len(proc_result.parts) <= 1:
            logger.info("Content not decomposable into multiple parts: %s", id)
            self._record_analyzed_tags(doc_coll, id, doc_record)
            return []

        # Phase 3: Apply — write parts + embeddings to stores (local)
        # apply_result records _analyzed_hash and _analyzed_version via mutations
        self.apply_result(
            id, doc_coll, proc_result,
            existing_tags=doc_record.tags,
        )

        # Phase 4: Generate vstring overview as @P{0}
        if len(chunk_dicts) >= 2 and proc_result.parts:
            overview_provider = analyzer_provider or self._get_summarization_provider()
            overview = self._generate_vstring_overview(
                chunk_dicts, overview_provider,
            )
            if overview:
                self._upsert_overview_part(id, doc_coll, overview, doc_record)

        return self.list_parts(id)

    def _generate_vstring_overview(self, chunk_dicts, provider):
        """Summarize assembled version chunks into a one-sentence overview."""
        from .processors import _llm_summarize

        full_text = "\n".join(c["content"] for c in chunk_dicts)
        system = (
            "Summarize the following in one sentence. "
            "Focus on the most specific, distinctive content — "
            "names, topics, facts, decisions, or events. "
            "Avoid generic descriptions."
        )
        result = _llm_summarize(full_text, provider, system_prompt_override=system)
        if result is None and hasattr(provider, "summarize"):
            # Non-LLM fallback: use summarize() for truncation-based summary
            try:
                result = provider.summarize(full_text, max_length=200)
            except TypeError:
                result = provider.summarize(full_text)
        return result

    def _record_analyzed_tags(self, doc_coll: str, id: str, doc_record) -> None:
        """Update _analyzed_hash and _analyzed_version tags after analysis."""
        chroma_coll = self._resolve_chroma_collection()
        updated_tags = dict(doc_record.tags)
        if doc_record.content_hash:
            updated_tags["_analyzed_hash"] = doc_record.content_hash
        versions = self._document_store.list_versions(doc_coll, id, limit=1)
        if versions:
            updated_tags["_analyzed_version"] = str(versions[0].version)
        self._document_store.update_tags(doc_coll, id, updated_tags)
        self._store.update_tags(chroma_coll, id, casefold_tags_for_index(updated_tags))

    def _append_incremental_parts(
        self, id: str, doc_coll: str, new_parts: list[dict],
        doc_record, provider,
    ) -> None:
        """Append new analysis parts without deleting existing ones."""
        from .document_store import PartInfo

        chroma_coll = self._resolve_chroma_collection()
        max_part = self._document_store.max_part_num(doc_coll, id)
        parent_user_tags = {
            k: v for k, v in doc_record.tags.items()
            if not k.startswith(SYSTEM_TAG_PREFIX)
        }
        embed = self._get_embedding_provider()
        now = utc_now()

        for i, raw in enumerate(new_parts, start=max_part + 1):
            part_tags = dict(parent_user_tags)
            if raw.get("tags"):
                part_tags.update(raw["tags"])
            part_tags["_base_id"] = id
            part_tags["_part_num"] = str(i)
            summary = str(raw.get("summary") or "")

            part = PartInfo(
                part_num=i,
                summary=summary,
                tags=part_tags,
                content=str(raw.get("content") or ""),
                created_at=now,
            )
            self._document_store.upsert_single_part(doc_coll, id, part)

            if summary:
                embedding = embed.embed(summary)
                self._store.upsert_part(
                    chroma_coll, id, i,
                    embedding, summary,
                    casefold_tags_for_index(part_tags),
                )

        # Regenerate overview from all part summaries
        all_parts = self._document_store.list_parts(doc_coll, id)
        overview_chunks = [
            {"content": p.summary, "tags": {}, "index": i}
            for i, p in enumerate(all_parts)
            if p.part_num > 0 and p.summary
        ]
        if len(overview_chunks) >= 2:
            overview_provider = provider or self._get_summarization_provider()
            overview = self._generate_vstring_overview(overview_chunks, overview_provider)
            if overview:
                self._upsert_overview_part(id, doc_coll, overview, doc_record)

    def _upsert_overview_part(
        self, id: str, doc_coll: str, overview: str, doc_record,
    ) -> None:
        """Write or replace the @P{0} overview part."""
        from .document_store import PartInfo

        parent_user_tags = {
            k: v for k, v in doc_record.tags.items()
            if not k.startswith(SYSTEM_TAG_PREFIX)
        }
        now = utc_now()
        overview_part = PartInfo(
            part_num=0,
            summary=overview,
            tags=dict(parent_user_tags, _part_type="overview"),
            content="",
            created_at=now,
        )
        self._document_store.upsert_single_part(doc_coll, id, overview_part)
        chroma_coll = self._resolve_chroma_collection()
        embed = self._get_embedding_provider()
        embedding = embed.embed(overview)
        self._store.upsert_part(
            chroma_coll, id, 0,
            embedding, overview,
            casefold_tags_for_index(overview_part.tags),
        )

    def get_part(self, id: str, part_num: int) -> Optional[Item]:
        """Get a specific part of a document.

        Returns the part as an Item with _part_num, _base_id, and
        _total_parts metadata tags.  Part 0 is the overview summary
        (when present); decomposed parts are numbered 1..N.
        _total_parts counts only decomposed parts (excludes @P{0}).

        Args:
            id: Document identifier
            part_num: Part number (0 for overview, 1+ for decomposed parts)

        Returns:
            Item if found, None otherwise
        """
        doc_coll = self._resolve_doc_collection()
        part = self._document_store.get_part(doc_coll, id, part_num)
        if part is None:
            return None

        # _total_parts counts decomposed parts only (excludes @P{0} overview)
        total = self._document_store.part_count(doc_coll, id)
        has_overview = self._document_store.get_part(doc_coll, id, 0) is not None
        if has_overview:
            total -= 1
        tags = dict(part.tags)
        tags["_part_num"] = str(part.part_num)
        tags["_base_id"] = id
        tags["_total_parts"] = str(total)

        return Item(
            id=id,
            summary=part.content if part.content else part.summary,
            tags=tags,
        )

    def list_parts(self, id: str) -> list[PartInfo]:
        """List all parts for a document.

        Args:
            id: Document identifier

        Returns:
            List of PartInfo, ordered by part_num
        """
        doc_coll = self._resolve_doc_collection()
        return self._document_store.list_parts(doc_coll, id)

    # -------------------------------------------------------------------------
    # Collection Management
    # -------------------------------------------------------------------------

    def list_collections(self) -> list[str]:
        """List all collections in the store."""
        # Merge collections from both stores
        doc_collections = set(self._document_store.list_collections())
        chroma_collections = set(self._store.list_collections())
        return sorted(doc_collections | chroma_collections)
    
    def count(self) -> int:
        """Count items in a collection.

        Returns count from document store if available, else vector store.
        """
        doc_coll = self._resolve_doc_collection()
        chroma_coll = self._resolve_chroma_collection()
        doc_count = self._document_store.count(doc_coll)
        if doc_count > 0:
            return doc_count
        return self._store.count(chroma_coll)

    def count_versions(self) -> int:
        """Count archived versions in the collection."""
        doc_coll = self._resolve_doc_collection()
        return self._document_store.count_versions(doc_coll)

    def list_items(
        self,
        *,
        prefix: Optional[str] = None,
        tags: Optional[TagMap] = None,
        tag_keys: Optional[list[str]] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        order_by: str = "updated",
        include_hidden: bool = False,
        include_history: bool = False,
        limit: int = 10,
    ) -> list[Item]:
        """List items with composable filters.

        All filters are AND'd together. Prefix narrows by ID, tags narrow
        by metadata, date narrows by time.

        Args:
            prefix: ID prefix filter (e.g. ".tag/act").
            tags: Tag key=value filters (all must match).
            tag_keys: Tag key-only filters (item must have key, any value).
            since: Only items updated since (ISO duration like P3D, or date).
            until: Only items updated before (ISO duration or date).
            order_by: Sort order - "updated" (default), "accessed", or "created".
            include_hidden: Include system notes (dot-prefix IDs).
            include_history: Include previous versions alongside current items.
            limit: Maximum results to return.

        Returns:
            List of Items, most recent first.
        """
        doc_coll = self._resolve_doc_collection()
        chroma_coll = self._resolve_chroma_collection()

        # Casefold tag filters to match casefolded storage
        if tags:
            tags = casefold_tags(tags)
            for k in tags:
                validate_tag_key(k)
        if tag_keys:
            tag_keys = [k.casefold() for k in tag_keys]
            for k in tag_keys:
                validate_tag_key(k)

        # ------------------------------------------------------------------
        # Paginated fetch-and-filter: fetches pages of results, applies
        # post-filters, and accumulates until we have `limit` results or
        # the data source is exhausted.
        # ------------------------------------------------------------------
        page_size = min(limit * 3, 200)
        max_rows = max(limit * 20, 200)
        offset = 0
        items: list[Item] = []

        while True:
            # --------------------------------------------------------------
            # Primary data source: pick the most selective query
            # --------------------------------------------------------------
            if tags:
                # Key=value tags: use ChromaDB metadata query
                where = self._build_tag_where(tags)
                if where is None:
                    batch = []
                    raw_count = 0
                else:
                    results = self._store.query_metadata(
                        chroma_coll, where, limit=page_size, offset=offset,
                    )
                    # Enrich from SQLite for original-case tag values
                    batch = []
                    for r in results:
                        doc = self._document_store.get(doc_coll, r.id)
                        if doc:
                            batch.append(_record_to_item(doc))
                        else:
                            batch.append(r.to_item())
                    raw_count = len(results)

            elif tag_keys:
                # Key-only: use SQLite tag key query (first key as primary,
                # additional keys as post-filter)
                since_date = _parse_date_param(since) if since else None
                until_date = _parse_date_param(until) if until else None
                docs = self._document_store.query_by_tag_key(
                    doc_coll, tag_keys[0], limit=page_size,
                    since_date=since_date, until_date=until_date,
                    offset=offset,
                )
                batch = [_record_to_item(d) for d in docs]
                raw_count = len(docs)
                # Post-filter additional key-only tags
                for extra_key in tag_keys[1:]:
                    batch = [i for i in batch if extra_key in i.tags]

            elif prefix is not None:
                # ID pattern query — glob if pattern contains * or ?, else prefix.
                if "*" in prefix or "?" in prefix:
                    records = self._document_store.query_by_id_glob(
                        doc_coll, prefix, limit=page_size, offset=offset,
                    )
                else:
                    # Prefix match — also finds children (e.g. ".tag" finds ".tag/foo").
                    records = self._document_store.query_by_id_prefix(
                        doc_coll, prefix, limit=page_size, offset=offset,
                    )
                batch = [_record_to_item(rec) for rec in records]
                raw_count = len(records)

            else:
                # Default: recent items
                if include_history:
                    records = self._document_store.list_recent_with_history(
                        doc_coll, page_size, order_by=order_by, offset=offset,
                    )
                else:
                    records = self._document_store.list_recent(
                        doc_coll, page_size, order_by=order_by, offset=offset,
                    )
                batch = [_record_to_item(rec) for rec in records]
                raw_count = len(records)

            # --------------------------------------------------------------
            # Post-filters (apply remaining predicates to this page)
            # --------------------------------------------------------------

            # Prefix filter (if primary query wasn't prefix-based)
            if prefix is not None and tags:
                batch = [i for i in batch if i.id.startswith(prefix)]

            # Tag-key filter (if primary query was something else)
            if tag_keys and tags:
                for k in tag_keys:
                    batch = [i for i in batch if k in i.tags]

            # Date filter (skip if already applied in SQL via tag_keys path)
            if (since is not None or until is not None) and not tag_keys:
                batch = _filter_by_date(batch, since=since, until=until)

            # Hidden filter (prefix queries always include hidden)
            if not include_hidden and prefix is None:
                batch = [i for i in batch if not _is_hidden(i)]

            items.extend(batch)
            offset += raw_count

            # Enough results, or data source exhausted?
            if len(items) >= limit or raw_count < page_size or offset >= max_rows:
                break

        return items[:limit]

    # -------------------------------------------------------------------------
    # Pending Work Queue (summaries + analysis)
    # -------------------------------------------------------------------------

    def enqueue_analyze(
        self,
        id: str,
        tags: Optional[list[str]] = None,
        force: bool = False,
    ) -> bool:
        """Enqueue a note for background analysis (decomposition into parts).

        Validates the document exists, then adds it to the pending work
        queue for serial processing by the background daemon.

        Skips enqueueing if the document's _analyzed_hash matches its
        content_hash (parts are already current). Use force=True to
        override.

        Args:
            id: Document ID to analyze
            tags: Guidance tag keys for decomposition
            force: Enqueue even if parts are already current

        Returns:
            True if enqueued, False if skipped (parts already current)
        """
        id = normalize_id(id)
        doc_coll = self._resolve_doc_collection()
        doc = self._document_store.get(doc_coll, id)
        if doc is None:
            raise ValueError(f"Document not found: {id}")

        # Skip if parts are already current
        if not force and doc.content_hash:
            analyzed_hash = doc.tags.get("_analyzed_hash")
            if analyzed_hash and analyzed_hash == doc.content_hash:
                logger.info("Skipping enqueue for %s: parts already current", id)
                return False

        metadata: dict[str, Any] = {}
        if tags:
            metadata["tags"] = list(tags)
        if force:
            metadata["force"] = True
        self._enqueue_task_background(
            task_type="analyze",
            id=id,
            doc_coll=doc_coll,
            content="",
            metadata=metadata,
        )
        self._spawn_processor()
        return True

    # -- Pending processing and delegation methods are in BackgroundProcessingMixin --
    # (_emit_processing_breakdown, process_pending, _delegate_task,
    #  _poll_delegated, _is_delegation_stale, _revert_stale_delegation)

    # -------------------------------------------------------------------------
    # Planner priors
    # -------------------------------------------------------------------------

    def get_planner_priors(
        self,
        scope_key: str | None = None,
        candidates: list[str] | None = None,
    ) -> dict:
        """Return minimal planner priors for flow discriminators.

        Shape:
        {
            "planner_priors": {
                "fanout": {...},
                "selectivity": {...},
                "cardinality": {...}
            },
            "staleness": {"stats_age_s": 14, "fallback_mode": false}
        }
        """
        if not self._planner_stats:
            return {
                "planner_priors": {},
                "staleness": {"stats_age_s": None, "fallback_mode": True},
            }

        from .planner_stats import build_scope_key
        if scope_key is None:
            scope_key = build_scope_key()

        raw_priors = self._planner_stats.get_priors(
            scope_key,
            metric_families=[
                "expansion.fanout",
                "expansion.selectivity",
                "facet.cardinality",
            ],
            subject_keys=candidates,
        )
        staleness = self._planner_stats.get_staleness(scope_key)

        priors = {
            "fanout": raw_priors.get("expansion.fanout", {}),
            "selectivity": raw_priors.get("expansion.selectivity", {}),
            "cardinality": raw_priors.get("facet.cardinality", {}),
        }

        return {
            "planner_priors": priors,
            "staleness": staleness,
        }

    def rebuild_planner_stats(self) -> dict:
        """Full rebuild of planner statistics from canonical data.

        Returns dict with count of stats upserted per metric family.
        """
        if not self._planner_stats:
            return {}
        doc_coll = self._resolve_doc_collection()
        return self._planner_stats.rebuild(self._document_store, doc_coll)

    # -- Processing pipeline methods are in BackgroundProcessingMixin --
    # (apply_result, _run_local_task_workflow, _process_pending_embed,
    #  _process_pending_reindex, _flush_edge_backfill, _process_pending_backfill_edges,
    #  _ocr_image, _ocr_pdf, pending_count, pending_work_count, process_pending_work,
    #  pending_stats, pending_stats_by_type, pending_status, _get_work_queue)


    def _run_read_flow(
        self,
        state: str,
        params: dict[str, Any],
        *,
        budget: int = 5,
        query_embedding: Any = None,
    ) -> "FlowResult":
        """Run a synchronous state-doc flow for the read/query path.

        Creates a read-only environment, wires up a state doc loader
        and action runner, then evaluates the flow to completion.
        State docs are loaded from ``.state/*`` notes in the store
        (seeded by system doc migration).

        Args:
            state: Name of the starting state doc (e.g. "get-context").
            params: Caller-supplied parameters.
            budget: Maximum ticks before forced stop.
            query_embedding: Optional embedding vector for semantic
                tiebreaking in traverse actions.

        Returns:
            FlowResult with terminal status and accumulated bindings.
        """
        from .state_doc_runtime import (
            FlowResult,
            make_action_runner,
            make_state_doc_loader,
            run_flow,
        )

        env = LocalFlowEnvironment(self)
        if query_embedding is not None:
            env._query_embedding = query_embedding
        loader = make_state_doc_loader(env)
        runner = make_action_runner(env)
        return run_flow(state, params, budget=budget, load_state_doc=loader, run_action=runner)

    def run_flow_command(
        self,
        state: str,
        *,
        params: dict[str, Any] | None = None,
        budget: int | None = None,
        cursor_token: str | None = None,
        state_doc_yaml: str | None = None,
        writable: bool = True,
    ) -> "FlowResult":
        """Run a state-doc flow synchronously.

        This is the public API behind ``keep flow``. Supports starting
        new flows, resuming stopped flows via cursor, and loading state
        docs from YAML or the store.

        Args:
            state: State doc name (e.g. "after-write", "query-resolve").
            params: Caller-supplied parameters.
            budget: Max ticks this invocation. Defaults to config.budget_per_flow.
            cursor_token: Opaque cursor from a previous stopped flow.
            state_doc_yaml: If provided, parse this YAML as the state doc
                instead of loading from the store.
            writable: If True, enable write actions (summarize, tag, etc.).

        Returns:
            FlowResult with status, bindings, cursor (if stopped), etc.
        """
        from .state_doc import parse_state_doc
        from .state_doc_runtime import (
            FlowResult,
            decode_cursor,
            make_action_runner,
            make_state_doc_loader,
            run_flow,
        )

        if budget is None:
            budget = self._config.budget_per_flow

        env = LocalFlowEnvironment(self)
        runner = make_action_runner(env, writable=writable)

        # Build loader: inline YAML overrides store lookup
        if state_doc_yaml is not None:
            inline_doc = parse_state_doc(state, state_doc_yaml)

            def _loader(name: str):
                if name == state:
                    return inline_doc
                # Fall through to store for transitions to other states
                return make_state_doc_loader(env)(name)
        else:
            _loader = make_state_doc_loader(env)

        # Decode cursor if resuming
        cursor = None
        if cursor_token:
            # Try server-side cursor first (short ID)
            cursor = self._load_cursor(cursor_token)
            if cursor is None:
                # Fall back to self-contained base64 cursor (backward compat)
                cursor = decode_cursor(cursor_token)

        # Inject defaults for query flow params
        merged_params: dict[str, Any] = {
            "limit": 10,
            "margin_high": 0.15,
            "margin_low": 0.03,
            "entropy_high": 0.8,
            "entropy_low": 0.3,
            "lineage_strong": 0.5,
            "explore_limit": 10,
            "explore_limit_wide": 15,
            "pivot_limit": 5,
            "bridge_limit": 5,
            "deep_limit": 10,
            "max_summary_length": 200,
            "similar_limit": 3,
            "meta_limit": 3,
            "parts_limit": 5,
            "versions_limit": 3,
            "edges_limit": 5,
        }
        merged_params.update(params or {})

        result = run_flow(
            state,
            merged_params,
            budget=budget,
            load_state_doc=_loader,
            run_action=runner,
            cursor=cursor,
        )

        # Store cursor server-side, replace with short ID
        if result.cursor:
            cursor_id = self._store_cursor(result.cursor)
            result.cursor = cursor_id

        return result

    def _store_cursor(self, cursor_token: str) -> str:
        """Store a self-contained cursor as a system note, return short ID."""
        import hashlib
        cursor_id = hashlib.sha256(cursor_token.encode()).hexdigest()[:12]
        note_id = f".cursor/{cursor_id}"
        self.put(cursor_token, id=note_id)
        return cursor_id

    def _load_cursor(self, cursor_id: str) -> "Optional[FlowCursor]":
        """Load a server-side cursor by short ID, delete after loading."""
        from .state_doc_runtime import decode_cursor
        note_id = f".cursor/{cursor_id}"
        item = self.get(note_id)
        if item is None:
            return None
        try:
            self.delete(note_id)
        except Exception:
            pass
        content = getattr(item, "content", None) or getattr(item, "summary", None)
        if not content:
            return None
        return decode_cursor(str(content))

    # -- Spawn/processor methods are in BackgroundProcessingMixin --

    def reconcile(
        self,
        fix: bool = False,
        _doc_store: "Optional[DocumentStore]" = None,
    ) -> dict:
        """Check and optionally fix consistency between document store and vector store.

        Detects:
        - Documents in document store missing from vector store (not searchable)
        - Documents in vector store missing from document store (orphaned embeddings)

        Args:
            fix: If True, re-index documents missing from vector store
            _doc_store: Optional separate DocumentStore for thread-safe reads
                (used by background reconcile to avoid concurrent SQLite access)

        Returns:
            Dict with 'missing_from_index', 'orphaned_in_index', 'fixed' counts
        """
        ds = _doc_store or self._document_store
        doc_coll = self._resolve_doc_collection()
        chroma_coll = self._resolve_chroma_collection()

        # Find mismatches between stores (exclude empty-summary items
        # that can't produce embeddings, e.g. bare .tag/* stubs)
        doc_ids = ds.list_ids(doc_coll)
        missing_from_chroma_raw = self._store.find_missing_ids(chroma_coll, doc_ids)
        missing_records: dict[str, Any] = {}
        for doc_id in missing_from_chroma_raw:
            rec = ds.get(doc_coll, doc_id)
            if rec and rec.summary:
                missing_records[doc_id] = rec

        # Skip versioned (@v{N}) and part (@p{N}) IDs — tracked in separate tables
        chroma_ids = self._store.list_ids(chroma_coll)
        doc_id_set = set(doc_ids)
        orphaned_in_chroma = {
            cid for cid in chroma_ids
            if cid not in doc_id_set and "@v" not in cid and "@p" not in cid
        }

        fixed = 0
        removed = 0
        if fix:
            # Re-index items missing from ChromaDB using stored summary
            for doc_id, doc_record in missing_records.items():
                if self._closing.is_set():
                    break
                try:
                    if doc_record:
                        embed_text = doc_record.summary or doc_id
                        embedding = self._get_embedding_provider().embed(embed_text)
                        self._store.upsert(
                            collection=chroma_coll,
                            id=doc_id,
                            embedding=embedding,
                            summary=doc_record.summary or doc_id,
                            tags=casefold_tags_for_index(doc_record.tags),
                        )
                        fixed += 1
                        logger.info("Reconciled: %s", doc_id)
                except Exception as e:
                    logger.warning("Failed to reconcile %s: %s", doc_id, e)

            # Remove orphaned ChromaDB entries
            for orphan_id in orphaned_in_chroma:
                if self._closing.is_set():
                    break
                try:
                    self._store.delete(chroma_coll, orphan_id)
                    removed += 1
                    logger.info("Removed orphan: %s", orphan_id)
                except Exception as e:
                    logger.warning("Failed to remove orphan %s: %s", orphan_id, e)

        return {
            "missing_from_index": len(missing_records),
            "orphaned_in_index": len(orphaned_in_chroma),
            "fixed": fixed,
            "removed": removed,
            "missing_ids": list(missing_records) if missing_records else [],
            "orphaned_ids": list(orphaned_in_chroma) if orphaned_in_chroma else [],
        }

    @property
    def config(self) -> "StoreConfig":
        """Public access to store configuration."""
        return self._config

    def close(self) -> None:
        """Close resources (stores, caches, queues).

        Waits for background reconcile to finish before tearing down stores,
        then releases model locks (freeing GPU memory) before file locks.

        Wraps each step in try/except because close() may be called from
        an atexit handler during interpreter shutdown, when C extension
        modules (sqlite3, chromadb) may already be partially finalized.
        """
        # Signal reconcile thread to stop and wait for it
        if hasattr(self, '_closing'):
            self._closing.set()
        if hasattr(self, '_reconcile_thread') and self._reconcile_thread is not None:
            self._reconcile_thread.join(timeout=10)

        # Release locked model providers (frees GPU memory + gc)
        try:
            self._release_embedding_provider()
        except Exception:
            pass
        try:
            self._release_summarization_provider()
        except Exception:
            pass

        if self._media_describer is not None:
            if hasattr(self._media_describer, 'release'):
                try:
                    self._media_describer.release()
                except Exception:
                    pass
            self._media_describer = None

        if self._content_extractor is not None:
            if hasattr(self._content_extractor, 'release'):
                try:
                    self._content_extractor.release()
                except Exception:
                    pass
            self._content_extractor = None

        # Close ChromaDB store
        if hasattr(self, '_store') and self._store is not None:
            try:
                self._store.close()
            except Exception:
                pass

        # Close document store (SQLite)
        if hasattr(self, '_document_store') and self._document_store is not None:
            try:
                self._document_store.close()
            except Exception:
                pass

        # Close pending summary queue
        if hasattr(self, '_pending_queue'):
            try:
                self._pending_queue.close()
            except Exception:
                pass

        # Close task delegation client
        if hasattr(self, '_task_client') and self._task_client is not None:
            self._task_client.close()
            self._task_client = None

        # Close work queue
        if hasattr(self, "_work_queue") and self._work_queue is not None:
            try:
                self._work_queue.close()
            except Exception:
                pass

        # Log final perf summary before removing ops log handler
        try:
            from .perf_stats import perf
            if perf.summary():
                perf.log_summary()
        except Exception:
            pass

        # Remove ops log handler to avoid handler accumulation
        if hasattr(self, '_ops_log_handler') and self._ops_log_handler:
            import logging
            logging.getLogger("keep").removeHandler(self._ops_log_handler)
            self._ops_log_handler.close()
            self._ops_log_handler = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close resources."""
        self.close()
        return False

    def __del__(self):
        """Cleanup on deletion."""
        try:
            self.close()
        except Exception:
            pass  # Suppress errors during garbage collection
