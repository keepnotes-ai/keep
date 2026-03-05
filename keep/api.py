"""
Core API for reflective memory.

This is the minimal working implementation focused on:
- put(): fetch/embed → summarize → store
- find(): embed query → search
- get(): retrieve by ID
"""

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

logger = logging.getLogger(__name__)


def _parse_date_param(value: str) -> str:
    """
    Parse a date/duration parameter and return a YYYY-MM-DD date.

    Accepts:
    - ISO 8601 duration: P3D (3 days), P1W (1 week), PT1H (1 hour), P1DT12H, etc.
    - ISO date: 2026-01-15
    - Date with slashes: 2026/01/15

    Returns:
        YYYY-MM-DD string
    """
    since = value.strip()

    # ISO 8601 duration: P[n]Y[n]M[n]W[n]DT[n]H[n]M[n]S
    if since.upper().startswith("P"):
        duration_str = since.upper()

        # Parse duration components
        years = months = weeks = days = hours = minutes = seconds = 0

        # Split on T to separate date and time parts
        if "T" in duration_str:
            date_part, time_part = duration_str.split("T", 1)
        else:
            date_part = duration_str
            time_part = ""

        # Parse date part (P[n]Y[n]M[n]W[n]D)
        date_part = date_part[1:]  # Remove leading P
        for match in re.finditer(r"(\d+)([YMWD])", date_part):
            value, unit = int(match.group(1)), match.group(2)
            if unit == "Y":
                years = value
            elif unit == "M":
                months = value
            elif unit == "W":
                weeks = value
            elif unit == "D":
                days = value

        # Parse time part ([n]H[n]M[n]S)
        for match in re.finditer(r"(\d+)([HMS])", time_part):
            value, unit = int(match.group(1)), match.group(2)
            if unit == "H":
                hours = value
            elif unit == "M":
                minutes = value
            elif unit == "S":
                seconds = value

        # Convert to timedelta (approximate months/years)
        total_days = years * 365 + months * 30 + weeks * 7 + days
        delta = timedelta(days=total_days, hours=hours, minutes=minutes, seconds=seconds)
        cutoff = datetime.now(timezone.utc) - delta
        return cutoff.strftime("%Y-%m-%d")

    # Try parsing as date
    # ISO format: 2026-01-15 or 2026-01-15T...
    # Slash format: 2026/01/15
    date_str = since.replace("/", "-").split("T")[0]

    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
        return parsed.strftime("%Y-%m-%d")
    except ValueError:
        pass

    raise ValueError(
        f"Invalid date/duration format: {value}. "
        "Use ISO duration (P3D, PT1H, P1W) or date (2026-01-15)"
    )


def _filter_by_date(
    items: list,
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> list:
    """Filter items by date range (since <= date, date < until)."""
    since_cutoff = _parse_date_param(since) if since else None
    until_cutoff = _parse_date_param(until) if until else None
    result = []
    for item in items:
        d = item.tags.get("_updated_date", "0000-00-00")
        if since_cutoff and d < since_cutoff:
            continue
        if until_cutoff and d >= until_cutoff:
            continue
        result.append(item)
    return result


def _enrich_updated_date(items: list, doc_store, doc_coll: str) -> None:
    """Fill in missing _updated_date tags from SQLite for date filtering.

    Version references (``id@v{N}``) stored in ChromaDB often lack
    ``_updated_date`` because the tag is synthesized by ``_record_to_item``
    from the SQLite ``updated_at`` column rather than stored in the raw
    tags dict.  Without enrichment, ``_filter_by_date`` defaults to
    ``"0000-00-00"`` and drops them.

    Uses a single batch lookup (``get_many``) to avoid N+1 queries.
    """
    # Collect unique base IDs that need enrichment
    needs: dict[str, list] = {}  # base_id → [items needing it]
    for item in items:
        if "_updated_date" in item.tags:
            continue
        base_id = item.tags.get("_base_id",
                                item.id.split("@")[0] if "@" in item.id else item.id)
        needs.setdefault(base_id, []).append(item)
    if not needs:
        return
    # Single batch fetch
    docs = doc_store.get_many(doc_coll, list(needs.keys()))
    for base_id, waiting in needs.items():
        doc = docs.get(base_id)
        if doc and doc.updated_at:
            date = doc.updated_at[:10]
            for item in waiting:
                item.tags["_updated_date"] = date


def _is_hidden(item) -> bool:
    """System notes (dot-prefix IDs like .conversations) are hidden by default."""
    base_id = item.tags.get("_base_id", item.id)
    if isinstance(base_id, list):
        base_id = base_id[0] if base_id else item.id
    return base_id.startswith(".")


def _truncate_ts(ts: str) -> str:
    """Normalize timestamp to canonical format: YYYY-MM-DDTHH:MM:SS.

    New data is already in this format (via utc_now()). This handles
    legacy timestamps that may have microseconds, 'Z', or '+00:00'.
    """
    # Strip fractional seconds
    dot = ts.find(".", 19)
    if dot != -1:
        # Skip past digits to any tz suffix
        end = dot
        for i in range(dot + 1, len(ts)):
            if ts[i] in "+-Z":
                break
        else:
            i = len(ts)
        ts = ts[:dot] + ts[i:] if i < len(ts) else ts[:dot]
    # Strip timezone suffix — all timestamps are UTC by convention
    if ts.endswith("+00:00"):
        ts = ts[:-6]
    elif ts.endswith("Z"):
        ts = ts[:-1]
    return ts


def _record_to_item(rec, score: float = None, changed: bool = None) -> "Item":
    """
    Convert a DocumentRecord to an Item with timestamp tags.

    Adds _updated, _created, _updated_date from the record's columns
    to ensure consistent timestamp exposure across all retrieval methods.
    """
    from .types import Item
    updated = _truncate_ts(rec.updated_at) if rec.updated_at else ""
    created = _truncate_ts(rec.created_at) if rec.created_at else ""
    accessed = _truncate_ts(rec.accessed_at or rec.updated_at) if (rec.accessed_at or rec.updated_at) else ""
    tags = {
        **rec.tags,
        "_updated": updated,
        "_created": created,
        "_updated_date": updated[:10],
        "_accessed": accessed,
        "_accessed_date": accessed[:10],
    }
    return Item(id=rec.id, summary=rec.summary, tags=tags, score=score, changed=changed)


import os
import subprocess
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
    Item, ItemContext, SimilarRef, MetaRef, EdgeRef, VersionRef, PartRef,
    EvidenceUnit, ContextWindow,
    PromptResult, PromptInfo, TagMap,
    casefold_tags, casefold_tags_for_index, filter_non_system_tags,
    iter_tag_pairs, set_tag_values, tag_values,
    SYSTEM_TAG_PREFIX, local_date, utc_now,
    parse_utc_timestamp, validate_tag_key, validate_id, normalize_id, is_part_id,
    MAX_TAG_VALUE_LENGTH,
)


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

# Maximum attempts before giving up on a pending summary
MAX_SUMMARY_ATTEMPTS = 5


# Collection name validation: lowercase ASCII and underscores only

# Environment variable prefix for auto-applied tags
ENV_TAG_PREFIX = "KEEP_TAG_"

# Fixed ID for the current working context (singleton)
NOWDOC_ID = "now"


from .system_docs import (
    SYSTEM_DOC_DIR,
    SYSTEM_DOC_IDS,
    _load_frontmatter,
)

# Pattern for meta-doc query lines: key=value pairs separated by spaces
_META_QUERY_PAIR = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*=\S+$')
# Pattern for context-match lines: key= (bare, no value)
_META_CONTEXT_KEY = re.compile(r'^([a-zA-Z_][a-zA-Z0-9_]*)=$')
# Pattern for prerequisite lines: key=* (item must have this tag)
_META_PREREQ_KEY = re.compile(r'^([a-zA-Z_][a-zA-Z0-9_]*)=\*$')

# Markdown extensions that may contain YAML frontmatter
_MARKDOWN_EXTENSIONS = {".md", ".markdown", ".mdx"}

def _extract_markdown_frontmatter(content: str) -> tuple[str, dict]:
    """
    Extract YAML frontmatter from markdown content.

    Returns (body, tags) where:
    - body: content with frontmatter stripped
    - tags: all scalar frontmatter values as string tags, plus
            values from a ``tags`` dict if present.
            Keys starting with ``_`` are skipped (reserved for system tags).
            Non-scalar values (lists, nested dicts) are dropped,
            except for the ``tags`` key which is expected to be a dict.
    """
    if not content.startswith("---"):
        return content, {}

    parts = content.split("---", 2)
    if len(parts) < 3:
        return content, {}

    try:
        import yaml
        frontmatter = yaml.safe_load(parts[1])
    except Exception:
        return content, {}

    body = parts[2].lstrip("\n")
    if not isinstance(frontmatter, dict):
        return body, {}

    tags: dict[str, str | list[str]] = {}

    for key, value in frontmatter.items():
        key_str = str(key)
        # Skip system-reserved keys
        if key_str.startswith("_"):
            continue

        if key_str == "tags" and isinstance(value, dict):
            # Obsidian/keep-style tags dict: {topic: foo, project: bar}
            for tk, tv in value.items():
                tk_str = str(tk)
                if tk_str.startswith("_"):
                    continue
                if isinstance(tv, (str, int, float, bool)):
                    tags[tk_str] = str(tv)
                elif isinstance(tv, list):
                    vals = [str(v) for v in tv if isinstance(v, (str, int, float, bool))]
                    if vals:
                        tags[tk_str] = vals
        elif isinstance(value, (str, int, float, bool)):
            # Top-level scalar: title, author, date, etc. → tag
            tags[key_str] = str(value)
        elif isinstance(value, list):
            vals = [str(v) for v in value if isinstance(v, (str, int, float, bool))]
            if vals:
                tags[key_str] = vals
        elif hasattr(value, "isoformat"):
            # datetime.date / datetime.datetime (YAML auto-parses dates)
            tags[key_str] = value.isoformat()
        # else: lists, nested dicts, None → drop silently

    return body, tags


def _parse_meta_doc(content: str) -> tuple[list[dict[str, str]], list[str], list[str]]:
    """
    Parse meta-doc content into query lines, context-match keys, and prerequisites.

    Returns:
        (query_lines, context_keys, prereq_keys) where:
        - query_lines: list of dicts, each {key: value, ...} for AND queries
        - context_keys: list of tag keys for context matching
        - prereq_keys: list of tag keys the current item must have
    """
    query_lines: list[dict[str, str]] = []
    context_keys: list[str] = []
    prereq_keys: list[str] = []

    for line in content.split("\n"):
        line = line.strip()
        if not line:
            continue

        # Check for prerequisite: exactly "key=*"
        prereq_match = _META_PREREQ_KEY.match(line)
        if prereq_match:
            prereq_keys.append(prereq_match.group(1))
            continue

        # Check for context-match: exactly "key=" with no value
        ctx_match = _META_CONTEXT_KEY.match(line)
        if ctx_match:
            context_keys.append(ctx_match.group(1))
            continue

        # Check for query line: all space-separated tokens are key=value
        tokens = line.split()
        pairs: dict[str, str] = {}
        is_query = True
        for token in tokens:
            if _META_QUERY_PAIR.match(token):
                k, v = token.split("=", 1)
                pairs[k] = v
            else:
                is_query = False
                break

        if is_query and pairs:
            query_lines.append(pairs)

    return query_lines, context_keys, prereq_keys

# Old IDs for migration (maps old → new)
def _get_env_tags() -> dict[str, str]:
    """
    Collect tags from KEEP_TAG_* environment variables.

    KEEP_TAG_PROJECT=foo -> {"project": "foo"}
    KEEP_TAG_MyTag=bar   -> {"mytag": "bar"}

    Tag keys are lowercased for consistency.
    """
    tags = {}
    for key, value in os.environ.items():
        if key.startswith(ENV_TAG_PREFIX) and value:
            tag_key = key[len(ENV_TAG_PREFIX):].lower()
            tags[tag_key] = value
    return tags


# Hash functions moved to processors.py — kept here for backwards compatibility
from .processors import _content_hash, _content_hash_full  # noqa: F401


def _user_tags_changed(old_tags: dict, new_tags: dict) -> bool:
    """
    Check if non-system tags differ between old and new.

    Used for contextual re-summarization: when user tags change,
    the summary context changes and should be regenerated.

    Args:
        old_tags: Existing tags from document store
        new_tags: New merged tags being applied

    Returns:
        True if user (non-system) tags differ
    """
    old_user = {
        (k, v)
        for k, v in iter_tag_pairs(old_tags, include_system=False)
    }
    new_user = {
        (k, v)
        for k, v in iter_tag_pairs(new_tags, include_system=False)
    }
    return old_user != new_user


def _merge_tags_additive(dst: dict, src: dict, *, replace_system: bool = False) -> None:
    """Merge tags by adding distinct values per key.

    Non-system keys are additive set union. System keys are replaced.
    """
    for raw_key in src:
        key = str(raw_key)
        vals = tag_values(src, raw_key)
        if not vals:
            continue
        if key.startswith(SYSTEM_TAG_PREFIX):
            if replace_system:
                dst[key] = vals[-1]
            continue
        existing = tag_values(dst, key)
        seen = set(existing)
        merged = list(existing)
        for value in vals:
            if value not in seen:
                merged.append(value)
                seen.add(value)
        set_tag_values(dst, key, merged)


def _split_tag_additions(tags: dict[str, Any]) -> tuple[set[str], dict[str, list[str]]]:
    """Split additive tag changes and legacy empty-value key deletions."""
    delete_keys: set[str] = set()
    add_by_key: dict[str, list[str]] = {}
    for key in tags:
        values = tag_values(tags, key)
        if not values:
            continue
        if "" in values:
            delete_keys.add(key)
            values = [v for v in values if v != ""]
        if values:
            add_by_key[key] = values
    for key in delete_keys:
        add_by_key.pop(key, None)
    return delete_keys, add_by_key


def _normalize_remove_keys(keys: Optional[list[str]]) -> set[str]:
    """Normalize and validate explicit key removals."""
    out: set[str] = set()
    if not keys:
        return out
    for raw_key in keys:
        key = str(raw_key).casefold()
        if key.startswith(SYSTEM_TAG_PREFIX):
            continue
        validate_tag_key(key)
        out.add(key)
    return out


def _apply_tag_mutations(
    base_tags: dict[str, Any],
    add_by_key: dict[str, Any],
    *,
    remove_keys: Optional[set[str]] = None,
    remove_by_key: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Apply explicit tag adds/removals and return updated tags."""
    merged = dict(base_tags)
    for key in (remove_keys or set()):
        merged.pop(key, None)

    remove_map = remove_by_key or {}
    for key in remove_map:
        current_vals = tag_values(merged, key)
        if not current_vals:
            continue
        remove_set = set(tag_values(remove_map, key))
        if not remove_set:
            continue
        kept = [v for v in current_vals if v not in remove_set]
        set_tag_values(merged, key, kept)

    if add_by_key:
        _merge_tags_additive(merged, add_by_key)

    return merged


def _text_content_id(content: str) -> str:
    """
    Generate a content-addressed ID for text updates.

    This makes text updates versioned by content:
    - `keep put "my note"` → ID = %{hash[:12]}
    - `keep put "my note" -t status=done` → same ID, new version
    - `keep put "different note"` → different ID

    Args:
        content: The text content

    Returns:
        Content-addressed ID in format %{hash[:12]}
    """
    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()[:12]
    return f"%{content_hash}"


# -------------------------------------------------------------------------
# Decomposition helpers (module-level, used by Keeper.analyze)
# -------------------------------------------------------------------------

class Keeper:
    """
    Reflective memory keeper - persistent storage with similarity search.

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
        """
        Initialize or open an existing reflective memory store.

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
        self._provider_init_lock = threading.Lock()
        self._last_spawn_time: float = 0.0

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

        # --- Planner stats (precomputed priors for continuation) ---
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

    def _migrate_system_documents(self) -> dict:
        """Migrate system documents to stable IDs and current version."""
        from .system_docs import migrate_system_documents
        result = migrate_system_documents(self)
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
                self._check_edge_backfill(td.id[5:], inverse, doc_coll)
                self._ensure_inverse_tagdoc(td.id[5:], inverse, doc_coll)

    def _get_embedding_provider(self) -> EmbeddingProvider:
        """
        Get embedding provider, creating it lazily on first use.

        Thread-safe: uses a lock to prevent concurrent model loading
        when the reconcile thread and main thread both need embeddings.

        This allows read-only operations to work without loading
        the embedding model upfront.
        """
        if self._embedding_provider is not None:
            return self._embedding_provider

        with self._provider_init_lock:
            # Double-check after acquiring lock (another thread may have created it)
            if self._embedding_provider is not None:
                return self._embedding_provider

            if self._config.embedding is None:
                raise RuntimeError(
                    "No embedding provider configured.\n"
                    "\n"
                    "To use keep, configure a provider:\n"
                    "  API-based:  export VOYAGE_API_KEY=...  (or OPENAI_API_KEY, GEMINI_API_KEY)\n"
                    "  Local:      pip install 'keep-skill[local]'\n"
                    "\n"
                    "Read-only operations (get, list, find) work without embeddings.\n"
                    "Find uses full-text search when no embedding provider is configured."
                )
            registry = get_registry()
            base_provider = registry.create_embedding(
                self._config.embedding.name,
                self._config.embedding.params,
            )
            # Wrap local GPU providers with lifecycle lock
            # Local-only: model locks and embedding cache use filesystem
            if self._is_local:
                if self._config.embedding.name == "mlx":
                    from .model_lock import LockedEmbeddingProvider
                    base_provider = LockedEmbeddingProvider(
                        base_provider,
                        self._store_path / ".embedding.lock",
                    )
                cache_path = self._store_path / "embedding_cache.db"
                self._embedding_provider = CachingEmbeddingProvider(
                    base_provider,
                    cache_path=cache_path,
                )
            else:
                self._embedding_provider = base_provider
            # Validate or record embedding identity
            self._validate_embedding_identity(self._embedding_provider)
            # Update store's embedding dimension if it wasn't known at init
            if self._store.embedding_dimension is None:
                self._store.reset_embedding_dimension(self._embedding_provider.dimension)
        return self._embedding_provider

    def _try_dedup_embedding(
        self,
        doc_coll: str,
        chroma_coll: str,
        content_hash: Optional[str],
        exclude_id: str,
        content: str = "",
    ) -> Optional[list[float]]:
        """Look up an existing embedding from a donor doc with the same content hash.

        Returns the embedding if found and dimension-validated, None otherwise.
        Passes the full SHA256 for collision-safe verification.
        """
        if not content_hash:
            return None
        full_hash = _content_hash_full(content) if content else ""
        donor = self._document_store.find_by_content_hash(
            doc_coll, content_hash,
            content_hash_full=full_hash,
            exclude_id=exclude_id,
        )
        if donor is None:
            return None
        donor_embedding = self._store.get_embedding(chroma_coll, donor.id)
        if donor_embedding is None:
            return None
        if len(donor_embedding) != self._get_embedding_provider().dimension:
            return None
        logger.debug("Dedup: reusing embedding from %s for %s", donor.id, exclude_id)
        return donor_embedding

    def _get_summarization_provider(self) -> SummarizationProvider:
        """Get summarization provider, creating it lazily on first use."""
        if self._summarization_provider is None:
            registry = get_registry()
            provider = registry.create_summarization(
                self._config.summarization.name,
                self._config.summarization.params,
            )
            if self._is_local and self._config.summarization.name == "mlx":
                from .model_lock import LockedSummarizationProvider
                provider = LockedSummarizationProvider(
                    provider,
                    self._store_path / ".summarization.lock",
                )
            self._summarization_provider = provider
        return self._summarization_provider

    def _release_summarization_provider(self) -> None:
        """Release summarization model to free GPU/unified memory.

        Always clears the provider reference so the lazy getter will
        reconstruct it on next use. For GPU-resident providers (MLX),
        also calls release() to free model weights immediately.

        Safe to call at any time.
        """
        if self._summarization_provider is not None:
            if hasattr(self._summarization_provider, 'release'):
                self._summarization_provider.release()
            self._summarization_provider = None

    def _release_embedding_provider(self) -> None:
        """Release embedding model to free GPU/unified memory.

        Always clears the provider reference so the lazy getter will
        reconstruct it on next use. For GPU-resident providers (MLX),
        also calls release() to free model weights immediately.

        Also closes the embedding cache when releasing.
        Safe to call at any time.
        """
        with self._provider_init_lock:
            provider = self._embedding_provider
            self._embedding_provider = None

        if provider is not None:
            # Release the locked inner provider (frees model weights)
            inner = getattr(provider, '_provider', None)
            if hasattr(inner, 'release'):
                inner.release()
            # Close the embedding cache
            if hasattr(provider, '_cache'):
                cache = provider._cache
                if hasattr(cache, 'close'):
                    cache.close()

    def _get_media_describer(self) -> Optional[MediaDescriber]:
        """
        Get media describer, creating it lazily on first use.

        Returns None if no media provider is configured or creation fails.
        """
        if self._media_describer is None:
            if self._config.media is None:
                return None
            registry = get_registry()
            try:
                provider = registry.create_media(
                    self._config.media.name,
                    self._config.media.params,
                )
            except (ValueError, RuntimeError) as e:
                logger.warning("Media describer unavailable: %s", e)
                return None
            if self._is_local and self._config.media.name == "mlx":
                from .model_lock import LockedMediaDescriber
                provider = LockedMediaDescriber(
                    provider,
                    self._store_path / ".media.lock",
                )
            self._media_describer = provider
        return self._media_describer

    def _get_content_extractor(self):
        """
        Get content extractor, creating it lazily on first use.

        Used by the background OCR processor. Returns None if no content
        extractor is configured or creation fails.
        """
        if self._content_extractor is None:
            if self._config.content_extractor is None:
                return None
            registry = get_registry()
            try:
                provider = registry.create_content_extractor(
                    self._config.content_extractor.name,
                    self._config.content_extractor.params,
                )
            except (ValueError, RuntimeError) as e:
                logger.warning("Content extractor unavailable: %s", e)
                return None
            if self._is_local and self._config.content_extractor.name == "mlx":
                from .model_lock import LockedContentExtractor
                provider = LockedContentExtractor(
                    provider,
                    self._store_path / ".extractor.lock",
                )
            self._content_extractor = provider
        return self._content_extractor

    def _release_content_extractor(self) -> None:
        """Release content extractor to free GPU/unified memory."""
        if self._content_extractor is not None:
            if hasattr(self._content_extractor, 'release'):
                self._content_extractor.release()
            self._content_extractor = None

    def _get_analyzer(self):
        """Get analyzer provider, creating it lazily on first use."""
        if self._analyzer is None:
            if self._config.analyzer:
                registry = get_registry()
                self._analyzer = registry.create_analyzer(
                    self._config.analyzer.name,
                    self._config.analyzer.params,
                )
            else:
                # Default: sliding-window analyzer with the summarization provider,
                # budget auto-selected based on the model's effective context quality.
                from .analyzers import SlidingWindowAnalyzer, get_budget_for_model
                provider = self._get_summarization_provider()
                model = getattr(provider, "model", "")
                provider_name = self._config.summarization.name if self._config.summarization else ""
                budget = get_budget_for_model(model, provider_name)
                self._analyzer = SlidingWindowAnalyzer(
                    provider=provider,
                    context_budget=budget,
                )
        return self._analyzer

    def _resolve_prompt_doc(
        self,
        prefix: str,
        doc_tags: dict[str, str],
    ) -> str | None:
        """
        Find a .prompt/* doc matching the given tags and return its prompt text.

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
        """
        Gather related item summaries that share any user tag.

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
        """
        Validate embedding provider matches stored identity, or record it.

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
        """
        Stream-export all documents as an iterator of dicts.

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
        """
        Export all documents, versions, and parts as a single dict.

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
        """
        Import documents from an export dict.

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

    def _validate_constrained_tags(self, tags: dict) -> None:
        """Check constrained tag values against sub-doc existence.

        For each user tag, looks up `.tag/KEY`. If that doc exists and has
        `_constrained=true`, checks that `.tag/KEY/VALUE` exists. Raises
        ValueError with valid values listed if not.
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

        # Collect tagdoc lookups (cache within this call)
        tagdoc_cache: dict[str, Optional[dict[str, str]]] = {}

        def _get_tagdoc_tags(key: str) -> Optional[dict[str, str]]:
            if key not in tagdoc_cache:
                parent = self._document_store.get(doc_coll, f".tag/{key}")
                tagdoc_cache[key] = parent.tags if parent else None
            return tagdoc_cache[key]

        for key in all_keys:
            td_tags = _get_tagdoc_tags(key)
            if td_tags is None:
                continue
            inverse = td_tags.get("_inverse")
            if not inverse:
                continue

            current_values = set(tag_values(merged_tags, key))
            previous_values = set(tag_values(existing_tags, key))

            removed_values = previous_values - current_values
            added_values = current_values - previous_values

            # Tag values removed → delete old edges for this predicate/target.
            for removed in removed_values:
                target_id = removed
                try:
                    target_id = normalize_id(removed)
                except ValueError:
                    # Best-effort cleanup for legacy/non-canonical rows.
                    pass
                self._document_store.delete_edge(doc_coll, id, key, target_id)

            # Tag present → upsert edge + auto-vivify target.
            # Skip sysdoc targets (names starting with '.'): this prevents
            # non-system writes from indirectly creating/updating system docs.
            for current_value in sorted(added_values):
                if not current_value:
                    continue
                try:
                    target_id = normalize_id(current_value)
                except ValueError as e:
                    raise ValueError(
                        f"Invalid edge target for tag '{key}': {current_value!r}. {e}"
                    ) from e
                if target_id.startswith("."):
                    continue
                # Auto-vivify: create target as empty doc if it doesn't exist
                # Only writes to document store — embedding is deferred to
                # avoid loading the model on the write path.
                if not self._document_store.exists(doc_coll, target_id):
                    reference_created = (
                        merged_tags.get("_created")
                        or merged_tags.get("_updated")
                        or utc_now()
                    )
                    now = utc_now()
                    self._document_store.upsert(
                        doc_coll, target_id,
                        summary="",
                        tags={
                            "_created": reference_created,
                            "_updated": now,
                            "_source": "auto-vivify",
                        },
                        created_at=reference_created,
                    )
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
                self._document_store.upsert_edge(
                    collection=doc_coll,
                    source_id=id,
                    predicate=key,
                    target_id=target_id,
                    inverse=inverse,
                    created=created,
                )

                # Trigger backfill check for this predicate
                self._check_edge_backfill(key, inverse, doc_coll)

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
            # Create minimal inverse tagdoc
            now = utc_now()
            self._document_store.upsert(
                doc_coll, inverse_tagdoc_id,
                summary="",
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
                logger.warning("System doc migration deferred: %s", e)

        # Get existing item to preserve tags (check document store first, fall back to ChromaDB)
        existing_tags = {}
        existing_doc = self._document_store.get(doc_coll, id)
        if existing_doc:
            existing_tags = filter_non_system_tags(existing_doc.tags)
        else:
            existing = self._store.get(chroma_coll, id)
            if existing:
                existing_tags = filter_non_system_tags(existing.tags)

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

        _merge_tags_additive(merged_tags, system_tags, replace_system=True)

        # Change detection (before embedding to allow early return)
        content_unchanged = (
            existing_doc is not None
            and existing_doc.content_hash == new_hash
        )
        tags_changed = (
            existing_doc is not None
            and _user_tags_changed(existing_doc.tags, merged_tags)
        )

        # Early return: nothing to do
        if content_unchanged and not tags_changed and summary is None and not force:
            logger.debug("Content and tags unchanged, skipping for %s", id)
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
            logger.debug("Tags changed, queueing re-summarization for %s", id)
            final_summary = existing_doc.summary
            if len(content) > max_len:
                self._pending_queue.enqueue(id, doc_coll, content)
        elif len(content) <= max_len:
            final_summary = content
        else:
            final_summary = content[:max_len] + "..."
            self._pending_queue.enqueue(id, doc_coll, content)

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
                    embedding = self._get_embedding_provider().embed(content)
            else:
                embedding = self._try_dedup_embedding(doc_coll, chroma_coll, new_hash, id, content)
                if embedding is None:
                    embedding = self._get_embedding_provider().embed(content)

        # Detect _inverse changes on tagdocs BEFORE storage overwrites old state
        if id.startswith(".tag/"):
            old_tagdoc_tags = existing_doc.tags if existing_doc else {}
            self._process_tagdoc_inverse_change(id, merged_tags, old_tagdoc_tags, doc_coll)

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
        if summary is None and len(content) > max_len and (not content_unchanged or tags_changed or force):
            self._spawn_processor()

        return _record_to_item(result, changed=not content_unchanged)

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
        """
        Store content in the memory.

        Provide either inline content or a URI to fetch — not both.

        **Inline mode** (content provided):
        - Stores text directly. Auto-generates an ID if not provided.
        - Short content is used verbatim as summary. Large content gets
          async summarization (truncated placeholder stored immediately).

        **URI mode** (uri provided):
        - Fetches the document, extracts text, generates embeddings.
        - Supports file://, http://, https:// URIs.
        - Non-text content (images, audio, PDF) gets media description.

        **Tag and summary behavior:**
        - Tags are merged with existing tags (new override on collision).
        - System tags (_prefixed) are managed automatically.
        - If summary is provided, it's used directly (skips auto-summarization).

        Args:
            content: Inline text to store
            uri: URI of document to fetch and index
            id: Custom ID (auto-generated for inline content if None)
            summary: User-provided summary (skips auto-summarization)
            tags: User-provided tags to merge with existing tags
            created_at: Override creation timestamp (ISO 8601).
                        For importing historical data via the Python API.
                        When updating an existing item, sets the head's
                        created_at so version archives carry accurate dates.

        Returns:
            The stored Item with merged tags and summary
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

            # Media description: enrich non-text content
            if doc.content_type and not doc.content_type.startswith("text/"):
                describer = self._get_media_describer()
                if describer:
                    try:
                        file_path = uri.removeprefix("file://") if uri.startswith("file://") else uri
                        description = describer.describe(file_path, doc.content_type)
                        if description:
                            doc = Document(
                                uri=doc.uri,
                                content=doc.content + "\n\nDescription:\n" + description,
                                content_type=doc.content_type,
                                metadata=doc.metadata,
                                tags=doc.tags,
                            )
                            logger.info("Added media description for %s (%d chars)",
                                        uri, len(description))
                    except Exception as e:
                        logger.warning("Media description failed for %s: %s", uri, e)

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
            )

            # Enqueue background OCR for scanned PDFs and images
            ocr_pages = (doc.metadata or {}).get("_ocr_pages")
            if ocr_pages and self._config.content_extractor:
                doc_coll = self._resolve_doc_collection()
                self._pending_queue.enqueue(
                    doc_id, doc_coll, "",
                    task_type="ocr",
                    metadata={
                        "uri": uri,
                        "ocr_pages": ocr_pages,
                        "content_type": doc.content_type,
                    },
                )
                self._spawn_processor()
                logger.info(
                    "Enqueued OCR for %s (%d pages)",
                    uri, len(ocr_pages),
                )

            return result
        else:
            # Inline mode: store content directly
            if id is None:
                timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
                id = f"mem:{timestamp}"
            else:
                id = normalize_id(id)

            return self._upsert(
                id, content,
                tags=tags, summary=summary,
                system_tags={"_source": "inline"},
                created_at=created_at,
                force=force,
            )

    # -------------------------------------------------------------------------
    # Query Operations
    # -------------------------------------------------------------------------
    
    def _apply_recency_decay(self, items: list[Item]) -> list[Item]:
        """
        Apply ACT-R style recency decay to search results.
        
        Multiplies each item's similarity score by a decay factor based on
        time since last update. Uses exponential decay with configurable half-life.
        
        Formula: effective_score = similarity × 0.5^(days_elapsed / half_life)
        """
        if self._decay_half_life_days <= 0:
            return items  # Decay disabled
        
        now = datetime.now(timezone.utc)
        decayed_items = []
        
        for item in items:
            # Get last update time from tags
            updated_str = item.tags.get("_updated")
            if updated_str and item.score is not None:
                try:
                    updated = parse_utc_timestamp(updated_str)
                    days_elapsed = (now - updated).total_seconds() / 86400
                    
                    # Exponential decay: 0.5^(days/half_life)
                    decay_factor = 0.5 ** (days_elapsed / self._decay_half_life_days)
                    decayed_score = item.score * decay_factor
                    
                    # Create new Item with decayed score
                    decayed_items.append(Item(
                        id=item.id,
                        summary=item.summary,
                        tags=item.tags,
                        score=decayed_score
                    ))
                except (ValueError, TypeError):
                    # If timestamp parsing fails, keep original
                    decayed_items.append(item)
            else:
                decayed_items.append(item)
        
        # Re-sort by decayed score (highest first)
        decayed_items.sort(key=lambda x: x.score if x.score is not None else 0, reverse=True)
        
        return decayed_items

    @staticmethod
    def _rrf_fuse(
        semantic_items: list[Item],
        fts_items: list[Item],
        k: int = 60,
        fts_weight: float = 2.0,
    ) -> list[Item]:
        """
        Fuse two ranked lists using weighted Reciprocal Rank Fusion.

        score(d) = 1/(k + rank_sem) + fts_weight/(k + rank_fts)

        FTS gets a higher weight because keyword matches signal entity-level
        relevance that semantic similarity can miss (e.g., "Max" the dog vs
        "How old is Luna?" which is semantically similar but wrong entity).

        Scores are normalized to [0, 1] where 1.0 = rank 1 in both lists.
        """
        scores: dict[str, float] = {}
        items_by_id: dict[str, Item] = {}

        for rank, item in enumerate(semantic_items, start=1):
            scores[item.id] = scores.get(item.id, 0) + 1 / (k + rank)
            items_by_id[item.id] = item  # prefer semantic item (has tags)

        for rank, item in enumerate(fts_items, start=1):
            scores[item.id] = scores.get(item.id, 0) + fts_weight / (k + rank)
            if item.id not in items_by_id:
                items_by_id[item.id] = item

        # Theoretical max: rank 1 in both lists
        max_score = (1.0 + fts_weight) / (k + 1)
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        result = []
        for item_id, rrf_score in ranked:
            source = items_by_id[item_id]
            result.append(Item(
                id=source.id,
                summary=source.summary,
                tags=source.tags,
                score=round(rrf_score / max_score, 4),
            ))
        return result

    def _deep_tag_follow(self, primary_items, chroma_coll, doc_coll, *,
                         embedding=None, top_k=10,
                         per_tag_fetch=1000, max_per_group=5):
        """Follow tags from primary results to discover bridge documents.

        Per-tag queries use ``query_embedding`` when an embedding is provided,
        giving each candidate a semantic similarity score that serves as a
        tiebreaker within the same tag-overlap tier.  Falls back to
        ``query_metadata`` (no semantic ranking) when no embedding is given.

        Versions and parts are collapsed to their parent document during
        collection so a single popular document doesn't consume all slots.

        Args:
            embedding: Query embedding for semantic tiebreaking (optional).
            top_k: Number of top primary items to collect tags from.
            per_tag_fetch: Max raw items to fetch per tag query.
            max_per_group: Max deep items to show per primary.

        Returns:
            dict mapping primary item ID to list of deep-discovered Items,
            sorted by (tag-overlap, semantic similarity) within each group.
        """
        # 1. Collect non-system tag pairs, tracking which primary has each
        tag_to_sources: dict[tuple[str, str], set[str]] = {}
        for item in primary_items[:top_k]:
            for k, v in iter_tag_pairs(item.tags, include_system=False):
                tag_to_sources.setdefault((k, v), set()).add(item.id)
        if not tag_to_sources:
            return {}
        # Drop tag pairs shared by ALL top primaries (non-distinctive)
        n_primaries = len(primary_items[:top_k])
        if n_primaries > 1:
            tag_to_sources = {
                tp: sids for tp, sids in tag_to_sources.items()
                if len(sids) < n_primaries
            }
        if not tag_to_sources:
            return {}

        # 1b. Compute IDF weights for tag-overlap scoring
        import math
        total_docs = max(self._document_store.count(doc_coll), 1)
        pair_counts = self._document_store.tag_pair_counts(doc_coll)
        idf: dict[tuple[str, str], float] = {}
        for (k, v), df in pair_counts.items():
            idf[(k.casefold(), v)] = math.log(total_docs / df)

        # 2. Metadata-only queries — find items sharing each tag,
        #    collapsing versions/parts to parent IDs immediately.
        #    Only exclude items from the top_k (not the full over-fetched pool)
        #    so genuine bridge items aren't accidentally excluded.
        top_ids = {item.id for item in primary_items[:top_k]}
        top_parents = set()
        for pid in top_ids:
            if "@" in pid:
                top_parents.add(pid.split("@")[0])
            else:
                top_parents.add(pid)

        candidates: dict[str, Item] = {}        # parent_id -> Item
        candidate_tags: dict[str, set] = {}     # parent_id -> matched (k, v)
        candidate_sources: dict[str, set] = {}  # parent_id -> primary IDs
        candidate_sem: dict[str, float] = {}    # parent_id -> best semantic score

        for (k, v), source_ids in tag_to_sources.items():
            where = self._build_tag_where({k: v})
            if where is None:
                continue
            if embedding is not None:
                results = self._store.query_embedding(
                    chroma_coll, embedding, limit=per_tag_fetch, where=where,
                )
            else:
                results = self._store.query_metadata(
                    chroma_coll, where=where, limit=per_tag_fetch,
                )
            seen_parents_this_tag: set[str] = set()
            for r in results:
                # Collapse to parent ID
                raw_id = r.id
                is_child = "@" in raw_id
                parent_id = raw_id.split("@")[0] if is_child else raw_id
                # Skip primaries (both raw and uplifted)
                if raw_id in top_ids or parent_id in top_parents:
                    continue
                # Deduplicate: only count each parent once per tag query
                if parent_id in seen_parents_this_tag:
                    continue
                seen_parents_this_tag.add(parent_id)
                # Track best semantic score for this parent
                item_from_r = r.to_item()
                if item_from_r.score is not None:
                    prev = candidate_sem.get(parent_id, 0)
                    candidate_sem[parent_id] = max(prev, item_from_r.score)
                # Register parent as candidate with head-doc tags
                if parent_id not in candidates:
                    head_doc = self._document_store.get(doc_coll, parent_id)
                    if head_doc:
                        head_item = _record_to_item(head_doc)
                        candidates[parent_id] = Item(
                            id=parent_id, summary=head_item.summary,
                            tags=head_item.tags, score=0,
                        )
                    else:
                        candidates[parent_id] = Item(
                            id=parent_id, summary=r.summary, tags=r.tags, score=0,
                        )
                candidate_tags.setdefault(parent_id, set()).add((k, v))
                candidate_sources.setdefault(parent_id, set()).update(source_ids)

        # 3. Assign each candidate to ONE primary (most shared HEAD tags)
        #    Score uses the candidate's head-doc tags (not version tags)
        primary_order = {item.id: i for i, item in enumerate(primary_items[:top_k])}
        # Pre-compute tag set per primary, limited to followed tag pairs
        followed_keys = set(tag_to_sources.keys())
        primary_tag_sets: dict[str, set] = {}
        for item in primary_items[:top_k]:
            primary_tag_sets[item.id] = {
                (k, v) for k, v in iter_tag_pairs(item.tags, include_system=False)
                if (k, v) in followed_keys
            }

        groups: dict[str, list[Item]] = {}
        for cid, item in candidates.items():
            # Use the candidate's HEAD doc tags for scoring (casefolded
            # to match ChromaDB's casefolded primary_tag_sets)
            head_tags = {
                (k.casefold(), v) for k, v in iter_tag_pairs(item.tags, include_system=False)
            }
            sources = candidate_sources.get(cid, set())

            def _idf_overlap(tag_set):
                return sum(idf.get(tp, 0) for tp in head_tags & tag_set)

            # Pick primary with highest IDF-weighted tag overlap
            best_source = min(
                sources,
                key=lambda sid: (
                    -_idf_overlap(primary_tag_sets.get(sid, set())),
                    primary_order.get(sid, 999),
                ),
            )
            # Composite score: IDF-weighted tag overlap + semantic tiebreaker
            overlap = _idf_overlap(primary_tag_sets.get(best_source, set()))
            sem = candidate_sem.get(cid, 0)
            scored = Item(id=cid, summary=item.summary,
                          tags=item.tags, score=overlap + sem)
            groups.setdefault(best_source, []).append(scored)

        # 4. Sort each group by composite score desc, cap size
        for source_id in groups:
            groups[source_id].sort(key=lambda x: x.score or 0, reverse=True)
            groups[source_id] = groups[source_id][:max_per_group]

        return groups

    def _deep_edge_follow(
        self,
        primary_items: list[Item],
        chroma_coll: str,
        doc_coll: str,
        *,
        query: str,
        embedding: list[float],
        top_k: int = 10,
        exclude_ids: set[str] | None = None,
    ) -> dict[str, list[Item]]:
        """Follow inverse edges from primary results to discover related items.

        For each primary, traverses its inverse edges (e.g. speaker→said)
        to collect candidate source IDs.  Then runs a scoped hybrid search
        (FTS pre-filter + embedding post-filter + RRF fusion) over only
        those candidates to surface relevant evidence.

        Returns all candidates per group — the renderer caps output via
        token budget.

        Args:
            query: Original search query text (used for FTS pre-filter).
            embedding: Query embedding vector.
            top_k: Number of top primary items to follow edges from.
            exclude_ids: IDs to exclude from deep results (e.g. items
                the user will already see as primaries).

        Returns:
            dict mapping primary item ID to list of deep-discovered Items,
            sorted by RRF score within each group.
        """
        query_stopwords: frozenset[str] = frozenset()
        get_stopwords = getattr(self._document_store, "get_stopwords", None)
        if callable(get_stopwords):
            try:
                query_stopwords = get_stopwords()
            except Exception:
                query_stopwords = frozenset()

        def _tokenize(text: str) -> set[str]:
            return {
                tok for tok in re.findall(r"[a-z0-9]+", (text or "").lower())
                if len(tok) > 2 and tok not in query_stopwords
            }

        def _extract_focus(id_value: str) -> tuple[str, Optional[str], Optional[str]]:
            """Return (parent_id, focus_part, focus_version) from a doc/part/version ID."""
            if "@p" in id_value:
                parent, suffix = id_value.rsplit("@p", 1)
                if suffix.isdigit():
                    return parent, suffix, None
            if "@v" in id_value:
                parent, suffix = id_value.rsplit("@v", 1)
                if suffix.isdigit():
                    return parent, None, suffix
            return id_value, None, None

        query_terms = _tokenize(query)

        def _query_overlap(text: str) -> float:
            if not query_terms:
                return 0.0
            # Normalize to [0, 1] so lexical signal doesn't swamp semantic score.
            return len(query_terms & _tokenize(text)) / max(len(query_terms), 1)

        # 1. Traverse edges for each primary, collect candidate IDs.
        #    Two traversal paths:
        #    a) Inverse edges: primary is a target → collect sources
        #       (e.g. entity "Melanie" ← said ← session docs)
        #    b) Forward + inverse (two-hop): primary is a source →
        #       follow forward to targets → collect THEIR inverse sources
        #       (e.g. session → speaker → entity → said → other sessions)
        #
        #    Caps prevent runaway fan-out on high-degree entities:
        #    - max_forward: forward edges per primary (hop-1 fan-out)
        #    - max_candidates: total candidate pool across all primaries
        _MAX_FORWARD = 20       # forward edges to traverse per primary
        _MAX_CANDIDATES = 500   # total candidate IDs before FTS/embedding

        primary_to_sources: dict[str, set[str]] = {}
        all_source_ids: set[str] = set()
        get_inverse_version_edges = getattr(
            self._document_store, "get_inverse_version_edges", None,
        )
        has_archived_versions = False
        if callable(get_inverse_version_edges):
            count_versions = getattr(self._document_store, "count_versions", None)
            if callable(count_versions):
                has_archived_versions = count_versions(doc_coll) > 0
            else:
                # Compatibility path for test doubles or alternate stores.
                has_archived_versions = True
        # IDs to exclude from deep results: caller-provided exclusion set
        # (items the user will see as primaries) or fall back to all items
        top_ids = set(exclude_ids) if exclude_ids is not None else set()

        for item in primary_items[:top_k]:
            parent_id = item.id.split("@")[0] if "@" in item.id else item.id
            if exclude_ids is None:
                top_ids.add(parent_id)
            sources: set[str] = set()

            # Path a: inverse edges (primary is target)
            inv_edges = self._document_store.get_inverse_edges(
                doc_coll, parent_id)
            for _inv, src_id, _created in inv_edges:
                sources.add(src_id)

            # Version-note parity: include sources whose archived versions
            # had edge tags pointing at this primary target.
            if has_archived_versions:
                ver_inv_edges = get_inverse_version_edges(
                    doc_coll, parent_id, limit=_MAX_CANDIDATES,
                )
                for _inv, src_id, _created in ver_inv_edges:
                    sources.add(src_id)

            # Path b: two-hop via forward edges (primary is source)
            fwd_edges = self._document_store.get_forward_edges(
                doc_coll, parent_id)
            for _pred, target_id, _created in fwd_edges[:_MAX_FORWARD]:
                hop2 = self._document_store.get_inverse_edges(
                    doc_coll, target_id)
                for _inv2, src_id2, _created2 in hop2:
                    sources.add(src_id2)

            if sources:
                primary_to_sources[parent_id] = sources
                all_source_ids.update(sources)
                if len(all_source_ids) >= _MAX_CANDIDATES:
                    break

        if not all_source_ids:
            return {}

        source_list = list(all_source_ids)

        # 2. FTS pre-filter: narrow candidates using cheap text matching
        fts_fetch = max(len(source_list), 100)
        fts_rows = self._document_store.query_fts_scoped(
            doc_coll, query, source_list, limit=fts_fetch,
        )
        fts_items = [Item(id=r[0], summary=r[1]) for r in fts_rows]

        # 3. Embedding search — post-filter to ALL edge source IDs.
        #    Unlike FTS (which is cheap enough to scope), embedding
        #    search queries the full collection and post-filters.
        sem_fetch = max(len(all_source_ids) * 3, 200)
        sem_results = self._store.query_embedding(
            chroma_coll, embedding, limit=sem_fetch,
        )
        sem_items = []
        for r in sem_results:
            base = r.id.split("@")[0] if "@" in r.id else r.id
            if base in all_source_ids:
                sem_items.append(r.to_item())

        sem_items = self._apply_recency_decay(sem_items)

        logger.debug("Deep edge follow: sources=%d fts=%d sem=%d top=%s",
                     len(all_source_ids), len(fts_items), len(sem_items),
                     top_ids)

        # 4. RRF fuse scoped FTS + embedding results
        if fts_items and sem_items:
            fused = self._rrf_fuse(sem_items, fts_items)
        elif fts_items:
            fused = fts_items
        elif sem_items:
            fused = sem_items
        else:
            return {}

        # 5. Candidate generation: map fused hits into evidence units.
        #    Keep this broad/high-recall; reranking happens in stage 6.
        candidate_units: list[tuple[str, EvidenceUnit, Item]] = []

        for item in fused:
            parent_id, focus_part, focus_version = _extract_focus(item.id)
            has_focus = focus_part is not None or focus_version is not None

            # Find which primary has an edge to this source.
            # Prefer non-excluded primaries (injected entities) so that
            # edge sources get grouped under the entity rather than a
            # sibling session that happens to share the same edges.
            best_primary = None
            for pid, sources in primary_to_sources.items():
                if parent_id in sources:
                    if pid not in top_ids:
                        best_primary = pid
                        break  # entity match — best possible
                    elif best_primary is None:
                        best_primary = pid  # fallback to excluded primary

            # Skip items that are already visible as primaries, UNLESS
            # they belong to an entity group (entity not in top_ids).
            if parent_id in top_ids and (best_primary is None or best_primary in top_ids):
                continue

            if best_primary is None:
                continue

            # Enrich with head-doc summary/tags from SQLite
            head_doc = self._document_store.get(doc_coll, parent_id)
            if head_doc:
                head_item = _record_to_item(head_doc)
                tags = dict(head_item.tags)
                # Preserve matched evidence text for renderer/LLM context.
                if item.summary and (has_focus or item.summary != head_item.summary):
                    tags["_focus_summary"] = item.summary
                if focus_part:
                    tags["_focus_part"] = focus_part
                if focus_version:
                    tags["_focus_version"] = focus_version
                tags["_anchor_id"] = item.id
                if focus_part:
                    tags["_anchor_type"] = "part"
                elif focus_version:
                    tags["_anchor_type"] = "version"
                else:
                    tags["_anchor_type"] = "document"
                scored = Item(
                    id=item.id, summary=head_item.summary,
                    tags=tags, score=item.score,
                )
            else:
                tags = dict(item.tags)
                if item.summary and has_focus:
                    tags["_focus_summary"] = item.summary
                if focus_part:
                    tags["_focus_part"] = focus_part
                if focus_version:
                    tags["_focus_version"] = focus_version
                tags["_anchor_id"] = item.id
                if focus_part:
                    tags["_anchor_type"] = "part"
                elif focus_version:
                    tags["_anchor_type"] = "version"
                else:
                    tags["_anchor_type"] = "document"
                scored = Item(
                    id=item.id, summary=item.summary,
                    tags=tags, score=item.score,
                )

            evidence_text = scored.tags.get("_focus_summary", scored.summary)
            lane = "derived" if focus_part is not None else "authoritative"
            scored.tags["_lane"] = lane

            unit = EvidenceUnit(
                unit_id=item.id,
                source_id=parent_id,
                version=int(focus_version) if focus_version else None,
                part_num=int(focus_part) if focus_part else None,
                lane=lane,
                text=evidence_text,
                parent_summary=scored.summary,
                created=scored.tags.get("_created"),
                score_sem=item.score or 0.0,
                score_lex=float(_query_overlap(evidence_text)),
                score_focus=1.0 if has_focus else 0.0,
                score_coherence=0.2 if scored.tags.get("_focus_summary") else 0.0,
                provenance={
                    "anchor_id": item.id,
                    "anchor_type": scored.tags.get("_anchor_type", "document"),
                },
            )

            candidate_units.append((best_primary, unit, scored))

        if not candidate_units:
            return {}

        # 6. Query-conditioned rerank and parent-level dedup.
        #    Weights are generic (dataset-agnostic): semantic + lexical
        #    relevance + focus quality + coherence signal.
        _W_SEM = 1.0
        _W_LEX = 0.6
        _W_FOCUS = 0.4
        _W_COH = 0.2
        _MAX_ANCHORS_PER_SOURCE = 2
        by_source: dict[str, list[tuple[float, str, ContextWindow, Item]]] = {}

        for primary_id, unit, scored in candidate_units:
            total = (
                _W_SEM * unit.score_sem
                + _W_LEX * unit.score_lex
                + _W_FOCUS * unit.score_focus
                + _W_COH * unit.score_coherence
            )
            reranked = EvidenceUnit(
                unit_id=unit.unit_id,
                source_id=unit.source_id,
                version=unit.version,
                part_num=unit.part_num,
                lane=unit.lane,
                text=unit.text,
                parent_summary=unit.parent_summary,
                created=unit.created,
                score_sem=unit.score_sem,
                score_lex=unit.score_lex,
                score_focus=unit.score_focus,
                score_coherence=unit.score_coherence,
                score_total=total,
                provenance=unit.provenance,
            )
            window = ContextWindow(
                anchor=reranked,
                members=[reranked],  # first slice: anchor-only windows
                score_total=total,
                tokens_est=max(len(reranked.text) // 4, 1),
            )
            by_source.setdefault(reranked.source_id, []).append(
                (window.score_total, primary_id, window, scored)
            )

        groups: dict[str, list[Item]] = {}
        for entries in by_source.values():
            entries.sort(key=lambda t: t[0], reverse=True)
            selected: list[tuple[float, str, ContextWindow, Item]] = []
            seen_focus: set[tuple[Optional[int], Optional[int], str]] = set()
            for score_total, primary_id, window, scored in entries:
                focus_key = (
                    window.anchor.version,
                    window.anchor.part_num,
                    (scored.tags.get("_focus_summary") or "").strip().lower(),
                )
                if focus_key in seen_focus:
                    continue
                seen_focus.add(focus_key)
                selected.append((score_total, primary_id, window, scored))
                if len(selected) >= _MAX_ANCHORS_PER_SOURCE:
                    break
            for score_total, primary_id, _window, scored in selected:
                scored.tags["_window_score"] = f"{score_total:.4f}"
                out_score = scored.score if scored.score is not None else score_total
                scored_item = Item(
                    id=scored.id,
                    summary=scored.summary,
                    tags=scored.tags,
                    score=out_score,
                )
                groups.setdefault(primary_id, []).append(scored_item)

        for primary_id, group in groups.items():
            group.sort(
                key=lambda x: (
                    float(x.tags.get("_window_score", "0") or 0),
                    x.score or 0.0,
                ),
                reverse=True,
            )
            # Hide internal rerank helper tag from downstream output.
            for gi in group:
                if "_window_score" in gi.tags:
                    del gi.tags["_window_score"]
            groups[primary_id] = group

        return groups

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
        """
        Find items by hybrid search (semantic + FTS5) or similarity to an existing note.

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
                logger.warning("System doc migration deferred: %s", e)

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
                deep_groups = self._deep_tag_follow(
                    items, chroma_coll, doc_coll, embedding=embedding,
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

    def get_context(
        self,
        id: str,
        *,
        version: int | None = None,
        similar_limit: int = 3,
        meta_limit: int = 3,
        include_similar: bool = True,
        include_meta: bool = True,
        include_parts: bool = True,
        include_versions: bool = True,
    ) -> ItemContext | None:
        """Assemble complete display context for a single item.

        Single implementation of the 5-call assembly pattern used by
        CLI get, now, and put commands.  Returns None if the item
        doesn't exist.

        Args:
            id: Document identifier
            version: Public version selector:
                - None or 0: current
                - N > 0: offset from current (1=previous)
                - N < 0: archived ordinal from oldest (-1=oldest)
            similar_limit: Max similar items to include
            meta_limit: Max items per meta-doc section
            include_similar: Whether to resolve similar items
            include_meta: Whether to resolve meta-doc sections
            include_parts: Whether to include parts manifest
            include_versions: Whether to include version navigation
        """
        id = normalize_id(id)
        resolved = self.resolve_version_offset(id, version)
        if resolved is None:
            return None
        offset = resolved
        if offset > 0:
            item = self.get_version(id, offset)
        else:
            item = self.get(id)
        if item is None:
            return None

        # Version navigation
        prev_refs: list[VersionRef] = []
        next_refs: list[VersionRef] = []
        if include_versions:
            if offset == 0:
                nav = self.get_version_nav(id, None)
                for i, v in enumerate(nav.get("prev", [])):
                    prev_refs.append(VersionRef(
                        offset=i + 1,
                        date=local_date(v.tags.get("_created") or v.created_at or ""),
                        summary=v.summary,
                    ))
            else:
                # Keep navigation in user-visible offset space.
                # This avoids mixing offset (V{N}) with internal version numbers.
                older = self.get_version(id, offset + 1)
                if older is not None:
                    prev_refs.append(VersionRef(
                        offset=offset + 1,
                        date=local_date(older.tags.get("_created", "")),
                        summary=older.summary,
                    ))
                if offset > 1:
                    newer = self.get_version(id, offset - 1)
                    if newer is not None:
                        next_refs.append(VersionRef(
                            offset=offset - 1,
                            date=local_date(newer.tags.get("_created", "")),
                            summary=newer.summary,
                        ))

        # Similar items (only for current version)
        similar_refs: list[SimilarRef] = []
        if include_similar and offset == 0:
            raw = self.get_similar_for_display(id, limit=similar_limit)
            for s in raw:
                s_offset = self.get_version_offset(s)
                similar_refs.append(SimilarRef(
                    id=s.tags.get("_base_id", s.id),
                    offset=s_offset,
                    score=s.score,
                    date=local_date(
                        s.tags.get("_updated") or s.tags.get("_created", "")
                    ),
                    summary=s.summary,
                ))

        # Meta-doc sections (only for current version)
        meta_refs: dict[str, list[MetaRef]] = {}
        if include_meta and offset == 0:
            raw_meta = self.resolve_meta(id, limit_per_doc=meta_limit)
            for name, meta_items in raw_meta.items():
                meta_refs[name] = [
                    MetaRef(id=mi.id, summary=mi.summary)
                    for mi in meta_items
                ]

        # Parts manifest (only for current version)
        part_refs: list[PartRef] = []
        if include_parts and offset == 0:
            for p in self.list_parts(id):
                part_refs.append(PartRef(
                    part_num=p.part_num,
                    summary=p.summary,
                    tags=dict(p.tags),
                ))

        # Edge references (only for current version)
        # - Inverse refs: current item is edge target (existing behavior)
        # - Explicit refs: current item has edge-tag values (query-time union)
        # Dedup key: (edge key, source_id). If both explicit+inverse produce
        # the same source, inverse data takes precedence.
        edge_refs: dict[str, list[EdgeRef]] = {}
        if offset == 0:
            doc_coll = self._resolve_doc_collection()
            edge_ref_index: dict[str, dict[str, EdgeRef]] = {}

            def _upsert_edge_ref(
                key: str,
                ref: EdgeRef,
                *,
                prefer_new: bool = False,
            ) -> None:
                refs_for_key = edge_ref_index.setdefault(key, {})
                existing = refs_for_key.get(ref.source_id)
                if existing is None:
                    refs_for_key[ref.source_id] = ref
                    return

                if prefer_new:
                    refs_for_key[ref.source_id] = EdgeRef(
                        source_id=ref.source_id,
                        date=ref.date or existing.date,
                        summary=ref.summary or existing.summary,
                    )
                else:
                    refs_for_key[ref.source_id] = EdgeRef(
                        source_id=ref.source_id,
                        date=existing.date or ref.date,
                        summary=existing.summary or ref.summary,
                    )

            # Explicit edge refs from current item's edge-tag values.
            # These are rendered alongside inverse refs under the same tag key.
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
                            target_id = normalize_id(raw_target)
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
                        _upsert_edge_ref(
                            key,
                            EdgeRef(
                                source_id=target_id,
                                date=created,
                                summary=summary,
                            ),
                        )

            raw_edges = self._document_store.get_inverse_edges(doc_coll, id)
            if raw_edges:
                # Batch-fetch source docs to avoid N+1
                source_ids = list({src for _, src, _ in raw_edges})
                source_docs = self._document_store.get_many(doc_coll, source_ids)
                for inverse, source_id, created in raw_edges:
                    doc = source_docs.get(source_id)
                    ref = EdgeRef(
                        source_id=source_id,
                        date=local_date(created),
                        summary=doc.summary if doc else "",
                    )
                    _upsert_edge_ref(inverse, ref, prefer_new=True)

            edge_refs = {
                key: list(refs_by_source.values())
                for key, refs_by_source in edge_ref_index.items()
            }

        return ItemContext(
            item=item,
            viewing_offset=offset,
            similar=similar_refs,
            meta=meta_refs,
            edges=edge_refs,
            parts=part_refs,
            prev=prev_refs,
            next=next_refs,
        )

    def resolve_version_offset(self, id: str, selector: int | None) -> Optional[int]:
        """
        Resolve a public version selector to a concrete offset.

        Public selector semantics:
        - None or 0: current version (offset 0)
        - N > 0: N versions back from current (offset N)
        - N < 0: Nth archived version from oldest (-1 oldest, -2 second-oldest)

        Returns:
            Resolved non-negative offset, or None if selector is out of range.
        """
        if selector is None or selector == 0:
            return 0
        if selector > 0:
            return selector

        doc_coll = self._resolve_doc_collection()
        archived_count = self._document_store.version_count(doc_coll, id)
        oldest_ordinal = -selector
        if oldest_ordinal < 1 or oldest_ordinal > archived_count:
            return None
        # oldest ordinal 1 maps to deepest offset (archived_count)
        return archived_count - oldest_ordinal + 1

    # ------------------------------------------------------------------
    # Agent prompts
    # ------------------------------------------------------------------

    def render_prompt(
        self,
        name: str,
        text: Optional[str] = None,
        *,
        id: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        tags: Optional[TagMap] = None,
        limit: int = 10,
        deep: bool = False,
        token_budget: Optional[int] = None,
    ) -> Optional[PromptResult]:
        """Render an agent prompt doc with injected context.

        Reads ``.prompt/agent/{name}`` from the store, extracts the
        ``## Prompt`` section, and assembles context.  The prompt text
        may contain ``{get}`` and ``{find}`` placeholders — the caller
        expands them with the rendered context and search results.
        Use ``{find:deep}`` to force deep tag-follow search regardless
        of the caller's ``deep`` parameter.

        Args:
            name: Prompt name (e.g. "reflect")
            text: Optional similarity key for search context
            id: Item ID for ``{get}`` context (default: "now")
            since: Time filter (ISO duration or date)
            until: Upper-bound time filter (ISO duration or date)
            tags: Tag filter for search results
            limit: Max search results
            deep: Follow tags from results to discover related items
            token_budget: Explicit token budget (None = use template default)

        Returns:
            PromptResult with context, search_results, and prompt template,
            or None if the prompt doc doesn't exist.
        """
        from .analyzers import extract_prompt_section

        doc_id = f".prompt/agent/{name}"
        doc = self.get(doc_id)
        if doc is None:
            return None

        prompt_body = extract_prompt_section(doc.summary)
        if not prompt_body:
            # Fall back to full content if no ## Prompt section
            prompt_body = doc.summary

        # Context: get_context for the target item (default "now")
        context_id = id or "now"
        if context_id == "now":
            self.get_now()  # ensure now exists (auto-creates from bundled doc)
        ctx = self.get_context(context_id)

        # {find:deep} or {find:deep:N} in the template forces deep search
        if "{find:deep" in prompt_body:
            deep = True

        # Ensure retrieval limit is high enough to fill the token budget.
        # Deep items are rendered in a separate pass from leftover budget,
        # so the primary fetch size doesn't need to shrink for deep mode.
        tokens_per_item = 50
        effective_budget = token_budget or 4000
        fetch_limit = min(200, max(limit, effective_budget // tokens_per_item))

        # Search: find similar items with available filters
        search_results = None
        if text:
            search_results = self.find(
                query=text, tags=tags, since=since, until=until, limit=fetch_limit,
                deep=deep,
            )
        elif tags or since or until:
            search_results = self.find(
                similar_to=context_id, tags=tags, since=since, until=until,
                limit=fetch_limit, deep=deep,
            )

        return PromptResult(
            context=ctx,
            search_results=search_results,
            prompt=prompt_body,
            text=text,
            since=since,
            until=until,
            token_budget=token_budget,
        )

    def list_prompts(self) -> list[PromptInfo]:
        """List available agent prompt docs.

        Returns:
            List of PromptInfo with name and summary for each
            ``.prompt/agent/*`` doc in the store.
        """
        prefix = ".prompt/agent/"
        items = self.list_items(
            prefix=prefix, include_hidden=True, limit=100,
        )
        result = []
        for item in items:
            name = item.id[len(prefix):]
            # Find first non-empty, non-heading line as description
            summary = ""
            for line in (item.summary or "").split("\n"):
                line = line.strip()
                if line and not line.startswith("#"):
                    summary = line
                    break
            result.append(PromptInfo(name=name, summary=summary))
        return result

    def get_similar_for_display(
        self,
        id: str,
        *,
        limit: int = 3,
    ) -> list[Item]:
        """
        Find similar items for frontmatter display using stored embedding.

        Optimized for display: uses stored embedding (no re-embedding),
        filters to distinct base documents, excludes source document versions.

        Args:
            id: ID of item to find similar items for
            limit: Maximum results to return

        Returns:
            List of similar items, one per unique base document
        """
        chroma_coll = self._resolve_chroma_collection()

        # Get the stored embedding (no re-embedding)
        embedding = self._store.get_embedding(chroma_coll, id)
        if embedding is None:
            return []

        # Fetch more than needed to account for version/hidden filtering
        fetch_limit = limit * 5
        results = self._store.query_embedding(chroma_coll, embedding, limit=fetch_limit)

        # Convert to Items
        items = [r.to_item() for r in results]

        # Extract base ID of source document
        source_base_id = id.split("@v")[0] if "@v" in id else id

        # Filter to distinct base IDs, excluding source document and hidden notes
        seen_base_ids: set[str] = set()
        filtered: list[Item] = []
        for item in items:
            # Get base ID from tags or parse from ID
            base_id = item.tags.get("_base_id", item.id.split("@v")[0] if "@v" in item.id else item.id)

            # Skip versions of source document and hidden system notes
            if base_id == source_base_id or base_id.startswith("."):
                continue

            # Keep only first version of each document
            if base_id not in seen_base_ids:
                seen_base_ids.add(base_id)
                filtered.append(item)

                if len(filtered) >= limit:
                    break

        return filtered

    def get_version_offset(self, item: Item) -> int:
        """
        Get version offset (0=current, 1=previous, ...) for an item.

        Converts the internal version number (1=oldest, 2=next...) to the
        user-visible offset format (0=current, 1=previous, 2=two-ago...).

        Args:
            item: Item to get version offset for

        Returns:
            Version offset (0 for current version)
        """
        version_tag = item.tags.get("_version")
        if not version_tag:
            return 0  # Current version
        base_id = item.tags.get("_base_id", item.id)
        doc_coll = self._resolve_doc_collection()
        # Count versions >= this one to get the offset (handles gaps)
        internal_version = int(version_tag)
        return self._document_store.count_versions_from(
            doc_coll, base_id, internal_version
        )

    def resolve_meta(
        self,
        item_id: str,
        *,
        limit_per_doc: int = 3,
    ) -> dict[str, list[Item]]:
        """
        Resolve all .meta/* docs against an item's tags.

        Meta-docs define tag-based queries that surface contextually relevant
        items — open commitments, past learnings, decisions to revisit.
        Results are ranked by similarity to the current item + recency decay,
        so the most relevant matches surface first.

        Args:
            item_id: ID of the item whose tags provide context
            limit_per_doc: Max results per meta-doc

        Returns:
            Dict of {meta_name: [matching Items]}. Empty results omitted.
        """
        doc_coll = self._resolve_doc_collection()

        # Find all .meta/* documents
        meta_records = self._document_store.query_by_id_prefix(doc_coll, ".meta/")
        if not meta_records:
            return {}

        # Get current item's tags for context
        current = self.get(item_id)
        if current is None:
            return {}
        current_tags = current.tags

        result: dict[str, list[Item]] = {}

        for rec in meta_records:
            meta_id = rec.id
            short_name = meta_id.split("/", 1)[1] if "/" in meta_id else meta_id

            query_lines, context_keys, prereq_keys = _parse_meta_doc(rec.summary)
            if not query_lines and not context_keys:
                continue

            matches = self._resolve_meta_queries(
                item_id, current_tags, query_lines, context_keys, prereq_keys, limit_per_doc,
            )
            if not matches:
                continue

            # Split: direct matches go to {name}, part-sourced go to {name}/provisional
            direct = [m for m in matches if not m.tags.get("_provisional")]
            provisional = [m for m in matches if m.tags.get("_provisional")]

            if direct:
                result[short_name] = direct
            if provisional:
                result[f"{short_name}/provisional"] = provisional

        return result

    def resolve_inline_meta(
        self,
        item_id: str,
        queries: list[dict[str, str]],
        context_keys: list[str] | None = None,
        prereq_keys: list[str] | None = None,
        *,
        limit: int = 3,
    ) -> list[Item]:
        """
        Resolve an inline meta query against an item's tags.

        Like resolve_meta() but with ad-hoc queries instead of persistent
        .meta/* documents. Queries use the same tag-based syntax.

        Args:
            item_id: ID of the item whose tags provide context
            queries: List of tag-match dicts, each {key: value} for AND queries;
                     multiple dicts are OR (union)
            context_keys: Tag keys to expand from the current item's tags
            prereq_keys: Tag keys the current item must have (or return empty)
            limit: Max results

        Returns:
            List of matching Items, ranked by similarity + recency.
        """
        current = self.get(item_id)
        if current is None:
            return []

        return self._resolve_meta_queries(
            item_id, current.tags,
            queries, context_keys or [], prereq_keys or [], limit,
        )

    def _resolve_meta_queries(
        self,
        item_id: str,
        current_tags: dict[str, str],
        query_lines: list[dict[str, str]],
        context_keys: list[str],
        prereq_keys: list[str],
        limit: int,
    ) -> list[Item]:
        """Shared resolution logic for persistent and inline metadocs."""
        # Check prerequisites: current item must have all required tags
        if prereq_keys:
            if not all(current_tags.get(k) for k in prereq_keys):
                return []

        # Get context values from current item's tags
        context_values: dict[str, str] = {}
        for key in context_keys:
            val = current_tags.get(key)
            if val and not key.startswith("_"):
                context_values[key] = val

        # Build expanded queries: cross-product of query lines × context values
        expanded: list[dict[str, str]] = []
        if context_values and query_lines:
            for query in query_lines:
                for ctx_key, ctx_val in context_values.items():
                    expanded.append({**query, ctx_key: ctx_val})
        elif context_values:
            # Context-only (group-by): each context value becomes a query
            for ctx_key, ctx_val in context_values.items():
                expanded.append({ctx_key: ctx_val})
        else:
            # No context → use query lines as-is
            expanded = list(query_lines)

        # Run each expanded query, union results (fetch generously for ranking)
        seen_ids: set[str] = set()
        matches: list[Item] = []
        for query in expanded:
            try:
                items = self.list_items(
                    tags=query,
                    limit=100,  # fetch all candidates for ranking
                )
            except (ValueError, Exception):
                continue
            for item in items:
                # Skip the current item, hidden system notes, and dupes
                if item.id == item_id or _is_hidden(item) or item.id in seen_ids:
                    continue
                seen_ids.add(item.id)
                matches.append(item)

        if not matches:
            return []

        # Part-to-parent uplift: replace part hits with parent doc ID
        # but keep the part's summary text.  Mark uplifted items with
        # _provisional tag so resolve_meta() can route them separately.
        doc_coll = self._resolve_doc_collection()
        direct_ids: set[str] = set()
        uplifted: list[Item] = []

        # First pass: collect direct (non-part) IDs
        for item in matches:
            if not is_part_id(item.id):
                direct_ids.add(item.id)

        # Second pass: uplift parts, skip if parent already matched directly
        seen_parents: set[str] = set()
        for item in matches:
            if is_part_id(item.id):
                parent_id = item.tags.get("_base_id", item.id.split("@")[0])
                if parent_id in direct_ids or parent_id in seen_parents:
                    continue  # Parent already matched directly, or another part did
                seen_parents.add(parent_id)
                tags = dict(item.tags)
                tags["_provisional"] = "1"
                uplifted.append(Item(
                    id=parent_id, summary=item.summary,
                    tags=tags, score=item.score,
                ))
            else:
                uplifted.append(item)

        if not uplifted:
            return []

        # Rank by similarity to current item + recency decay
        uplifted = self._rank_by_relevance(self._resolve_chroma_collection(), item_id, uplifted)
        return uplifted[:limit]

    def _rank_by_relevance(
        self,
        coll: str,
        anchor_id: str,
        candidates: list[Item],
    ) -> list[Item]:
        """
        Rank candidate items by similarity to anchor + recency decay.

        Uses stored embeddings — no re-embedding needed.
        Falls back to recency-only ranking if embeddings unavailable.
        """
        import math

        if not candidates:
            return candidates

        # Get anchor + candidate embeddings from store
        try:
            candidate_ids = [c.id for c in candidates]
            all_ids = [anchor_id] + candidate_ids
            entries = self._store.get_entries_full(coll, all_ids)
        except Exception as e:
            logger.debug("Embedding lookup failed, falling back to recency: %s", e)
            return self._apply_recency_decay(candidates)

        # Build id → embedding lookup
        emb_lookup: dict[str, list[float]] = {}
        for entry in entries:
            if entry.get("embedding") is not None:
                emb_lookup[entry["id"]] = entry["embedding"]

        # Extract anchor embedding
        anchor_emb = emb_lookup.get(anchor_id)
        if anchor_emb is None:
            return self._apply_recency_decay(candidates)

        # Score each candidate: cosine similarity
        def _cosine_sim(a: list[float], b: list[float]) -> float:
            dot = sum(x * y for x, y in zip(a, b))
            norm_a = math.sqrt(sum(x * x for x in a))
            norm_b = math.sqrt(sum(x * x for x in b))
            if norm_a == 0 or norm_b == 0:
                return 0.0
            return dot / (norm_a * norm_b)

        scored = []
        for item in candidates:
            emb = emb_lookup.get(item.id)
            sim = _cosine_sim(anchor_emb, emb) if emb is not None else 0.0
            scored.append(Item(id=item.id, summary=item.summary, tags=item.tags, score=sim))

        # Apply recency decay to the similarity scores
        candidates = self._apply_recency_decay(scored)

        # Sort by score descending
        candidates.sort(key=lambda x: x.score or 0.0, reverse=True)
        return candidates

    def list_tags(
        self,
        key: Optional[str] = None,
    ) -> list[str]:
        """
        List distinct tag keys or values.

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
        """
        Retrieve a specific item by ID.

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
        """
        Get a specific version of a document by public selector.

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
        """
        List version history for a document.

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
        """
        Get version navigation info (prev/next) for display.

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
        """
        Check if an item exists in the store.
        """
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
        """
        Delete an item from both stores.

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
        """
        Revert to the previous version, or delete if no versions exist.

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
        """
        Delete a specific archived version by public selector.

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
        """
        Get the current working intentions.

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
        """
        Set the current working intentions.

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
        """
        Move versions from a source document into a named item.

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
        """
        Update tags on an existing document without re-processing.

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
                logger.warning("System doc migration deferred: %s", e)

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
            self._validate_constrained_tags(add_changes)
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
        """
        Update user tags on a part without re-analyzing.

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
            self._validate_constrained_tags(add_changes)
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

    def _gather_analyze_chunks(self, id: str, doc_record) -> list[dict]:
        """Gather content chunks for analysis as serializable dicts.

        For URI sources: re-fetch the document content.
        For inline notes: assemble version history chronologically.
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

        if not chunks:
            versions = self._document_store.list_versions(doc_coll, id, limit=100)
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
        """
        Decompose a note or string into meaningful parts.

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

        # Phase 1: Gather — assemble chunks, guide context, tag specs (local)
        chunk_dicts = self._gather_analyze_chunks(id, doc_record)

        total_content = "".join(c["content"] for c in chunk_dicts)
        if len(total_content.strip()) < 50:
            raise ValueError(f"Document content too short to analyze: {id}")

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
            # Record _analyzed_hash so we don't re-run the LLM next time
            if doc_record.content_hash:
                chroma_coll = self._resolve_chroma_collection()
                updated_tags = dict(doc_record.tags)
                updated_tags["_analyzed_hash"] = doc_record.content_hash
                self._document_store.update_tags(doc_coll, id, updated_tags)
                self._store.update_tags(chroma_coll, id,
                                        casefold_tags_for_index(updated_tags))
            return []

        # Phase 3: Apply — write parts + embeddings to stores (local)
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
                from .document_store import PartInfo
                from .types import utc_now

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
                # SQLite: INSERT OR REPLACE (doesn't touch parts 1..N)
                self._document_store.upsert_single_part(
                    doc_coll, id, overview_part,
                )
                # ChromaDB: upsert with embedding
                chroma_coll = self._resolve_chroma_collection()
                embed = self._get_embedding_provider()
                embedding = embed.embed(overview)
                self._store.upsert_part(
                    chroma_coll, id, 0,
                    embedding, overview,
                    casefold_tags_for_index(overview_part.tags),
                )

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

    def get_part(self, id: str, part_num: int) -> Optional[Item]:
        """
        Get a specific part of a document.

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
        """
        List all parts for a document.

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
        """
        List all collections in the store.
        """
        # Merge collections from both stores
        doc_collections = set(self._document_store.list_collections())
        chroma_collections = set(self._store.list_collections())
        return sorted(doc_collections | chroma_collections)
    
    def count(self) -> int:
        """
        Count items in a collection.

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
        """
        List items with composable filters.

        All filters are AND'd together. Prefix narrows by ID, tags narrow
        by metadata, date narrows by time.

        Args:
            prefix: ID prefix filter (e.g. ".tag/act").
            tags: Tag key=value filters (all must match).
            tag_keys: Tag key-only filters (item must have key, any value).
            since: Only items updated since (ISO duration like P3D, or date).
            until: Only items updated before (ISO duration or date).
            order_by: Sort order - "updated" (default) or "accessed".
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
        """
        Enqueue a note for background analysis (decomposition into parts).

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

        metadata = {}
        if tags:
            metadata["tags"] = tags
        if force:
            metadata["force"] = True

        self._pending_queue.enqueue(
            id, doc_coll, "",
            task_type="analyze",
            metadata=metadata,
        )
        self._spawn_processor()
        return True

    def process_pending(self, limit: int = 10) -> dict:
        """
        Process pending work items (embedding, summaries, OCR, and analysis).

        Handles task types serially:
        - "embed": computes and stores embeddings (cloud mode deferred writes)
        - "ocr": OCR scanned PDF pages, update document content
        - "summarize": generates real summaries for lazy-indexed items
        - "analyze": decomposes documents into parts via LLM

        Items that fail MAX_SUMMARY_ATTEMPTS times are removed from
        the queue.

        Args:
            limit: Maximum number of items to process in this batch

        Returns:
            Dict with: processed (int), failed (int), abandoned (int), errors (list)
        """
        from .processors import DELEGATABLE_TASK_TYPES

        items = self._pending_queue.dequeue(limit=limit)
        result = {"processed": 0, "failed": 0, "abandoned": 0, "delegated": 0, "errors": []}

        for item in items:
            # Skip items that have failed too many times
            # (attempts was already incremented by dequeue, so check >= MAX)
            if item.attempts >= MAX_SUMMARY_ATTEMPTS:
                # Move to dead letter — preserved for diagnosis
                self._pending_queue.abandon(
                    item.id, item.collection, item.task_type,
                    error=f"Exhausted {item.attempts} attempts",
                )
                result["abandoned"] += 1
                logger.warning(
                    "Abandoned pending %s after %d attempts: %s",
                    item.task_type, item.attempts, item.id
                )
                continue

            # Delegate to hosted service if available and appropriate
            if (
                self._task_client
                and item.task_type in DELEGATABLE_TASK_TYPES
                and not (item.metadata or {}).get("_local_only")
            ):
                try:
                    self._delegate_task(item)
                    result["delegated"] += 1
                    continue
                except Exception as e:
                    logger.warning(
                        "Delegation failed for %s %s, falling back to local: %s",
                        item.task_type, item.id, e,
                    )
                    # Fall through to local processing

            try:
                _task_verbs = {
                    "analyze": "Analyzing",
                    "backfill-edges": "Backfilling edges",
                    "embed": "Embedding",
                    "ocr": "OCR",
                    "planner-rebuild": "Rebuilding planner stats",
                    "reindex": "Re-embedding",
                    "summarize": "Summarizing",
                }
                verb = _task_verbs.get(item.task_type, item.task_type)
                logger.info("%s %s (attempt %d)", verb, item.id, item.attempts)
                if item.task_type == "analyze":
                    self._process_pending_analyze(item)
                    # analyze releases summarization internally;
                    # release embedding after parts are embedded
                    self._release_embedding_provider()
                elif item.task_type == "backfill-edges":
                    self._process_pending_backfill_edges(item)
                elif item.task_type == "embed":
                    self._process_pending_embed(item)
                    self._release_embedding_provider()
                elif item.task_type == "ocr":
                    self._process_pending_ocr(item)
                    self._release_content_extractor()
                    self._release_summarization_provider()
                    self._release_embedding_provider()
                elif item.task_type == "planner-rebuild":
                    if self._planner_stats:
                        self._planner_stats.rebuild(
                            self._document_store, item.collection,
                        )
                elif item.task_type == "reindex":
                    self._process_pending_reindex(item)
                    self._release_embedding_provider()
                else:
                    self._process_pending_summarize(item)
                    # Release summarization model between items to
                    # prevent both models residing in memory at once
                    self._release_summarization_provider()

                # Remove from queue
                self._pending_queue.complete(
                    item.id, item.collection, item.task_type
                )
                result["processed"] += 1
                logger.info("%s %s done", verb, item.id)

                # Brief yield between items so interactive processes
                # (keep now, keep find) can acquire the model lock
                # without waiting for the entire batch to finish.
                time.sleep(0.1)

            except Exception as e:
                error_msg = f"{type(e).__name__}: {e}"

                # Permanent failures: content issues that won't resolve
                # on retry (e.g. scanned PDF with no text layer).
                _permanent = (
                    "content too short" in str(e).lower()
                    or "no text extracted" in str(e).lower()
                    or "no content extractor" in str(e).lower()
                )
                if _permanent:
                    self._pending_queue.abandon(
                        item.id, item.collection, item.task_type,
                        error=f"Permanent: {error_msg}",
                    )
                    result["abandoned"] = result.get("abandoned", 0) + 1
                    logger.info(
                        "Abandoned %s %s (permanent failure): %s",
                        item.task_type, item.id, e,
                    )
                    continue

                # Transient failure — retry on next batch.
                # (attempt counter already incremented by dequeue)
                self._pending_queue.fail(
                    item.id, item.collection, item.task_type,
                    error=error_msg,
                )
                result["failed"] += 1
                result["errors"].append(f"{item.id}: {error_msg}")
                logger.warning("Failed to %s %s (attempt %d): %s",
                             item.task_type, item.id, item.attempts, e)

        # Drain planner outbox (bounded)
        if self._planner_stats:
            try:
                doc_coll = self._resolve_doc_collection()
                planner_result = self._planner_stats.drain_outbox(
                    self._document_store, doc_coll,
                    max_items=20, max_ms=200,
                )
                if planner_result["processed"]:
                    logger.info(
                        "Planner stats: processed=%d failed=%d",
                        planner_result["processed"], planner_result["failed"],
                    )
            except Exception as e:
                logger.debug("Planner drain skipped: %s", e)

        # Poll for delegated task results
        if self._task_client:
            self._poll_delegated(result)

        return result

    def _delegate_task(self, item) -> None:
        """Submit a task to the hosted service and mark as delegated."""
        from .task_client import TaskClientError

        content = item.content
        # For summarize tasks, gather context to include in metadata
        meta = dict(item.metadata or {})

        if item.task_type == "summarize":
            doc = self._document_store.get(item.collection, item.id)
            if doc:
                user_tags = filter_non_system_tags(doc.tags)
                if user_tags:
                    context = self._gather_context(item.id, user_tags)
                    if context:
                        meta["context"] = context
                # Resolve prompt from .prompt/summarize/* docs
                try:
                    prompt = self._resolve_prompt_doc("summarize", doc.tags)
                    if prompt:
                        meta["system_prompt_override"] = prompt
                except Exception:
                    pass  # Remote falls back to its default

        elif item.task_type == "analyze":
            doc = self._document_store.get(item.collection, item.id)
            if not doc:
                logger.info("Skipping delegation for deleted doc: %s", item.id)
                return
            # Gather chunks, guide context, and tag specs locally
            meta["chunks"] = self._gather_analyze_chunks(item.id, doc)
            guidance_tags = (item.metadata or {}).get("tags")
            if guidance_tags:
                meta["guide_context"] = self._gather_guide_context(guidance_tags)
            try:
                from .analyzers import TagClassifier
                classifier = TagClassifier(
                    provider=self._get_summarization_provider(),
                )
                specs = classifier.load_specs(self)
                if specs:
                    meta["tag_specs"] = specs
            except Exception as e:
                logger.warning("Could not load tag specs for delegation: %s", e)
            # Resolve prompt from .prompt/analyze/* docs
            try:
                prompt = self._resolve_prompt_doc("analyze", doc.tags)
                if prompt:
                    meta["prompt_override"] = prompt
            except Exception:
                pass  # Remote falls back to its default
            content = ""  # content is in chunks, not top-level

        try:
            remote_task_id = self._task_client.submit(
                item.task_type, content, meta or None,
            )
        except TaskClientError:
            raise  # Let caller handle fallback

        self._pending_queue.mark_delegated(
            item.id, item.collection, item.task_type, remote_task_id,
        )
        logger.info(
            "Delegated %s %s → remote task %s",
            item.task_type, item.id, remote_task_id,
        )

    # Max time a task can stay delegated before falling back to local
    DELEGATION_STALE_SECONDS = 3600  # 1 hour

    def _poll_delegated(self, result: dict) -> None:
        """Poll for results of delegated tasks and apply them."""
        from .processors import ProcessorResult
        from .task_client import TaskClientError

        delegated = self._pending_queue.list_delegated()
        if not delegated:
            return

        now = datetime.now(timezone.utc)

        for item in delegated:
            remote_task_id = item.metadata.get("_remote_task_id")
            if not remote_task_id:
                continue

            try:
                resp = self._task_client.poll(remote_task_id)
            except TaskClientError as e:
                logger.warning("Poll failed for %s: %s", remote_task_id, e)
                # Check staleness even on poll failure
                if self._is_delegation_stale(item, now):
                    self._revert_stale_delegation(item, result,
                                                  "Poll unreachable and delegation stale")
                continue

            if resp["status"] == "completed":
                task_result = resp.get("result") or {}
                proc_result = ProcessorResult(
                    task_type=item.task_type,
                    summary=task_result.get("summary"),
                    content=task_result.get("content"),
                    content_hash=task_result.get("content_hash"),
                    content_hash_full=task_result.get("content_hash_full"),
                    parts=task_result.get("parts"),
                )
                # Get existing doc tags for apply_result
                doc = self._document_store.get(item.collection, item.id)
                existing_tags = doc.tags if doc else None
                self.apply_result(
                    item.id, item.collection, proc_result,
                    existing_tags=existing_tags,
                )
                self._pending_queue.complete(item.id, item.collection, item.task_type)
                try:
                    self._task_client.acknowledge(remote_task_id)
                except Exception:
                    pass  # Non-critical
                result["processed"] = result.get("processed", 0) + 1
                logger.info("Delegated %s %s completed", item.task_type, item.id)

            elif resp["status"] == "failed":
                error = resp.get("error", "Remote processing failed")
                self._pending_queue.fail(
                    item.id, item.collection, item.task_type,
                    error=f"Remote: {error}",
                )
                result["failed"] = result.get("failed", 0) + 1
                logger.warning(
                    "Delegated %s %s failed: %s",
                    item.task_type, item.id, error,
                )

            elif resp["status"] == "not_found":
                # Task disappeared from server (cleanup, bug, etc.)
                # Revert to pending for local processing
                self._revert_stale_delegation(item, result,
                                              "Remote task not found")

            else:
                # Still queued/processing — check staleness
                if self._is_delegation_stale(item, now):
                    self._revert_stale_delegation(item, result,
                                                  "Delegation stale (>1h)")

    def _is_delegation_stale(self, item, now) -> bool:
        """Check if a delegated item has exceeded the staleness threshold."""
        if not item.delegated_at:
            return False
        try:
            delegated_time = datetime.fromisoformat(item.delegated_at)
            age = (now - delegated_time).total_seconds()
            return age > self.DELEGATION_STALE_SECONDS
        except (ValueError, TypeError):
            return False

    def _revert_stale_delegation(self, item, result: dict, reason: str) -> None:
        """Revert a delegated item to pending for local processing."""
        self._pending_queue.fail(
            item.id, item.collection, item.task_type,
            error=f"Delegation reverted: {reason}",
        )
        result["failed"] = result.get("failed", 0) + 1
        logger.warning(
            "Delegated %s %s reverted to local: %s",
            item.task_type, item.id, reason,
        )

    # -------------------------------------------------------------------------
    # Planner priors
    # -------------------------------------------------------------------------

    def get_planner_priors(
        self,
        scope_key: str | None = None,
        candidates: list[str] | None = None,
    ) -> dict:
        """Return minimal planner priors for continuation discriminators.

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

    def apply_result(self, item_id, collection, result, *, existing_tags=None):
        """Apply a ProcessorResult to the local store."""
        if result.task_type == "summarize":
            self._document_store.update_summary(collection, item_id, result.summary)
            self._store.update_summary(collection, item_id, result.summary)

        elif result.task_type == "ocr":
            self._document_store.update_summary(collection, item_id, result.summary)
            self._document_store.update_content_hash(
                collection, item_id,
                content_hash=result.content_hash,
                content_hash_full=result.content_hash_full,
            )
            chroma_coll = self._resolve_chroma_collection()
            embedding = self._get_embedding_provider().embed(result.summary)
            self._store.upsert(
                collection=chroma_coll,
                id=item_id,
                embedding=embedding,
                summary=result.summary,
                tags=casefold_tags_for_index(existing_tags or {}),
            )

        elif result.task_type == "analyze":
            from .document_store import PartInfo
            from .types import utc_now

            doc = self._document_store.get(collection, item_id)
            if not doc:
                return

            parent_user_tags = {
                k: v for k, v in (existing_tags or {}).items()
                if not k.startswith(SYSTEM_TAG_PREFIX)
            }

            now = utc_now()
            parts = []
            for i, raw in enumerate(result.parts or [], 1):
                part_tags = dict(parent_user_tags)
                if raw.get("tags"):
                    part_tags.update(raw["tags"])
                parts.append(PartInfo(
                    part_num=i,
                    summary=raw.get("summary", ""),
                    tags=part_tags,
                    content=raw.get("content", ""),
                    created_at=now,
                ))

            chroma_coll = self._resolve_chroma_collection()

            # Atomic replace: delete old, insert new
            self._store.delete_parts(chroma_coll, item_id)
            self._document_store.delete_parts(collection, item_id)
            self._document_store.upsert_parts(collection, item_id, parts)

            # Embed each part
            embed = self._get_embedding_provider()
            for part in parts:
                embedding = embed.embed(part.summary)
                self._store.upsert_part(
                    chroma_coll, item_id, part.part_num,
                    embedding, part.summary, casefold_tags_for_index(part.tags),
                )

            # Record _analyzed_hash
            if doc.content_hash:
                updated_tags = dict(doc.tags)
                updated_tags["_analyzed_hash"] = doc.content_hash
                self._document_store.update_tags(collection, item_id, updated_tags)
                self._store.update_tags(chroma_coll, item_id,
                                        casefold_tags_for_index(updated_tags))

    def _process_pending_summarize(self, item) -> None:
        """Process a pending summarization work item."""
        from .processors import process_summarize

        doc = self._document_store.get(item.collection, item.id)
        if doc is None:
            # Doc was deleted/moved since enqueue — skip (no point retrying)
            logger.info("Skipping summary for deleted doc: %s", item.id)
            return

        # Resolve summarization prompt from .prompt/summarize/* docs
        summarize_prompt = None
        try:
            summarize_prompt = self._resolve_prompt_doc("summarize", doc.tags)
        except Exception as e:
            logger.debug("Summarize prompt doc resolution failed: %s", e)

        context = None
        user_tags = filter_non_system_tags(doc.tags)
        if user_tags:
            context = self._gather_context(item.id, user_tags)

        result = process_summarize(
            item.content, context=context,
            summarization_provider=self._get_summarization_provider(),
            system_prompt_override=summarize_prompt,
        )
        self.apply_result(item.id, item.collection, result)

    def _process_pending_embed(self, item) -> None:
        """Process a deferred embedding task (cloud mode).

        Computes the embedding for the content and writes it to the
        vector store.  If the doc's content changed (metadata flag),
        archives the old embedding as a versioned entry first.
        """
        doc_coll = item.collection
        chroma_coll = self._resolve_chroma_collection()

        # Get current doc record (may have been deleted before we got here)
        doc = self._document_store.get(doc_coll, item.id)
        if doc is None:
            return

        content_changed = (item.metadata or {}).get("content_changed", False)

        # Archive old embedding before overwriting (version archival)
        if content_changed:
            old_embedding = self._store.get_embedding(chroma_coll, item.id)
            max_ver = self._document_store.max_version(doc_coll, item.id)
            if max_ver > 0 and old_embedding is not None:
                # Get the archived version's metadata for the versioned entry
                archived = self._document_store.get_version(
                    doc_coll, item.id, offset=1
                )
                if archived:
                    self._store.upsert_version(
                        collection=chroma_coll,
                        id=item.id,
                        version=max_ver,
                        embedding=old_embedding,
                        summary=archived.summary,
                        tags=casefold_tags_for_index(archived.tags),
                    )

        # Compute embedding (try dedup first)
        embedding = self._try_dedup_embedding(
            doc_coll, chroma_coll, doc.content_hash, item.id, item.content,
        )
        if embedding is None:
            embedding = self._get_embedding_provider().embed(item.content)

        # Write to vector store
        self._store.upsert(
            collection=chroma_coll,
            id=item.id,
            embedding=embedding,
            summary=doc.summary,
            tags=casefold_tags_for_index(doc.tags),
        )

    def _process_pending_analyze(self, item) -> None:
        """Process a pending analysis work item."""
        tags = item.metadata.get("tags") if item.metadata else None
        force = item.metadata.get("force", False) if item.metadata else False
        parts = self.analyze(item.id, tags=tags, force=force)
        logger.info("Analyzed %s into %d parts", item.id, len(parts))

    def _process_pending_reindex(self, item) -> None:
        """Process a reindex task: embed summary and write to vector store.

        Handles both main docs and versioned entries.
        The item.content contains the summary text to embed.
        """
        chroma_coll = self._resolve_chroma_collection()
        meta = item.metadata or {}
        version = meta.get("version")
        base_id = meta.get("base_id")

        embedding = self._get_embedding_provider().embed(item.content)

        if version is not None and base_id is not None:
            # Versioned entry
            self._store.upsert_version(
                collection=chroma_coll,
                id=base_id,
                version=version,
                embedding=embedding,
                summary=item.content,
                tags=casefold_tags_for_index(meta.get("tags", {})),
            )
        else:
            # Main doc entry — use fresh tags from doc store
            doc = self._document_store.get(item.collection, item.id)
            if doc is None:
                return  # deleted since enqueue
            self._store.upsert(
                collection=chroma_coll,
                id=item.id,
                embedding=embedding,
                summary=item.content,
                tags=casefold_tags_for_index(doc.tags),
            )

    def _flush_edge_backfill(self, doc_coll: str) -> None:
        """Synchronously backfill edges for tag docs with _inverse.

        Called from find(deep=True) after system-doc migration so that
        newly-created tag definitions produce edges before the search
        runs.  Finds incomplete backfills and runs them inline.
        """
        # Find tag docs with _inverse that haven't been backfilled yet
        tag_docs = self._document_store.query_by_id_prefix(doc_coll, ".tag/")
        for td in tag_docs:
            inverse = td.tags.get("_inverse")
            if not inverse:
                continue
            predicate = td.id[5:]  # strip ".tag/"
            if "/" in predicate:
                continue  # sub-tags like .tag/act/commitment
            if self._document_store.get_backfill_status(doc_coll, predicate):
                continue
            # Build a synthetic pending item for the backfill processor
            from .pending_summaries import PendingSummary
            synthetic = PendingSummary(
                id=f"_backfill:{predicate}",
                collection=doc_coll,
                content="",
                queued_at="",
                attempts=1,
                task_type="backfill-edges",
                metadata={"predicate": predicate, "inverse": inverse},
            )
            try:
                logger.info("Inline edge backfill: %s → %s", predicate, inverse)
                self._process_pending_backfill_edges(synthetic)
            except Exception as e:
                logger.warning("Edge backfill failed for %s: %s", predicate, e)

    def _process_pending_backfill_edges(self, item) -> None:
        """Process a backfill-edges task: populate edges for all docs with a given tag.

        Scans all documents that have the predicate tag and creates edge
        rows + auto-vivifies targets. Marks the backfill as completed.
        """
        meta = item.metadata or {}
        predicate = meta.get("predicate")
        inverse = meta.get("inverse")
        if not predicate or not inverse:
            logger.warning("Backfill-edges task missing predicate/inverse: %s", item.id)
            return

        doc_coll = item.collection

        # Verify the tagdoc still has this _inverse — it may have been
        # removed or changed since this task was enqueued.
        tagdoc = self._document_store.get(doc_coll, f".tag/{predicate}")
        current_inverse = tagdoc.tags.get("_inverse") if tagdoc else None
        if current_inverse != inverse:
            logger.info(
                "Backfill for %s skipped: _inverse changed (%s → %s)",
                predicate, inverse, current_inverse,
            )
            # Clean up stale backfill record
            self._document_store.delete_backfill(doc_coll, predicate)
            return

        # Paginate — query_by_tag_key defaults to limit=100
        edge_count = 0
        page_size = 500
        offset = 0
        while True:
            docs = self._document_store.query_by_tag_key(
                doc_coll, predicate, limit=page_size, offset=offset,
            )
            if not docs:
                break
            offset += len(docs)
            for doc in docs:
                for raw_target in tag_values(doc.tags, predicate):
                    if not raw_target:
                        continue
                    try:
                        target_id = normalize_id(raw_target)
                    except ValueError:
                        logger.warning(
                            "Skipping invalid backfill edge target %r for %s:%s",
                            raw_target, doc.id, predicate,
                        )
                        continue
                    if target_id.startswith("."):
                        continue
                    # Auto-vivify target (doc store only — embedding via reindex queue)
                    if not self._document_store.exists(doc_coll, target_id):
                        reference_created = (
                            doc.tags.get("_created")
                            or doc.tags.get("_updated")
                            or utc_now()
                        )
                        now = utc_now()
                        self._document_store.upsert(
                            doc_coll, target_id,
                            summary="",
                            tags={
                                "_created": reference_created,
                                "_updated": now,
                                "_source": "auto-vivify",
                            },
                            created_at=reference_created,
                        )
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
                    created = doc.tags.get("_created") or doc.tags.get("_updated") or utc_now()
                    self._document_store.upsert_edge(
                        collection=doc_coll,
                        source_id=doc.id,
                        predicate=predicate,
                        target_id=target_id,
                        inverse=inverse,
                        created=created,
                    )
                    edge_count += 1
        # Materialize archived-version edges for this predicate too.
        self._document_store.backfill_version_edges_for_predicate(
            doc_coll, predicate, inverse,
        )
        # Mark backfill complete
        self._document_store.upsert_backfill(doc_coll, predicate, inverse, completed=utc_now())
        logger.info("Backfilled %d edges for predicate=%s inverse=%s", edge_count, predicate, inverse)

    def _ocr_image(self, path: Path, content_type: str, extractor) -> str | None:
        """OCR a single image file. Returns extracted text or None."""
        from .processors import ocr_image
        return ocr_image(path, content_type, extractor)

    def _ocr_pdf(self, path: Path, ocr_pages: list[int], extractor) -> str | None:
        """OCR scanned PDF pages and merge with text-layer pages."""
        from .processors import ocr_pdf
        return ocr_pdf(path, ocr_pages, extractor)

    def _process_pending_ocr(self, item) -> None:
        """Process a pending OCR work item: OCR scanned PDFs or images.

        For PDFs: renders blank pages to images, runs OCR, merges with
        text-layer pages.
        For images: runs OCR directly on the image file.

        Then updates the document summary and embedding with the content.
        """
        from .processors import process_ocr
        meta = item.metadata or {}
        uri = meta.get("uri") or item.id
        ocr_pages = meta.get("ocr_pages", [])
        content_type = meta.get("content_type", "")

        if not ocr_pages:
            return

        # Cap page count to prevent unbounded OCR on huge documents
        MAX_OCR_PAGES = 1000
        if len(ocr_pages) > MAX_OCR_PAGES:
            logger.warning(
                "OCR page count %d exceeds limit %d for %s — truncating",
                len(ocr_pages), MAX_OCR_PAGES, uri,
            )
            ocr_pages = ocr_pages[:MAX_OCR_PAGES]

        # Load content extractor
        extractor = self._get_content_extractor()
        if not extractor:
            raise IOError("No content extractor configured for OCR")

        path = Path(uri.removeprefix("file://")).resolve()
        if not path.exists():
            logger.warning("File no longer exists for OCR: %s", path)
            return

        # Re-validate path is within home directory (may differ from enqueue time)
        home = Path.home().resolve()
        if not path.is_relative_to(home):
            logger.warning("OCR path outside home directory, skipping: %s", path)
            return

        is_image = content_type.startswith("image/") if content_type else False
        # Fallback: detect from extension if content_type not in metadata
        if not content_type:
            from .providers.documents import FileDocumentProvider
            ext = path.suffix.lower()
            ct = FileDocumentProvider.EXTENSION_TYPES.get(ext, "")
            is_image = ct.startswith("image/")
            if is_image:
                content_type = ct

        if is_image:
            full_content = self._ocr_image(path, content_type, extractor)
        else:
            full_content = self._ocr_pdf(path, ocr_pages, extractor)

        if not full_content or not full_content.strip():
            logger.info("OCR produced no usable text for %s", uri)
            return

        # Get existing document for context + tags
        doc_coll = item.collection
        existing = self._document_store.get(doc_coll, item.id)
        if not existing:
            return  # deleted since enqueue

        context = None
        user_tags = filter_non_system_tags(existing.tags)
        if user_tags:
            context = self._gather_context(item.id, user_tags)

        # Process (pure function)
        try:
            result = process_ocr(
                full_content,
                max_summary_length=self._config.max_summary_length,
                context=context,
                summarization_provider=self._get_summarization_provider(),
            )
        except Exception as e:
            logger.warning("OCR summarization failed for %s: %s", uri, e)
            result = process_ocr(
                full_content,
                max_summary_length=self._config.max_summary_length,
                context=context,
                summarization_provider=None,
            )

        # Apply to store
        self.apply_result(item.id, doc_coll, result, existing_tags=existing.tags)

        logger.info(
            "OCR complete for %s: %d chars, type=%s",
            uri, len(full_content), content_type or "pdf",
        )

    def pending_count(self) -> int:
        """Get count of pending summaries awaiting processing."""
        return self._pending_queue.count()

    def pending_stats(self) -> dict:
        """
        Get pending summary queue statistics.

        Returns dict with: pending, collections, max_attempts, oldest, queue_path, by_type
        """
        return self._pending_queue.stats()

    def pending_stats_by_type(self) -> dict[str, int]:
        """Get pending queue counts grouped by task type."""
        return self._pending_queue.stats_by_type()

    def pending_status(self, id: str) -> Optional[dict]:
        """
        Get pending task status for a specific note.

        Returns dict with id, task_type, status, queued_at if the note
        has pending work, or None if no work is pending. Requires a
        queue implementation that supports get_status().
        """
        return self._pending_queue.get_status(id)

    @property
    def _processor_pid_path(self) -> Path:
        """Path to the processor PID file."""
        return self._store_path / "processor.pid"

    def _is_processor_running(self) -> bool:
        """Check if a processor is already running via lock probe."""
        from .model_lock import ModelLock

        lock = ModelLock(self._store_path / ".processor.lock")
        return lock.is_locked()

    def _spawn_processor(self) -> bool:
        """
        Spawn a background processor if not already running.

        Uses an exclusive file lock to prevent TOCTOU race conditions
        where two processes could both check, find no processor, and
        both spawn one.

        Throttled: skips if < 30s since last spawn.
        Gated: waits up to 5s for background reconcile to finish.

        Returns True if a new processor was spawned, False if one was
        already running or spawn failed.
        """
        import time
        from .model_lock import ModelLock

        # Throttle: don't spawn more than once per 30 seconds
        now = time.monotonic()
        if now - self._last_spawn_time < 30:
            return False

        # Gate on reconcile: wait briefly, skip if not done
        if not self._reconcile_done.wait(timeout=5):
            logger.debug("Skipping spawn: reconcile still in progress")
            return False

        spawn_lock = ModelLock(self._store_path / ".processor_spawn.lock")

        # Non-blocking: if another process is already spawning, let it handle it
        if not spawn_lock.acquire(blocking=False):
            return False

        log_fd = None
        try:
            if self._is_processor_running():
                return False

            # Spawn detached process
            # Use sys.executable to ensure we use the same Python
            cmd = [
                sys.executable, "-m", "keep.cli",
                "pending",
                "--daemon",
                "--store", str(self._store_path),
            ]

            # Platform-specific detachment
            # Redirect daemon stderr to ops log for crash diagnostics
            log_path = self._store_path / "keep-ops.log"
            try:
                log_fd = open(log_path, "a")
            except OSError:
                log_fd = None
            kwargs: dict = {
                "stdout": subprocess.DEVNULL,
                "stderr": log_fd if log_fd else subprocess.DEVNULL,
                "stdin": subprocess.DEVNULL,
            }

            if sys.platform != "win32":
                # Unix: start new session to fully detach
                kwargs["start_new_session"] = True
            else:
                # Windows: use CREATE_NEW_PROCESS_GROUP
                kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP

            subprocess.Popen(cmd, **kwargs)
            self._last_spawn_time = time.monotonic()
            logger.info("Spawned background processor")
            return True

        except Exception as e:
            # Spawn failed - log for debugging, queue will be processed later
            logger.warning("Failed to spawn background processor: %s", e)
            return False
        finally:
            # Close parent's copy of the log fd (child inherited it)
            if log_fd:
                log_fd.close()
            spawn_lock.release()

    def reconcile(
        self,
        fix: bool = False,
        _doc_store: "Optional[DocumentStore]" = None,
    ) -> dict:
        """
        Check and optionally fix consistency between document store and vector store.

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

        # Find mismatches between stores
        doc_ids = ds.list_ids(doc_coll)
        missing_from_chroma = self._store.find_missing_ids(chroma_coll, doc_ids)

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
            for doc_id in missing_from_chroma:
                if self._closing.is_set():
                    break
                try:
                    doc_record = ds.get(doc_coll, doc_id)
                    if doc_record:
                        embedding = self._get_embedding_provider().embed(doc_record.summary)
                        self._store.upsert(
                            collection=chroma_coll,
                            id=doc_id,
                            embedding=embedding,
                            summary=doc_record.summary,
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
            "missing_from_index": len(missing_from_chroma),
            "orphaned_in_index": len(orphaned_in_chroma),
            "fixed": fixed,
            "removed": removed,
            "missing_ids": list(missing_from_chroma) if missing_from_chroma else [],
            "orphaned_ids": list(orphaned_in_chroma) if orphaned_in_chroma else [],
        }

    @property
    def config(self) -> "StoreConfig":
        """Public access to store configuration."""
        return self._config

    def close(self) -> None:
        """
        Close resources (stores, caches, queues).

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
