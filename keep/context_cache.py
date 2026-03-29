"""In-memory action-level cache for flow read operations.

Caches action results at the flow engine's action runner, keyed by
action name + params. Two specialized caches handle different
invalidation strategies:

- FindCache: cached ``find(similar_to=...)`` results with precise
  selector invalidation for a simple exact-tag subset
- PartsCache: item-based invalidation for ``find(prefix=...@p)`` queries

The ``ContextCache`` orchestrator routes check/store/invalidation to
the appropriate sub-cache.

Find results store IDs+scores rather than full items so cache hits
hydrate fresh note data. Parts results are stored in full because
parts are not regular items and cannot be hydrated via ``ctx.peek()``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from .actions import item_to_result
from .types import casefold_tags, is_part_id, is_system_id, tag_values

logger = logging.getLogger(__name__)

_ACTION_FIND = "find"


# ---------------------------------------------------------------------------
# Cache key
# ---------------------------------------------------------------------------

def _cache_key(action_name: str, params: dict[str, Any]) -> str:
    """Deterministic cache key from action name + params."""
    try:
        canonical = json.dumps(
            {"a": action_name, **params},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    except (TypeError, ValueError):
        return ""
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def _find_cache_key(params: dict[str, Any]) -> str:
    """Cache key for cached find selectors, normalized across limits."""
    key_params = dict(params)
    key_params.pop("limit", None)
    return _cache_key(_ACTION_FIND, key_params)


# ---------------------------------------------------------------------------
# Hydration helpers
# ---------------------------------------------------------------------------

def _hydrate_find_results(
    ids_scores: list[tuple[str, float | None]],
    ctx: Any,
) -> dict[str, Any]:
    """Rebuild find action output from cached IDs+scores."""
    results = []
    for item_id, score in ids_scores:
        item = ctx.peek(item_id)
        if item is not None:
            r = item_to_result(item)
            r["score"] = score  # override with cached score
            results.append(r)
    return {"results": results, "count": len(results)}


# ---------------------------------------------------------------------------
# Extraction helpers (result dict -> IDs+scores)
# ---------------------------------------------------------------------------

def _extract_ids_scores(result: dict[str, Any]) -> list[tuple[str, float | None]]:
    """Extract [(id, score)] from a find action result."""
    return [
        (r["id"], r.get("score"))
        for r in result.get("results", [])
        if "id" in r
    ]


def _normalize_selector_tags(
    tags: dict[str, Any] | None,
) -> dict[str, tuple[str, ...]] | None:
    """Normalize exact-match tag filters for cache invalidation."""
    if not isinstance(tags, dict) or not tags:
        return None
    normalized = casefold_tags(tags)
    selector: dict[str, tuple[str, ...]] = {}
    for key in normalized:
        values = tuple(tag_values(normalized, key))
        if values:
            selector[key] = values
    return selector or None


def _selector_tags_match(
    selector_tags: dict[str, tuple[str, ...]] | None,
    tags: dict[str, Any] | None,
) -> bool:
    """Return True when ``tags`` satisfy the exact-match selector."""
    if not selector_tags or not isinstance(tags, dict):
        return False
    normalized = casefold_tags(tags)
    for key, wanted_values in selector_tags.items():
        stored_values = set(tag_values(normalized, key))
        if not set(wanted_values).issubset(stored_values):
            return False
    return True


def _is_precise_find_params(params: dict[str, Any]) -> bool:
    """Return True when a cached find entry supports filtered invalidation."""
    if not params.get("similar_to"):
        return False
    if params.get("query"):
        return False
    if params.get("deep"):
        return False
    if params.get("prefix"):
        return False
    if params.get("tag_keys"):
        return False
    if params.get("scope"):
        return False
    if params.get("since"):
        return False
    if params.get("until"):
        return False
    return _normalize_selector_tags(params.get("tags")) is not None


def _find_limit(params: dict[str, Any]) -> int:
    """Coerce a cached find request's limit to an integer."""
    try:
        return int(params.get("limit", 10))
    except (TypeError, ValueError):
        return 10


# ---------------------------------------------------------------------------
# Cache entries
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class _FindEntry:
    ids_scores: list[tuple[str, float | None]]
    anchor_id: str | None
    selector_tags: dict[str, tuple[str, ...]] | None
    include_hidden: bool
    scope: str | None
    since: str | None
    until: str | None
    materialized_limit: int
    exhaustive: bool
    precise: bool
    generation: int
    created_at: float


@dataclass(slots=True)
class _PartsEntry:
    result: dict[str, Any]  # full result (parts aren't regular items)
    item_id: str  # base item


# ---------------------------------------------------------------------------
# FindCache
# ---------------------------------------------------------------------------

class FindCache:
    """Cache for ``find(similar_to=...)`` results."""

    def __init__(self, *, max_entries: int = 500, ttl: float = 60.0) -> None:
        self._entries: OrderedDict[str, _FindEntry] = OrderedDict()
        self._generation: int = 0
        self._ttl = ttl
        self._max = max_entries
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(
        self,
        key: str,
        *,
        limit: int,
    ) -> list[tuple[str, float | None]] | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self.misses += 1
                return None
            now = time.monotonic()
            if entry.precise or entry.generation == self._generation:
                if not self._can_serve(entry, limit):
                    self.misses += 1
                    return None
                self._entries.move_to_end(key)
                self.hits += 1
                return entry.ids_scores[:limit]
            if (now - entry.created_at) < self._ttl:
                if not self._can_serve(entry, limit):
                    self.misses += 1
                    return None
                self._entries.move_to_end(key)
                self.hits += 1
                return entry.ids_scores[:limit]
            # Expired
            del self._entries[key]
            self.misses += 1
            return None

    def _can_serve(self, entry: _FindEntry, limit: int) -> bool:
        return entry.exhaustive or entry.materialized_limit >= limit

    def put(
        self,
        key: str,
        ids_scores: list[tuple[str, float | None]],
        *,
        params: dict[str, Any],
    ) -> None:
        if not key:
            return
        selector_tags = _normalize_selector_tags(params.get("tags"))
        precise = _is_precise_find_params(params)
        requested_limit = _find_limit(params)
        with self._lock:
            self._entries[key] = _FindEntry(
                ids_scores=ids_scores,
                anchor_id=str(params.get("similar_to")) if params.get("similar_to") else None,
                selector_tags=selector_tags,
                include_hidden=bool(params.get("include_hidden", False)),
                scope=str(params.get("scope")) if params.get("scope") else None,
                since=str(params.get("since")) if params.get("since") else None,
                until=str(params.get("until")) if params.get("until") else None,
                materialized_limit=requested_limit,
                exhaustive=len(ids_scores) < requested_limit,
                precise=precise,
                generation=self._generation,
                created_at=time.monotonic(),
            )
            self._entries.move_to_end(key)
            self._evict_lru()

    def on_write(
        self,
        item_id: str,
        *,
        old_tags: dict[str, Any] | None,
        new_tags: dict[str, Any] | None,
    ) -> int:
        with self._lock:
            self._generation += 1
            evicted = 0
            for key, entry in list(self._entries.items()):
                if entry.anchor_id == item_id:
                    del self._entries[key]
                    evicted += 1
                    continue
                if not entry.precise:
                    continue
                if self._entry_matches_item(entry, item_id, old_tags):
                    del self._entries[key]
                    evicted += 1
                    continue
                if self._entry_matches_item(entry, item_id, new_tags):
                    del self._entries[key]
                    evicted += 1
            return evicted

    def _entry_matches_item(
        self,
        entry: _FindEntry,
        item_id: str,
        tags: dict[str, Any] | None,
    ) -> bool:
        if not entry.include_hidden and is_system_id(item_id):
            return False
        return _selector_tags_match(entry.selector_tags, tags)

    def on_delete(
        self,
        item_id: str,
        *,
        old_tags: dict[str, Any] | None,
    ) -> int:
        return self.on_write(item_id, old_tags=old_tags, new_tags=None)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._generation = 0

    def _evict_lru(self) -> None:
        """Remove oldest entries if over capacity.  Caller holds lock."""
        while len(self._entries) > self._max:
            self._entries.popitem(last=False)


# ---------------------------------------------------------------------------
# PartsCache
# ---------------------------------------------------------------------------

class PartsCache:
    """Cache for find(prefix=...@p) results.  Item-based invalidation.

    Stores full result dicts because parts aren't regular items and
    can't be hydrated via ctx.get().
    """

    def __init__(self, *, max_entries: int = 500) -> None:
        self._entries: OrderedDict[str, _PartsEntry] = OrderedDict()
        self._max = max_entries
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self.misses += 1
                return None
            self._entries.move_to_end(key)
            self.hits += 1
            return entry.result

    def put(self, key: str, result: dict[str, Any], item_id: str) -> None:
        if not key:
            return
        with self._lock:
            self._entries[key] = _PartsEntry(
                result=result, item_id=item_id,
            )
            self._entries.move_to_end(key)
            self._evict_lru()

    def on_write(self, item_id: str, tags: dict[str, Any]) -> int:
        with self._lock:
            base = item_id.split("@p")[0] if is_part_id(item_id) else item_id
            evicted = 0
            to_del = [k for k, e in self._entries.items() if e.item_id == base]
            for k in to_del:
                del self._entries[k]
                evicted += 1
            return evicted

    def on_delete(self, item_id: str) -> int:
        return self.on_write(item_id, {})

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def _evict_lru(self) -> None:
        while len(self._entries) > self._max:
            self._entries.popitem(last=False)


# ---------------------------------------------------------------------------
# ContextCache orchestrator
# ---------------------------------------------------------------------------

class ContextCache:
    """Routes cache operations to the appropriate sub-cache."""

    def __init__(
        self,
        *,
        similar_max: int = 2500,
        similar_ttl: float = 60.0,
        meta_max: int = 500,
        parts_max: int = 2500,
    ) -> None:
        del meta_max  # legacy compatibility; resolve_meta is no longer cached
        self.find = FindCache(max_entries=similar_max, ttl=similar_ttl)
        self.parts = PartsCache(max_entries=parts_max)

    def check(
        self, action_name: str, params: dict[str, Any], ctx: Any,
    ) -> dict[str, Any] | None:
        """Check cache.  Returns hydrated result dict or None."""
        key = _find_cache_key(params) if action_name == _ACTION_FIND and params.get("similar_to") else _cache_key(action_name, params)
        if not key:
            return None

        if action_name == _ACTION_FIND and params.get("similar_to"):
            ids_scores = self.find.get(key, limit=_find_limit(params))
            if ids_scores is None:
                return None
            return _hydrate_find_results(ids_scores, ctx)

        if action_name == _ACTION_FIND and str(params.get("prefix", "")).endswith("@p"):
            return self.parts.get(key)

        return None

    def store(
        self, action_name: str, params: dict[str, Any], result: dict[str, Any],
    ) -> None:
        """Extract IDs+scores and store in the appropriate cache."""
        key = _find_cache_key(params) if action_name == _ACTION_FIND and params.get("similar_to") else _cache_key(action_name, params)
        if not key:
            return

        if action_name == _ACTION_FIND and params.get("similar_to"):
            self.find.put(
                key,
                _extract_ids_scores(result),
                params=params,
            )
        elif action_name == _ACTION_FIND and str(params.get("prefix", "")).endswith("@p"):
            prefix = str(params["prefix"])
            base_id = prefix[:-2] if prefix.endswith("@p") else prefix
            self.parts.put(key, result, item_id=base_id)

    def notify_write(
        self,
        item_id: str,
        *,
        old_tags: dict[str, Any] | None,
        new_tags: dict[str, Any] | None,
    ) -> None:
        self.find.on_write(item_id, old_tags=old_tags, new_tags=new_tags)
        self.parts.on_write(item_id, new_tags or old_tags or {})

    def notify_delete(
        self,
        item_id: str,
        *,
        old_tags: dict[str, Any] | None = None,
    ) -> None:
        self.notify_write(item_id, old_tags=old_tags, new_tags=None)

    def clear(self) -> None:
        self.find.clear()
        self.parts.clear()

    def stats(self) -> dict[str, dict[str, int]]:
        return {
            "find": {"hits": self.find.hits, "misses": self.find.misses},
            "parts": {"hits": self.parts.hits, "misses": self.parts.misses},
        }


SimilarCache = FindCache
