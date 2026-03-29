"""Tests for the flow action-result cache."""

from __future__ import annotations

import time

from keep.context_cache import (
    ContextCache,
    FindCache,
    PartsCache,
    SimilarCache,
    _cache_key,
    _extract_ids_scores,
    _hydrate_find_results,
)


class TestCacheKey:
    def test_deterministic(self):
        k1 = _cache_key("find", {"similar_to": "abc", "limit": 3})
        k2 = _cache_key("find", {"similar_to": "abc", "limit": 3})
        assert k1 == k2
        assert len(k1) == 16

    def test_param_order_irrelevant(self):
        k1 = _cache_key("find", {"limit": 3, "similar_to": "abc"})
        k2 = _cache_key("find", {"similar_to": "abc", "limit": 3})
        assert k1 == k2

    def test_different_action_different_key(self):
        k1 = _cache_key("find", {"limit": 3})
        k2 = _cache_key("resolve_meta", {"limit": 3})
        assert k1 != k2

    def test_different_params_different_key(self):
        k1 = _cache_key("find", {"similar_to": "abc"})
        k2 = _cache_key("find", {"similar_to": "def"})
        assert k1 != k2

    def test_empty_params(self):
        k = _cache_key("find", {})
        assert len(k) == 16

    def test_non_serializable_returns_key(self):
        k = _cache_key("find", {"obj": object()})
        assert len(k) == 16


class TestExtraction:
    def test_extract_ids_scores(self):
        result = {
            "results": [
                {"id": "a", "summary": "x", "tags": {}, "score": 0.9},
                {"id": "b", "summary": "y", "tags": {}, "score": 0.8},
            ],
            "count": 2,
        }
        assert _extract_ids_scores(result) == [("a", 0.9), ("b", 0.8)]

    def test_extract_ids_scores_none_score(self):
        result = {"results": [{"id": "a", "summary": "x"}], "count": 1}
        assert _extract_ids_scores(result) == [("a", None)]


class _FakeItem:
    def __init__(self, id, summary="", tags=None):
        self.id = id
        self.summary = summary
        self.tags = tags or {}


class _FakeCtx:
    def __init__(self, items: dict[str, _FakeItem]):
        self._items = items

    def get(self, id):
        return self._items.get(id)

    def peek(self, id):
        return self._items.get(id)


class TestHydration:
    def test_hydrate_find_results(self):
        ctx = _FakeCtx({
            "a": _FakeItem("a", "summary a", {"tag": "1"}),
            "b": _FakeItem("b", "summary b"),
        })
        result = _hydrate_find_results([("a", 0.9), ("b", 0.8)], ctx)
        assert result["count"] == 2
        assert result["results"][0]["id"] == "a"
        assert result["results"][0]["summary"] == "summary a"
        assert result["results"][0]["score"] == 0.9
        assert result["results"][0]["tags"] == {"tag": "1"}

    def test_hydrate_find_skips_missing(self):
        ctx = _FakeCtx({"a": _FakeItem("a", "ok")})
        result = _hydrate_find_results([("a", 0.9), ("gone", 0.8)], ctx)
        assert result["count"] == 1
        assert result["results"][0]["id"] == "a"


class TestFindCache:
    def test_alias_still_points_to_find_cache(self):
        assert SimilarCache is FindCache

    def test_put_and_get(self):
        c = FindCache()
        c.put("k1", [("a", 0.9)], params={"similar_to": "x"})
        assert c.get("k1", limit=10) == [("a", 0.9)]
        assert c.hits == 1
        assert c.misses == 0

    def test_miss(self):
        c = FindCache()
        assert c.get("nonexistent", limit=10) is None
        assert c.misses == 1

    def test_non_precise_generation_invalidation_within_ttl(self):
        c = FindCache(ttl=60.0)
        c.put("k1", [("a", 0.9)], params={"similar_to": "x"})
        c.on_write("other", old_tags={"act": "request"}, new_tags={"act": "request"})
        assert c.get("k1", limit=10) == [("a", 0.9)]

    def test_direct_anchor_eviction(self):
        c = FindCache()
        c.put("k1", [("a", 0.9)], params={"similar_to": "target"})
        c.on_write("target", old_tags={}, new_tags={})
        assert c.get("k1", limit=10) is None

    def test_ttl_expiry_for_non_precise_entry(self):
        c = FindCache(ttl=0.01)
        c.put("k1", [("a", 0.9)], params={"similar_to": "x"})
        c.on_write("other", old_tags={}, new_tags={})
        time.sleep(0.02)
        assert c.get("k1", limit=10) is None

    def test_precise_entry_survives_unrelated_writes(self):
        c = FindCache(ttl=0.01)
        c.put(
            "k1",
            [("a", 0.9)],
            params={
                "similar_to": "anchor",
                "tags": {"act": "commitment", "status": "open"},
            },
        )
        c.on_write(
            "other",
            old_tags={"act": "request", "status": "open"},
            new_tags={"act": "request", "status": "open"},
        )
        time.sleep(0.02)
        assert c.get("k1", limit=10) == [("a", 0.9)]

    def test_precise_eviction_on_old_tag_match(self):
        c = FindCache()
        c.put(
            "k1",
            [("a", 0.9)],
            params={
                "similar_to": "anchor",
                "tags": {"act": "commitment", "status": "open"},
            },
        )
        c.on_write(
            "other",
            old_tags={"act": "commitment", "status": "open"},
            new_tags={"act": "commitment", "status": "fulfilled"},
        )
        assert c.get("k1", limit=10) is None

    def test_precise_eviction_on_new_tag_match(self):
        c = FindCache()
        c.put(
            "k1",
            [("a", 0.9)],
            params={
                "similar_to": "anchor",
                "tags": {"act": "commitment", "status": "open"},
            },
        )
        c.on_write(
            "other",
            old_tags={"act": "commitment", "status": "fulfilled"},
            new_tags={"act": "commitment", "status": "open"},
        )
        assert c.get("k1", limit=10) is None

    def test_precise_match_casefolds_keys(self):
        c = FindCache()
        c.put(
            "k1",
            [("a", 0.9)],
            params={
                "similar_to": "anchor",
                "tags": {"User": "alice"},
            },
        )
        c.on_write(
            "other",
            old_tags={"user": "alice"},
            new_tags={"user": "alice"},
        )
        assert c.get("k1", limit=10) is None

    def test_smaller_limit_reuses_larger_materialized_entry(self):
        c = FindCache()
        c.put(
            "k1",
            [("a", 0.9), ("b", 0.8), ("c", 0.7), ("d", 0.6), ("e", 0.5)],
            params={"similar_to": "anchor", "limit": 5},
        )
        assert c.get("k1", limit=3) == [("a", 0.9), ("b", 0.8), ("c", 0.7)]

    def test_larger_limit_misses_when_entry_was_truncated(self):
        c = FindCache()
        c.put(
            "k1",
            [("a", 0.9), ("b", 0.8), ("c", 0.7)],
            params={"similar_to": "anchor", "limit": 3},
        )
        assert c.get("k1", limit=10) is None

    def test_larger_limit_hits_when_entry_is_exhaustive(self):
        c = FindCache()
        c.put(
            "k1",
            [("a", 0.9), ("b", 0.8)],
            params={"similar_to": "anchor", "limit": 3},
        )
        assert c.get("k1", limit=10) == [("a", 0.9), ("b", 0.8)]

    def test_lru_eviction(self):
        c = FindCache(max_entries=3)
        c.put("k1", [("a", 0.9)], params={"similar_to": "x"})
        c.put("k2", [("b", 0.8)], params={"similar_to": "y"})
        c.put("k3", [("c", 0.7)], params={"similar_to": "z"})
        c.put("k4", [("d", 0.6)], params={"similar_to": "w"})
        assert c.get("k1", limit=10) is None
        assert c.get("k2", limit=10) is not None

    def test_clear(self):
        c = FindCache()
        c.put("k1", [("a", 0.9)], params={"similar_to": "x"})
        c.clear()
        assert c.get("k1", limit=10) is None

    def test_empty_key_skipped(self):
        c = FindCache()
        c.put("", [("a", 0.9)], params={"similar_to": "x"})
        assert c.get("", limit=10) is None


class TestPartsCache:
    def test_put_and_get(self):
        c = PartsCache()
        result = {"results": [{"id": "x@p1"}], "count": 1}
        c.put("k1", result, item_id="x")
        assert c.get("k1") == result

    def test_miss(self):
        c = PartsCache()
        assert c.get("nope") is None

    def test_evict_on_base_write(self):
        c = PartsCache()
        c.put("k1", {"results": [], "count": 0}, item_id="base")
        c.on_write("base", {})
        assert c.get("k1") is None

    def test_evict_on_part_write(self):
        c = PartsCache()
        c.put("k1", {"results": [], "count": 0}, item_id="base")
        c.on_write("base@p1", {})
        assert c.get("k1") is None

    def test_unrelated_write_no_eviction(self):
        c = PartsCache()
        c.put("k1", {"results": [], "count": 0}, item_id="base")
        c.on_write("other", {})
        assert c.get("k1") is not None

    def test_lru_eviction(self):
        c = PartsCache(max_entries=2)
        c.put("k1", {"results": [], "count": 0}, item_id="a")
        c.put("k2", {"results": [], "count": 0}, item_id="b")
        c.put("k3", {"results": [], "count": 0}, item_id="c")
        assert c.get("k1") is None
        assert c.get("k2") is not None

    def test_on_delete(self):
        c = PartsCache()
        c.put("k1", {"results": [], "count": 0}, item_id="base")
        c.on_delete("base")
        assert c.get("k1") is None


class TestContextCache:
    def _ctx(self, items=None):
        return _FakeCtx(items or {})

    def test_routing_find(self):
        cc = ContextCache()
        result = {
            "results": [
                {"id": "a", "score": 0.9},
                {"id": "b", "score": 0.8},
                {"id": "c", "score": 0.7},
            ],
            "count": 3,
        }
        cc.store("find", {"similar_to": "x", "limit": 3}, result)
        ctx = self._ctx({
            "a": _FakeItem("a", "sum a"),
            "b": _FakeItem("b", "sum b"),
            "c": _FakeItem("c", "sum c"),
        })
        hydrated = cc.check("find", {"similar_to": "x", "limit": 2}, ctx)
        assert hydrated is not None
        assert hydrated["results"][0]["id"] == "a"
        assert [r["id"] for r in hydrated["results"]] == ["a", "b"]

    def test_routing_parts(self):
        cc = ContextCache()
        result = {"results": [{"id": "x@p1"}], "count": 1}
        cc.store("find", {"prefix": "x@p", "limit": 10}, result)
        hydrated = cc.check("find", {"prefix": "x@p", "limit": 10}, self._ctx())
        assert hydrated == result

    def test_routing_resolve_meta_is_uncached(self):
        cc = ContextCache()
        cc.store(
            "resolve_meta",
            {"item_id": "x", "limit": 3},
            {"sections": {"todo": [{"id": "t1", "score": 0.7}]}, "count": 1},
        )
        assert cc.check("resolve_meta", {"item_id": "x", "limit": 3}, self._ctx()) is None

    def test_routing_uncacheable_find_is_miss(self):
        cc = ContextCache()
        assert cc.check("find", {"query": "test"}, self._ctx()) is None
        assert cc.check("summarize", {}, self._ctx()) is None

    def test_notify_write_propagates(self):
        cc = ContextCache()
        cc.store(
            "find",
            {"similar_to": "x", "tags": {"act": "commitment", "status": "open"}},
            {"results": [{"id": "a", "score": 0.9}], "count": 1},
        )
        cc.store(
            "find",
            {"prefix": "x@p", "limit": 10},
            {"results": [{"id": "x@p1"}], "count": 1},
        )

        cc.notify_write("x", old_tags={"act": "test"}, new_tags={"act": "test"})

        ctx = self._ctx({"a": _FakeItem("a")})
        assert cc.check(
            "find",
            {"similar_to": "x", "tags": {"act": "commitment", "status": "open"}},
            ctx,
        ) is None
        assert cc.check("find", {"prefix": "x@p", "limit": 10}, ctx) is None

    def test_notify_delete_propagates_old_tags(self):
        cc = ContextCache()
        cc.store(
            "find",
            {"similar_to": "anchor", "tags": {"act": "commitment", "status": "open"}},
            {"results": [{"id": "a", "score": 0.9}], "count": 1},
        )
        cc.notify_delete(
            "doc1",
            old_tags={"act": "commitment", "status": "open"},
        )
        ctx = self._ctx({"a": _FakeItem("a")})
        assert cc.check(
            "find",
            {"similar_to": "anchor", "tags": {"act": "commitment", "status": "open"}},
            ctx,
        ) is None

    def test_precise_find_survives_unrelated_write(self):
        cc = ContextCache()
        cc.store(
            "find",
            {"similar_to": "anchor", "tags": {"act": "commitment", "status": "open"}},
            {"results": [{"id": "a", "score": 0.9}], "count": 1},
        )
        cc.notify_write(
            "other",
            old_tags={"act": "request", "status": "open"},
            new_tags={"act": "request", "status": "open"},
        )
        ctx = self._ctx({"a": _FakeItem("a", "sum")})
        assert cc.check(
            "find",
            {"similar_to": "anchor", "tags": {"act": "commitment", "status": "open"}},
            ctx,
        ) is not None

    def test_larger_limit_recomputes_after_smaller_cached_result(self):
        cc = ContextCache()
        cc.store(
            "find",
            {"similar_to": "anchor", "limit": 3},
            {
                "results": [
                    {"id": "a", "score": 0.9},
                    {"id": "b", "score": 0.8},
                    {"id": "c", "score": 0.7},
                ],
                "count": 3,
            },
        )
        ctx = self._ctx({
            "a": _FakeItem("a"),
            "b": _FakeItem("b"),
            "c": _FakeItem("c"),
        })
        assert cc.check("find", {"similar_to": "anchor", "limit": 10}, ctx) is None

    def test_larger_limit_reuses_exhaustive_smaller_result(self):
        cc = ContextCache()
        cc.store(
            "find",
            {"similar_to": "anchor", "limit": 3},
            {
                "results": [
                    {"id": "a", "score": 0.9},
                    {"id": "b", "score": 0.8},
                ],
                "count": 2,
            },
        )
        ctx = self._ctx({
            "a": _FakeItem("a"),
            "b": _FakeItem("b"),
        })
        hydrated = cc.check("find", {"similar_to": "anchor", "limit": 10}, ctx)
        assert hydrated is not None
        assert [r["id"] for r in hydrated["results"]] == ["a", "b"]

    def test_stats(self):
        cc = ContextCache()
        cc.store("find", {"similar_to": "x", "limit": 3}, {"results": [{"id": "a"}], "count": 1})
        ctx = self._ctx({"a": _FakeItem("a")})
        cc.check("find", {"similar_to": "x", "limit": 3}, ctx)
        cc.check("find", {"similar_to": "y", "limit": 3}, ctx)
        stats = cc.stats()
        assert stats["find"]["hits"] == 1
        assert stats["find"]["misses"] == 1

    def test_clear(self):
        cc = ContextCache()
        cc.store("find", {"similar_to": "x", "limit": 3}, {"results": [{"id": "a"}], "count": 1})
        cc.clear()
        ctx = self._ctx({"a": _FakeItem("a")})
        assert cc.check("find", {"similar_to": "x", "limit": 3}, ctx) is None

    def test_find_generation_does_not_affect_parts(self):
        cc = ContextCache()
        cc.store(
            "find",
            {"prefix": "base@p", "limit": 10},
            {"results": [{"id": "base@p1"}], "count": 1},
        )
        cc.notify_write("unrelated", old_tags={"act": "test"}, new_tags={"act": "test"})
        result = cc.check("find", {"prefix": "base@p", "limit": 10}, self._ctx())
        assert result is not None
