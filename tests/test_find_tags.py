"""
Tests for find(tags=...) pre-filter parameter.

Uses mock providers — no ML models or network.
"""

import pytest

from keep.api import Keeper


@pytest.fixture
def kp(mock_providers, tmp_path):
    """Create a Keeper with tagged seed data."""
    kp = Keeper(store_path=tmp_path)
    kp._get_embedding_provider()

    # Seed items with different tags
    kp.put("Alice likes cats and dogs", id="alice:pets", tags={"user": "alice", "topic": "pets"})
    kp.put("Alice works on project X", id="alice:work", tags={"user": "alice", "topic": "work"})
    kp.put("Bob likes birds", id="bob:pets", tags={"user": "bob", "topic": "pets"})
    kp.put("Bob works on project Y", id="bob:work", tags={"user": "bob", "topic": "work"})

    return kp


class TestFindTagsFilter:
    """Test find() with tags pre-filter."""

    def test_find_without_tags_returns_all(self, kp):
        """find() without tags returns results from all users."""
        results = kp.find("pets")
        ids = {r.id for r in results}
        assert "alice:pets" in ids
        assert "bob:pets" in ids

    def test_find_with_single_tag_filters(self, kp):
        """find() with tags={user: alice} only returns alice's items."""
        results = kp.find("pets", tags={"user": "alice"})
        ids = {r.id for r in results}
        assert "alice:pets" in ids
        assert "bob:pets" not in ids

    def test_find_with_multiple_tags(self, kp):
        """find() with multiple tags filters by all of them."""
        results = kp.find("work", tags={"user": "alice", "topic": "work"})
        ids = {r.id for r in results}
        assert "alice:work" in ids
        assert "bob:work" not in ids
        assert "alice:pets" not in ids

    def test_find_tags_key_case_insensitive_value_case_sensitive(self, kp):
        """Keys are case-insensitive; values remain case-sensitive."""
        results = kp.find("pets", tags={"User": "Alice"})
        ids = {r.id for r in results}
        assert "alice:pets" not in ids
        exact = kp.find("pets", tags={"User": "alice"})
        assert "alice:pets" in {r.id for r in exact}

    def test_find_tags_no_match(self, kp):
        """find() with non-matching tags returns empty."""
        results = kp.find("pets", tags={"user": "charlie"})
        assert len(results) == 0

    def test_put_same_key_adds_distinct_values(self, kp):
        """Repeated writes with same key keep distinct values."""
        kp.put("alice with two topics", id="alice:pets", tags={"topic": "animals"})
        item = kp.get("alice:pets")
        assert item is not None
        assert set(item.tags["topic"]) == {"pets", "animals"}

    def test_find_matches_each_value_for_multivalue_key(self, kp):
        """Tag filters match any stored value for the key."""
        kp.put("alice with two topics", id="alice:pets", tags={"topic": "animals"})
        by_pets = {r.id for r in kp.find("alice", tags={"topic": "pets"})}
        by_animals = {r.id for r in kp.find("alice", tags={"topic": "animals"})}
        assert "alice:pets" in by_pets
        assert "alice:pets" in by_animals

    def test_find_hybrid_with_tags(self, kp):
        """find() hybrid search also respects tags filter."""
        results = kp.find("cats", tags={"user": "alice"})
        ids = {r.id for r in results}
        assert "alice:pets" in ids
        assert "bob:pets" not in ids

    def test_find_tags_none_is_noop(self, kp):
        """find() with tags=None is same as no tags."""
        results_none = kp.find("pets", tags=None)
        results_default = kp.find("pets")
        assert {r.id for r in results_none} == {r.id for r in results_default}

    def test_find_rejects_invalid_tag_key(self, kp):
        """find() rejects tag keys that fail the shared key validator."""
        with pytest.raises(ValueError, match="invalid characters"):
            kp.find("pets", tags={"bad!key": "alice"})


class TestFindSinceFilter:
    """Regression tests: find() with since= must not drop items whose
    ChromaDB metadata lacks _updated_date (version refs, legacy items)."""

    def test_find_since_returns_recent_items(self, kp):
        """find(since='P1D') includes items stored today."""
        results = kp.find("cats and dogs", since="P1D")
        assert any(r.id == "alice:pets" for r in results)

    def test_find_since_includes_versioned_items(self, mock_providers, tmp_path):
        """find(since='P1D') includes items even when only version
        embeddings are in ChromaDB (whose metadata lacks _updated_date)."""
        kp = Keeper(store_path=tmp_path)
        kp._get_embedding_provider()
        kp.put("Alpha content about animals", id="test:ver")
        kp.put("Beta content about animals", id="test:ver")

        results = kp.find("animals", since="P1D")
        assert any(r.id == "test:ver" for r in results)

    def test_find_similar_to_with_since(self, mock_providers, tmp_path):
        """find(similar_to=..., since='P1D') returns recently-updated matches."""
        kp = Keeper(store_path=tmp_path)
        kp._get_embedding_provider()
        kp.put("Reference doc about weather", id="test:ref")
        kp.put("Similar doc about climate", id="test:sim")

        results = kp.find(similar_to="test:ref", since="P1D")
        assert any(r.id == "test:sim" for r in results)

    def test_find_until_excludes_recent(self, mock_providers, tmp_path):
        """find(until=<yesterday>) excludes items stored today."""
        kp = Keeper(store_path=tmp_path)
        kp._get_embedding_provider()
        kp.put("Today doc about planets", id="test:today")

        # until=yesterday should exclude items created today
        from datetime import datetime, timedelta, timezone
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        results = kp.find("planets", until=yesterday)
        assert not any(r.id == "test:today" for r in results)

    def test_find_until_includes_older(self, mock_providers, tmp_path):
        """find(until=<tomorrow>) includes items stored today."""
        kp = Keeper(store_path=tmp_path)
        kp._get_embedding_provider()
        kp.put("Today doc about galaxies", id="test:gal")

        from datetime import datetime, timedelta, timezone
        tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
        results = kp.find("galaxies", until=tomorrow)
        assert any(r.id == "test:gal" for r in results)

    def test_find_since_and_until_combined(self, mock_providers, tmp_path):
        """find(since=..., until=...) applies both bounds."""
        kp = Keeper(store_path=tmp_path)
        kp._get_embedding_provider()
        kp.put("Bounded doc about oceans", id="test:ocean")

        from datetime import datetime, timedelta, timezone
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        tomorrow = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
        # Window that includes today
        results = kp.find("oceans", since=yesterday, until=tomorrow)
        assert any(r.id == "test:ocean" for r in results)
        # Window that excludes today (past)
        last_week = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        two_days_ago = (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
        results = kp.find("oceans", since=last_week, until=two_days_ago)
        assert not any(r.id == "test:ocean" for r in results)

    def test_find_mixed_items_with_and_without_updated_date(self, mock_providers, tmp_path):
        """find(since=...) handles a mix of items where some have
        _updated_date in ChromaDB and some don't (e.g. head vs version)."""
        kp = Keeper(store_path=tmp_path)
        kp._get_embedding_provider()
        # head item — ChromaDB will have _updated_date
        kp.put("Fresh doc about rivers", id="test:fresh")
        # versioned item — version entry in ChromaDB lacks _updated_date
        kp.put("First draft about rivers and streams", id="test:versioned")
        kp.put("Second draft about rivers and streams", id="test:versioned")

        results = kp.find("rivers", since="P1D")
        ids = {r.id for r in results}
        assert "test:fresh" in ids
        assert "test:versioned" in ids
