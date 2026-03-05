"""Tests for _singular tag cardinality enforcement.

When a tagdoc (.tag/KEY) has _singular="true", the key allows at most one
value.  New values replace old ones instead of accumulating via set-union.
"""

import pytest

from keep.api import Keeper
from keep.document_store import PartInfo
from keep.types import tag_values, utc_now


def _create_tagdoc(kp, key, *, singular=True, constrained=False):
    """Create a .tag/KEY tagdoc with optional _singular and _constrained."""
    doc_coll = kp._resolve_doc_collection()
    now = utc_now()
    tags = {
        "_created": now,
        "_updated": now,
        "_source": "inline",
        "category": "system",
        "context": "tag-description",
    }
    if singular:
        tags["_singular"] = "true"
    if constrained:
        tags["_constrained"] = "true"
    kp._document_store.upsert(
        collection=doc_coll,
        id=f".tag/{key}",
        summary=f"Tag: {key}",
        tags=tags,
    )


def _create_value_doc(kp, key, value):
    """Create a .tag/KEY/VALUE sub-doc (for constrained tags)."""
    doc_coll = kp._resolve_doc_collection()
    now = utc_now()
    kp._document_store.upsert(
        collection=doc_coll,
        id=f".tag/{key}/{value}",
        summary=f"{key}={value}",
        tags={"_created": now, "_updated": now, "_source": "inline"},
    )


@pytest.fixture
def kp(mock_providers, tmp_path):
    k = Keeper(store_path=tmp_path)
    yield k
    k.close()


# --- tag() path ---

class TestSingularOnTag:

    def test_singular_tag_replaces_on_tag(self, kp):
        """tag() with a singular key replaces existing value."""
        _create_tagdoc(kp, "priority", singular=True)

        kp.put(content="item one", id="t1", tags={"priority": "low"})
        result = kp.tag("t1", tags={"priority": "high"})

        assert result is not None
        vals = tag_values(result.tags, "priority")
        assert vals == ["high"]

    def test_singular_tag_multi_value_error(self, kp):
        """Providing >1 value for a singular key in one call raises ValueError."""
        _create_tagdoc(kp, "priority", singular=True)

        kp.put(content="item", id="t2")
        with pytest.raises(ValueError, match="singular"):
            kp.tag("t2", tags={"priority": ["low", "high"]})

    def test_singular_no_tagdoc(self, kp):
        """Without a tagdoc, tags remain additive (unchanged behavior)."""
        kp.put(content="item", id="t3", tags={"color": "red"})
        result = kp.tag("t3", tags={"color": "blue"})

        assert result is not None
        vals = tag_values(result.tags, "color")
        assert set(vals) == {"red", "blue"}

    def test_singular_false(self, kp):
        """Tagdoc without _singular → additive (unchanged behavior)."""
        _create_tagdoc(kp, "flavor", singular=False)

        kp.put(content="item", id="t4", tags={"flavor": "sweet"})
        result = kp.tag("t4", tags={"flavor": "sour"})

        assert result is not None
        vals = tag_values(result.tags, "flavor")
        assert set(vals) == {"sweet", "sour"}


# --- put() / _upsert path ---

class TestSingularOnPut:

    def test_singular_tag_replaces_on_put(self, kp):
        """Second put() with a singular key replaces existing value."""
        _create_tagdoc(kp, "priority", singular=True)

        kp.put(content="item", id="t5", tags={"priority": "low"})
        kp.put(content="item updated", id="t5", tags={"priority": "high"})
        item = kp.get("t5")

        assert item is not None
        vals = tag_values(item.tags, "priority")
        assert vals == ["high"]

    def test_singular_multi_value_error_on_put(self, kp):
        """put() with >1 value for singular key raises ValueError."""
        _create_tagdoc(kp, "priority", singular=True)

        with pytest.raises(ValueError, match="singular"):
            kp.put(content="item", id="t6", tags={"priority": ["a", "b"]})


# --- tag_part() path ---

class TestSingularOnTagPart:

    def test_singular_tag_on_tag_part(self, kp):
        """tag_part() with a singular key replaces existing value on part."""
        _create_tagdoc(kp, "priority", singular=True)

        kp.put(content="parent", id="tp1")
        # Create a part directly via the mock store
        doc_coll = kp._resolve_doc_collection()
        part = PartInfo(
            part_num=1, summary="part one",
            tags={"priority": "low"}, content="part one",
            created_at=utc_now(),
        )
        kp._document_store.upsert_parts(doc_coll, "tp1", [part])
        result = kp.tag_part("tp1", 1, tags={"priority": "high"})

        assert result is not None
        vals = tag_values(result.tags, "priority")
        assert vals == ["high"]


# --- Edge tag with singular ---

class TestSingularEdgeTag:

    def test_singular_edge_tag(self, kp):
        """Edge-tag with _singular replaces edge correctly."""
        doc_coll = kp._resolve_doc_collection()
        now = utc_now()
        # Create tagdoc with both _inverse and _singular
        kp._document_store.upsert(
            collection=doc_coll,
            id=".tag/assignee",
            summary="Tag: assignee",
            tags={
                "_inverse": "assigned",
                "_singular": "true",
                "_created": now,
                "_updated": now,
                "_source": "inline",
                "category": "system",
            },
        )

        kp.put(content="task", id="task1", tags={"assignee": "alice"})
        kp.tag("task1", tags={"assignee": "bob"})

        item = kp.get("task1")
        assert item is not None
        vals = tag_values(item.tags, "assignee")
        assert vals == ["bob"]


# --- Singular + unconstrained (no _constrained flag) ---

class TestSingularUnconstrained:

    def test_singular_unconstrained(self, kp):
        """Unconstrained key with _singular still enforces cardinality."""
        _create_tagdoc(kp, "mood", singular=True, constrained=False)

        kp.put(content="note", id="u1", tags={"mood": "happy"})
        result = kp.tag("u1", tags={"mood": "sad"})

        assert result is not None
        vals = tag_values(result.tags, "mood")
        assert vals == ["sad"]


# --- Singular + constrained ---

class TestSingularConstrained:

    def test_singular_constrained_replaces(self, kp):
        """status tag (constrained + singular) replaces on tag()."""
        _create_tagdoc(kp, "lifecycle", singular=True, constrained=True)
        _create_value_doc(kp, "lifecycle", "open")
        _create_value_doc(kp, "lifecycle", "closed")

        kp.put(content="item", id="sc1", tags={"lifecycle": "open"})
        result = kp.tag("sc1", tags={"lifecycle": "closed"})

        assert result is not None
        vals = tag_values(result.tags, "lifecycle")
        assert vals == ["closed"]
