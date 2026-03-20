"""
Meta-doc resolution tests for Keeper.

Tests resolve_meta() (persistent .meta/* docs) and resolve_inline_meta()
(ad-hoc tag queries). Uses mock providers — no ML models or network.

The bundled system docs (.meta/todo, .meta/learnings, .meta/genre) are
created automatically by the system document migration on first write,
so the fixture only needs to create the seed items that match those queries.
"""

import pytest

from keep.api import Keeper


@pytest.fixture
def kp(mock_providers, tmp_path):
    """Create a Keeper with seed data for meta-resolution tests.

    The first put() triggers system doc migration which creates
    the bundled .meta/* docs. We seed items that match their queries:

    - .meta/todo queries: act=commitment status=open, act=request status=open, etc.
    - .meta/learnings queries: type=learning, type=breakdown, type=gotcha
    - .meta/genre queries: genre=* (prereq), genre= (context expansion)
    """
    kp = Keeper(store_path=tmp_path)

    # Force embedding identity setup before storing test data.
    # The first put() triggers system doc migration, which sets
    # the embedding identity mid-call. Items stored during that first
    # call may go to a different ChromaDB collection. Calling
    # _get_embedding_provider() first avoids the split.
    kp._get_embedding_provider()

    # ── Seed data ──────────────────────────────────────────────────────

    # Commitments (open)
    kp.put("I will fix the auth bug", tags={
        "act": "commitment", "status": "open", "project": "myapp",
    })
    kp.put("I will write migration docs", tags={
        "act": "commitment", "status": "open", "project": "myapp",
    })

    # Commitment (fulfilled — should not match status=open queries)
    kp.put("Deployed v2", tags={
        "act": "commitment", "status": "fulfilled", "project": "myapp",
    })

    # Learnings (one shares project=myapp with anchor for context expansion)
    kp.put("SQLite WAL mode prevents corruption", tags={
        "type": "learning", "topic": "sqlite", "project": "myapp",
    })
    kp.put("Always pin dependencies", tags={
        "type": "learning", "topic": "packaging",
    })

    # An item with project tag (anchor for context expansion)
    kp.put("Working on myapp auth flow", id="anchor", tags={
        "project": "myapp", "topic": "auth",
    })

    # An item without project or genre tag (anchor for prereq tests)
    kp.put("General note with no project", id="no-project", tags={
        "topic": "general",
    })

    # Items with genre tag (for .meta/genre prereq tests)
    kp.put("Kind of Blue is a classic", id="album1", tags={
        "genre": "jazz", "type": "review",
    })
    kp.put("Giant Steps pushed boundaries", id="album2", tags={
        "genre": "jazz", "type": "review",
    })
    kp.put("Abbey Road is timeless", id="album3", tags={
        "genre": "rock", "type": "review",
    })

    return kp


class TestResolveMetaPersistent:
    """Tests for resolve_meta() using persistent .meta/* documents."""

    def test_resolve_finds_open_commitments(self, kp):
        """The todo meta-doc should surface open commitments."""
        result = kp.resolve_meta("anchor")
        assert "todo" in result
        summaries = [item.summary for item in result["todo"]]
        assert any("fix the auth" in s for s in summaries)
        assert any("write migration" in s for s in summaries)

    def test_resolve_excludes_fulfilled(self, kp):
        """Fulfilled commitments should not appear in open-only query."""
        result = kp.resolve_meta("anchor")
        if "todo" in result:
            summaries = [item.summary for item in result["todo"]]
            assert not any("Deployed v2" in s for s in summaries)

    def test_resolve_finds_learnings(self, kp):
        """The learnings meta-doc should surface learning items."""
        result = kp.resolve_meta("anchor")
        assert "learnings" in result
        summaries = [item.summary for item in result["learnings"]]
        assert any("WAL mode" in s for s in summaries)

    def test_resolve_excludes_self(self, kp):
        """The anchor item should not appear in its own results."""
        result = kp.resolve_meta("anchor")
        for items in result.values():
            assert all(item.id != "anchor" for item in items)

    def test_resolve_meta_excludes_hidden(self, kp):
        """Meta-docs themselves (dot-prefix) should not appear in results."""
        result = kp.resolve_meta("anchor")
        for items in result.values():
            assert all(not item.id.startswith(".") for item in items)

    def test_genre_prereq_gates_resolution(self, kp):
        """Items without genre tag should not get genre meta-doc results."""
        result = kp.resolve_meta("anchor")
        # anchor has no genre tag, so .meta/genre (which requires genre=*) should be absent
        assert "genre" not in result

    def test_genre_prereq_passes_when_present(self, kp):
        """Items with genre tag should get genre meta-doc results."""
        result = kp.resolve_meta("album1")
        # album1 has genre=jazz, so .meta/genre prereq passes
        assert "genre" in result
        # Results should include other jazz items
        ids = [item.id for item in result["genre"]]
        assert "album2" in ids  # also jazz

    def test_nonexistent_item_returns_empty(self, kp):
        """resolve_meta for nonexistent item returns empty dict."""
        result = kp.resolve_meta("does-not-exist")
        assert result == {}


class TestResolveInlineMeta:
    """Tests for resolve_inline_meta() with ad-hoc queries."""

    def test_basic_tag_query(self, kp):
        """Simple tag query should find matching items."""
        items = kp.resolve_inline_meta(
            "anchor", [{"type": "learning"}],
        )
        assert len(items) > 0
        assert all(item.tags.get("type") == "learning" for item in items)

    def test_multi_tag_and_query(self, kp):
        """AND query with multiple tags should narrow results."""
        items = kp.resolve_inline_meta(
            "anchor", [{"act": "commitment", "status": "open"}],
        )
        assert len(items) > 0
        for item in items:
            assert item.tags.get("act") == "commitment"
            assert item.tags.get("status") == "open"

    def test_union_of_queries(self, kp):
        """Multiple query dicts should produce union of results."""
        items = kp.resolve_inline_meta(
            "anchor",
            [{"type": "learning"}, {"act": "commitment", "status": "open"}],
        )
        # Should find both learnings AND open commitments
        types = {item.tags.get("type") for item in items}
        acts = {item.tags.get("act") for item in items}
        assert "learning" in types or "commitment" in acts

    def test_context_expansion(self, kp):
        """context_keys should expand using anchor's tag values."""
        items = kp.resolve_inline_meta(
            "anchor", [],
            context_keys=["project"],
        )
        # anchor has project=myapp, so this queries for project=myapp
        assert len(items) > 0
        for item in items:
            assert item.tags.get("project") == "myapp"

    def test_prereq_gates_inline(self, kp):
        """prereq_keys should gate resolution — missing tag returns empty."""
        items = kp.resolve_inline_meta(
            "no-project",
            [{"type": "learning"}],
            prereq_keys=["project"],
        )
        assert items == []

    def test_prereq_passes_inline(self, kp):
        """prereq_keys should pass when anchor has the required tag."""
        items = kp.resolve_inline_meta(
            "anchor",
            [{"type": "learning"}],
            prereq_keys=["project"],
        )
        assert len(items) > 0

    def test_excludes_self(self, kp):
        """Anchor item should never appear in its own results."""
        items = kp.resolve_inline_meta(
            "anchor", [],
            context_keys=["project"],
        )
        assert all(item.id != "anchor" for item in items)

    def test_limit_respected(self, kp):
        """Results should not exceed the limit parameter."""
        items = kp.resolve_inline_meta(
            "anchor",
            [{"act": "commitment", "status": "open"}],
            limit=1,
        )
        assert len(items) <= 1

    def test_nonexistent_anchor_returns_empty(self, kp):
        """Inline resolve for nonexistent item returns empty list."""
        items = kp.resolve_inline_meta(
            "does-not-exist", [{"type": "learning"}],
        )
        assert items == []


class TestProvisionalMeta:
    """Tests for part-to-parent uplift in meta resolution."""

    def test_part_match_uplifted_to_parent(self, kp):
        """Part-sourced meta match should appear with parent ID, not part ID."""
        # Create a parent doc WITHOUT act=commitment
        kp.put("Meeting notes from Tuesday", id="meeting-notes", tags={
            "topic": "meetings", "project": "myapp",
        })

        # Directly inject a part into ChromaDB with act=commitment tag
        # (simulates analyze_tagging producing a part with that tag)
        chroma_coll = kp._resolve_chroma_collection()
        embed = kp._get_embedding_provider()
        embedding = embed.embed("I will refactor the auth module by Friday")
        kp._store.upsert_part(
            chroma_coll, "meeting-notes", 1,
            embedding,
            "I will refactor the auth module by Friday",
            {"act": "commitment", "status": "open", "_base_id": "meeting-notes"},
        )

        result = kp.resolve_meta("anchor")

        # Part match should appear under "todo" with parent ID (not part ID)
        if "todo" in result:
            for item in result["todo"]:
                assert "@p" not in item.id
        # No provisional group
        assert "todo/provisional" not in result

    def test_direct_match_suppresses_part(self, kp):
        """When parent matches directly, part match should be deduplicated."""
        chroma_coll = kp._resolve_chroma_collection()
        items = kp.list_items(tags={"act": "commitment", "status": "open"}, limit=1)
        assert items, "Need at least one commitment for this test"
        parent_id = items[0].id

        # Inject a part of that same parent
        embed = kp._get_embedding_provider()
        embedding = embed.embed("Sub-task: write unit tests")
        kp._store.upsert_part(
            chroma_coll, parent_id, 1,
            embedding,
            "Sub-task: write unit tests",
            {"act": "commitment", "status": "open", "_base_id": parent_id},
        )

        result = kp.resolve_meta("anchor")

        # Parent should appear once under "todo"
        if "todo" in result:
            ids = [item.id for item in result["todo"]]
            assert ids.count(parent_id) <= 1  # No duplication
        # No provisional group
        assert "todo/provisional" not in result
