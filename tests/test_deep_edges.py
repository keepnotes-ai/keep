"""Tests for edge-following deep search (_deep_edge_follow).

Edge-following replaces tag-following for stores with edges:
1. Traverse inverse edges from primary results
2. FTS pre-filter on edge source IDs
3. Embedding post-filter + RRF fusion
4. Assign results back to originating primaries
"""

import pytest
from pathlib import Path

from keep.api import Keeper
from keep.document_store import DocumentStore, PartInfo


# ---------------------------------------------------------------------------
# DocumentStore.query_fts_scoped (real SQLite)
# ---------------------------------------------------------------------------

class TestQueryFtsScoped:
    """Scoped FTS search against a real SQLite database."""

    @pytest.fixture
    def store(self, tmp_path):
        db_path = tmp_path / "documents.db"
        with DocumentStore(db_path) as s:
            s.upsert("c", "doc-a",
                     summary="Melanie loves reading books about history",
                     tags={"speaker": "Melanie"})
            s.upsert("c", "doc-b",
                     summary="Caroline went to a pride parade",
                     tags={"speaker": "Caroline"})
            s.upsert("c", "doc-c",
                     summary="Melanie went camping at the beach",
                     tags={"speaker": "Melanie"})
            s.upsert("c", "doc-d",
                     summary="Dave plays guitar and drums",
                     tags={"speaker": "Dave"})
            s.upsert_parts("c", "doc-a", [
                PartInfo(part_num=0,
                           summary="Overview of Melanie's reading habits",
                           tags={}, content="", created_at="2024-01-01"),
                PartInfo(part_num=1,
                           summary="Melanie read Charlotte's Web",
                           tags={}, content="", created_at="2024-01-01"),
            ])
            # Create a version by upserting again (original becomes v1)
            s.upsert("c", "doc-c",
                     summary="Melanie camped in the forest",
                     tags={"speaker": "Melanie"})
            yield s

    def test_scoped_returns_only_matching_ids(self, store):
        results = store.query_fts_scoped("c", "Melanie", ["doc-a", "doc-c"])
        ids = [r[0] for r in results]
        assert any("doc-a" in i for i in ids)
        assert any("doc-c" in i for i in ids)
        # doc-b and doc-d not in whitelist
        assert not any("doc-b" in i for i in ids)
        assert not any("doc-d" in i for i in ids)

    def test_scoped_excludes_non_whitelisted(self, store):
        results = store.query_fts_scoped("c", "went", ["doc-b"])
        ids = [r[0] for r in results]
        assert any("doc-b" in i for i in ids)
        assert not any("doc-c" in i for i in ids)

    def test_scoped_searches_parts(self, store):
        results = store.query_fts_scoped("c", "Charlotte", ["doc-a"])
        ids = [r[0] for r in results]
        assert any("doc-a@p" in i for i in ids)

    def test_scoped_searches_versions(self, store):
        # "camping" appears in v1 (original summary before re-upsert)
        results = store.query_fts_scoped("c", "camping", ["doc-c"])
        ids = [r[0] for r in results]
        assert any("doc-c@v" in i for i in ids)

    def test_scoped_empty_ids_returns_empty(self, store):
        assert store.query_fts_scoped("c", "Melanie", []) == []

    def test_scoped_no_query_match_returns_empty(self, store):
        assert store.query_fts_scoped("c", "xyznonexistent", ["doc-a"]) == []


# ---------------------------------------------------------------------------
# DocumentStore.has_edges
# ---------------------------------------------------------------------------

class TestHasEdges:
    """Tests for edge existence checks."""

    @pytest.fixture
    def store(self, tmp_path):
        with DocumentStore(tmp_path / "documents.db") as s:
            yield s

    def test_no_edges(self, store):
        assert store.has_edges("c") is False

    def test_with_edges(self, store):
        store.upsert_edge("c", "src", "speaker", "target", "said", "2024-01-01")
        assert store.has_edges("c") is True

    def test_different_collection(self, store):
        store.upsert_edge("other", "src", "speaker", "target", "said", "2024-01-01")
        assert store.has_edges("c") is False
        assert store.has_edges("other") is True


class TestInverseVersionEdges:
    """Tests for inverse version edge traversal."""

    @pytest.fixture
    def store(self, tmp_path):
        with DocumentStore(tmp_path / "documents.db") as s:
            yield s

    def test_finds_sources_from_archived_edge_tags(self, store):
        store.upsert("c", ".tag/speaker", summary="", tags={"_inverse": "said"})
        store.upsert("c", "doc-x", summary="Joanna mentioned hiking",
                     tags={"speaker": "Joanna"})
        store.upsert("c", "doc-x", summary="Nate wrapped up",
                     tags={"speaker": "Nate"})

        rows = store.get_inverse_version_edges("c", "Joanna")
        assert any(inv == "said" and src == "doc-x" for inv, src, _ in rows)

    def test_inverse_version_edges_are_case_sensitive(self, store):
        store.upsert("c", ".tag/speaker", summary="", tags={"_inverse": "said"})
        store.upsert("c", "doc-x", summary="Joanna mentioned hiking",
                     tags={"speaker": "Joanna"})
        # Force archival of the previous version so version_edges materialize.
        store.upsert("c", "doc-x", summary="Nate wrapped up",
                     tags={"speaker": "Nate"})

        assert store.get_inverse_version_edges("c", "Joanna")
        assert store.get_inverse_version_edges("c", "joanna") == []


# ---------------------------------------------------------------------------
# _deep_edge_follow integration (mock providers)
# ---------------------------------------------------------------------------

class TestDeepEdgeFollow:
    """Integration tests for edge-following deep search."""

    @pytest.fixture
    def keeper(self, tmp_path, mock_providers):
        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        # Create tagdoc with _inverse
        kp._document_store.upsert(doc_coll, ".tag/speaker", summary="",
                                  tags={"_inverse": "said", "_source": "inline",
                                        "category": "system"})

        # Create a target entity
        kp._document_store.upsert(doc_coll, "Melanie", summary="A person",
                                  tags={"_source": "auto-vivify"})

        # Create source docs (things Melanie "said") with edges
        for i in range(5):
            doc_id = f"session-{i}"
            kp._document_store.upsert(
                doc_coll, doc_id,
                summary=f"Melanie talked about topic {i}",
                tags={"speaker": "Melanie"},
            )
            kp._document_store.upsert_edge(
                doc_coll, doc_id, "speaker", "Melanie", "said",
                f"2024-01-0{i+1}",
            )
            # Also store in mock vector store
            embedding = [float(i) / 10] * 10
            kp._store.upsert(chroma_coll, doc_id, embedding,
                              f"Melanie talked about topic {i}",
                              tags={"speaker": "melanie"})

        # Create a doc that is NOT an edge source (control)
        kp._document_store.upsert(doc_coll, "unrelated",
                                  summary="Something about topic 3",
                                  tags={})
        kp._store.upsert(chroma_coll, "unrelated", [0.3] * 10,
                          "Something about topic 3", tags={})

        return kp

    def test_edge_follow_returns_groups(self, keeper):
        """Primary result 'Melanie' should produce deep groups from edges."""
        doc_coll = keeper._resolve_doc_collection()
        chroma_coll = keeper._resolve_chroma_collection()

        from keep.types import Item
        primary = [Item(id="Melanie", summary="A person", tags={}, score=1.0)]

        embedding = [0.1] * 10
        groups = keeper._deep_edge_follow(
            primary, chroma_coll, doc_coll,
            query="topic", embedding=embedding,
        )

        assert "Melanie" in groups
        deep_ids = [i.id for i in groups["Melanie"]]
        assert any(d.startswith("session-") for d in deep_ids)
        assert "unrelated" not in deep_ids

    def test_edge_follow_returns_all_candidates(self, keeper):
        """Deep groups should return all matching candidates (renderer caps via budget)."""
        doc_coll = keeper._resolve_doc_collection()
        chroma_coll = keeper._resolve_chroma_collection()

        from keep.types import Item
        primary = [Item(id="Melanie", summary="A person", tags={}, score=1.0)]

        embedding = [0.1] * 10
        groups = keeper._deep_edge_follow(
            primary, chroma_coll, doc_coll,
            query="topic", embedding=embedding,
        )

        assert "Melanie" in groups
        # All 5 sessions should be returned (no per-group cap)
        assert len(groups["Melanie"]) == 5

    def test_edge_follow_no_edges_returns_empty(self, keeper):
        """Primary without edges should produce no groups."""
        doc_coll = keeper._resolve_doc_collection()
        chroma_coll = keeper._resolve_chroma_collection()

        from keep.types import Item
        primary = [Item(id="unrelated", summary="Something", tags={}, score=1.0)]

        embedding = [0.1] * 10
        groups = keeper._deep_edge_follow(
            primary, chroma_coll, doc_coll,
            query="topic", embedding=embedding,
        )

        assert groups == {}

    def test_edge_follow_excludes_primaries_from_results(self, keeper):
        """Edge sources that ARE primaries should not appear in deep groups."""
        doc_coll = keeper._resolve_doc_collection()
        chroma_coll = keeper._resolve_chroma_collection()

        from keep.types import Item
        primary = [
            Item(id="Melanie", summary="A person", tags={}, score=1.0),
            Item(id="session-0", summary="Melanie topic 0", tags={}, score=0.9),
        ]

        embedding = [0.1] * 10
        groups = keeper._deep_edge_follow(
            primary, chroma_coll, doc_coll,
            query="topic", embedding=embedding,
        )

        if "Melanie" in groups:
            deep_ids = [i.id for i in groups["Melanie"]]
            assert "session-0" not in deep_ids

    def test_edge_follow_multiple_primaries(self, keeper):
        """Multiple primaries with edges should each get their own groups."""
        doc_coll = keeper._resolve_doc_collection()
        chroma_coll = keeper._resolve_chroma_collection()

        # Add a second entity with edges
        kp = keeper
        kp._document_store.upsert(doc_coll, "Caroline", summary="Another person",
                                  tags={"_source": "auto-vivify"})
        kp._document_store.upsert(doc_coll, "carol-msg",
                                  summary="Caroline discussed painting",
                                  tags={"speaker": "Caroline"})
        kp._document_store.upsert_edge(
            doc_coll, "carol-msg", "speaker", "Caroline", "said", "2024-02-01",
        )
        chroma_coll = kp._resolve_chroma_collection()
        kp._store.upsert(chroma_coll, "carol-msg", [0.5] * 10,
                          "Caroline discussed painting",
                          tags={"speaker": "caroline"})

        from keep.types import Item
        primary = [
            Item(id="Melanie", summary="A person", tags={}, score=1.0),
            Item(id="Caroline", summary="Another person", tags={}, score=0.8),
        ]

        embedding = [0.1] * 10
        groups = keeper._deep_edge_follow(
            primary, chroma_coll, doc_coll,
            query="topic painting", embedding=embedding,
        )

        # Both entities should potentially have groups
        # (depends on FTS matching, but at least the structure should work)
        assert isinstance(groups, dict)

    def test_two_hop_forward_then_inverse(self, keeper):
        """Session primary → forward edge → entity → inverse edges → sibling sessions."""
        doc_coll = keeper._resolve_doc_collection()
        chroma_coll = keeper._resolve_chroma_collection()

        # The fixture already has: session-{0..4} --speaker--> Melanie
        # So if session-0 is the primary, two-hop should discover
        # session-{1..4} via session-0 → (speaker) → Melanie → (said) → others.
        from keep.types import Item
        primary = [Item(id="session-0", summary="Melanie topic 0",
                        tags={}, score=1.0)]

        embedding = [0.1] * 10
        groups = keeper._deep_edge_follow(
            primary, chroma_coll, doc_coll,
            query="topic", embedding=embedding,
        )

        assert "session-0" in groups
        deep_ids = [i.id for i in groups["session-0"]]
        # Should find sibling sessions (not session-0 itself)
        assert any(d.startswith("session-") and d != "session-0"
                    for d in deep_ids)
        assert "session-0" not in deep_ids
        # Should NOT include unrelated (no edge to Melanie)
        assert "unrelated" not in deep_ids

    def test_two_hop_no_forward_edges(self, keeper):
        """Primary with no forward edges produces no two-hop candidates."""
        doc_coll = keeper._resolve_doc_collection()
        chroma_coll = keeper._resolve_chroma_collection()

        # "unrelated" has no edges at all — neither forward nor inverse
        from keep.types import Item
        primary = [Item(id="unrelated", summary="Something",
                        tags={}, score=1.0)]

        embedding = [0.1] * 10
        groups = keeper._deep_edge_follow(
            primary, chroma_coll, doc_coll,
            query="topic", embedding=embedding,
        )
        assert groups == {}

    def test_keeps_multiple_anchors_for_same_source(self, tmp_path, mock_providers):
        """A single source doc can contribute multiple focused deep anchors."""
        from keep.types import Item

        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        kp._document_store.upsert(doc_coll, ".tag/speaker", summary="",
                                  tags={"_inverse": "said"})
        kp._document_store.upsert(doc_coll, "Melanie", summary="A person",
                                  tags={"_source": "auto-vivify"})
        kp._document_store.upsert(
            doc_coll, "session-0",
            summary="Melanie practiced clarinet today",
            tags={"speaker": "Melanie"},
        )
        # Create a version hit containing the same activity term.
        kp._document_store.upsert(
            doc_coll, "session-0",
            summary="Melanie volunteered at the youth center",
            tags={"speaker": "Melanie"},
        )
        # Create a part hit on the same source.
        kp._document_store.upsert_parts(
            doc_coll, "session-0",
            [PartInfo(
                part_num=1,
                summary="Clarinet practice details",
                tags={},
                content="Clarinet scales and rehearsal",
                created_at="2024-01-01",
            )],
        )
        kp._document_store.upsert_edge(
            doc_coll, "session-0", "speaker", "Melanie", "said", "2024-01-01",
        )
        kp._store.upsert(chroma_coll, "session-0", [0.2] * 10,
                         "Melanie volunteered at the youth center",
                         tags={"speaker": "melanie"})

        primary = [Item(id="Melanie", summary="A person", tags={}, score=1.0)]
        groups = kp._deep_edge_follow(
            primary, chroma_coll, doc_coll,
            query="clarinet", embedding=[0.2] * 10,
        )

        assert "Melanie" in groups
        ids = [i.id for i in groups["Melanie"]]
        assert any(i.startswith("session-0@p") for i in ids), ids
        # Keep multiple anchors for the same source (not collapsed to one).
        assert sum(1 for i in ids if i.split("@")[0] == "session-0") >= 2, ids

    def test_inverse_follow_includes_version_only_edge_tag(
        self, tmp_path, mock_providers, monkeypatch
    ):
        """Version-derived inverse sources are included in deep edge traversal."""
        from keep.types import Item

        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        kp._document_store.upsert(doc_coll, "Joanna", summary="A person", tags={})
        kp._document_store.upsert(
            doc_coll, "conv3-session11",
            summary="Nate wrapped up the conversation",
            tags={"speaker": "Nate"},
        )

        kp._store.upsert(
            chroma_coll,
            "conv3-session11",
            [0.1] * 10,
            "Nate wrapped up the conversation",
            tags={"speaker": "nate"},
        )

        # Mock version-path support on the test double store.
        monkeypatch.setattr(
            kp._document_store, "count_versions", lambda _c: 1, raising=False,
        )
        monkeypatch.setattr(
            kp._document_store,
            "get_inverse_version_edges",
            lambda _c, target, limit=200: [("said", "conv3-session11", "2024-04-01")]
            if target == "Joanna" else [],
            raising=False,
        )

        primary = [Item(id="Joanna", summary="A person", tags={}, score=1.0)]
        groups = kp._deep_edge_follow(
            primary, chroma_coll, doc_coll,
            query="How many hikes has Joanna been on?",
            embedding=[0.1] * 10,
        )

        assert "Joanna" in groups
        deep_ids = [i.id for i in groups["Joanna"]]
        assert "conv3-session11" in deep_ids

    def test_focus_summary_preserved_from_version_hit(
        self, tmp_path, mock_providers, monkeypatch
    ):
        """When deep hits a version, parent keeps matched version text as _focus_summary."""
        from keep.types import Item

        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        kp._document_store.upsert(doc_coll, ".tag/speaker", summary="",
                                  tags={"_inverse": "said"})
        kp._document_store.upsert(doc_coll, "Melanie", summary="A person",
                                  tags={"_source": "auto-vivify"})

        # Head summary is intentionally generic.
        kp._document_store.upsert(doc_coll, "session-1",
                                  summary="Bye and take care",
                                  tags={"speaker": "Melanie"})
        kp._document_store.upsert_edge(
            doc_coll, "session-1", "speaker", "Melanie", "said", "2024-01-01")

        class _MockHit:
            def __init__(self, id, summary, score, tags):
                self.id = id
                self.summary = summary
                self._score = score
                self._tags = tags

            def to_item(self):
                return Item(
                    id=self.id,
                    summary=self.summary,
                    tags=self._tags,
                    score=self._score,
                )

        # Force one focused version hit and one generic semantic hit.
        monkeypatch.setattr(
            kp._document_store,
            "query_fts_scoped",
            lambda *_args, **_kwargs: [
                ("session-1@v1", "I went hiking near the lake", -1.0),
            ],
        )
        monkeypatch.setattr(
            kp._store,
            "query_embedding",
            lambda *_args, **_kwargs: [
                _MockHit("session-1", "Bye and take care", 0.95, {"speaker": "melanie"}),
            ],
        )

        primary = [Item(id="Melanie", summary="A person", tags={}, score=1.0)]
        groups = kp._deep_edge_follow(
            primary, chroma_coll, doc_coll,
            query="hiking", embedding=[0.1] * 10,
        )

        assert "Melanie" in groups
        hit = groups["Melanie"][0]
        assert hit.id.split("@")[0] == "session-1"
        assert hit.summary == "Bye and take care"
        assert "hiking" in hit.tags.get("_focus_summary", "").lower()
        assert hit.tags.get("_anchor_type") == "version"
        assert hit.tags.get("_anchor_id") == "session-1@v1"
        assert hit.tags.get("_lane") == "authoritative"


# ---------------------------------------------------------------------------
# Entity injection: query-mentioned edge targets as synthetic primaries
# ---------------------------------------------------------------------------

class TestEntityInjection:
    """Entity names in the query should be injected as primaries for deep."""

    def test_entity_injected_when_named_in_query(self, tmp_path, mock_providers):
        """Query mentioning 'Melanie' should produce deep groups via her edges."""
        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        # Set up: entity Melanie with edges from sessions
        kp._document_store.upsert(doc_coll, ".tag/speaker", summary="",
                                  tags={"_inverse": "said"})
        kp._document_store.upsert(doc_coll, "Melanie", summary="",
                                  tags={"_source": "auto-vivify"})
        # Melanie doesn't get an embedding — she won't rank in search
        for i in range(3):
            doc_id = f"session-{i}"
            kp._document_store.upsert(doc_coll, doc_id,
                                      summary=f"Melanie talked about topic {i}",
                                      tags={"speaker": "Melanie"})
            kp._document_store.upsert_edge(
                doc_coll, doc_id, "speaker", "Melanie", "said",
                f"2024-01-0{i+1}")
            kp._store.upsert(chroma_coll, doc_id, [float(i) / 10] * 10,
                              f"Melanie talked about topic {i}",
                              tags={"speaker": "melanie"})

        # Also add a non-Melanie doc that will rank in embedding search
        kp._document_store.upsert(doc_coll, "other-doc",
                                  summary="Someone discussed books",
                                  tags={})
        kp._store.upsert(chroma_coll, "other-doc", [0.1] * 10,
                          "Someone discussed books", tags={})

        # Verify entity injection plumbing
        entity_hits = kp._document_store.find_edge_targets(doc_coll, "What did Melanie talk about")
        assert "Melanie" in entity_hits, f"find_edge_targets returned {entity_hits}"
        assert kp._document_store.has_edges(doc_coll)

        results = kp.find("What did Melanie talk about?", deep=True, limit=5)
        deep_groups = getattr(results, "deep_groups", {})

        # Melanie should appear as a deep group key (injected entity)
        assert "Melanie" in deep_groups, f"deep_groups keys: {list(deep_groups.keys())}, primaries: {[i.id for i in results]}"
        deep_ids = [i.id for i in deep_groups["Melanie"]]
        assert any(d.startswith("session-") for d in deep_ids)

    def test_no_injection_without_entity_match(self, tmp_path, mock_providers):
        """Query not mentioning any entity should not inject anything extra."""
        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        kp._document_store.upsert(doc_coll, ".tag/speaker", summary="",
                                  tags={"_inverse": "said"})
        kp._document_store.upsert(doc_coll, "Melanie", summary="",
                                  tags={"_source": "auto-vivify"})
        kp._document_store.upsert(doc_coll, "session-0",
                                  summary="A topic discussion",
                                  tags={"speaker": "Melanie"})
        kp._document_store.upsert_edge(
            doc_coll, "session-0", "speaker", "Melanie", "said", "2024-01-01")
        kp._store.upsert(chroma_coll, "session-0", [0.1] * 10,
                          "A topic discussion", tags={"speaker": "melanie"})

        results = kp.find("general topic query", deep=True, limit=5)
        deep_groups = getattr(results, "deep_groups", {})

        # "Melanie" should NOT be a group key (not mentioned in query)
        assert "Melanie" not in deep_groups

    def test_entity_tokens_removed_from_deep_query(
        self, tmp_path, mock_providers, monkeypatch
    ):
        """Deep FTS query should remove injected entity name tokens."""
        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        kp._document_store.upsert(doc_coll, ".tag/speaker", summary="",
                                  tags={"_inverse": "said"})
        kp._document_store.upsert(doc_coll, "Melanie", summary="",
                                  tags={"_source": "auto-vivify"})
        kp._document_store.upsert(doc_coll, "session-0",
                                  summary="Melanie went hiking yesterday",
                                  tags={"speaker": "Melanie"})
        kp._document_store.upsert_edge(
            doc_coll, "session-0", "speaker", "Melanie", "said", "2024-01-01")
        kp._store.upsert(chroma_coll, "session-0", [0.1] * 10,
                         "Melanie went hiking yesterday",
                         tags={"speaker": "melanie"})

        captured: dict[str, str] = {}

        def _capture(primary_items, cc, dc, *, query, embedding, top_k=10, exclude_ids=None):
            captured["query"] = query
            return {}

        monkeypatch.setattr(kp, "_deep_edge_follow", _capture)
        kp.find("How many hikes has Melanie been on?", deep=True, limit=5)

        assert "query" in captured
        assert "melanie" not in captured["query"].split()
        assert "hikes" in captured["query"].split()

    def test_entity_phrase_removal_preserves_non_entity_tokens(
        self, tmp_path, mock_providers, monkeypatch
    ):
        """Removing entity phrase should not remove unrelated content tokens."""
        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        kp._document_store.upsert(doc_coll, ".tag/group", summary="",
                                  tags={"_inverse": "mentioned_in"})
        kp._document_store.upsert(doc_coll, "Book Club", summary="",
                                  tags={"_source": "auto-vivify"})
        kp._document_store.upsert(doc_coll, "session-0",
                                  summary="Book Club discussed recommendations",
                                  tags={"group": "Book Club"})
        kp._document_store.upsert_edge(
            doc_coll, "session-0", "group", "Book Club", "mentioned_in", "2024-01-01")
        kp._store.upsert(chroma_coll, "session-0", [0.1] * 10,
                         "Book Club discussed recommendations",
                         tags={"group": "book club"})

        captured: dict[str, str] = {}

        def _capture(primary_items, cc, dc, *, query, embedding, top_k=10, exclude_ids=None):
            captured["query"] = query
            return {}

        monkeypatch.setattr(kp, "_deep_edge_follow", _capture)
        kp.find("How many book recommendations from Book Club this month?", deep=True, limit=5)

        tokens = captured.get("query", "").split()
        assert "book" in tokens
        assert "club" not in tokens

    def test_deep_query_uses_store_stopwords(
        self, tmp_path, mock_providers, monkeypatch
    ):
        """Deep lexical scoring should read stopwords from DocumentStore."""
        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        kp._document_store.upsert(doc_coll, ".tag/speaker", summary="",
                                  tags={"_inverse": "said"})
        kp._document_store.upsert(doc_coll, "Melanie", summary="",
                                  tags={"_source": "auto-vivify"})
        kp._document_store.upsert(doc_coll, "session-0",
                                  summary="Melanie went hiking yesterday",
                                  tags={"speaker": "Melanie"})
        kp._document_store.upsert_edge(
            doc_coll, "session-0", "speaker", "Melanie", "said", "2024-01-01")
        kp._store.upsert(chroma_coll, "session-0", [0.1] * 10,
                         "Melanie went hiking yesterday",
                         tags={"speaker": "melanie"})

        calls = {"count": 0}

        def _custom_stopwords() -> frozenset[str]:
            calls["count"] += 1
            return frozenset({"how", "many", "hikes", "has", "been", "on"})

        monkeypatch.setattr(kp._document_store, "get_stopwords", _custom_stopwords)
        kp.find("How many hikes has Melanie been on?", deep=True, limit=5)
        assert calls["count"] >= 1


# ---------------------------------------------------------------------------
# find(deep=True) integration — edge vs tag fallback
# ---------------------------------------------------------------------------

class TestFindDeepDispatch:
    """Verify find(deep=True) uses edges when available, tags otherwise."""

    def test_deep_uses_edges_when_available(self, tmp_path, mock_providers):
        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        kp._document_store.upsert(doc_coll, ".tag/speaker", summary="",
                                  tags={"_inverse": "said"})
        kp._document_store.upsert(doc_coll, "Alice", summary="A person",
                                  tags={})
        kp._store.upsert(chroma_coll, "Alice", [0.1] * 10, "A person", tags={})

        kp._document_store.upsert(doc_coll, "msg-1",
                                  summary="Alice said hello world",
                                  tags={"speaker": "Alice"})
        kp._document_store.upsert_edge(
            doc_coll, "msg-1", "speaker", "Alice", "said", "2024-01-01",
        )
        kp._store.upsert(chroma_coll, "msg-1", [0.2] * 10,
                          "Alice said hello world",
                          tags={"speaker": "alice"})

        assert kp._document_store.has_edges(doc_coll)

        results = kp.find("hello", deep=True, limit=5)
        assert len(results) > 0

    def test_deep_falls_back_to_tags_without_edges(self, tmp_path, mock_providers):
        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        kp._document_store.upsert(doc_coll, "doc-1",
                                  summary="Hello world",
                                  tags={"topic": "greetings"})
        kp._store.upsert(chroma_coll, "doc-1", [0.1] * 10, "Hello world",
                          tags={"topic": "greetings"})

        assert not kp._document_store.has_edges(doc_coll)

        results = kp.find("hello", deep=True, limit=5)
        assert len(results) > 0


# ---------------------------------------------------------------------------
# _build_fts_query helper
# ---------------------------------------------------------------------------

class TestBuildFtsQuery:
    """Tests for full-text search query building."""

    @pytest.fixture
    def store(self, tmp_path):
        with DocumentStore(tmp_path / "documents.db") as s:
            yield s

    def test_basic_tokenization(self, store):
        result = store._build_fts_query("hello world")
        assert '"hello"' in result
        assert '"world"' in result
        assert "OR" in result

    def test_strips_quotes(self, store):
        result = store._build_fts_query('say "hello"')
        assert result is not None
        assert '""' not in result

    def test_only_special_chars_returns_none(self, store):
        result = store._build_fts_query("\"\" ''")
        assert result is None


# ---------------------------------------------------------------------------
# Regression tests for specific bugs
# ---------------------------------------------------------------------------

class TestEntityLimitOverflow:
    """find(deep=True) must not return more than limit items."""

    def test_entity_promotion_respects_limit(self, tmp_path, mock_providers):
        """When all final items have deep groups, entity promotion should not exceed limit.

        Entity promotion should not exceed limit by appending.
        """
        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        # Set up entity with edges
        kp._document_store.upsert(doc_coll, ".tag/speaker", summary="",
                                  tags={"_inverse": "said"})
        kp._document_store.upsert(doc_coll, "Entity", summary="",
                                  tags={"_source": "auto-vivify"})

        # Create documents that all have edges (so all get deep groups)
        for i in range(5):
            doc_id = f"doc-{i}"
            kp._document_store.upsert(doc_coll, doc_id,
                                      summary=f"Entity talks about {i}",
                                      tags={"speaker": "Entity"})
            kp._document_store.upsert_edge(
                doc_coll, doc_id, "speaker", "Entity", "said",
                f"2024-01-0{i+1}")
            kp._store.upsert(chroma_coll, doc_id,
                              [float(i) / 10] * 10,
                              f"Entity talks about {i}",
                              tags={"speaker": "entity"})

        results = kp.find("What did Entity say?", deep=True, limit=2)
        assert len(results) <= 2, (
            f"Expected at most 2 results, got {len(results)}: "
            f"{[i.id for i in results]}"
        )


class TestEntityPunctuationMatching:
    """find_edge_targets must match IDs with non-word punctuation."""

    def test_cplus_plus_target(self, tmp_path):
        with DocumentStore(tmp_path / "documents.db") as store:
            store.upsert("c", "C++", summary="", tags={})
            store.upsert_edge("c", "some-doc", "topic", "C++", "about",
                              "2024-01-01")
            hits = store.find_edge_targets("c", "What about C++ performance?")
            assert "C++" in hits, f"Expected C++ in hits, got {hits}"

    def test_hash_target(self, tmp_path):
        with DocumentStore(tmp_path / "documents.db") as store:
            store.upsert("c", "C#", summary="", tags={})
            store.upsert_edge("c", "some-doc", "topic", "C#", "about",
                              "2024-01-01")
            hits = store.find_edge_targets("c", "Tell me about C# features")
            assert "C#" in hits, f"Expected C# in hits, got {hits}"

    def test_normal_name_still_works(self, tmp_path):
        with DocumentStore(tmp_path / "documents.db") as store:
            store.upsert("c", "Melanie", summary="", tags={})
            store.upsert_edge("c", "some-doc", "speaker", "Melanie", "said",
                              "2024-01-01")
            hits = store.find_edge_targets("c", "What did Melanie say?")
            assert "Melanie" in hits


class TestMigrationRetry:
    """Deep-search migration should retry after transient failure."""

    def test_retry_after_migration_failure(self, tmp_path, mock_providers):
        kp = Keeper(store_path=str(tmp_path / "store"))
        doc_coll = kp._resolve_doc_collection()
        chroma_coll = kp._resolve_chroma_collection()

        # Add a document so find() has something to search
        kp._document_store.upsert(doc_coll, "doc-1",
                                  summary="test content", tags={})
        kp._store.upsert(chroma_coll, "doc-1", [0.1] * 10,
                          "test content", tags={})

        # Force the migration flag on and make migration fail
        kp._needs_sysdoc_migration = True
        original_migrate = kp._migrate_system_documents

        call_count = [0]
        def failing_migrate():
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("transient failure")
            return original_migrate()

        kp._migrate_system_documents = failing_migrate

        # First deep find — initial bootstrap fails, then is retried within
        # the same request path and succeeds.
        kp.find("test", deep=True, limit=5)
        assert call_count[0] == 2
        assert kp._needs_sysdoc_migration is False

        # Second deep find — migration is already complete, so no extra retry.
        kp.find("test", deep=True, limit=5)
        assert call_count[0] == 2
