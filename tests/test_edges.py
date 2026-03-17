"""
Tests for tag-driven edges (_inverse on tagdocs).

Edge lifecycle:
- A tagdoc `.tag/KEY` with `_inverse: VERB` makes KEY an edge-tag
- When doc-X has `KEY: target_id`, an edge row is created
- `get target_id` shows inverse edges under the VERB heading
- Auto-vivification: targets that don't exist are created as empty docs
- Deleting source/target cleans up edges
- Changing _inverse triggers backfill/cleanup
"""

import pytest
from pathlib import Path

from keep.api import Keeper
from keep.document_store import DocumentStore
from keep.types import EdgeRef


# ---------------------------------------------------------------------------
# DocumentStore edge CRUD (real SQLite)
# ---------------------------------------------------------------------------

class TestDocumentStoreEdges:
    """Edge table CRUD on a real SQLite database."""

    @pytest.fixture
    def store(self, tmp_path):
        db_path = tmp_path / "documents.db"
        with DocumentStore(db_path) as s:
            yield s

    def test_upsert_and_get_inverse_edges(self, store):
        store.upsert_edge("default", "conv1@v5", "speaker", "nate", "said", "2025-01-01T00:00:00")
        store.upsert_edge("default", "conv1@v12", "speaker", "nate", "said", "2025-01-02T00:00:00")

        edges = store.get_inverse_edges("default", "nate")
        assert len(edges) == 2
        # Ordered by (inverse, created DESC)
        assert edges[0] == ("said", "conv1@v12", "2025-01-02T00:00:00")
        assert edges[1] == ("said", "conv1@v5", "2025-01-01T00:00:00")

    def test_upsert_edge_keeps_multiple_targets_per_predicate(self, store):
        store.upsert_edge("default", "doc1", "speaker", "alice", "said", "2025-01-01T00:00:00")
        store.upsert_edge("default", "doc1", "speaker", "bob", "said", "2025-01-02T00:00:00")

        # PK includes target_id — both edges coexist.
        edges_alice = store.get_inverse_edges("default", "alice")
        edges_bob = store.get_inverse_edges("default", "bob")
        assert len(edges_alice) == 1
        assert len(edges_bob) == 1

    def test_delete_edges_for_source(self, store):
        store.upsert_edge("default", "doc1", "speaker", "nate", "said", "2025-01-01T00:00:00")
        store.upsert_edge("default", "doc1", "topic", "ai", "discussed_in", "2025-01-01T00:00:00")
        assert store.delete_edges_for_source("default", "doc1") == 2
        assert store.get_inverse_edges("default", "nate") == []

    def test_delete_edges_for_target(self, store):
        store.upsert_edge("default", "doc1", "speaker", "nate", "said", "2025-01-01T00:00:00")
        store.upsert_edge("default", "doc2", "speaker", "nate", "said", "2025-01-02T00:00:00")
        assert store.delete_edges_for_target("default", "nate") == 2
        assert store.get_inverse_edges("default", "nate") == []

    def test_delete_edges_for_predicate(self, store):
        store.upsert_edge("default", "doc1", "speaker", "nate", "said", "2025-01-01T00:00:00")
        store.upsert_edge("default", "doc2", "speaker", "bob", "said", "2025-01-02T00:00:00")
        assert store.delete_edges_for_predicate("default", "speaker") == 2

    def test_backfill_lifecycle(self, store):
        # Not found initially
        assert store.get_backfill_status("default", "speaker") is None

        # Create pending backfill
        store.upsert_backfill("default", "speaker", "said")
        assert store.get_backfill_status("default", "speaker") is None  # completed is NULL

        # Mark complete
        store.upsert_backfill("default", "speaker", "said", completed="2025-01-01T00:00:00")
        assert store.get_backfill_status("default", "speaker") == "2025-01-01T00:00:00"

        # Delete
        store.delete_backfill("default", "speaker")
        assert store.get_backfill_status("default", "speaker") is None

    def test_no_edges_returns_empty(self, store):
        assert store.get_inverse_edges("default", "nonexistent") == []

    def test_cross_collection_isolation(self, store):
        store.upsert_edge("coll_a", "doc1", "speaker", "nate", "said", "2025-01-01T00:00:00")
        assert store.get_inverse_edges("coll_b", "nate") == []
        assert len(store.get_inverse_edges("coll_a", "nate")) == 1


# ---------------------------------------------------------------------------
# Integration: Keeper edge processing with mocks
# ---------------------------------------------------------------------------

class TestEdgeIntegration:
    """End-to-end edge creation/deletion through Keeper."""

    @pytest.fixture
    def kp(self, mock_providers, tmp_path):
        return Keeper(store_path=tmp_path)

    def _create_tagdoc(self, kp, key, inverse):
        """Create a .tag/KEY tagdoc with _inverse.

        Uses direct document_store write (like system_docs.py) because
        _inverse is a system tag that gets filtered by put().
        """
        from keep.types import utc_now
        doc_coll = kp._resolve_doc_collection()
        now = utc_now()
        tags = {
            "_inverse": inverse,
            "_created": now,
            "_updated": now,
            "_source": "inline",
            "category": "system",
        }
        kp._document_store.upsert(
            collection=doc_coll,
            id=f".tag/{key}",
            summary=f"Tag: {key}",
            tags=tags,
        )

    def test_edge_created_on_put(self, kp):
        """Tag with _inverse tagdoc → edge in table → get target shows inverse."""
        self._create_tagdoc(kp, "speaker", "said")

        kp.put(content="Nate said hello", id="conv1", summary="Greeting",
               tags={"speaker": "nate"})

        ctx = kp.get_context("nate")
        assert ctx is not None
        assert "said" in ctx.edges
        assert len(ctx.edges["said"]) == 1
        assert ctx.edges["said"][0].source_id == "conv1"

    def test_edge_target_uri_value_is_normalized(self, kp):
        """Edge target values that are HTTP URIs use canonical ID normalization."""
        self._create_tagdoc(kp, "speaker", "said")

        raw_target = "HTTPS://Example.COM:443/a/../b/%41?q=1"
        canonical_target = "https://example.com/b/A?q=1"
        kp.put(content="URI target edge", id="conv1", summary="Edge to URI",
               tags={"speaker": raw_target})

        # Target doc is auto-vivified under canonical URI ID.
        assert kp.get(canonical_target) is not None

        # Inverse edge lookup uses canonical target ID.
        ctx = kp.get_context(canonical_target)
        assert "said" in ctx.edges
        assert len(ctx.edges["said"]) == 1
        assert ctx.edges["said"][0].source_id == "conv1"

    def test_edge_target_unicode_nfc_is_normalized(self, kp):
        """Edge target values normalize to the same NFC canonical form as IDs."""
        self._create_tagdoc(kp, "speaker", "said")

        raw_target = "Cafe\u0301"  # decomposed
        canonical_target = "Café"   # composed NFC
        kp.put(content="Unicode target edge", id="conv2", summary="Edge to unicode",
               tags={"speaker": raw_target})

        assert kp.get(canonical_target) is not None
        ctx = kp.get_context(canonical_target)
        assert "said" in ctx.edges
        assert len(ctx.edges["said"]) == 1
        assert ctx.edges["said"][0].source_id == "conv2"

    def test_auto_vivification(self, kp):
        """Target that doesn't exist is created as empty doc."""
        self._create_tagdoc(kp, "speaker", "said")

        # "nate" doesn't exist yet
        assert kp.get("nate") is None

        kp.put(content="Nate said hello", id="conv1", summary="Greeting",
               tags={"speaker": "nate"})

        # Now "nate" should exist (auto-vivified)
        item = kp.get("nate")
        assert item is not None
        assert item.tags.get("_source") == "auto-vivify"

    def test_auto_vivify_created_inherits_source_created(self, kp):
        """Auto-vivified target _created should inherit the source note timestamp."""
        self._create_tagdoc(kp, "speaker", "said")
        doc_coll = kp._resolve_doc_collection()
        source_created = "2024-01-02T03:04:05"
        source_updated = "2024-01-02T03:04:06"

        kp._process_edge_tags(
            "conv-ts",
            {"speaker": "nate", "_created": source_created, "_updated": source_updated},
            {},
            doc_coll,
        )

        item = kp.get("nate")
        assert item is not None
        assert item.tags.get("_source") == "auto-vivify"
        assert item.tags.get("_created") == source_created

    def test_backfill_auto_vivify_created_inherits_source_created(self, kp):
        """Backfill-created targets inherit _created from the referencing source doc."""
        from keep.pending_summaries import PendingSummary

        self._create_tagdoc(kp, "speaker", "said")
        doc_coll = kp._resolve_doc_collection()
        source_created = "2024-02-03T10:11:12"
        source_updated = "2024-02-03T10:11:13"

        kp._document_store.upsert(
            doc_coll,
            "conv-backfill",
            summary="Backfill source",
            tags={
                "speaker": "zoe",
                "_created": source_created,
                "_updated": source_updated,
                "_source": "inline",
            },
        )

        pending = PendingSummary(
            id="_backfill:speaker",
            collection=doc_coll,
            content="",
            queued_at="",
            attempts=1,
            task_type="backfill-edges",
            metadata={"predicate": "speaker", "inverse": "said"},
        )
        original_q = kp._document_store.query_by_tag_key
        kp._document_store.query_by_tag_key = (
            lambda collection, key, limit=100, offset=0, since_date=None, until_date=None:
            original_q(collection, key, limit=offset + limit, since_date=since_date, until_date=until_date)[
                offset:offset + limit
            ]
        )
        kp._process_pending_backfill_edges(pending)

        item = kp.get("zoe")
        assert item is not None
        assert item.tags.get("_source") == "auto-vivify"
        assert item.tags.get("_created") == source_created

    def test_multiple_edges_same_target(self, kp):
        """Multiple docs pointing at the same target show as multiple edges."""
        self._create_tagdoc(kp, "speaker", "said")

        kp.put(content="Nate said hello", id="conv1", summary="Greeting",
               tags={"speaker": "nate"})
        kp.put(content="Nate said goodbye", id="conv2", summary="Farewell",
               tags={"speaker": "nate"})

        ctx = kp.get_context("nate")
        assert len(ctx.edges["said"]) == 2
        source_ids = {e.source_id for e in ctx.edges["said"]}
        assert source_ids == {"conv1", "conv2"}

    def test_delete_source_cleans_edges(self, kp):
        """Deleting source doc removes its edges."""
        self._create_tagdoc(kp, "speaker", "said")
        kp.put(content="Nate said hello", id="conv1", summary="Greeting",
               tags={"speaker": "nate"})

        kp.delete("conv1")

        ctx = kp.get_context("nate")
        assert ctx.edges.get("said", []) == []

    def test_delete_target_cleans_inverse_edges(self, kp):
        """Deleting target doc removes edges pointing at it."""
        self._create_tagdoc(kp, "speaker", "said")
        kp.put(content="Nate said hello", id="conv1", summary="Greeting",
               tags={"speaker": "nate"})

        kp.delete("nate")

        # Edge should be gone even though source still exists
        # (re-create nate to check)
        kp.put(content="", id="nate", summary="")
        ctx = kp.get_context("nate")
        assert ctx.edges.get("said", []) == []

    def test_tag_value_change_replaces_edge(self, kp):
        """Changing tag value via put() replaces the edge (not accumulate)."""
        self._create_tagdoc(kp, "speaker", "said")

        kp.put(content="Someone said hello", id="conv1", summary="Greeting",
               tags={"speaker": "alice"})

        # alice has the edge
        ctx_alice = kp.get_context("alice")
        assert len(ctx_alice.edges.get("said", [])) == 1

        # Change speaker from alice to bob (replace, not add)
        kp.put(content="Someone said hello", id="conv1", summary="Greeting",
               tags={"speaker": "bob"})

        # bob has the edge, alice's edge is removed
        ctx_bob = kp.get_context("bob")
        assert len(ctx_bob.edges.get("said", [])) == 1
        ctx_alice = kp.get_context("alice")
        assert len(ctx_alice.edges.get("said", [])) == 0

    def test_multivalue_edge_tag_creates_all_edges(self, kp):
        """A single put with multiple tag values creates one edge per value."""
        self._create_tagdoc(kp, "speaker", "said")

        kp.put(content="Group discussion", id="conv1", summary="Meeting",
               tags={"speaker": ["alice", "bob", "carol"]})

        # Each target is auto-vivified
        for name in ("alice", "bob", "carol"):
            item = kp.get(name)
            assert item is not None, f"{name} should be auto-vivified"
            assert item.tags.get("_source") == "auto-vivify"

        # Each target has an inverse edge back to conv1
        for name in ("alice", "bob", "carol"):
            ctx = kp.get_context(name)
            assert "said" in ctx.edges, f"{name} missing 'said' edges"
            assert len(ctx.edges["said"]) == 1
            assert ctx.edges["said"][0].source_id == "conv1"

    def test_multivalue_edge_tag_add_and_remove_values(self, kp):
        """Adding/removing values from a multivalue edge tag updates edges."""
        self._create_tagdoc(kp, "speaker", "said")

        kp.put(content="Meeting", id="conv1", summary="Meeting",
               tags={"speaker": ["alice", "bob"]})

        # Both have edges
        for name in ("alice", "bob"):
            ctx = kp.get_context(name)
            assert len(ctx.edges.get("said", [])) == 1

        # Update: remove bob, add carol
        kp.put(content="Meeting", id="conv1", summary="Meeting",
               tags={"speaker": ["alice", "carol"]})

        # alice still has edge, carol now has edge
        for name in ("alice", "carol"):
            ctx = kp.get_context(name)
            assert len(ctx.edges.get("said", [])) == 1, f"{name} should have edge"

        # bob's edge is removed (replace semantics — new values replace old)
        ctx_bob = kp.get_context("bob")
        assert len(ctx_bob.edges.get("said", [])) == 0

    def test_get_context_unions_explicit_and_inverse_edge_refs(self, kp):
        """Query-time edge refs include both explicit and inverse values per key."""
        self._create_tagdoc(kp, "speaker", "said")
        self._create_tagdoc(kp, "said", "speaker")

        kp.put(content="Bob profile", id="bob", summary="Bob entity")
        kp.put(content="Alice profile", id="alice", summary="Alice entity",
               tags={"said": "bob"})
        kp.put(content="Conversation", id="conv1", summary="Gina said hello",
               tags={"speaker": "alice"})

        ctx = kp.get_context("alice")
        refs = ctx.edges.get("said", [])
        by_id = {r.source_id: r for r in refs}

        assert "bob" in by_id      # explicit edge-tag value on alice
        assert "conv1" in by_id    # inverse edge from speaker=alice
        assert by_id["bob"].summary == "Bob entity"
        assert by_id["conv1"].summary == "Gina said hello"

    def test_get_context_dedups_union_refs_by_source_id(self, kp):
        """When explicit+inverse refs collide on key/source_id, context dedups."""
        self._create_tagdoc(kp, "knows", "knows")

        kp.put(content="Alice profile", id="alice", summary="Alice entity",
               tags={"knows": "bob"})
        kp.put(content="Bob profile", id="bob", summary="Bob entity",
               tags={"knows": "alice"})

        ctx = kp.get_context("alice")
        refs = ctx.edges.get("knows", [])
        bob_refs = [r for r in refs if r.source_id == "bob"]
        assert len(bob_refs) == 1
        assert bob_refs[0].summary == "Bob entity"

    def test_removing_one_edge_tag_preserves_others(self, kp):
        """Removing one edge tag must not delete edges from other predicates."""
        self._create_tagdoc(kp, "speaker", "said")
        self._create_tagdoc(kp, "location", "visited_by")

        kp.put(content="Meeting", id="conv1", summary="Meeting notes",
               tags={"speaker": "alice", "location": "office"})

        # Both edges exist
        ctx_alice = kp.get_context("alice")
        assert len(ctx_alice.edges.get("said", [])) == 1
        ctx_office = kp.get_context("office")
        assert len(ctx_office.edges.get("visited_by", [])) == 1

        # Remove speaker tag (set to "") but keep location
        kp.put(content="Meeting", id="conv1", summary="Meeting notes",
               tags={"speaker": "", "location": "office"})

        # office edge should survive
        ctx_office = kp.get_context("office")
        assert len(ctx_office.edges.get("visited_by", [])) == 1
        # alice edge should be gone
        ctx_alice = kp.get_context("alice")
        assert ctx_alice.edges.get("said", []) == []

    def test_sysdoc_target_skipped(self, kp):
        """Targets starting with '.' (sysdoc names) don't create edges."""
        self._create_tagdoc(kp, "speaker", "said")
        kp.put(content="System ref", id="doc1", summary="Ref",
               tags={"speaker": ".meta/todo"})

        # No edge should exist for .meta/todo
        doc_coll = kp._resolve_doc_collection()
        edges = kp._document_store.get_inverse_edges(doc_coll, ".meta/todo")
        assert edges == []

    def test_sysdoc_target_not_autovivified_or_mutated(self, kp):
        """Non-system writes must not mutate dot-prefixed docs via edge processing."""
        self._create_tagdoc(kp, "speaker", "said")
        kp.put(content="original", id=".meta/todo", summary="Meta baseline")

        before = kp.get(".meta/todo")
        assert before is not None
        before_summary = before.summary
        before_created = before.tags.get("_created")

        kp.put(content="System ref", id="doc1", summary="Ref",
               tags={"speaker": ".meta/todo"})

        after = kp.get(".meta/todo")
        assert after is not None
        assert after.summary == before_summary
        assert after.tags.get("_created") == before_created

    def test_no_edge_without_inverse_tagdoc(self, kp):
        """Tags without _inverse tagdoc don't create edges."""
        kp.put(content="Some doc", id="doc1", summary="Doc",
               tags={"topic": "ai"})

        # "ai" should not have edges
        kp.put(content="", id="ai", summary="AI topic")
        ctx = kp.get_context("ai")
        assert ctx.edges == {}

    def test_inverse_removal_cleans_edges(self, kp):
        """Removing _inverse from tagdoc cleans up all edges for that predicate."""
        self._create_tagdoc(kp, "speaker", "said")
        kp.put(content="Nate said hello", id="conv1", summary="Greeting",
               tags={"speaker": "nate"})

        # Verify edge exists
        ctx = kp.get_context("nate")
        assert len(ctx.edges.get("said", [])) == 1

        # Remove _inverse from tagdoc by rewriting without it (direct store write)
        from keep.types import utc_now
        doc_coll = kp._resolve_doc_collection()
        now = utc_now()
        new_tags = {"_created": now, "_updated": now, "_source": "inline"}
        # Get old tags before overwriting
        old_doc = kp._document_store.get(doc_coll, ".tag/speaker")
        old_tags = old_doc.tags if old_doc else {}
        # Detect inverse change before storage
        kp._process_tagdoc_inverse_change(
            ".tag/speaker", new_tags, old_tags, doc_coll,
        )
        kp._document_store.upsert(
            collection=doc_coll,
            id=".tag/speaker",
            summary="Tag: speaker",
            tags=new_tags,
        )

        ctx = kp.get_context("nate")
        assert ctx.edges.get("said", []) == []

    def test_get_context_includes_edge_summaries(self, kp):
        """EdgeRefs include the source document's summary."""
        self._create_tagdoc(kp, "speaker", "said")
        kp.put(content="Nate said something interesting", id="conv1",
               summary="Interesting remark", tags={"speaker": "nate"})

        ctx = kp.get_context("nate")
        assert ctx.edges["said"][0].summary == "Interesting remark"

    def test_get_context_no_edges_when_none(self, kp):
        """get_context returns empty edges dict for doc with no inverse edges."""
        kp.put(content="Just a normal doc", id="doc1", summary="Normal doc")
        ctx = kp.get_context("doc1")
        assert ctx.edges == {}


# ---------------------------------------------------------------------------
# ItemContext serialization
# ---------------------------------------------------------------------------

class TestEdgeRefSerialization:
    """EdgeRef round-trips through to_dict/from_dict."""

    def _create_tagdoc(self, kp, key, inverse):
        from keep.types import utc_now
        doc_coll = kp._resolve_doc_collection()
        now = utc_now()
        kp._document_store.upsert(
            collection=doc_coll, id=f".tag/{key}",
            summary=f"Tag: {key}",
            tags={"_inverse": inverse, "_created": now, "_updated": now, "_source": "inline"},
        )

    def test_round_trip(self, mock_providers, tmp_path):
        kp = Keeper(store_path=tmp_path)
        self._create_tagdoc(kp, "speaker", "said")
        kp.put(content="Hello", id="conv1", summary="Greeting",
               tags={"speaker": "nate"})

        ctx = kp.get_context("nate")
        d = ctx.to_dict()
        ctx2 = type(ctx).from_dict(d)
        assert ctx2.edges == ctx.edges
        assert len(ctx2.edges["said"]) == 1
        assert ctx2.edges["said"][0].source_id == "conv1"


# ---------------------------------------------------------------------------
# Inverse tagdoc materialization
# ---------------------------------------------------------------------------

class TestInverseTagdocMaterialization:
    """When .tag/speaker has _inverse=said, .tag/said must exist with _inverse=speaker."""

    @pytest.fixture
    def kp(self, mock_providers, tmp_path):
        return Keeper(store_path=tmp_path)

    def _get_tagdoc(self, kp, key):
        """Read a tagdoc from the document store."""
        doc_coll = kp._resolve_doc_collection()
        return kp._document_store.get(doc_coll, f".tag/{key}")

    def test_inverse_tagdoc_created(self, kp):
        """Creating .tag/speaker with _inverse=said also creates .tag/said."""
        from keep.types import utc_now
        doc_coll = kp._resolve_doc_collection()
        now = utc_now()
        old_tags = {}
        new_tags = {
            "_inverse": "said",
            "_created": now,
            "_updated": now,
            "category": "system",
        }
        kp._process_tagdoc_inverse_change(".tag/speaker", new_tags, old_tags, doc_coll)

        inverse_doc = self._get_tagdoc(kp, "said")
        assert inverse_doc is not None
        assert inverse_doc.tags["_inverse"] == "speaker"

    def test_inverse_tagdoc_already_correct(self, kp):
        """No error when inverse tagdoc already has the right _inverse."""
        from keep.types import utc_now
        doc_coll = kp._resolve_doc_collection()
        now = utc_now()

        # Pre-create .tag/said with _inverse=speaker
        kp._document_store.upsert(
            doc_coll, ".tag/said", summary="Tag: said",
            tags={"_inverse": "speaker", "_created": now, "_updated": now},
        )

        # Creating .tag/speaker with _inverse=said should not error
        new_tags = {"_inverse": "said", "_created": now, "_updated": now}
        kp._process_tagdoc_inverse_change(".tag/speaker", new_tags, {}, doc_coll)

        # .tag/said is unchanged
        inverse_doc = self._get_tagdoc(kp, "said")
        assert inverse_doc.tags["_inverse"] == "speaker"

    def test_inverse_tagdoc_conflict_raises(self, kp):
        """Error when inverse tagdoc exists with a different _inverse."""
        from keep.types import utc_now
        doc_coll = kp._resolve_doc_collection()
        now = utc_now()

        # Pre-create .tag/said with _inverse=listener (conflicting)
        kp._document_store.upsert(
            doc_coll, ".tag/said", summary="Tag: said",
            tags={"_inverse": "listener", "_created": now, "_updated": now},
        )

        new_tags = {"_inverse": "said", "_created": now, "_updated": now}
        with pytest.raises(ValueError, match="Inverse conflict"):
            kp._process_tagdoc_inverse_change(".tag/speaker", new_tags, {}, doc_coll)

    def test_inverse_tagdoc_added_to_existing_without_inverse(self, kp):
        """Existing tagdoc without _inverse gets _inverse added."""
        from keep.types import utc_now
        doc_coll = kp._resolve_doc_collection()
        now = utc_now()

        # Pre-create .tag/said without _inverse
        kp._document_store.upsert(
            doc_coll, ".tag/said", summary="Tag: said",
            tags={"_created": now, "_updated": now, "category": "system"},
        )

        new_tags = {"_inverse": "said", "_created": now, "_updated": now}
        kp._process_tagdoc_inverse_change(".tag/speaker", new_tags, {}, doc_coll)

        inverse_doc = self._get_tagdoc(kp, "said")
        assert inverse_doc.tags["_inverse"] == "speaker"

    def test_inverse_tagdoc_triggers_backfill(self, kp):
        """Creating inverse tagdoc also queues backfill for the inverse direction."""
        from keep.types import utc_now
        doc_coll = kp._resolve_doc_collection()
        now = utc_now()

        new_tags = {"_inverse": "said", "_created": now, "_updated": now}
        kp._process_tagdoc_inverse_change(".tag/speaker", new_tags, {}, doc_coll)

        # Backfill should be queued for the inverse direction: predicate=said
        assert kp._document_store.backfill_exists(doc_coll, "said")

    def test_bidirectional_edges_work(self, kp):
        """Full flow: tagdoc + inverse tagdoc → edges work in both directions."""
        from keep.types import utc_now
        doc_coll = kp._resolve_doc_collection()
        now = utc_now()

        # Create .tag/speaker with _inverse=said (also materializes .tag/said)
        kp._document_store.upsert(
            doc_coll, ".tag/speaker", summary="Tag: speaker",
            tags={"_inverse": "said", "_created": now, "_updated": now, "category": "system"},
        )
        kp._ensure_inverse_tagdoc("speaker", "said", doc_coll)

        # Tag a doc with speaker=nate → edge: nate gets said:
        kp.put(content="Nate said hello", id="conv1", summary="Greeting",
               tags={"speaker": "nate"})

        ctx_nate = kp.get_context("nate")
        assert "said" in ctx_nate.edges
        assert ctx_nate.edges["said"][0].source_id == "conv1"

        # Tag a doc with said=nate → edge: nate gets speaker:
        kp.put(content="Quote attributed to Nate", id="quote1", summary="A quote",
               tags={"said": "nate"})

        ctx_nate = kp.get_context("nate")
        assert "said" in ctx_nate.edges
        assert "speaker" in ctx_nate.edges
        assert ctx_nate.edges["speaker"][0].source_id == "quote1"
