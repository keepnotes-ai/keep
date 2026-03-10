"""
Tests for planner stats precompute system.

Covers:
- PlannerStatsStore lifecycle and CRUD
- Scope key generation
- Outbox triggers on documents and edges
- Outbox drain and stat computation
- Metric computation (fanout, selectivity, facet.cardinality)
- Full rebuild
- Staleness detection
- Idempotency and lease/claim safety
"""

import json
import math
import os
import time

import pytest
from pathlib import Path

from keep.document_store import DocumentStore
from keep.planner_stats import PlannerStatsStore, build_scope_key
from keep.api import Keeper


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def doc_store(tmp_path):
    db_path = tmp_path / "documents.db"
    with DocumentStore(db_path) as s:
        yield s


@pytest.fixture
def stats_store(tmp_path):
    db_path = tmp_path / "planner_stats.db"
    with PlannerStatsStore(db_path) as s:
        yield s


@pytest.fixture
def stores(tmp_path):
    """Both stores together for integration tests."""
    doc_db = tmp_path / "documents.db"
    stats_db = tmp_path / "planner_stats.db"
    ds = DocumentStore(doc_db)
    ss = PlannerStatsStore(stats_db)
    yield ds, ss
    ss.close()
    ds.close()


# ---------------------------------------------------------------------------
# PlannerStatsStore init
# ---------------------------------------------------------------------------

class TestPlannerStatsStoreInit:

    def test_creates_db(self, tmp_path):
        db_path = tmp_path / "planner_stats.db"
        assert not db_path.exists()
        with PlannerStatsStore(db_path) as s:
            assert db_path.exists()

    def test_tables_exist(self, stats_store):
        tables = stats_store._execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = [r[0] for r in tables]
        assert "planner_stat" in names
        assert "planner_node_stat" in names
        assert "planner_watermark" in names


# ---------------------------------------------------------------------------
# Scope key
# ---------------------------------------------------------------------------

class TestScopeKey:

    def test_deterministic(self):
        k1 = build_scope_key("local", "default", None)
        k2 = build_scope_key("local", "default", None)
        assert k1 == k2

    def test_different_inputs_different_keys(self):
        k1 = build_scope_key("local", "default")
        k2 = build_scope_key("local", "other_project")
        assert k1 != k2

    def test_partition_appended(self):
        k1 = build_scope_key("local", "default")
        k2 = build_scope_key("local", "default", partition="topic=physics")
        assert k2.startswith(k1)
        assert ":" in k2

    def test_namespace_affects_key(self):
        k1 = build_scope_key("local", "default", namespace=None)
        k2 = build_scope_key("local", "default", namespace="ns1")
        assert k1 != k2


# ---------------------------------------------------------------------------
# Stat CRUD
# ---------------------------------------------------------------------------

class TestStatCRUD:

    def test_upsert_and_get(self, stats_store):
        scope = build_scope_key()
        stats_store.upsert_stat(
            "expansion.fanout", scope, "speaker",
            {"mean": 3.7, "p50": 2.0, "p90": 9.0, "max": 41}, 100,
        )
        priors = stats_store.get_priors(scope)
        assert "expansion.fanout" in priors
        assert "speaker" in priors["expansion.fanout"]
        assert priors["expansion.fanout"]["speaker"]["mean"] == 3.7

    def test_upsert_idempotent(self, stats_store):
        scope = build_scope_key()
        stats_store.upsert_stat("expansion.fanout", scope, "speaker",
                                {"mean": 3.7}, 100)
        stats_store.upsert_stat("expansion.fanout", scope, "speaker",
                                {"mean": 5.0}, 200)
        priors = stats_store.get_priors(scope)
        assert priors["expansion.fanout"]["speaker"]["mean"] == 5.0

    def test_delete_stat(self, stats_store):
        scope = build_scope_key()
        stats_store.upsert_stat("expansion.fanout", scope, "speaker",
                                {"mean": 3.7}, 100)
        stats_store.delete_stat("expansion.fanout", scope, "speaker")
        priors = stats_store.get_priors(scope)
        assert priors == {}

    def test_get_priors_filter_by_family(self, stats_store):
        scope = build_scope_key()
        stats_store.upsert_stat("expansion.fanout", scope, "speaker",
                                {"mean": 3.7}, 100)
        stats_store.upsert_stat("facet.cardinality", scope, "topic",
                                {"distinct_values": 5}, 50)

        priors = stats_store.get_priors(scope, metric_families=["facet.cardinality"])
        assert "expansion.fanout" not in priors
        assert "facet.cardinality" in priors

    def test_get_priors_filter_by_subject(self, stats_store):
        scope = build_scope_key()
        stats_store.upsert_stat("expansion.fanout", scope, "speaker",
                                {"mean": 3.7}, 100)
        stats_store.upsert_stat("expansion.fanout", scope, "topic",
                                {"mean": 1.2}, 50)

        priors = stats_store.get_priors(scope, subject_keys=["speaker"])
        assert "speaker" in priors["expansion.fanout"]
        assert "topic" not in priors["expansion.fanout"]


# ---------------------------------------------------------------------------
# Outbox triggers
# ---------------------------------------------------------------------------

class TestOutboxTriggers:

    def test_doc_insert_fires_trigger(self, doc_store):
        doc_store._execute(
            """INSERT INTO documents (id, collection, summary, tags_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("doc1", "default", "test", '{"topic": "ai"}',
             "2025-01-01T00:00:00", "2025-01-01T00:00:00"),
        )
        doc_store._conn.commit()

        rows = doc_store._execute(
            "SELECT mutation, entity_id, collection FROM planner_outbox"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "doc_insert"
        assert rows[0][1] == "doc1"

    def test_doc_update_tags_fires_trigger(self, doc_store):
        doc_store._execute(
            """INSERT INTO documents (id, collection, summary, tags_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("doc1", "default", "test", '{"topic": "ai"}',
             "2025-01-01T00:00:00", "2025-01-01T00:00:00"),
        )
        doc_store._conn.commit()

        # Clear insert trigger row
        doc_store._execute("DELETE FROM planner_outbox")
        doc_store._conn.commit()

        # Update tags
        doc_store._execute(
            "UPDATE documents SET tags_json = ? WHERE id = ?",
            ('{"topic": "ml"}', "doc1"),
        )
        doc_store._conn.commit()

        rows = doc_store._execute(
            "SELECT mutation, payload_json FROM planner_outbox"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "doc_update"
        payload = json.loads(rows[0][1])
        assert "old_tags_json" in payload
        assert "new_tags_json" in payload

    def test_doc_delete_fires_trigger(self, doc_store):
        doc_store._execute(
            """INSERT INTO documents (id, collection, summary, tags_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("doc1", "default", "test", '{"topic": "ai"}',
             "2025-01-01T00:00:00", "2025-01-01T00:00:00"),
        )
        doc_store._conn.commit()
        doc_store._execute("DELETE FROM planner_outbox")
        doc_store._conn.commit()

        doc_store._execute("DELETE FROM documents WHERE id = ?", ("doc1",))
        doc_store._conn.commit()

        rows = doc_store._execute(
            "SELECT mutation, entity_id FROM planner_outbox"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "doc_delete"

    def test_edge_insert_fires_trigger(self, doc_store):
        doc_store.upsert_edge("default", "doc1", "speaker", "alice", "said",
                              "2025-01-01T00:00:00")

        rows = doc_store._execute(
            "SELECT mutation, entity_id, payload_json FROM planner_outbox"
        ).fetchall()
        # May have doc insert trigger rows too, filter to edge_insert
        edge_rows = [r for r in rows if r[0] == "edge_insert"]
        assert len(edge_rows) == 1
        payload = json.loads(edge_rows[0][2])
        assert payload["predicate"] == "speaker"
        assert payload["target_id"] == "alice"

    def test_edge_delete_fires_trigger(self, doc_store):
        doc_store.upsert_edge("default", "doc1", "speaker", "alice", "said",
                              "2025-01-01T00:00:00")
        doc_store._execute("DELETE FROM planner_outbox")
        doc_store._conn.commit()

        doc_store.delete_edge("default", "doc1", "speaker", "alice")

        rows = doc_store._execute(
            "SELECT mutation, payload_json FROM planner_outbox"
        ).fetchall()
        edge_rows = [r for r in rows if r[0] == "edge_delete"]
        assert len(edge_rows) == 1
        payload = json.loads(edge_rows[0][1])
        assert payload["predicate"] == "speaker"


# ---------------------------------------------------------------------------
# Outbox dequeue / claim
# ---------------------------------------------------------------------------

class TestOutboxDequeue:

    def test_dequeue_returns_items(self, doc_store):
        doc_store._execute(
            """INSERT INTO documents (id, collection, summary, tags_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("doc1", "default", "test", '{"topic": "ai"}',
             "2025-01-01T00:00:00", "2025-01-01T00:00:00"),
        )
        doc_store._conn.commit()

        items = doc_store.dequeue_outbox(limit=10)
        assert len(items) == 1
        assert items[0]["mutation"] == "doc_insert"
        assert items[0]["entity_id"] == "doc1"

    def test_dequeue_claims_items(self, doc_store):
        doc_store._execute(
            """INSERT INTO documents (id, collection, summary, tags_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("doc1", "default", "test", '{}',
             "2025-01-01T00:00:00", "2025-01-01T00:00:00"),
        )
        doc_store._conn.commit()

        items = doc_store.dequeue_outbox(limit=10)
        assert len(items) == 1

        # Second dequeue should get nothing (items are claimed)
        items2 = doc_store.dequeue_outbox(limit=10)
        assert len(items2) == 0

    def test_complete_outbox_removes_items(self, doc_store):
        doc_store._execute(
            """INSERT INTO documents (id, collection, summary, tags_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("doc1", "default", "test", '{}',
             "2025-01-01T00:00:00", "2025-01-01T00:00:00"),
        )
        doc_store._conn.commit()

        items = doc_store.dequeue_outbox(limit=10)
        doc_store.complete_outbox([items[0]["outbox_id"]])

        count = doc_store._execute(
            "SELECT COUNT(*) FROM planner_outbox"
        ).fetchone()[0]
        assert count == 0

    def test_fail_outbox_releases_items(self, doc_store):
        doc_store._execute(
            """INSERT INTO documents (id, collection, summary, tags_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("doc1", "default", "test", '{}',
             "2025-01-01T00:00:00", "2025-01-01T00:00:00"),
        )
        doc_store._conn.commit()

        items = doc_store.dequeue_outbox(limit=10)
        doc_store.fail_outbox([items[0]["outbox_id"]])

        # Item should be available again
        items2 = doc_store.dequeue_outbox(limit=10)
        assert len(items2) == 1


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

class TestMetricComputation:

    def _insert_doc(self, doc_store, doc_id, tags=None):
        doc_store._execute(
            """INSERT OR REPLACE INTO documents
               (id, collection, summary, tags_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (doc_id, "default", "test", json.dumps(tags or {}),
             "2025-01-01T00:00:00", "2025-01-01T00:00:00"),
        )
        doc_store._conn.commit()

    def test_compute_fanout(self, stores):
        doc_store, stats_store = stores
        scope = build_scope_key()

        # doc1 -> speaker -> [alice, bob]   (fanout=2)
        # doc2 -> speaker -> [alice]         (fanout=1)
        doc_store.upsert_edge("default", "doc1", "speaker", "alice", "said",
                              "2025-01-01T00:00:00")
        doc_store.upsert_edge("default", "doc1", "speaker", "bob", "said",
                              "2025-01-01T00:00:00")
        doc_store.upsert_edge("default", "doc2", "speaker", "alice", "said",
                              "2025-01-01T00:00:00")

        count = stats_store.compute_fanout(doc_store, scope, "default")
        assert count == 1  # one predicate: "speaker"

        priors = stats_store.get_priors(scope, metric_families=["expansion.fanout"])
        fanout = priors["expansion.fanout"]["speaker"]
        assert fanout["mean"] == 1.5  # (2 + 1) / 2
        assert fanout["max"] == 2

    def test_compute_selectivity(self, stores):
        doc_store, stats_store = stores
        scope = build_scope_key()

        # 3 documents, 2 have speaker edges
        self._insert_doc(doc_store, "doc1")
        self._insert_doc(doc_store, "doc2")
        self._insert_doc(doc_store, "doc3")
        doc_store.upsert_edge("default", "doc1", "speaker", "alice", "said",
                              "2025-01-01T00:00:00")
        doc_store.upsert_edge("default", "doc2", "speaker", "bob", "said",
                              "2025-01-01T00:00:00")

        count = stats_store.compute_selectivity(doc_store, scope, "default")
        assert count == 1

        priors = stats_store.get_priors(scope, metric_families=["expansion.selectivity"])
        sel = priors["expansion.selectivity"]["speaker"]
        assert sel["selectivity"] == pytest.approx(2 / 3, abs=0.01)
        assert sel["sources_total"] == 3
        assert sel["sources_with_hits"] == 2

    def test_compute_facet_cardinality(self, stores):
        doc_store, stats_store = stores
        scope = build_scope_key()

        self._insert_doc(doc_store, "doc1", {"topic": "ai", "level": "intro"})
        self._insert_doc(doc_store, "doc2", {"topic": "ml", "level": "intro"})
        self._insert_doc(doc_store, "doc3", {"topic": "ai", "level": "advanced"})

        count = stats_store.compute_facet_cardinality(doc_store, scope, "default")
        assert count == 2  # topic, level

        priors = stats_store.get_priors(scope, metric_families=["facet.cardinality"])
        topic = priors["facet.cardinality"]["topic"]
        assert topic["distinct_values"] == 2  # ai, ml
        assert topic["top_values"][0][0] == "ai"  # ai appears twice
        assert topic["top_values"][0][1] == 2
        assert topic["entropy"] > 0

    def test_compute_facet_cardinality_empty(self, stores):
        doc_store, stats_store = stores
        scope = build_scope_key()
        count = stats_store.compute_facet_cardinality(doc_store, scope, "default")
        assert count == 0


# ---------------------------------------------------------------------------
# Drain
# ---------------------------------------------------------------------------

class TestDrain:

    def _insert_doc(self, doc_store, doc_id, tags=None):
        doc_store._execute(
            """INSERT OR REPLACE INTO documents
               (id, collection, summary, tags_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (doc_id, "default", "test", json.dumps(tags or {}),
             "2025-01-01T00:00:00", "2025-01-01T00:00:00"),
        )
        doc_store._conn.commit()

    def test_drain_auto_rebuilds_on_first_call(self, stores):
        """First drain triggers a full rebuild when no watermark exists."""
        doc_store, stats_store = stores

        self._insert_doc(doc_store, "doc1", {"topic": "ai"})
        self._insert_doc(doc_store, "doc2", {"topic": "ml"})
        doc_store.upsert_edge("default", "doc1", "speaker", "alice", "said",
                              "2025-01-01T00:00:00")

        assert stats_store.needs_rebuild("default")

        result = stats_store.drain_outbox(doc_store, "default")
        assert result.get("rebuilt") is True

        # After rebuild, stats exist and outbox is cleared
        assert not stats_store.needs_rebuild("default")
        assert doc_store.outbox_depth() == 0

        scope = build_scope_key()
        priors = stats_store.get_priors(scope)
        assert "expansion.fanout" in priors
        assert "facet.cardinality" in priors

    def test_drain_processes_outbox_after_rebuild(self, stores):
        """After initial rebuild, subsequent drains process outbox incrementally."""
        doc_store, stats_store = stores

        self._insert_doc(doc_store, "doc1", {"topic": "ai"})

        # First drain triggers rebuild
        stats_store.drain_outbox(doc_store, "default")

        # New mutation
        self._insert_doc(doc_store, "doc2", {"topic": "ml"})
        depth = doc_store.outbox_depth()
        assert depth > 0

        result = stats_store.drain_outbox(doc_store, "default")
        assert result["processed"] > 0
        assert result["failed"] == 0
        assert doc_store.outbox_depth() == 0

    def test_drain_empty_outbox(self, stores):
        doc_store, stats_store = stores
        # First call triggers rebuild (even with no data), then outbox is empty
        result = stats_store.drain_outbox(doc_store, "default")
        assert result.get("rebuilt") is True
        # Second call: no outbox items
        result2 = stats_store.drain_outbox(doc_store, "default")
        assert result2["processed"] == 0
        assert result2["failed"] == 0

    def test_drain_idempotent(self, stores):
        doc_store, stats_store = stores

        self._insert_doc(doc_store, "doc1", {"topic": "ai"})

        result1 = stats_store.drain_outbox(doc_store, "default")
        result2 = stats_store.drain_outbox(doc_store, "default")

        # Second drain has nothing to process
        assert result2["processed"] == 0

        # Stats should be the same
        scope = build_scope_key()
        priors = stats_store.get_priors(scope)
        assert "facet.cardinality" in priors


# ---------------------------------------------------------------------------
# Rebuild
# ---------------------------------------------------------------------------

class TestRebuild:

    def _insert_doc(self, doc_store, doc_id, tags=None):
        doc_store._execute(
            """INSERT OR REPLACE INTO documents
               (id, collection, summary, tags_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (doc_id, "default", "test", json.dumps(tags or {}),
             "2025-01-01T00:00:00", "2025-01-01T00:00:00"),
        )
        doc_store._conn.commit()

    def test_rebuild_computes_all_metrics(self, stores):
        doc_store, stats_store = stores

        self._insert_doc(doc_store, "doc1", {"topic": "ai"})
        self._insert_doc(doc_store, "doc2", {"topic": "ml"})
        doc_store.upsert_edge("default", "doc1", "speaker", "alice", "said",
                              "2025-01-01T00:00:00")

        result = stats_store.rebuild(doc_store, "default")
        assert result["expansion.fanout"] >= 1
        assert result["expansion.selectivity"] >= 1
        assert result["facet.cardinality"] >= 1

    def test_rebuild_clears_outbox(self, stores):
        doc_store, stats_store = stores

        self._insert_doc(doc_store, "doc1", {"topic": "ai"})
        assert doc_store.outbox_depth() > 0

        stats_store.rebuild(doc_store, "default")
        assert doc_store.outbox_depth() == 0

    def test_rebuild_matches_incremental(self, stores):
        doc_store, stats_store = stores
        scope = build_scope_key()

        self._insert_doc(doc_store, "doc1", {"topic": "ai"})
        self._insert_doc(doc_store, "doc2", {"topic": "ml"})
        doc_store.upsert_edge("default", "doc1", "speaker", "alice", "said",
                              "2025-01-01T00:00:00")

        # Incremental via drain
        stats_store.drain_outbox(doc_store, "default")
        priors_incremental = stats_store.get_priors(scope)

        # Full rebuild (into a fresh store)
        stats_store2 = PlannerStatsStore(
            Path(str(stats_store._db_path) + ".rebuild")
        )
        stats_store2.rebuild(doc_store, "default")
        priors_rebuild = stats_store2.get_priors(scope)
        stats_store2.close()

        # Same metric families present
        assert set(priors_incremental.keys()) == set(priors_rebuild.keys())

        # Same subject keys per family
        for family in priors_incremental:
            assert set(priors_incremental[family].keys()) == set(priors_rebuild[family].keys())


# ---------------------------------------------------------------------------
# Staleness
# ---------------------------------------------------------------------------

class TestStaleness:

    def test_no_stats_is_fallback(self, stats_store):
        scope = build_scope_key()
        staleness = stats_store.get_staleness(scope)
        assert staleness["fallback_mode"] is True
        assert staleness["stats_age_s"] is None

    def test_fresh_stats_not_fallback(self, stats_store):
        scope = build_scope_key()
        stats_store.upsert_stat("expansion.fanout", scope, "speaker",
                                {"mean": 3.7}, 100)
        staleness = stats_store.get_staleness(scope)
        assert staleness["fallback_mode"] is False
        assert staleness["stats_age_s"] is not None
        assert staleness["stats_age_s"] < 10  # just created


# ---------------------------------------------------------------------------
# get_planner_priors shape
# ---------------------------------------------------------------------------

class TestPlannerPriorsShape:

    def test_priors_shape(self, stores):
        doc_store, stats_store = stores
        scope = build_scope_key()

        stats_store.upsert_stat("expansion.fanout", scope, "speaker",
                                {"mean": 3.7, "p50": 2.0, "p90": 9.0, "max": 41}, 100)
        stats_store.upsert_stat("expansion.selectivity", scope, "speaker",
                                {"selectivity": 0.42, "sources_total": 100,
                                 "sources_with_hits": 42}, 100)
        stats_store.upsert_stat("facet.cardinality", scope, "topic",
                                {"distinct_values": 38, "top_values": [["v1", 412]],
                                 "entropy": 2.31}, 412)

        priors = stats_store.get_priors(scope)

        # Verify shape matches spec section 5.2
        assert "expansion.fanout" in priors
        assert "expansion.selectivity" in priors
        assert "facet.cardinality" in priors

        fanout = priors["expansion.fanout"]["speaker"]
        assert set(fanout.keys()) >= {"mean", "p50", "p90", "max"}

        sel = priors["expansion.selectivity"]["speaker"]
        assert set(sel.keys()) >= {"selectivity", "sources_total", "sources_with_hits"}

        card = priors["facet.cardinality"]["topic"]
        assert set(card.keys()) >= {"distinct_values", "top_values", "entropy"}


class TestKeeperPlannerPriorsAPI:

    def test_keeper_returns_minimal_priors_shape(self, mock_providers, tmp_path):
        kp = Keeper(store_path=tmp_path)
        try:
            # Seed some stats directly; API should map internal metric names
            # to the minimal external shape.
            scope = build_scope_key()
            kp._planner_stats.upsert_stat(
                "expansion.fanout", scope, "speaker",
                {"mean": 3.2, "p50": 2.0, "p90": 6.0, "max": 12}, 50,
            )
            kp._planner_stats.upsert_stat(
                "expansion.selectivity", scope, "speaker",
                {"selectivity": 0.41, "sources_total": 100, "sources_with_hits": 41}, 100,
            )
            kp._planner_stats.upsert_stat(
                "facet.cardinality", scope, "topic",
                {"distinct_values": 9, "top_values": [["x", 4]], "entropy": 1.2}, 20,
            )

            out = kp.get_planner_priors(scope_key=scope)
            assert "planner_priors" in out
            assert "staleness" in out
            assert set(out["planner_priors"].keys()) == {"fanout", "selectivity", "cardinality"}
            assert "speaker" in out["planner_priors"]["fanout"]
            assert "speaker" in out["planner_priors"]["selectivity"]
            assert "topic" in out["planner_priors"]["cardinality"]
        finally:
            kp.close()
