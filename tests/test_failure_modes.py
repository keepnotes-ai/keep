"""Tests for production failure modes.

Covers the scenarios that actually broke in production:
- Embedding dimension mismatches (384d errors)
- Dual-write partial failures (doc store OK, vector store fails)
- process_pending retry/abandon flow (pending stalls)
- Provider timeout propagation
- Concurrent writers on same doc
"""

import threading
import time

import pytest

from keep.api import Keeper
from keep._background_processing import MAX_SUMMARY_ATTEMPTS
from keep.pending_summaries import PendingSummaryQueue


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FailingChromaStore:
    """ChromaDB mock that raises on upsert — simulates partial dual-write."""

    def __init__(self, real_store):
        self._real = real_store
        self.fail_upsert = False
        self.upsert_calls = 0

    def __getattr__(self, name):
        return getattr(self._real, name)

    def upsert(self, *args, **kwargs):
        self.upsert_calls += 1
        if self.fail_upsert:
            raise RuntimeError("ChromaDB simulated failure")
        return self._real.upsert(*args, **kwargs)


class TimeoutEmbeddingProvider:
    """Embedding provider that times out on Nth call."""

    dimension = 384
    model_name = "timeout-model"

    def __init__(self, fail_until: int = 0):
        self.embed_calls = 0
        self.fail_until = fail_until

    def embed(self, text: str) -> list[float]:
        self.embed_calls += 1
        if self.embed_calls <= self.fail_until:
            raise TimeoutError(f"Connection timed out (call {self.embed_calls})")
        return [0.1] * self.dimension

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [self.embed(t) for t in texts]


# ---------------------------------------------------------------------------
# Embedding dimension validation
# ---------------------------------------------------------------------------

class TestEmbeddingDimensionValidation:
    """Tests for _validate_embedding_identity — the 384d production failure."""

    def test_first_use_records_identity(self, mock_providers, tmp_path):
        """First embed should record identity to config."""
        kp = Keeper(store_path=tmp_path)
        embed = mock_providers["embedding"]
        assert embed.dimension == 384

        kp.put("hello", id="doc1")

        assert kp._config.embedding_identity is not None
        assert kp._config.embedding_identity.dimension == 384

        kp.close()

    def test_first_use_sets_store_dimension(self, mock_providers, tmp_path):
        """First embed should set the vector store dimension."""
        kp = Keeper(store_path=tmp_path)
        kp.put("hello", id="doc1")

        assert kp._store.embedding_dimension == 384
        kp.close()

    def test_dimension_change_drops_collection_and_updates(self, mock_providers, tmp_path):
        """Changing embedding dimension should drop collection and update config."""
        kp = Keeper(store_path=tmp_path)

        # First put establishes 384d identity
        kp.put("hello", id="doc1")
        assert kp._config.embedding_identity.dimension == 384

        # Simulate a new provider with different dimension
        new_embed = mock_providers["embedding"]
        new_embed.dimension = 768

        # Directly call validation — this is what _get_embedding_provider calls
        kp._validate_embedding_identity(new_embed)

        # Identity updated to 768d
        assert kp._config.embedding_identity.dimension == 768
        # Store dimension updated
        assert kp._store.embedding_dimension == 768
        kp.close()

    def test_same_dimension_different_model_reindexes(self, mock_providers, tmp_path):
        """Same dimension but different model should update identity and reindex."""
        kp = Keeper(store_path=tmp_path)
        kp.put("hello", id="doc1")

        # Create a provider with different model name (same dimension)
        new_embed = mock_providers["embedding"]
        new_embed.model_name = "different-model"

        kp._validate_embedding_identity(new_embed)

        # Identity should reflect new model
        assert kp._config.embedding_identity.model == "different-model"
        kp.close()


# ---------------------------------------------------------------------------
# Dual-write partial failures
# ---------------------------------------------------------------------------

class TestDualWriteRecovery:
    """Tests for partial failures during dual-write to doc store + vector store."""

    def test_doc_store_ok_vector_store_fails(self, mock_providers, tmp_path):
        """If ChromaDB fails after doc store write, doc should still exist."""
        from tests.conftest import MockChromaStore

        kp = Keeper(store_path=tmp_path)
        embed = mock_providers["embedding"]

        # Warmup
        kp.put("warmup", id="_w")
        kp.delete("_w")
        embed.embed_calls = 0

        # Wrap the vector store to fail on next upsert
        real_store = kp._store
        failing_store = FailingChromaStore(real_store)
        kp._store = failing_store
        failing_store.fail_upsert = True

        # put() should raise (ChromaDB failure propagates)
        with pytest.raises(RuntimeError, match="ChromaDB simulated"):
            kp.put("important content", id="doc1")

        # Doc store should have the record (written first)
        doc = kp._document_store.get("default", "doc1")
        assert doc is not None
        assert doc.summary == "important content"

        # Vector store should NOT have it
        chroma_coll = kp._resolve_chroma_collection()
        assert real_store.get(chroma_coll, "doc1") is None

        kp.close()

    def test_reconcile_detects_drift(self, mock_providers, tmp_path):
        """reconcile() should detect doc store / vector store drift."""
        kp = Keeper(store_path=tmp_path)

        # Create a doc normally
        kp.put("content", id="doc1")

        # Delete from vector store only (simulate partial failure)
        chroma_coll = kp._resolve_chroma_collection()
        kp._store.delete(chroma_coll, "doc1")
        assert kp._store.get(chroma_coll, "doc1") is None

        # Reconcile (detect only) should report missing
        stats = kp.reconcile(fix=False)
        assert stats["missing_from_index"] > 0
        assert "doc1" in stats.get("missing_ids", [])
        kp.close()

    def test_reconcile_fixes_drift(self, mock_providers, tmp_path):
        """reconcile(fix=True) should re-embed docs missing from vector store."""
        kp = Keeper(store_path=tmp_path)
        embed = mock_providers["embedding"]

        # Create a doc normally
        kp.put("content", id="doc1")

        # Delete from vector store only (simulate partial failure)
        chroma_coll = kp._resolve_chroma_collection()
        kp._store.delete(chroma_coll, "doc1")

        # Reconcile with fix — should re-embed
        embed.embed_calls = 0
        stats = kp.reconcile(fix=True)

        assert stats["fixed"] > 0
        # doc1 should be back in vector store
        assert kp._store.get(chroma_coll, "doc1") is not None
        kp.close()


# ---------------------------------------------------------------------------
# process_pending retry and abandon flow
# ---------------------------------------------------------------------------

class TestProcessPendingRetry:
    """Tests for the full retry → backoff → abandon → dead letter flow."""

    def _make_keeper(self, mock_providers, tmp_path):
        """Create a Keeper with real pending queue for retry testing."""
        kp = Keeper(store_path=tmp_path)
        kp._needs_sysdoc_migration = False
        # Replace mock pending queue with real SQLite-backed one
        kp._pending_queue = PendingSummaryQueue(tmp_path / "pending.db")
        embed = mock_providers["embedding"]
        kp._embedding_provider = embed
        embed.embed_calls = 0
        return kp

    def test_failed_item_retried_on_next_process(self, mock_providers, tmp_path):
        """Failed items should be retried (after backoff)."""
        kp = self._make_keeper(mock_providers, tmp_path)

        # Enqueue an embed task
        kp._pending_queue.enqueue("doc1", "default", "content", task_type="embed")
        assert kp._pending_queue.count() == 1

        # Inject an embedding provider that fails on first call
        embed = mock_providers["embedding"]
        original_embed = embed.embed
        call_count = [0]
        def failing_embed(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 1:
                raise RuntimeError("transient embedding failure")
            return original_embed(*args, **kwargs)
        embed.embed = failing_embed

        # Put a doc so embed actually runs
        kp._document_store.upsert("default", "doc1", "raw content", {})
        result = kp.process_pending(limit=10)
        assert result["failed"] == 1

        # Item still in queue (with backoff)
        assert kp._pending_queue.count() == 1

        # Clear backoff for test speed
        kp._pending_queue._conn.execute(
            "UPDATE pending_summaries SET retry_after = NULL"
        )
        kp._pending_queue._conn.commit()

        # Second process — provider succeeds now
        embed.embed = original_embed

        result = kp.process_pending(limit=10)
        assert result["processed"] == 1
        assert kp._pending_queue.count() == 0

        kp.close()

    def test_item_abandoned_after_max_attempts(self, mock_providers, tmp_path):
        """Items should be dead-lettered after MAX_SUMMARY_ATTEMPTS."""
        kp = self._make_keeper(mock_providers, tmp_path)

        kp._pending_queue.enqueue("doc1", "default", "content", task_type="embed")

        # Simulate MAX_SUMMARY_ATTEMPTS worth of failed dequeues
        for _ in range(MAX_SUMMARY_ATTEMPTS):
            kp._pending_queue._conn.execute(
                "UPDATE pending_summaries SET retry_after = NULL"
            )
            kp._pending_queue._conn.commit()
            kp._pending_queue.dequeue(limit=1)
            kp._pending_queue.fail("doc1", "default", task_type="embed",
                                   error="simulated failure")

        # Clear backoff for the final dequeue
        kp._pending_queue._conn.execute(
            "UPDATE pending_summaries SET retry_after = NULL"
        )
        kp._pending_queue._conn.commit()

        # Process — should abandon (attempts >= MAX)
        kp._document_store.upsert("default", "doc1", "content", {})
        result = kp.process_pending(limit=10)
        assert result["abandoned"] == 1
        assert result["processed"] == 0

        # Item is in dead letter, not pending
        assert kp._pending_queue.count() == 0
        failed = kp._pending_queue.list_failed()
        assert len(failed) == 1
        assert failed[0]["id"] == "doc1"

        kp.close()

    def test_deleted_doc_completed_not_failed(self, mock_providers, tmp_path):
        """Deleted docs should be completed (not retried forever)."""
        kp = self._make_keeper(mock_providers, tmp_path)

        # Enqueue embed for a doc, then delete the doc
        kp._pending_queue.enqueue("doc1", "default", "content", task_type="embed")
        # Don't put doc in doc store — simulates deletion

        result = kp.process_pending(limit=10)
        # Should complete (not fail) — deleted doc is a no-op
        assert result["processed"] == 1
        assert result["failed"] == 0
        assert kp._pending_queue.count() == 0

        kp.close()

    def test_backoff_prevents_immediate_retry(self, mock_providers, tmp_path):
        """Failed items should not be dequeued during backoff period."""
        kp = self._make_keeper(mock_providers, tmp_path)

        kp._pending_queue.enqueue("doc1", "default", "content", task_type="embed")
        kp._document_store.upsert("default", "doc1", "content", {})

        # Make embed fail
        kp._embedding_provider = TimeoutEmbeddingProvider(fail_until=99)


        # First process — fails, sets backoff
        result = kp.process_pending(limit=10)
        assert result["failed"] == 1

        # Immediate second process — item should be skipped (backoff active)
        result = kp.process_pending(limit=10)
        assert result["processed"] == 0
        assert result["failed"] == 0

        kp.close()


# ---------------------------------------------------------------------------
# Provider timeout propagation
# ---------------------------------------------------------------------------

class TestProviderTimeouts:
    """Tests for timeout/error propagation from providers to pending queue."""

    def test_embed_timeout_marks_item_failed(self, mock_providers, tmp_path):
        """TimeoutError from embed() should be caught and item marked failed."""
        kp = Keeper(store_path=tmp_path)
        kp._needs_sysdoc_migration = False
        kp._pending_queue = PendingSummaryQueue(tmp_path / "pending.db")

        kp._pending_queue.enqueue("doc1", "default", "content", task_type="embed")
        kp._document_store.upsert("default", "doc1", "content", {})

        # Inject timeout provider
        kp._embedding_provider = TimeoutEmbeddingProvider(fail_until=1)


        result = kp.process_pending(limit=10)
        assert result["failed"] == 1
        assert "timed out" in result["errors"][0].lower() or "TimeoutError" in result["errors"][0]

        # Item still pending (will be retried after backoff)
        assert kp._pending_queue.count() == 1
        kp.close()

    def test_provider_error_doesnt_crash_batch(self, mock_providers, tmp_path):
        """One item failing shouldn't prevent others from processing."""
        kp = Keeper(store_path=tmp_path)
        kp._needs_sysdoc_migration = False
        kp._pending_queue = PendingSummaryQueue(tmp_path / "pending.db")

        # Enqueue two embed tasks
        kp._pending_queue.enqueue("doc1", "default", "content1", task_type="embed")
        kp._pending_queue.enqueue("doc2", "default", "content2", task_type="embed")
        kp._document_store.upsert("default", "doc1", "content1", {})
        kp._document_store.upsert("default", "doc2", "content2", {})

        # Fails on first call, succeeds on second
        kp._embedding_provider = TimeoutEmbeddingProvider(fail_until=1)


        result = kp.process_pending(limit=10)
        # One fails, one succeeds
        assert result["failed"] + result["processed"] == 2
        assert result["failed"] >= 1
        kp.close()


# ---------------------------------------------------------------------------
# Concurrent writers
# ---------------------------------------------------------------------------

class TestConcurrentWriters:
    """Tests for concurrent put() calls on same document."""

    def test_concurrent_puts_both_succeed(self, mock_providers, tmp_path):
        """Two threads writing to same ID should both complete without error."""
        kp = Keeper(store_path=tmp_path)
        embed = mock_providers["embedding"]

        # Warmup system docs
        kp.put("warmup", id="_w")
        kp.delete("_w")

        errors = []
        results = [None, None]

        def writer(idx, content):
            try:
                results[idx] = kp.put(content, id="shared-doc")
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=writer, args=(0, "version A content"))
        t2 = threading.Thread(target=writer, args=(1, "version B content"))

        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        # No crashes
        assert not errors, f"Concurrent writes caused errors: {errors}"

        # Doc exists (one version wins)
        doc = kp.get("shared-doc")
        assert doc is not None
        assert doc.summary in ("version A content", "version B content")

        kp.close()

    def test_concurrent_put_and_delete(self, mock_providers, tmp_path):
        """Concurrent put + delete shouldn't crash."""
        kp = Keeper(store_path=tmp_path)

        # Warmup
        kp.put("warmup", id="_w")
        kp.delete("_w")

        errors = []

        def putter():
            try:
                for i in range(5):
                    kp.put(f"content {i}", id="contested-doc")
            except Exception as e:
                errors.append(e)

        def deleter():
            try:
                for _ in range(5):
                    kp.delete("contested-doc")
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=putter)
        t2 = threading.Thread(target=deleter)

        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        # Should not crash regardless of outcome
        assert not errors, f"Concurrent put/delete caused errors: {errors}"
        kp.close()

    def test_concurrent_process_pending(self, mock_providers, tmp_path):
        """Two process_pending calls shouldn't double-process items."""
        kp = Keeper(store_path=tmp_path)
        kp._needs_sysdoc_migration = False
        kp._pending_queue = PendingSummaryQueue(tmp_path / "pending.db")
        embed = mock_providers["embedding"]
        kp._embedding_provider = embed
        embed.embed_calls = 0

        # Enqueue 10 embed tasks
        for i in range(10):
            kp._pending_queue.enqueue(
                f"doc{i}", "default", f"content {i}", task_type="embed"
            )
            kp._document_store.upsert("default", f"doc{i}", f"content {i}", {})

        results = [None, None]

        def processor(idx):
            results[idx] = kp.process_pending(limit=10)

        t1 = threading.Thread(target=processor, args=(0,))
        t2 = threading.Thread(target=processor, args=(1,))

        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)

        # Total processed should be 10 (no double-processing)
        total = (results[0] or {}).get("processed", 0) + (results[1] or {}).get("processed", 0)
        assert total == 10, f"Expected 10 total, got {total}: {results}"

        kp.close()


