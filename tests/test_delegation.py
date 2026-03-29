"""Integration tests for task delegation in process_pending()."""

import tempfile
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from keep.pending_summaries import PendingSummaryQueue, PendingSummary
from keep.processors import DELEGATABLE_TASK_TYPES
from keep.task_client import TaskClient, TaskClientError


class TestDelegateTask:
    """Test _delegate_task method on Keeper."""

    def test_delegates_summarize_task(self, mock_providers, tmp_path):
        """Summarize tasks are delegated when TaskClient is available."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        # Set up a real pending queue for this test
        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        # Mock TaskClient
        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.submit.return_value = "remote-task-001"
        kp._task_client = mock_tc

        # Enqueue and dequeue a summarize task
        queue.enqueue("doc1", "default", "Long content to summarize")
        items = queue.dequeue(limit=1)
        assert len(items) == 1

        # Call _delegate_task
        kp._delegate_task(items[0])

        # Should have submitted to remote
        mock_tc.submit.assert_called_once()
        call_args = mock_tc.submit.call_args
        assert call_args[0][0] == "summarize"
        assert call_args[0][1] == "Long content to summarize"
        assert call_args[1]["action_name"] == "summarize"
        assert call_args[1]["action_params"]["item_id"] == "doc1"

        # Should be marked as delegated
        delegated = queue.list_delegated()
        assert len(delegated) == 1
        assert delegated[0].metadata["_remote_task_id"] == "remote-task-001"

        queue.close()
        kp.close()

    def test_delegates_summarize_with_prepared_action_metadata(self, mock_providers, tmp_path):
        """Delegation should use summarize action preparation for context and prompt."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.submit.return_value = "remote-task-ctx"
        kp._task_client = mock_tc

        kp.put(content="Long content to summarize", id="doc1", tags={"topic": "auth"})
        queue.clear()
        queue.enqueue("doc1", "default", "Long content to summarize")
        items = queue.dequeue(limit=1)

        with (
            patch.object(kp, "_gather_context", return_value="related auth context"),
            patch.object(kp, "_resolve_prompt_doc", return_value="Summarize with auth framing"),
        ):
            kp._delegate_task(items[0])

        mock_tc.submit.assert_called_once()
        call_args = mock_tc.submit.call_args
        assert call_args[0][0] == "summarize"
        assert call_args[0][1] == "Long content to summarize"
        meta = call_args[1].get("metadata") or call_args[0][2]
        assert meta["context"] == "related auth context"
        assert meta["system_prompt_override"] == "Summarize with auth framing"
        assert call_args[1]["action_name"] == "summarize"
        assert call_args[1]["action_params"]["context"] == "related auth context"
        assert call_args[1]["action_params"]["system_prompt"] == "Summarize with auth framing"

        queue.close()
        kp.close()

    def test_process_pending_delegates_instead_of_local(self, mock_providers, tmp_path):
        """process_pending delegates tasks when TaskClient is available."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.submit.return_value = "remote-task-002"
        kp._task_client = mock_tc

        # Enqueue a summarize task
        queue.enqueue("doc1", "default", "content to summarize")

        result = kp.process_pending(limit=10)

        assert result["delegated"] == 1
        assert result["processed"] == 0  # Not processed locally

        queue.close()
        kp.close()

    def test_local_only_tasks_not_delegated(self, mock_providers, tmp_path):
        """embed/reindex tasks are never delegated."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        mock_tc = MagicMock(spec=TaskClient)
        kp._task_client = mock_tc

        # Enqueue an embed task (local-only)
        queue.enqueue("doc1", "default", "content", task_type="embed")

        result = kp.process_pending(limit=10)

        # embed should not be delegated
        mock_tc.submit.assert_not_called()
        assert result["delegated"] == 0

        queue.close()
        kp.close()

    def test_local_only_metadata_prevents_delegation(self, mock_providers, tmp_path):
        """_local_only metadata flag prevents delegation."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        mock_tc = MagicMock(spec=TaskClient)
        kp._task_client = mock_tc

        # Enqueue with _local_only flag
        queue.enqueue(
            "doc1", "default", "content",
            task_type="summarize",
            metadata={"_local_only": True},
        )

        result = kp.process_pending(limit=10)

        mock_tc.submit.assert_not_called()
        assert result["delegated"] == 0

        queue.close()
        kp.close()

    def test_fallback_to_local_on_delegation_error(self, mock_providers, tmp_path):
        """When delegation fails, falls back to local processing."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.submit.side_effect = TaskClientError("Service unavailable")
        kp._task_client = mock_tc

        # Put a doc in the store so summarize can find it
        kp.put(content="This is test content for summarization", id="doc1")

        # The enqueue from put should create a summarize task
        # Clear and re-enqueue to control the content
        queue.clear()
        queue.enqueue("doc1", "default", "This is test content for summarization")

        result = kp.process_pending(limit=10)

        # Delegation failed, so it should fall back to local
        assert result["delegated"] == 0
        assert result["processed"] >= 1

        queue.close()
        kp.close()


class TestAnalyzeDelegation:
    """Test analyze task delegation."""

    def test_delegates_analyze_task(self, mock_providers, tmp_path):
        """Analyze tasks are delegated when TaskClient is available."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.submit.return_value = "remote-analyze-001"
        kp._task_client = mock_tc

        # Create a doc to analyze
        kp.put(content="This is a long document with multiple sections about different topics.", id="doc1")

        # Clear auto-enqueued tasks and enqueue analyze
        queue.clear()
        queue.enqueue("doc1", "default", "", task_type="analyze",
                       metadata={"tags": ["topic"]})

        result = kp.process_pending(limit=10)

        assert result["delegated"] == 1
        # Submit should include chunks in metadata
        call_args = mock_tc.submit.call_args
        assert call_args[0][0] == "analyze"
        meta = call_args[1].get("metadata") or call_args[0][2]
        assert "chunks" in meta
        assert call_args[1]["action_name"] == "analyze"
        assert "chunks" in call_args[1]["action_params"]

        queue.close()
        kp.close()

    def test_poll_applies_analyze_result(self, mock_providers, tmp_path):
        """Completed analyze delegation applies action-output mutations."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        # Create a doc
        kp.put(content="Content to decompose into parts", id="doc1")

        # Simulate delegated analyze task
        queue.enqueue("doc1", "default", "", task_type="analyze")
        queue.dequeue(limit=1)
        queue.mark_delegated("doc1", "default", "analyze", "rt-analyze-001")

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.poll.return_value = {
            "status": "completed",
            "output": {
                "parts": [
                    {"summary": "Part 1: Introduction", "content": "Intro text", "tags": {}},
                    {"summary": "Part 2: Details", "content": "Detail text", "tags": {"topic": "tech"}},
                ],
                "mutations": [
                    {"op": "delete_prefix", "prefix": "doc1@p"},
                    {
                        "op": "put_item",
                        "id": "doc1@p1",
                        "content": "Intro text",
                        "summary": "Part 1: Introduction",
                        "tags": {"_base_id": "doc1", "_part_num": "1"},
                        "queue_background_tasks": False,
                    },
                    {
                        "op": "put_item",
                        "id": "doc1@p2",
                        "content": "Detail text",
                        "summary": "Part 2: Details",
                        "tags": {
                            "_base_id": "doc1",
                            "_part_num": "2",
                            "topic": "tech",
                        },
                        "queue_background_tasks": False,
                    },
                ],
            },
            "result": None,
            "error": None,
            "task_type": "analyze",
        }
        kp._task_client = mock_tc

        result = {"processed": 0, "failed": 0}
        kp._poll_delegated(result)

        assert result["processed"] == 1

        # Parts should be in the store
        parts = kp.list_parts("doc1")
        assert len(parts) == 2
        assert parts[0].summary == "Part 1: Introduction"
        assert parts[1].summary == "Part 2: Details"

        queue.close()
        kp.close()

    def test_poll_translates_legacy_result(self, mock_providers, tmp_path):
        """Legacy processor-shaped delegated results still apply during rollout."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        kp.put(content="Original content", id="doc1")

        queue.enqueue("doc1", "default", "content to summarize")
        queue.dequeue(limit=1)
        queue.mark_delegated("doc1", "default", "summarize", "rt-legacy-001")

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.poll.return_value = {
            "status": "completed",
            "output": None,
            "result": {"summary": "Legacy remote summary"},
            "error": None,
            "task_type": "summarize",
        }
        kp._task_client = mock_tc

        result = {"processed": 0, "failed": 0}
        kp._poll_delegated(result)

        assert result["processed"] == 1
        assert kp.get("doc1").summary == "Legacy remote summary"

        queue.close()
        kp.close()

    def test_analyze_fallback_to_local(self, mock_providers, tmp_path):
        """When analyze delegation fails, falls back to local processing."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.submit.side_effect = TaskClientError("Service unavailable")
        kp._task_client = mock_tc

        # Create a doc with enough content
        kp.put(content="A long document with enough content to be analyzed. " * 10, id="doc1")

        queue.clear()
        queue.enqueue("doc1", "default", "", task_type="analyze")

        result = kp.process_pending(limit=10)

        # Delegation failed, should fall back to local
        assert result["delegated"] == 0

        queue.close()
        kp.close()


class TestPollDelegated:
    """Test _poll_delegated method on Keeper."""

    def test_polls_and_applies_completed(self, mock_providers, tmp_path):
        """Completed delegated tasks are applied to the store."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        # Create a doc in store first
        kp.put(content="Original content", id="doc1")

        # Simulate a delegated task
        queue.enqueue("doc1", "default", "content to summarize")
        items = queue.dequeue(limit=1)
        queue.mark_delegated("doc1", "default", "summarize", "rt-001")

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.poll.return_value = {
            "status": "completed",
            "output": {
                "summary": "Remote summary result",
                "mutations": [
                    {"op": "set_summary", "target": "doc1", "summary": "Remote summary result"},
                ],
            },
            "result": None,
            "error": None,
            "task_type": "summarize",
        }
        kp._task_client = mock_tc

        result = {"processed": 0, "failed": 0}
        kp._poll_delegated(result)

        assert result["processed"] == 1
        mock_tc.poll.assert_called_once_with("rt-001")
        mock_tc.acknowledge.assert_called_once_with("rt-001")

        # Task should be removed from queue
        assert queue.count_delegated() == 0

        queue.close()
        kp.close()

    def test_polls_failed_task(self, mock_providers, tmp_path):
        """Failed delegated tasks are returned to pending via fail()."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        queue.enqueue("doc1", "default", "content")
        queue.dequeue(limit=1)
        queue.mark_delegated("doc1", "default", "summarize", "rt-002")

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.poll.return_value = {
            "status": "failed",
            "result": None,
            "error": "Model crashed",
            "task_type": "summarize",
        }
        kp._task_client = mock_tc

        result = {"processed": 0, "failed": 0}
        kp._poll_delegated(result)

        assert result["failed"] == 1
        assert queue.count_delegated() == 0
        # Should be back in pending (via fail)
        assert queue.count() == 1

        queue.close()
        kp.close()

    def test_skips_still_processing(self, mock_providers, tmp_path):
        """Still-processing tasks are left alone."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        queue.enqueue("doc1", "default", "content")
        queue.dequeue(limit=1)
        queue.mark_delegated("doc1", "default", "summarize", "rt-003")

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.poll.return_value = {
            "status": "processing",
            "result": None,
            "error": None,
            "task_type": "summarize",
        }
        kp._task_client = mock_tc

        result = {"processed": 0, "failed": 0}
        kp._poll_delegated(result)

        assert result["processed"] == 0
        assert result["failed"] == 0
        # Still delegated
        assert queue.count_delegated() == 1

        queue.close()
        kp.close()

    def test_poll_error_skips_gracefully(self, mock_providers, tmp_path):
        """TaskClientError during poll doesn't crash the loop."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        queue.enqueue("doc1", "default", "content")
        queue.dequeue(limit=1)
        queue.mark_delegated("doc1", "default", "summarize", "rt-004")

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.poll.side_effect = TaskClientError("Network error")
        kp._task_client = mock_tc

        result = {"processed": 0, "failed": 0}
        kp._poll_delegated(result)

        # No crashes, task still delegated (not stale yet)
        assert result["processed"] == 0
        assert result["failed"] == 0
        assert queue.count_delegated() == 1

        queue.close()
        kp.close()

    def test_not_found_reverts_to_pending(self, mock_providers, tmp_path):
        """Task that disappeared from server is reverted to pending."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        queue.enqueue("doc1", "default", "content")
        queue.dequeue(limit=1)
        queue.mark_delegated("doc1", "default", "summarize", "rt-gone")

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.poll.return_value = {
            "status": "not_found",
            "result": None,
            "error": "Task not found",
            "task_type": "summarize",
        }
        kp._task_client = mock_tc

        result = {"processed": 0, "failed": 0}
        kp._poll_delegated(result)

        assert result["failed"] == 1
        assert queue.count_delegated() == 0
        # Should be back in pending for local processing
        assert queue.count() == 1

        queue.close()
        kp.close()

    def test_stale_delegation_reverts(self, mock_providers, tmp_path):
        """Delegation older than threshold is reverted to pending."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        queue.enqueue("doc1", "default", "content")
        queue.dequeue(limit=1)
        queue.mark_delegated("doc1", "default", "summarize", "rt-stale")

        # Backdate the delegated_at to 2 hours ago
        two_hours_ago = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        queue._conn.execute(
            "UPDATE pending_summaries SET delegated_at = ? WHERE id = ?",
            (two_hours_ago, "doc1"),
        )
        queue._conn.commit()

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.poll.return_value = {
            "status": "processing",  # still processing but too old
            "result": None,
            "error": None,
            "task_type": "summarize",
        }
        kp._task_client = mock_tc

        result = {"processed": 0, "failed": 0}
        kp._poll_delegated(result)

        assert result["failed"] == 1
        assert queue.count_delegated() == 0
        assert queue.count() == 1  # back in pending

        queue.close()
        kp.close()

    def test_fresh_delegation_not_reverted(self, mock_providers, tmp_path):
        """Recent delegation that's still processing is left alone."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        queue.enqueue("doc1", "default", "content")
        queue.dequeue(limit=1)
        queue.mark_delegated("doc1", "default", "summarize", "rt-fresh")

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.poll.return_value = {
            "status": "processing",
            "result": None,
            "error": None,
            "task_type": "summarize",
        }
        kp._task_client = mock_tc

        result = {"processed": 0, "failed": 0}
        kp._poll_delegated(result)

        # Fresh delegation should stay delegated
        assert result["processed"] == 0
        assert result["failed"] == 0
        assert queue.count_delegated() == 1

        queue.close()
        kp.close()

    def test_stale_poll_error_reverts(self, mock_providers, tmp_path):
        """Poll failure + stale delegation = revert to local."""
        from keep import Keeper

        kp = Keeper(store_path=tmp_path)

        queue = PendingSummaryQueue(tmp_path / "pending.db")
        kp._pending_queue = queue

        queue.enqueue("doc1", "default", "content")
        queue.dequeue(limit=1)
        queue.mark_delegated("doc1", "default", "summarize", "rt-unreachable")

        # Backdate
        two_hours_ago = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        queue._conn.execute(
            "UPDATE pending_summaries SET delegated_at = ? WHERE id = ?",
            (two_hours_ago, "doc1"),
        )
        queue._conn.commit()

        mock_tc = MagicMock(spec=TaskClient)
        mock_tc.poll.side_effect = TaskClientError("Connection refused")
        kp._task_client = mock_tc

        result = {"processed": 0, "failed": 0}
        kp._poll_delegated(result)

        # Stale + unreachable = revert
        assert result["failed"] == 1
        assert queue.count_delegated() == 0
        assert queue.count() == 1

        queue.close()
        kp.close()
