"""Tests for work_processor and task_workflows — the background task execution path.

Covers:
- process_work_batch: claim → execute → complete/fail lifecycle
- _execute_work_item: input parsing, outcome propagation
- Supersede skipping
- Error handling and stats
- run_local_task dispatch to individual workflows
- Tag.run_task, Analyze.run_task skip conditions
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from keep.work_queue import WorkQueue
from keep.work_processor import process_work_batch, _execute_work_item
from keep.task_workflows import (
    TaskRequest,
    TaskRunResult,
    run_local_task,
)
from keep.actions.analyze import Analyze
from keep.actions.tag import Tag


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def queue(tmp_path):
    q = WorkQueue(tmp_path / "work.db")
    yield q
    q.close()


@pytest.fixture
def mock_keeper():
    """Minimal mock Keeper for work processor tests."""
    kp = MagicMock()
    kp._resolve_doc_collection.return_value = "default"
    kp._run_local_task_workflow.return_value = {"status": "applied", "details": {}}
    return kp


# ---------------------------------------------------------------------------
# process_work_batch
# ---------------------------------------------------------------------------

class TestProcessWorkBatch:
    def test_empty_queue_returns_zero_stats(self, mock_keeper, queue):
        stats = process_work_batch(mock_keeper, queue, limit=5)
        assert stats["claimed"] == 0
        assert stats["processed"] == 0
        assert stats["failed"] == 0

    def test_processes_single_item(self, mock_keeper, queue):
        queue.enqueue("analyze", {"task_type": "analyze", "item_id": "doc1", "content": "x"})
        stats = process_work_batch(mock_keeper, queue, limit=5, worker_id="w1")
        assert stats["claimed"] == 1
        assert stats["processed"] == 1
        assert stats["failed"] == 0
        mock_keeper._run_local_task_workflow.assert_called_once()

    def test_processes_multiple_items(self, mock_keeper, queue):
        for i in range(3):
            queue.enqueue("tag", {"task_type": "tag", "item_id": f"d{i}", "content": "x"})
        stats = process_work_batch(mock_keeper, queue, limit=10, worker_id="w1")
        assert stats["claimed"] == 3
        assert stats["processed"] == 3

    def test_failure_increments_failed_count(self, mock_keeper, queue):
        mock_keeper._run_local_task_workflow.side_effect = RuntimeError("boom")
        queue.enqueue("analyze", {"task_type": "analyze", "item_id": "doc1", "content": "x"})
        stats = process_work_batch(mock_keeper, queue, limit=5, worker_id="w1")
        assert stats["failed"] == 1
        assert stats["processed"] == 0
        assert len(stats["errors"]) == 1
        assert "boom" in stats["errors"][0]["error"]

    def test_mixed_success_and_failure(self, mock_keeper, queue):
        queue.enqueue("tag", {"task_type": "tag", "item_id": "ok1", "content": "x"})
        queue.enqueue("tag", {"task_type": "tag", "item_id": "bad", "content": "x"})
        queue.enqueue("tag", {"task_type": "tag", "item_id": "ok2", "content": "x"})

        call_count = [0]
        def side_effect(**kwargs):
            call_count[0] += 1
            if call_count[0] == 2:
                raise ValueError("bad item")
            return {"status": "applied", "details": {}}
        mock_keeper._run_local_task_workflow.side_effect = side_effect

        stats = process_work_batch(mock_keeper, queue, limit=10, worker_id="w1")
        assert stats["processed"] == 2
        assert stats["failed"] == 1

    def test_superseded_item_completed_not_executed(self, mock_keeper, queue):
        # Enqueue two items with same supersede key.
        # The second enqueue marks the first as 'superseded' at enqueue time,
        # so only the newer item is claimable. Verify the newer one executes.
        queue.enqueue("analyze", {"task_type": "analyze", "item_id": "doc1", "content": "v1"},
                       supersede_key="analyze:doc1")
        queue.enqueue("analyze", {"task_type": "analyze", "item_id": "doc1", "content": "v2"},
                       supersede_key="analyze:doc1")
        stats = process_work_batch(mock_keeper, queue, limit=10, worker_id="w1")
        # Only the newer item is claimed and executed
        assert stats["claimed"] == 1
        assert stats["processed"] == 1
        assert stats["failed"] == 0
        assert mock_keeper._run_local_task_workflow.call_count == 1

    def test_skipped_outcome_logged_as_processed(self, mock_keeper, queue):
        mock_keeper._run_local_task_workflow.return_value = {
            "status": "skipped", "details": {"reason": "content_too_short"},
        }
        queue.enqueue("analyze", {"task_type": "analyze", "item_id": "doc1", "content": "x"})
        stats = process_work_batch(mock_keeper, queue, limit=5, worker_id="w1")
        assert stats["processed"] == 1
        assert stats["failed"] == 0

    def test_limit_respected(self, mock_keeper, queue):
        for i in range(5):
            queue.enqueue("tag", {"task_type": "tag", "item_id": f"d{i}", "content": "x"})
        stats = process_work_batch(mock_keeper, queue, limit=2, worker_id="w1")
        assert stats["claimed"] == 2
        assert stats["processed"] == 2


# ---------------------------------------------------------------------------
# _execute_work_item
# ---------------------------------------------------------------------------

class TestExecuteWorkItem:
    def test_extracts_fields_from_input(self, mock_keeper):
        _execute_work_item(mock_keeper, "analyze", {
            "task_type": "analyze",
            "item_id": "doc1",
            "collection": "coll1",
            "content": "hello",
            "metadata": {"force": True},
        })
        mock_keeper._run_local_task_workflow.assert_called_once_with(
            task_type="analyze",
            item_id="doc1",
            collection="coll1",
            content="hello",
            metadata={"force": True},
        )

    def test_missing_item_id_raises(self, mock_keeper):
        with pytest.raises(ValueError, match="missing item_id"):
            _execute_work_item(mock_keeper, "tag", {"content": "x"})

    def test_falls_back_to_kind_for_task_type(self, mock_keeper):
        _execute_work_item(mock_keeper, "tag", {"item_id": "d1", "content": "x"})
        call_kwargs = mock_keeper._run_local_task_workflow.call_args.kwargs
        assert call_kwargs["task_type"] == "tag"

    def test_falls_back_to_resolve_collection(self, mock_keeper):
        mock_keeper._resolve_doc_collection.return_value = "fallback_coll"
        _execute_work_item(mock_keeper, "tag", {"item_id": "d1", "content": "x"})
        call_kwargs = mock_keeper._run_local_task_workflow.call_args.kwargs
        assert call_kwargs["collection"] == "fallback_coll"

    def test_non_dict_metadata_becomes_empty_dict(self, mock_keeper):
        _execute_work_item(mock_keeper, "tag", {
            "item_id": "d1", "content": "x", "metadata": "not-a-dict",
        })
        call_kwargs = mock_keeper._run_local_task_workflow.call_args.kwargs
        assert call_kwargs["metadata"] == {}

    def test_returns_outcome(self, mock_keeper):
        mock_keeper._run_local_task_workflow.return_value = {"status": "skipped", "details": {"reason": "x"}}
        result = _execute_work_item(mock_keeper, "tag", {"item_id": "d1", "content": "x"})
        assert result["status"] == "skipped"

    def test_none_outcome_defaults_to_applied(self, mock_keeper):
        mock_keeper._run_local_task_workflow.return_value = None
        result = _execute_work_item(mock_keeper, "tag", {"item_id": "d1", "content": "x"})
        assert result["status"] == "applied"


# ---------------------------------------------------------------------------
# run_local_task dispatch
# ---------------------------------------------------------------------------

class TestRunLocalTask:
    def test_unsupported_task_type_raises(self):
        kp = MagicMock()
        req = TaskRequest(task_type="bogus", id="d1", collection="c", content="x")
        with pytest.raises(ValueError, match="unknown action"):
            run_local_task(kp, req)

    def test_dispatches_to_analyze(self):
        kp = MagicMock()
        kp.analyze.return_value = [MagicMock()]
        req = TaskRequest(task_type="analyze", id="d1", collection="c", content="x")
        result = run_local_task(kp, req)
        assert result.status == "applied"
        kp.analyze.assert_called_once()

    def test_dispatches_to_tag(self):
        kp = MagicMock()
        req = TaskRequest(
            task_type="tag", id="d1", collection="c", content="some content",
            metadata={"provider": "noop"},
        )
        with patch("keep.providers.base.get_registry") as mock_reg:
            mock_provider = MagicMock()
            mock_provider.tag.return_value = {"topic": "test"}
            mock_reg.return_value.create_tagging.return_value = mock_provider
            result = run_local_task(kp, req)
        assert result.status == "applied"
        kp.tag.assert_called_once()


# ---------------------------------------------------------------------------
# Individual workflow skip conditions
# ---------------------------------------------------------------------------

class TestAnalyzeSkipConditions:
    def test_empty_parts_returns_skipped(self):
        kp = MagicMock()
        kp.analyze.return_value = []
        req = TaskRequest(task_type="analyze", id="d1", collection="c", content="x")
        result = Analyze().run_task(kp, req)
        assert result.status == "skipped"
        assert result.details["reason"] == "content_too_short"


class TestTagSkipConditions:
    def test_empty_content_returns_skipped(self):
        kp = MagicMock()
        req = TaskRequest(task_type="tag", id="d1", collection="c", content="")
        result = Tag().run_task(kp, req)
        assert result.status == "skipped"
        assert result.details["reason"] == "no_content"

    def test_provider_with_no_tag_method_returns_skipped(self):
        kp = MagicMock()
        req = TaskRequest(
            task_type="tag", id="d1", collection="c", content="hello",
            metadata={"provider": "fake"},
        )
        with patch("keep.providers.base.get_registry") as mock_reg:
            mock_provider = MagicMock(spec=[])  # no tag method
            mock_reg.return_value.create_tagging.return_value = mock_provider
            result = Tag().run_task(kp, req)
        assert result.status == "skipped"
        assert result.details["reason"] == "provider_has_no_tag"

    def test_provider_returning_empty_tags_skipped(self):
        kp = MagicMock()
        req = TaskRequest(
            task_type="tag", id="d1", collection="c", content="hello",
            metadata={"provider": "noop"},
        )
        with patch("keep.providers.base.get_registry") as mock_reg:
            mock_provider = MagicMock()
            mock_provider.tag.return_value = {}
            mock_reg.return_value.create_tagging.return_value = mock_provider
            result = Tag().run_task(kp, req)
        assert result.status == "skipped"
        assert result.details["reason"] == "empty_tags"
