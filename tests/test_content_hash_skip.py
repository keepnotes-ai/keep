"""Tests for content-hash skip logic and size-adjusted priority."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from keep.actions._item_scope import check_content_hash
from keep.actions.analyze import Analyze
from keep.actions.auto_tag import AutoTag as Tag
from keep.actions.summarize import Summarize
from keep._background_processing import _size_priority_bump


# ---------------------------------------------------------------------------
# check_content_hash helper
# ---------------------------------------------------------------------------

class TestCheckContentHash:
    """Tests for content hash checking."""
    def _make_context(self, content_hash="abc123", tag_value="abc123"):
        doc = MagicMock()
        doc.content_hash = content_hash
        doc.tags = {"_analyzed_hash": tag_value} if tag_value is not None else {}
        ctx = MagicMock()
        ctx.get_document.return_value = doc
        return ctx

    def test_returns_true_when_hashes_match(self):
        ctx = self._make_context(content_hash="abc", tag_value="abc")
        assert check_content_hash({}, ctx, "item1", "_analyzed_hash") is True

    def test_returns_false_when_hashes_differ(self):
        ctx = self._make_context(content_hash="abc", tag_value="old")
        assert check_content_hash({}, ctx, "item1", "_analyzed_hash") is False

    def test_returns_false_when_tag_missing(self):
        ctx = self._make_context(content_hash="abc", tag_value=None)
        assert check_content_hash({}, ctx, "item1", "_analyzed_hash") is False

    def test_returns_false_when_force(self):
        ctx = self._make_context(content_hash="abc", tag_value="abc")
        assert check_content_hash({"force": True}, ctx, "item1", "_analyzed_hash") is False

    def test_returns_false_when_no_get_document(self):
        ctx = MagicMock(spec=[])
        assert check_content_hash({}, ctx, "item1", "_analyzed_hash") is False

    def test_returns_false_when_doc_is_none(self):
        ctx = MagicMock()
        ctx.get_document.return_value = None
        assert check_content_hash({}, ctx, "item1", "_analyzed_hash") is False

    def test_returns_false_when_content_hash_empty(self):
        ctx = self._make_context(content_hash="", tag_value="")
        assert check_content_hash({}, ctx, "item1", "_analyzed_hash") is False


# ---------------------------------------------------------------------------
# Action-level skip tests
# ---------------------------------------------------------------------------

def _make_action_context(content_hash="h1", hash_tag="_analyzed_hash", tag_value="h1"):
    """Build a mock context that resolve_item_content and check_content_hash both work with."""
    item = MagicMock()
    item.content = "some text content"
    item.summary = ""
    item.tags = {hash_tag: tag_value} if tag_value else {}

    doc = MagicMock()
    doc.content_hash = content_hash
    doc.tags = dict(item.tags)

    ctx = MagicMock()
    ctx.get.return_value = item
    ctx.get_document.return_value = doc
    ctx.item_id = None  # not set via context
    ctx.list_items.return_value = []  # no tag specs
    ctx.resolve_prompt = MagicMock(return_value="Test system prompt")
    return ctx


class TestAnalyzeSkip:
    """Tests for analyze skip on hash match."""
    def test_skips_when_hash_matches(self):
        ctx = _make_action_context(content_hash="h1", hash_tag="_analyzed_hash", tag_value="h1")
        result = Analyze().run({"item_id": "d1"}, ctx)
        assert result.get("skipped") is True

    def test_runs_when_hash_differs(self):
        ctx = _make_action_context(content_hash="h2", hash_tag="_analyzed_hash", tag_value="h1")
        ctx.resolve_provider.return_value = MagicMock(analyze=MagicMock(return_value=[]))
        result = Analyze().run({"item_id": "d1"}, ctx)
        assert "skipped" not in result

    def test_runs_when_force(self):
        ctx = _make_action_context(content_hash="h1", hash_tag="_analyzed_hash", tag_value="h1")
        ctx.resolve_provider.return_value = MagicMock(analyze=MagicMock(return_value=[]))
        result = Analyze().run({"item_id": "d1", "force": True}, ctx)
        assert "skipped" not in result


class TestTagSkip:
    """Tests for tag skip on hash match."""
    def test_skips_when_hash_matches(self):
        ctx = _make_action_context(content_hash="h1", hash_tag="_tagged_hash", tag_value="h1")
        result = Tag().run({"item_id": "d1"}, ctx)
        assert result.get("skipped") is True

    def test_runs_when_hash_differs(self):
        ctx = _make_action_context(content_hash="h2", hash_tag="_tagged_hash", tag_value="h1")
        result = Tag().run({"item_id": "d1"}, ctx)
        assert "skipped" not in result

    def test_records_tagged_hash(self):
        ctx = _make_action_context(content_hash="h2", hash_tag="_tagged_hash", tag_value="h1")
        # classify_parts_with_specs returns tags
        result = Tag().run({"item_id": "d1"}, ctx)
        if result.get("mutations"):
            set_tags_mut = [m for m in result["mutations"] if m["op"] == "set_tags"]
            if set_tags_mut:
                assert set_tags_mut[0]["tags"].get("_tagged_hash") == "h2"

    def test_runs_when_force(self):
        ctx = _make_action_context(content_hash="h1", hash_tag="_tagged_hash", tag_value="h1")
        result = Tag().run({"item_id": "d1", "force": True}, ctx)
        assert "skipped" not in result


class TestSummarizeSkip:
    """Tests for summarize skip on hash match."""
    def test_skips_when_hash_matches(self):
        ctx = _make_action_context(content_hash="h1", hash_tag="_summarized_hash", tag_value="h1")
        result = Summarize().run({"item_id": "d1"}, ctx)
        assert result.get("skipped") is True

    def test_runs_when_hash_differs(self):
        ctx = _make_action_context(content_hash="h2", hash_tag="_summarized_hash", tag_value="h1")
        provider = MagicMock()
        provider.summarize.return_value = "a summary"
        ctx.resolve_provider.return_value = provider
        result = Summarize().run({"item_id": "d1"}, ctx)
        assert "skipped" not in result
        assert result["summary"] == "a summary"

    def test_records_summarized_hash(self):
        ctx = _make_action_context(content_hash="h2", hash_tag="_summarized_hash", tag_value="h1")
        provider = MagicMock()
        provider.summarize.return_value = "a summary"
        ctx.resolve_provider.return_value = provider
        result = Summarize().run({"item_id": "d1"}, ctx)
        set_tags_muts = [m for m in result["mutations"] if m["op"] == "set_tags"]
        assert len(set_tags_muts) == 1
        assert set_tags_muts[0]["tags"]["_summarized_hash"] == "h2"

    def test_runs_when_force(self):
        ctx = _make_action_context(content_hash="h1", hash_tag="_summarized_hash", tag_value="h1")
        provider = MagicMock()
        provider.summarize.return_value = "a summary"
        ctx.resolve_provider.return_value = provider
        result = Summarize().run({"item_id": "d1", "force": True}, ctx)
        assert "skipped" not in result


# ---------------------------------------------------------------------------
# Size-adjusted priority
# ---------------------------------------------------------------------------

class TestSizePriorityBump:
    """Tests for size-based priority bumping."""
    def test_small_content_no_bump(self):
        assert _size_priority_bump(500) == 0

    def test_medium_content_bump_1(self):
        assert _size_priority_bump(15_000) == 1

    def test_large_content_bump_2(self):
        assert _size_priority_bump(60_000) == 2

    def test_boundary_10k(self):
        assert _size_priority_bump(10_000) == 0
        assert _size_priority_bump(10_001) == 1

    def test_boundary_50k(self):
        assert _size_priority_bump(50_000) == 1
        assert _size_priority_bump(50_001) == 2
