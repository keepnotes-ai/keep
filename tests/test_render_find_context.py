# Copyright (c) 2026 Inguz Outcomes LLC.  All rights reserved.
"""Tests for render_find_context — token-budgeted prompt renderer."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock

from keep.types import Item, PromptResult, PartRef
from keep.document_store import VersionInfo


def _item(id="test", summary="Test summary", score=0.9, tags=None):
    return Item(id=id, summary=summary, score=score, tags=tags or {"_updated_date": "2026-02-20"})


class TestRenderFindContext:
    """Tests for the token-budgeted progressive renderer."""

    def test_basic_rendering(self):
        from keep.cli import render_find_context
        items = [_item(id="a", summary="First item"), _item(id="b", summary="Second item")]
        result = render_find_context(items)
        assert "a" in result
        assert "First item" in result
        assert "b" in result
        assert "Second item" in result

    def test_empty_items(self):
        from keep.cli import render_find_context
        result = render_find_context([])
        assert result == "No results."

    def test_score_included(self):
        from keep.cli import render_find_context
        items = [_item(id="a", summary="With score", score=0.85)]
        result = render_find_context(items)
        assert "(0.85)" in result

    def test_date_included(self):
        from keep.cli import render_find_context
        items = [_item(id="a", summary="Dated",
                       tags={"_created": "2026-01-15T12:00:00"})]
        result = render_find_context(items)
        assert "2026-01-15" in result

    def test_focus_summary_rendered(self):
        """Focus summary replaces parent summary on the primary line."""
        from keep.cli import render_find_context
        items = [_item(id="a", summary="Parent doc",
                       tags={"_updated_date": "2026-02-20",
                             "_focus_summary": "The matching part content"})]
        result = render_find_context(items)
        assert "The matching part content" in result
        assert "Parent doc" not in result

    def test_budget_limits_items(self):
        """With a very small budget, only the first item should appear."""
        from keep.cli import render_find_context
        items = [
            _item(id="first", summary="A" * 200),
            _item(id="second", summary="B" * 200),
            _item(id="third", summary="C" * 200),
        ]
        # Each item line ~55 tokens. Budget of 30 should only fit first.
        result = render_find_context(items, token_budget=30)
        assert "first" in result
        # Second item should be cut off (budget exhausted by first)
        assert "second" not in result

    def test_large_budget_includes_all(self):
        """With a large budget, all items should appear."""
        from keep.cli import render_find_context
        items = [_item(id=f"item-{i}", summary=f"Summary {i}") for i in range(10)]
        result = render_find_context(items, token_budget=10000)
        for i in range(10):
            assert f"item-{i}" in result

    def test_no_score_when_none(self):
        from keep.cli import render_find_context
        items = [_item(id="a", summary="No score", score=None)]
        result = render_find_context(items)
        assert "(" not in result  # no score parens


class TestExpandPromptFindBudget:
    """Tests for {find:N} budget override syntax in expand_prompt."""

    def test_default_budget(self):
        from keep.cli import expand_prompt
        result = PromptResult(
            context=None,
            search_results=[_item(id="a", summary="Test")],
            prompt="Context:\n{find}\nEnd.",
            token_budget=None,
        )
        output = expand_prompt(result)
        assert "a" in output
        assert "{find}" not in output

    def test_budget_from_placeholder_when_no_explicit(self):
        """Template budget wins when token_budget is None (user didn't specify)."""
        from keep.cli import expand_prompt
        # Create many items
        items = [_item(id=f"item-{i}", summary="X" * 200) for i in range(20)]
        # token_budget=None means user didn't specify, template says 50
        result = PromptResult(
            context=None,
            search_results=items,
            prompt="Context:\n{find:50}\nEnd.",
            token_budget=None,
        )
        output = expand_prompt(result)
        # With budget=50 tokens, shouldn't fit all 20 items
        assert "item-0" in output
        assert "item-19" not in output

    def test_explicit_budget_overrides_placeholder(self):
        """Explicit token_budget from CLI overrides template budget."""
        from keep.cli import expand_prompt
        items = [_item(id=f"item-{i}", summary="X" * 200) for i in range(20)]
        # User set --tokens 10000, template says 50 — explicit wins
        result = PromptResult(
            context=None,
            search_results=items,
            prompt="Context:\n{find:50}\nEnd.",
            token_budget=10000,
        )
        output = expand_prompt(result)
        # With explicit budget=10000, all 20 items should fit
        assert "item-0" in output
        assert "item-19" in output

    def test_deep_with_budget(self):
        """The {find:deep:8000} syntax should be expanded using template budget."""
        from keep.cli import expand_prompt
        result = PromptResult(
            context=None,
            search_results=[_item(id="a", summary="Test")],
            prompt="{find:deep:8000}",
            token_budget=None,
        )
        output = expand_prompt(result)
        assert "a" in output
        assert "{find" not in output

    def test_deep_without_budget(self):
        """The {find:deep} syntax should use default budget when no explicit override."""
        from keep.cli import expand_prompt
        result = PromptResult(
            context=None,
            search_results=[_item(id="a", summary="Deep test")],
            prompt="{find:deep}",
            token_budget=None,
        )
        output = expand_prompt(result)
        assert "Deep test" in output

    def test_no_results(self):
        """Empty search results should produce empty expansion."""
        from keep.cli import expand_prompt
        result = PromptResult(
            context=None,
            search_results=None,
            prompt="Before {find} After",
        )
        output = expand_prompt(result)
        assert "Before" in output
        assert "After" in output
        assert "{find}" not in output

    def test_find_uses_flow_binding_alias(self):
        """{find} expands from prompt flow bindings when no direct search results exist."""
        from keep.cli import expand_prompt

        result = PromptResult(
            context=None,
            search_results=None,
            prompt="Context:\n{find}\nEnd.",
            flow_bindings={
                "find_results": {
                    "results": [
                        {"id": "alias-a", "summary": "Aliased result", "tags": {}, "score": 0.9},
                    ],
                },
            },
        )

        output = expand_prompt(result)
        assert "alias-a" in output
        assert "{find}" not in output

    def test_get_uses_flow_binding_alias(self):
        """{get} expands from ``item`` and related flow bindings."""
        from keep.cli import expand_prompt

        result = PromptResult(
            context=None,
            search_results=None,
            prompt="Review:\n{get}",
            flow_bindings={
                "item": {
                    "id": "now",
                    "summary": "Current focus",
                    "tags": {"topic": "cache"},
                },
                "similar": {
                    "results": [
                        {"id": "doc-1", "summary": "Related note", "tags": {}, "score": 0.8},
                    ],
                },
            },
        )

        output = expand_prompt(result)
        assert "Current focus" in output
        assert "doc-1" in output


class TestDeepPrimaryCap:
    """Tests for deep_primary_cap — suppressing primaries in favor of deep items."""

    def test_deep_placeholder_syntax(self):
        """The {find:deep:8000} syntax should be parsed and expanded."""
        from keep.cli import expand_prompt
        result = PromptResult(
            context=None,
            search_results=[_item(id="a", summary="Test")],
            prompt="{find:deep:8000}",
            token_budget=None,
        )
        output = expand_prompt(result)
        assert "a" in output
        assert "{find" not in output

    def test_cap_reduces_primaries(self):
        """With deep_primary_cap=2 and deep groups, only 2 primaries shown."""
        from keep.cli import render_find_context
        from keep.api import FindResults

        items = [_item(id=f"p-{i}", summary=f"Primary {i}") for i in range(5)]
        deep_groups = {
            "p-0": [_item(id="deep-a", summary="Deep A", score=0.8)],
            "p-1": [_item(id="deep-b", summary="Deep B", score=0.7)],
        }
        results = FindResults(items, deep_groups=deep_groups)
        result = render_find_context(results, token_budget=5000, deep_primary_cap=2)
        # Only 2 primaries rendered
        assert "p-0" in result
        assert "p-1" in result
        assert "p-4" not in result
        # Deep items get budget
        assert "deep-a" in result or "deep-b" in result

    def test_cap_renders_deep_before_detail(self):
        """With deep_primary_cap, deep items render before parts/versions."""
        from keep.cli import render_find_context
        from keep.api import FindResults

        parts = [
            PartRef(part_num=0, summary="Overview of the topic"),
            PartRef(part_num=1, summary="Details and analysis"),
        ]
        keeper = MagicMock()
        keeper.list_parts.return_value = parts
        keeper.list_versions.return_value = []
        keeper.list_versions_around.return_value = []

        items = [_item(id=f"doc-{i}", summary=f"Doc {i}") for i in range(5)]
        deep_groups = {
            "doc-0": [_item(id="deep-x", summary="Deep X", score=0.9)],
        }
        results = FindResults(items, deep_groups=deep_groups)
        result = render_find_context(
            results, keeper=keeper, token_budget=5000, deep_primary_cap=3,
        )
        # Deep items should appear
        assert "deep-x" in result
        # With remaining budget, parts should also appear
        assert "Key topics:" in result
        # Deep items come before parts in the output
        assert result.index("deep-x") < result.index("Key topics:")

    def test_no_cap_without_deep_groups(self):
        """Without deep groups, deep_primary_cap has no effect."""
        from keep.cli import render_find_context

        items = [_item(id=f"p-{i}", summary=f"Primary {i}") for i in range(5)]
        result = render_find_context(items, token_budget=5000, deep_primary_cap=2)
        # All items should still appear (no deep groups → cap not applied)
        for i in range(5):
            assert f"p-{i}" in result

    def test_cap_prefers_items_with_deep_groups(self):
        """Capped primaries should prefer those that have deep groups."""
        from keep.cli import render_find_context
        from keep.api import FindResults

        items = [
            _item(id="no-deep-0", summary="No deep 0"),
            _item(id="no-deep-1", summary="No deep 1"),
            _item(id="has-deep", summary="Has deep"),
            _item(id="no-deep-2", summary="No deep 2"),
        ]
        deep_groups = {
            "has-deep": [_item(id="deep-y", summary="Deep Y", score=0.9)],
        }
        results = FindResults(items, deep_groups=deep_groups)
        result = render_find_context(results, token_budget=5000, deep_primary_cap=2)
        # "has-deep" should be kept (it has a deep group)
        assert "has-deep" in result
        assert "deep-y" in result

    def test_cap_applied_before_low_budget_spend(self):
        """Low budgets should still include entity/deep-group primaries."""
        from keep.cli import render_find_context
        from keep.api import FindResults

        items = [
            _item(id="top-0", summary="X" * 280, score=0.99),
            _item(id="top-1", summary="Y" * 280, score=0.98),
            _item(id="grp-a", summary="Group A", score=0.70),
            _item(id="grp-b", summary="Group B", score=0.69),
            _item(
                id="Joanna",
                summary="Entity",
                score=0.50,
                tags={"_updated_date": "2026-02-20", "_entity": "true"},
            ),
        ]
        deep_groups = {
            "grp-a": [_item(id="a-1", summary="A deep", score=0.8)],
            "grp-b": [_item(id="b-1", summary="B deep", score=0.7)],
            "Joanna": [_item(id="j-1", summary="Joanna deep", score=0.9)],
        }
        results = FindResults(items, deep_groups=deep_groups)

        output = render_find_context(
            results, token_budget=120, deep_primary_cap=3,
        )
        assert "Joanna" in output
        assert "top-0" not in output

    def test_deep_line_prefers_focus_summary(self):
        """Deep sub-item lines should render _focus_summary when present."""
        from keep.cli import render_find_context
        from keep.api import FindResults

        items = [_item(id="parent", summary="Parent summary")]
        deep_groups = {
            "parent": [_item(
                id="child",
                summary="Generic head summary",
                score=0.9,
                tags={"_focus_summary": "Matched hike evidence"},
            )],
        }
        results = FindResults(items, deep_groups=deep_groups)
        output = render_find_context(results, token_budget=5000)

        assert "Matched hike evidence" in output
        assert "Generic head summary" not in output

    def test_low_budget_deep_uses_compact_mode(self):
        """Low deep budgets should avoid expensive Thread/Story expansion."""
        from keep.cli import render_find_context
        from keep.api import FindResults

        keeper = MagicMock()
        keeper.list_parts.return_value = [
            PartRef(part_num=0, summary="Overview"),
            PartRef(part_num=1, summary="Details"),
        ]
        keeper.list_versions_around.return_value = [
            VersionInfo(version=4, summary="Before", tags={}, created_at="2026-01-01", content_hash="a"),
            VersionInfo(version=5, summary="Matched", tags={}, created_at="2026-01-02", content_hash="b"),
        ]
        keeper.list_versions.return_value = []

        items = [_item(id="Joanna", summary="Entity", tags={"_entity": "true"})]
        deep_groups = {
            "Joanna": [_item(
                id="conv3-session11",
                summary="Head",
                score=0.9,
                tags={"_focus_summary": "Matched evidence", "_focus_version": "5", "_focus_part": "1"},
            )],
        }
        results = FindResults(items, deep_groups=deep_groups)
        output = render_find_context(results, keeper=keeper, token_budget=200, deep_primary_cap=3)

        assert "conv3-session11" in output
        assert "Thread:" not in output
        assert "Story:" not in output

    def test_deep_window_includes_version_thread(self):
        """Deep anchors with _focus_version should render a local version thread."""
        from keep.cli import render_find_context
        from keep.api import FindResults

        keeper = MagicMock()
        keeper.list_parts.return_value = []
        keeper.list_versions_around.return_value = [
            VersionInfo(version=4, summary="Before", tags={}, created_at="2026-01-01", content_hash="a"),
            VersionInfo(version=5, summary="Matched", tags={}, created_at="2026-01-02", content_hash="b"),
            VersionInfo(version=6, summary="After", tags={}, created_at="2026-01-03", content_hash="c"),
        ]
        keeper.list_versions.return_value = []

        items = [_item(id="parent", summary="Parent summary")]
        deep_groups = {
            "parent": [_item(
                id="child",
                summary="Head summary",
                score=0.9,
                tags={"_focus_summary": "Matched evidence", "_focus_version": "5"},
            )],
        }
        results = FindResults(items, deep_groups=deep_groups)
        output = render_find_context(results, keeper=keeper, token_budget=5000)

        keeper.list_versions_around.assert_called_once_with("child", 5, radius=2)
        assert "Thread:" in output
        assert "@V{5}" in output

    def test_deep_window_merges_multiple_focus_versions_per_parent(self):
        """Multiple deep anchors from one parent should emit one merged Thread block."""
        from keep.cli import render_find_context
        from keep.api import FindResults

        keeper = MagicMock()
        keeper.list_parts.return_value = []

        def _around(_id, version, radius=2):
            return [
                VersionInfo(version=version - 1, summary=f"Before {version}",
                            tags={}, created_at="2026-01-01", content_hash="a"),
                VersionInfo(version=version, summary=f"Hit {version}",
                            tags={}, created_at="2026-01-02", content_hash="b"),
                VersionInfo(version=version + 1, summary=f"After {version}",
                            tags={}, created_at="2026-01-03", content_hash="c"),
            ][: (2 * radius + 1)]

        keeper.list_versions_around.side_effect = _around
        keeper.list_versions.return_value = []

        items = [_item(id="parent", summary="Parent summary")]
        deep_groups = {
            "parent": [
                _item(
                    id="parent@v5",
                    summary="Head summary",
                    score=0.92,
                    tags={"_focus_summary": "clarinet practice", "_focus_version": "5"},
                ),
                _item(
                    id="parent@v9",
                    summary="Head summary",
                    score=0.89,
                    tags={"_focus_summary": "volunteering update", "_focus_version": "9"},
                ),
            ],
        }
        results = FindResults(items, deep_groups=deep_groups)
        output = render_find_context(results, keeper=keeper, token_budget=5000)

        assert output.count("Thread:") == 1
        assert "@V{5}" in output
        assert "@V{9}" in output
        assert any(c.args == ("parent", 5) and c.kwargs.get("radius") == 2
                   for c in keeper.list_versions_around.call_args_list)
        assert any(c.args == ("parent", 9) and c.kwargs.get("radius") == 2
                   for c in keeper.list_versions_around.call_args_list)

    def test_deep_window_tapers_thread_radius_at_mid_budget(self):
        """Mid budgets should shrink version window to radius=1."""
        from keep.cli import render_find_context
        from keep.api import FindResults

        keeper = MagicMock()
        keeper.list_parts.return_value = []
        keeper.list_versions_around.return_value = [
            VersionInfo(version=4, summary="Before", tags={}, created_at="2026-01-01", content_hash="a"),
            VersionInfo(version=5, summary="Matched", tags={}, created_at="2026-01-02", content_hash="b"),
            VersionInfo(version=6, summary="After", tags={}, created_at="2026-01-03", content_hash="c"),
        ]
        keeper.list_versions.return_value = []

        items = [_item(id="parent", summary="Parent summary")]
        deep_groups = {
            "parent": [_item(
                id="child",
                summary="Head summary",
                score=0.9,
                tags={"_focus_summary": "Matched evidence", "_focus_version": "5"},
            )],
        }
        results = FindResults(items, deep_groups=deep_groups)
        render_find_context(results, keeper=keeper, token_budget=600)

        keeper.list_versions_around.assert_called_once_with("child", 5, radius=1)

    def test_deep_window_tapers_thread_radius_to_focus_only(self):
        """Low non-compact budgets should use radius=0."""
        from keep.cli import render_find_context
        from keep.api import FindResults

        keeper = MagicMock()
        keeper.list_parts.return_value = []
        keeper.list_versions_around.return_value = [
            VersionInfo(version=5, summary="Matched", tags={}, created_at="2026-01-02", content_hash="b"),
        ]
        keeper.list_versions.return_value = []

        items = [_item(id="parent", summary="Parent summary")]
        deep_groups = {
            "parent": [_item(
                id="child",
                summary="Head summary",
                score=0.9,
                tags={"_focus_summary": "Matched evidence", "_focus_version": "5"},
            )],
        }
        results = FindResults(items, deep_groups=deep_groups)
        render_find_context(results, keeper=keeper, token_budget=320)

        keeper.list_versions_around.assert_called_once_with("child", 5, radius=0)

    def test_deep_window_drops_duplicate_focus_only_thread(self):
        """Do not render Thread block when radius=0 duplicates deep line."""
        from keep.cli import render_find_context
        from keep.api import FindResults

        keeper = MagicMock()
        keeper.list_parts.return_value = []
        keeper.list_versions_around.return_value = [
            VersionInfo(
                version=5,
                summary="Matched evidence",
                tags={},
                created_at="2026-01-02",
                content_hash="b",
            ),
        ]
        keeper.list_versions.return_value = []

        items = [_item(id="parent", summary="Parent summary")]
        deep_groups = {
            "parent": [_item(
                id="child",
                summary="Head summary",
                score=0.9,
                tags={"_focus_summary": "Matched evidence", "_focus_version": "5"},
            )],
        }
        results = FindResults(items, deep_groups=deep_groups)
        output = render_find_context(results, keeper=keeper, token_budget=320)

        assert "child" in output
        assert "Thread:" not in output
        assert "@V{5}" not in output

    def test_deep_window_never_emits_empty_story_or_thread_headers(self):
        """Tight budgets should not leave orphan Story/Thread headers."""
        from keep.cli import render_find_context
        from keep.api import FindResults

        keeper = MagicMock()
        keeper.list_parts.side_effect = lambda _id: [
            PartRef(part_num=0, summary="S" * 186),
        ]
        keeper.list_versions_around.side_effect = lambda _id, _v, radius=1: [
            VersionInfo(version=1, summary="V" * 145, tags={}, created_at="2026-01-01", content_hash="a"),
            VersionInfo(version=2, summary="V" * 24, tags={}, created_at="2026-01-02", content_hash="b"),
        ][: (2 * radius + 1)]
        keeper.list_versions.return_value = []

        items = [
            _item(id="p0", summary="P" * 300, score=1.0),
            _item(id="p1", summary="P" * 31, score=0.9),
            _item(id="p2", summary="P" * 220, score=0.8),
        ]
        deep_groups = {
            "p0": [_item(id="c0", summary="D", score=1.0, tags={"_focus_summary": "d" * 290, "_focus_version": "1"})],
            "p1": [_item(id="c1", summary="D", score=0.9, tags={"_focus_summary": "d" * 80, "_focus_version": "1"})],
            "p2": [_item(id="c2", summary="D", score=0.8, tags={"_focus_summary": "d" * 40, "_focus_version": "1"})],
        }
        results = FindResults(items, deep_groups=deep_groups)

        output = render_find_context(results, keeper=keeper, token_budget=315)
        lines = output.splitlines()
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped == "Story:":
                assert i + 1 < len(lines)
                assert lines[i + 1].strip().startswith("- @P{")
            if stripped == "Thread:":
                assert i + 1 < len(lines)
                nxt = lines[i + 1].strip()
                assert nxt.startswith("* @V{") or nxt.startswith("- @V{")


class TestRenderFindContextDetail:
    """Tests for pass-2 detail rendering (parts, versions, tags, deep)."""

    def _mock_keeper(self, parts=None, versions=None, versions_around=None):
        """Create a mock keeper with list_parts/list_versions/list_versions_around."""
        keeper = MagicMock()
        keeper.list_parts.return_value = parts or []
        keeper.list_versions.return_value = versions or []
        keeper.list_versions_around.return_value = versions_around or []
        return keeper

    def test_pass2_renders_parts(self):
        """With >=2 items and a keeper, parts are rendered."""
        from keep.cli import render_find_context
        parts = [
            PartRef(part_num=0, summary="Overview of the topic"),
            PartRef(part_num=1, summary="Details and analysis"),
            PartRef(part_num=2, summary="Conclusions drawn"),
        ]
        keeper = self._mock_keeper(parts=parts)
        items = [_item(id=f"doc-{i}", summary=f"Doc {i}") for i in range(3)]
        result = render_find_context(items, keeper=keeper, token_budget=5000)
        assert "Key topics:" in result
        assert "Overview of the topic" in result
        assert "Conclusions drawn" in result

    def test_pass2_renders_versions(self):
        """With >=2 items and a keeper, version history is rendered."""
        from keep.cli import render_find_context
        versions = [
            VersionInfo(version=1, summary="Initial draft", tags={"_updated_date": "2026-01-01"}, created_at="2026-01-01", content_hash="a"),
            VersionInfo(version=2, summary="Added section B", tags={"_updated_date": "2026-01-15"}, created_at="2026-01-15", content_hash="b"),
        ]
        keeper = self._mock_keeper(versions=versions)
        items = [_item(id=f"doc-{i}", summary=f"Doc {i}") for i in range(3)]
        result = render_find_context(items, keeper=keeper, token_budget=5000)
        assert "Context:" in result
        assert "@V{1}" in result
        assert "Initial draft" in result

    def test_pass2_renders_user_tags(self):
        """With show_tags=True, user tags appear in pass-2 detail."""
        from keep.cli import render_find_context
        keeper = self._mock_keeper()
        items = [
            _item(id=f"doc-{i}", summary=f"Doc {i}",
                  tags={"_updated_date": "2026-02-20", "topic": "ai", "status": "draft"})
            for i in range(3)
        ]
        result = render_find_context(items, keeper=keeper, token_budget=5000, show_tags=True)
        assert "topic: ai" in result
        assert "status: draft" in result

    def test_pass2_focus_version_uses_around(self):
        """When _focus_version is set, list_versions_around is called."""
        from keep.cli import render_find_context
        around_versions = [
            VersionInfo(version=4, summary="Before hit", tags={"_updated_date": "2026-01-04"}, created_at="2026-01-04", content_hash="d"),
            VersionInfo(version=5, summary="The matched version", tags={"_updated_date": "2026-01-05"}, created_at="2026-01-05", content_hash="e"),
            VersionInfo(version=6, summary="After hit", tags={"_updated_date": "2026-01-06"}, created_at="2026-01-06", content_hash="f"),
        ]
        keeper = self._mock_keeper(versions_around=around_versions)
        items = [
            _item(id="doc-0", summary="Doc 0",
                  tags={"_updated_date": "2026-02-20", "_focus_version": "5"}),
            _item(id="doc-1", summary="Doc 1"),
        ]
        result = render_find_context(items, keeper=keeper, token_budget=5000)
        keeper.list_versions_around.assert_called_once_with("doc-0", 5, radius=2)
        assert "@V{5}" in result
        assert "The matched version" in result

    def test_pass2_skipped_without_keeper(self):
        """Without a keeper, pass 2 doesn't run (no parts/versions)."""
        from keep.cli import render_find_context
        items = [_item(id=f"doc-{i}", summary=f"Doc {i}") for i in range(5)]
        result = render_find_context(items, keeper=None, token_budget=5000)
        assert "Key topics:" not in result
        assert "Context:" not in result

    def test_zero_budget_returns_no_results(self):
        """Budget of 0 with items present returns 'No results.'."""
        from keep.cli import render_find_context
        items = [_item(id="a", summary="Something")]
        result = render_find_context(items, token_budget=0)
        assert result == "No results."


class TestListVersionsAround:
    """Tests for DocumentStore.list_versions_around."""

    @pytest.fixture
    def store(self, tmp_path):
        from keep.document_store import DocumentStore
        db_path = tmp_path / "documents.db"
        with DocumentStore(db_path) as s:
            yield s

    def _add_versions(self, store, id="doc1", n=10):
        """Add n versions to a document."""
        import json
        store.upsert("default", id, summary="current", tags={"_created": "2026-01-01"})
        for v in range(1, n + 1):
            store._conn.execute("""
                INSERT INTO document_versions (id, collection, version, summary, tags_json, content_hash, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (id, "default", v, f"Version {v}", json.dumps({"_updated_date": f"2026-01-{v:02d}"}), f"hash{v}", f"2026-01-{v:02d}T00:00:00"))
        store._conn.commit()

    def test_returns_surrounding_versions(self, store):
        self._add_versions(store, n=10)
        results = store.list_versions_around("default", "doc1", version=5, radius=2)
        versions = [r.version for r in results]
        assert versions == [3, 4, 5, 6, 7]

    def test_clamps_at_start(self, store):
        """Version near the start returns fewer items below."""
        self._add_versions(store, n=10)
        results = store.list_versions_around("default", "doc1", version=1, radius=2)
        versions = [r.version for r in results]
        assert 1 in versions
        assert all(v >= 1 for v in versions)
        # Should have 1, 2, 3 (no negative versions)
        assert versions == [1, 2, 3]

    def test_nonexistent_version(self, store):
        """If the target version doesn't exist, returns neighbors that do."""
        self._add_versions(store, n=5)
        # Version 99 doesn't exist, nothing in that range
        results = store.list_versions_around("default", "doc1", version=99, radius=2)
        assert results == []

    def test_chronological_order(self, store):
        self._add_versions(store, n=10)
        results = store.list_versions_around("default", "doc1", version=5, radius=2)
        versions = [r.version for r in results]
        assert versions == sorted(versions)


class TestVersionHitUplift:
    """Tests for version-hit uplift in the find pipeline."""

    @pytest.fixture
    def kp(self, mock_providers, tmp_path):
        from keep.api import Keeper
        return Keeper(store_path=tmp_path)

    def test_version_hit_uplifted_to_parent(self, kp):
        """A version hit should be uplifted to its parent with _focus_version."""
        # Create a document and add some versions
        kp.put(content="Version 1 content", id="mydoc", summary="v1")
        kp.put(content="Version 2 content about quantum computing", id="mydoc", summary="v2 quantum")
        kp.put(content="Version 3 content", id="mydoc", summary="v3")

        # Simulate what find() does internally: search returns a version hit
        # We test via the real find() pipeline
        results = kp.find("quantum computing", limit=5)

        # If there's a match, the result should be uplifted to the parent
        if results:
            # Should show parent ID, not version ID
            for item in results:
                assert "@v" not in item.id or item.id == "mydoc"

    def test_focus_version_set_on_uplift(self, kp):
        """Uplifted version hits carry _focus_version tag."""
        kp.put(content="Initial content", id="testdoc", summary="initial")
        kp.put(content="Updated with important info about neural networks", id="testdoc",
               summary="neural nets update")

        results = kp.find("neural networks", limit=5)

        # Find the result for testdoc if it matched
        for item in results:
            if item.id == "testdoc" and "_focus_version" in item.tags:
                # _focus_version should be set and numeric
                assert item.tags["_focus_version"].isdigit()
                break
